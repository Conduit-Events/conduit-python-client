import asyncio
from typing import Any

import pytest

from conduit_python_client.client import Client
from conduit_python_client.transports.rabbitmq import QueueOptions

from .fakes import (
    FakeEnvelopes,
    FakeTransport,
    FakeValidator,
    make_runtime_subscription,
)


async def noop_handler(_message: dict[str, Any], _context: Any) -> None:
    pass


async def tick(times: int = 5) -> None:
    for _ in range(times):
        await asyncio.sleep(0)


def make_client(
    *,
    service: str = "test-service",
    namespace: str = "studio",
    transport: FakeTransport | None = None,
    envelopes: FakeEnvelopes | None = None,
    validator: FakeValidator | None = None,
) -> tuple[Client, FakeTransport, FakeEnvelopes, FakeValidator]:
    transport = transport or FakeTransport()
    envelopes = envelopes or FakeEnvelopes()
    validator = validator or FakeValidator()

    client = Client(
        namespace=namespace,
        service=service,
        transport=transport,
        envelopes=envelopes,
        validator=validator,
    )

    return client, transport, envelopes, validator


def make_parent_message(**meta_overrides: Any) -> dict[str, Any]:
    meta = {
        "id": "parent-message-id",
        "kind": "event",
        "type": "user.created",
        "version": "1.0.0",
        "source": "user-service",
        "streamId": "user_123",
        "correlationId": "root-correlation-id",
        "timestamp": "2026-07-07T12:00:00.000Z",
        **meta_overrides,
    }
    return {"meta": meta, "data": {"userId": "user_123"}}


class TestConstruction:
    def test_requires_service_or_source_for_the_default_envelope_factory(self) -> None:
        with pytest.raises(ValueError, match="requires service or source"):
            Client(transport=FakeTransport(), validator=FakeValidator())


class TestStart:
    async def test_connects_the_transport_and_returns_the_client(self) -> None:
        client, transport, _, _ = make_client()

        result = await client.start()

        assert result is client
        assert transport.connect_calls == 1

    async def test_does_not_connect_twice_when_start_is_called_repeatedly(self) -> None:
        client, transport, _, _ = make_client()

        await client.start()
        await client.start()

        assert transport.connect_calls == 1

    async def test_coalesces_concurrent_start_calls(self) -> None:
        client, transport, _, _ = make_client()
        deferred = asyncio.get_running_loop().create_future()
        transport.connect_impl = lambda: deferred

        first = asyncio.ensure_future(client.start())
        second = asyncio.ensure_future(client.start())
        await tick()

        assert transport.connect_calls == 1

        deferred.set_result(None)

        assert await first is client
        assert await second is client
        assert transport.connect_calls == 1

    async def test_activates_subscriptions_registered_before_start(self) -> None:
        client, transport, _, _ = make_client()

        client.on("user.created", noop_handler)
        assert transport.subscribe_calls == []

        await client.start()

        assert len(transport.subscribe_calls) == 1
        assert transport.subscribe_calls[0]["type"] == "user.created"

    async def test_rejects_when_the_transport_cannot_connect(self) -> None:
        client, transport, _, _ = make_client()
        transport.connect_error = RuntimeError("Connection failed")

        with pytest.raises(RuntimeError, match="Connection failed"):
            await client.start()

    async def test_cleans_up_and_can_retry_after_subscription_activation_fails(
        self,
    ) -> None:
        client, transport, _, _ = make_client()

        client.on("user.created", noop_handler)
        client.on("user.updated", noop_handler)

        calls = 0

        def subscribe_impl(call: dict[str, Any]) -> Any:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("Subscription activation failed")
            return make_runtime_subscription()

        transport.subscribe_impl = subscribe_impl

        with pytest.raises(RuntimeError, match="Subscription activation failed"):
            await client.start()

        assert transport.disconnect_calls == 1

        await client.start()

        assert transport.connect_calls == 2
        assert [call["type"] for call in transport.subscribe_calls] == [
            "user.created",
            "user.updated",
            "user.created",
            "user.updated",
        ]

    async def test_raises_an_exception_group_when_cleanup_also_fails(self) -> None:
        client, transport, _, _ = make_client()
        transport.connect_error = RuntimeError("Connection failed")
        transport.disconnect_error = RuntimeError("Disconnect also failed")

        with pytest.raises(ExceptionGroup) as excinfo:
            await client.start()

        assert len(excinfo.value.exceptions) == 2


class TestStop:
    async def test_does_nothing_when_the_client_has_not_started(self) -> None:
        client, transport, _, _ = make_client()

        result = await client.stop()

        assert result is client
        assert transport.disconnect_calls == 0

    async def test_disconnects_the_transport_and_returns_the_client(self) -> None:
        client, transport, _, _ = make_client()
        await client.start()

        result = await client.stop()

        assert result is client
        assert transport.disconnect_calls == 1

    async def test_does_not_disconnect_twice_when_stop_is_called_repeatedly(
        self,
    ) -> None:
        client, transport, _, _ = make_client()
        await client.start()

        await client.stop()
        await client.stop()

        assert transport.disconnect_calls == 1

    async def test_preserves_logical_subscriptions_across_a_restart(self) -> None:
        client, transport, _, _ = make_client()
        client.on("user.created", noop_handler)

        await client.start()
        assert len(transport.subscribe_calls) == 1

        await client.stop()
        await client.start()

        assert len(transport.subscribe_calls) == 2


class TestLifecycleInterleaving:
    async def test_stop_waits_for_an_in_flight_start_before_disconnecting(
        self,
    ) -> None:
        client, transport, _, _ = make_client()
        connecting = asyncio.get_running_loop().create_future()
        transport.connect_impl = lambda: connecting

        starting = asyncio.ensure_future(client.start())
        await tick()
        assert transport.connect_calls == 1

        stopping = asyncio.ensure_future(client.stop())
        await tick()

        # start() hasn't resolved connect() yet, so stop() must not have
        # touched the transport - connect() and disconnect() must never run
        # concurrently.
        assert transport.disconnect_calls == 0

        connecting.set_result(None)

        assert await starting is client
        assert await stopping is client

        assert transport.connect_calls == 1
        assert transport.disconnect_calls == 1

        with pytest.raises(RuntimeError, match="must be started"):
            await client.emit("user.created", {})

    async def test_start_waits_for_an_in_flight_stop_before_reconnecting(
        self,
    ) -> None:
        client, transport, _, _ = make_client()
        await client.start()
        assert transport.connect_calls == 1

        disconnecting = asyncio.get_running_loop().create_future()
        transport.disconnect_impl = lambda: disconnecting

        stopping = asyncio.ensure_future(client.stop())
        await tick()
        assert transport.disconnect_calls == 1

        starting = asyncio.ensure_future(client.start())
        await tick()

        # stop() hasn't resolved disconnect() yet, so start() must not have
        # reconnected - connect() and disconnect() must never run
        # concurrently.
        assert transport.connect_calls == 1

        disconnecting.set_result(None)

        assert await stopping is client
        assert await starting is client

        assert transport.disconnect_calls == 1
        assert transport.connect_calls == 2


class TestPublishing:
    async def test_rejects_emit_before_the_client_is_started(self) -> None:
        client, _, _, _ = make_client()

        with pytest.raises(RuntimeError, match="must be started"):
            await client.emit("user.created", {})

    async def test_rejects_publishing_without_a_type(self) -> None:
        client, _, _, _ = make_client()
        await client.start()

        with pytest.raises(ValueError, match="requires a type"):
            await client.emit("", {})

    async def test_creates_validates_and_publishes_an_event(self) -> None:
        client, transport, envelopes, validator = make_client()
        await client.start()

        await client.emit("user.created", {"userId": "user_123"}, stream_id="user_123")

        assert envelopes.create_calls == [
            {
                "kind": "event",
                "type": "user.created",
                "data": {"userId": "user_123"},
                "stream_id": "user_123",
                "correlation_id": None,
                "causation_id": None,
                "extensions": None,
            }
        ]

        assert len(validator.validate_calls) == 1
        assert len(transport.publish_calls) == 1
        assert transport.publish_calls[0]["options"] == {"routing_key": "user.created"}

    async def test_uses_the_message_type_as_the_default_routing_key(self) -> None:
        client, transport, _, _ = make_client()
        await client.start()

        await client.emit("user.created", {})

        assert transport.publish_calls[0]["options"]["routing_key"] == "user.created"

    async def test_allows_overriding_the_routing_key(self) -> None:
        client, transport, _, _ = make_client()
        await client.start()

        await client.emit("user.created", {}, routing_key="another.event")

        assert transport.publish_calls[0]["options"]["routing_key"] == "another.event"

    async def test_creates_and_publishes_a_command(self) -> None:
        client, transport, envelopes, _ = make_client()
        await client.start()

        await client.command("user.create", {"email": "jan@example.com"})

        assert envelopes.create_calls[0]["kind"] == "command"
        assert transport.publish_calls[0]["options"]["routing_key"] == "user.create"

    async def test_does_not_publish_when_validation_fails(self) -> None:
        client, transport, _, validator = make_client()
        validator.validate_error = ValueError("Invalid event")
        await client.start()

        with pytest.raises(ValueError, match="Invalid event"):
            await client.emit("user.created", {})

        assert transport.publish_calls == []


class TestOn:
    def test_rejects_a_missing_subscription_type(self) -> None:
        client, _, _, _ = make_client()

        with pytest.raises(ValueError, match="requires a type"):
            client.on("", noop_handler)

    def test_rejects_a_non_callable_handler(self) -> None:
        client, _, _, _ = make_client()

        with pytest.raises(ValueError, match="requires a handler function"):
            client.on("user.created", None)  # type: ignore[arg-type]

    def test_requires_a_service_or_explicit_queue_name(self) -> None:
        client, _, _, _ = make_client(service=None)

        with pytest.raises(ValueError, match="requires config.service"):
            client.on("user.created", noop_handler)

    def test_allows_an_explicit_queue_when_service_is_absent(self) -> None:
        client, _, _, _ = make_client(service=None)

        client.on(
            "user.created",
            noop_handler,
            queue=QueueOptions(name="studio.custom-service"),
        )

    def test_registers_before_start_without_touching_the_transport(self) -> None:
        client, transport, _, _ = make_client()

        handle = client.on("user.created", noop_handler)

        assert handle.id == 1
        assert handle.type == "user.created"
        assert transport.subscribe_calls == []

    async def test_activates_when_registered_after_start(self) -> None:
        client, transport, _, _ = make_client()
        await client.start()

        handle = client.on("user.created", noop_handler)
        await handle.ready

        assert len(transport.subscribe_calls) == 1
        assert transport.subscribe_calls[0]["type"] == "user.created"

    async def test_passes_subscription_options_to_the_transport(self) -> None:
        client, transport, _, _ = make_client()
        client.on(
            "user.created",
            noop_handler,
            queue=QueueOptions(name="studio.user-projection"),
            requeue_on_error=True,
        )

        await client.start()

        options = transport.subscribe_calls[0]["options"]
        assert options["queue"] == QueueOptions(name="studio.user-projection")
        assert options["requeue_on_error"] is True

    async def test_supports_multiple_handlers_for_the_same_event_type(self) -> None:
        client, transport, _, _ = make_client()
        client.on("user.created", noop_handler)
        client.on("user.created", noop_handler)

        await client.start()

        assert len(transport.subscribe_calls) == 2

    async def test_validates_incoming_messages_before_calling_the_handler(self) -> None:
        client, transport, _, validator = make_client()
        handled = []

        async def handler(message: dict[str, Any], _context: Any) -> None:
            handled.append(message)

        client.on("user.created", handler)
        await client.start()

        message = make_parent_message()
        await transport.subscribe_calls[0]["handler"](message)

        assert handled == [message]
        assert len(validator.validate_calls) == 1

    async def test_does_not_call_the_handler_when_incoming_validation_fails(
        self,
    ) -> None:
        client, transport, _, validator = make_client()
        handler_calls = 0

        async def handler(_message: dict[str, Any], _context: Any) -> None:
            nonlocal handler_calls
            handler_calls += 1

        client.on("user.created", handler)
        await client.start()

        validator.validate_error = ValueError("Incoming message invalid")

        with pytest.raises(ValueError, match="Incoming message invalid"):
            await transport.subscribe_calls[0]["handler"](make_parent_message())

        assert handler_calls == 0

    async def test_passes_transport_context_and_the_client_to_the_handler(self) -> None:
        client, transport, _, _ = make_client()
        received_context = None

        async def handler(_message: dict[str, Any], context: Any) -> None:
            nonlocal received_context
            received_context = context

        client.on("user.created", handler)
        await client.start()

        await transport.subscribe_calls[0]["handler"](
            make_parent_message(), {"routing_key": "user.created"}
        )

        assert received_context.transport == {"routing_key": "user.created"}
        assert received_context.client is client
        assert callable(received_context.emit)
        assert callable(received_context.command)


class TestSubscribe:
    async def test_registers_and_waits_until_active(self) -> None:
        client, transport, _, _ = make_client()
        await client.start()

        handle = await client.subscribe("user.created", noop_handler)

        assert handle.type == "user.created"
        assert len(transport.subscribe_calls) == 1

    async def test_rejects_before_start(self) -> None:
        client, transport, _, _ = make_client()

        with pytest.raises(RuntimeError, match="must be started before subscribe"):
            await client.subscribe("user.created", noop_handler)

        assert transport.subscribe_calls == []


class TestUnsubscribe:
    async def test_removes_a_pre_start_subscription_without_touching_the_transport(
        self,
    ) -> None:
        client, transport, _, _ = make_client()

        handle = client.on("user.created", noop_handler)
        await handle.unsubscribe()
        await client.start()

        assert transport.subscribe_calls == []

    async def test_unsubscribes_the_active_transport_subscription(self) -> None:
        client, transport, _, _ = make_client()
        await client.start()

        handle = client.on("user.created", noop_handler)
        await handle.ready

        runtime_subscription = transport.runtime_subscriptions[0]
        await handle.unsubscribe()

        assert runtime_subscription["_state"]["unsubscribe_calls"] == 1

    async def test_does_not_unsubscribe_the_transport_twice(self) -> None:
        client, transport, _, _ = make_client()
        await client.start()

        handle = client.on("user.created", noop_handler)
        await handle.ready

        runtime_subscription = transport.runtime_subscriptions[0]
        await handle.unsubscribe()
        await handle.unsubscribe()

        assert runtime_subscription["_state"]["unsubscribe_calls"] == 1

    async def test_does_not_restore_an_unsubscribed_handler_after_restart(self) -> None:
        client, transport, _, _ = make_client()
        handle = client.on("user.created", noop_handler)

        await client.start()
        await handle.unsubscribe()

        await client.stop()
        await client.start()

        assert len(transport.subscribe_calls) == 1


class TestHandlerContext:
    async def test_emits_a_child_event_with_inherited_tracing_metadata(self) -> None:
        client, transport, envelopes, _ = make_client()

        client.on(
            "user.created",
            lambda _message, context: context.emit(
                "welcome-email.requested", {"userId": "user_123"}
            ),
        )

        await client.start()
        await transport.subscribe_calls[0]["handler"](make_parent_message())

        assert envelopes.create_calls[-1] == {
            "kind": "event",
            "type": "welcome-email.requested",
            "data": {"userId": "user_123"},
            "stream_id": "user_123",
            "correlation_id": "root-correlation-id",
            "causation_id": "parent-message-id",
            "extensions": None,
        }

    async def test_uses_the_parent_id_as_correlation_id_when_the_parent_has_none(
        self,
    ) -> None:
        client, transport, envelopes, _ = make_client()

        client.on(
            "user.created",
            lambda _message, context: context.emit("child.created", {}),
        )

        await client.start()

        parent = make_parent_message()
        del parent["meta"]["correlationId"]

        await transport.subscribe_calls[0]["handler"](parent)

        assert envelopes.create_calls[-1]["correlation_id"] == "parent-message-id"

    async def test_allows_child_stream_id_and_correlation_id_to_override(self) -> None:
        client, transport, envelopes, _ = make_client()

        client.on(
            "user.created",
            lambda _message, context: context.emit(
                "child.created",
                {},
                stream_id="custom-stream",
                correlation_id="custom-correlation",
            ),
        )

        await client.start()
        await transport.subscribe_calls[0]["handler"](make_parent_message())

        call = envelopes.create_calls[-1]
        assert call["stream_id"] == "custom-stream"
        assert call["correlation_id"] == "custom-correlation"
        assert call["causation_id"] == "parent-message-id"

    async def test_does_not_allow_child_causation_id_to_override_the_parent_id(
        self,
    ) -> None:
        client, transport, envelopes, _ = make_client()

        client.on(
            "user.created",
            lambda _message, context: context.emit(
                "child.created", {}, causation_id="custom-causation"
            ),
        )

        await client.start()
        await transport.subscribe_calls[0]["handler"](make_parent_message())

        assert envelopes.create_calls[-1]["causation_id"] == "parent-message-id"

    async def test_creates_child_commands_with_inherited_tracing_metadata(self) -> None:
        client, transport, envelopes, _ = make_client()

        client.on(
            "user.created",
            lambda _message, context: context.command(
                "welcome-email.send", {"userId": "user_123"}
            ),
        )

        await client.start()
        await transport.subscribe_calls[0]["handler"](make_parent_message())

        call = envelopes.create_calls[-1]
        assert call["kind"] == "command"
        assert call["causation_id"] == "parent-message-id"
