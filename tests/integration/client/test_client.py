"""Integration tests for Client - require a real RabbitMQ broker reachable
at RABBITMQ_URL (default amqp://localhost).

Mirrors conduit-node-client's own client.test.js and
client.wire-interop.test.js, so both clients are held to the same proven
behavioral contract for cross-service pub/sub, typed payload validation,
fan-out to multiple handlers/services, child-message metadata derivation,
and wire compatibility with an independent, non-client producer.

Two of conduit-node-client's client-level integration scenarios are
deliberately not ported here:

- "stopping one client does not prevent another client that shares the
  connection from publishing" - conduit-node-client's RabbitMqTransport has
  a connection-sharing registry (connectionName); this client's
  RabbitMqTransport intentionally doesn't (see the README), so each Client
  always owns its own connection and the scenario doesn't apply.
- client.lifecycle.test.js (forked consumer processes, simulating a hard
  crash mid-consumption) exercises RabbitMQ's own redelivery/requeue
  guarantees on connection loss, not Client-specific behaviour, and would
  need a comparable subprocess-based worker script to reproduce faithfully.
"""

import asyncio
import json
import time
import uuid
from dataclasses import dataclass
from typing import Any

import aio_pika
import pytest

from conduit_python_client import Client
from conduit_python_client.schema import SchemaValidator

RABBITMQ_URL = "amqp://localhost"


def create_test_id() -> str:
    return f"test_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"


@dataclass(frozen=True)
class ResourceNames:
    exchange: str
    dead_letter_exchange: str
    queue: str
    dead_letter_queue: str


def names_for(namespace: str, service: str) -> ResourceNames:
    return ResourceNames(
        exchange=f"conduit.{namespace}.events",
        dead_letter_exchange=f"conduit.{namespace}.events.dlx",
        queue=f"{namespace}.{service}",
        dead_letter_queue=f"{namespace}.{service}.dlq",
    )


async def cleanup_rabbitmq(names: ResourceNames) -> None:
    connection = await aio_pika.connect(RABBITMQ_URL)
    channel = await connection.channel()

    try:
        for queue in (names.queue, names.dead_letter_queue):
            try:
                await channel.queue_delete(queue)
            except Exception:  # noqa: BLE001, S110 - best-effort teardown
                pass

        for exchange in (names.exchange, names.dead_letter_exchange):
            try:
                await channel.exchange_delete(exchange)
            except Exception:  # noqa: BLE001, S110 - best-effort teardown
                pass
    finally:
        await channel.close()
        await connection.close()


@pytest.fixture
async def make_client():
    namespace = create_test_id()
    created: list[tuple[Client, ResourceNames]] = []

    async def _make(service: str, **client_kwargs: Any) -> tuple[Client, ResourceNames]:
        names = names_for(namespace, service)
        client = Client(
            namespace=namespace,
            service=service,
            rabbitmq={"url": RABBITMQ_URL},
            **client_kwargs,
        )
        created.append((client, names))
        return client, names

    yield _make

    for client, names in created:
        try:
            await client.stop()
        except Exception:  # noqa: BLE001, S110 - best-effort teardown
            pass

        await cleanup_rabbitmq(names)


class TestClientIntegration:
    async def test_two_clients_can_receive_each_others_messages(
        self, make_client
    ) -> None:
        service_a, _ = await make_client("service-a")
        service_b, _ = await make_client("service-b")

        await service_a.start()
        await service_b.start()

        received_by_a = asyncio.get_event_loop().create_future()
        received_by_b = asyncio.get_event_loop().create_future()

        async def handle_from_b(message, _ctx):
            received_by_a.set_result(message)

        async def handle_from_a(message, _ctx):
            received_by_b.set_result(message)

        sub1 = service_a.on("service-b.sent", handle_from_b)
        sub2 = service_b.on("service-a.sent", handle_from_a)
        await sub1.ready
        await sub2.ready

        await service_a.emit("service-a.sent", {"text": "Hello from A"})
        await service_b.emit("service-b.sent", {"text": "Hello from B"})

        message_from_b = await asyncio.wait_for(received_by_a, timeout=5)
        message_from_a = await asyncio.wait_for(received_by_b, timeout=5)

        assert message_from_b["data"] == {"text": "Hello from B"}
        assert message_from_b["meta"]["kind"] == "event"
        assert message_from_b["meta"]["type"] == "service-b.sent"
        assert message_from_b["meta"]["source"] == "service-b"

        assert message_from_a["data"] == {"text": "Hello from A"}
        assert message_from_a["meta"]["kind"] == "event"
        assert message_from_a["meta"]["type"] == "service-a.sent"
        assert message_from_a["meta"]["source"] == "service-a"

    async def test_validates_typed_event_data_before_publishing_and_receiving(
        self, make_client
    ) -> None:
        schemas = {
            "service-a.sent": {
                "type": "object",
                "required": ["text"],
                "additionalProperties": False,
                "properties": {"text": {"type": "string", "minLength": 1}},
            },
            "service-b.sent": {
                "type": "object",
                "required": ["text", "priority"],
                "additionalProperties": False,
                "properties": {
                    "text": {"type": "string", "minLength": 1},
                    "priority": {"type": "integer", "minimum": 1, "maximum": 5},
                },
            },
        }

        service_a, _ = await make_client("service-a", schemas=schemas)
        service_b, _ = await make_client("service-b", schemas=schemas)

        await service_a.start()
        await service_b.start()

        received_by_a = asyncio.get_event_loop().create_future()
        received_by_b = asyncio.get_event_loop().create_future()

        async def handle_from_b(message, ctx):
            received_by_a.set_result((message, ctx.transport.routing_key))

        async def handle_from_a(message, ctx):
            received_by_b.set_result((message, ctx.transport.routing_key))

        sub1 = service_a.on("service-b.sent", handle_from_b)
        sub2 = service_b.on("service-a.sent", handle_from_a)
        await sub1.ready
        await sub2.ready

        await service_a.emit("service-a.sent", {"text": "Hello from A"})
        await service_b.emit("service-b.sent", {"text": "Hello from B", "priority": 3})

        message_a, routing_key_a = await asyncio.wait_for(received_by_a, timeout=5)
        message_b, routing_key_b = await asyncio.wait_for(received_by_b, timeout=5)

        assert message_a["meta"]["type"] == "service-b.sent"
        assert message_a["data"] == {"text": "Hello from B", "priority": 3}
        assert routing_key_a == "service-b.sent"

        assert message_b["meta"]["type"] == "service-a.sent"
        assert message_b["data"] == {"text": "Hello from A"}
        assert routing_key_b == "service-a.sent"

        with pytest.raises(ValueError, match="Invalid"):
            await service_a.emit("service-a.sent", {"text": ""})

        with pytest.raises(ValueError, match="Invalid"):
            await service_b.emit("service-b.sent", {"text": "Hello from B"})

    async def test_delivers_the_same_event_to_multiple_handlers_and_services(
        self, make_client
    ) -> None:
        publisher, _ = await make_client("publisher-service")
        service_a, _ = await make_client("service-a")
        service_b, _ = await make_client("service-b")

        event_type = "shared.message-sent"

        await publisher.start()
        await service_a.start()
        await service_b.start()

        first_handler = asyncio.get_event_loop().create_future()
        second_handler = asyncio.get_event_loop().create_future()
        b_handler = asyncio.get_event_loop().create_future()

        async def handle_first(message, ctx):
            first_handler.set_result((message, ctx.transport.routing_key))

        async def handle_second(message, ctx):
            second_handler.set_result((message, ctx.transport.routing_key))

        async def handle_b(message, ctx):
            b_handler.set_result((message, ctx.transport.routing_key))

        sub1 = service_a.on(event_type, handle_first)
        sub2 = service_a.on(event_type, handle_second)
        sub3 = service_b.on(event_type, handle_b)
        await sub1.ready
        await sub2.ready
        await sub3.ready

        await publisher.emit(event_type, {"text": "Hello to everyone"})

        (
            (message_1, routing_key_1),
            (message_2, routing_key_2),
            (message_3, routing_key_3),
        ) = await asyncio.gather(
            asyncio.wait_for(first_handler, timeout=5),
            asyncio.wait_for(second_handler, timeout=5),
            asyncio.wait_for(b_handler, timeout=5),
        )

        for message in (message_1, message_2, message_3):
            assert message["data"] == {"text": "Hello to everyone"}
            assert message["meta"]["type"] == event_type
            assert message["meta"]["source"] == "publisher-service"

        assert routing_key_1 == event_type
        assert routing_key_2 == event_type
        assert routing_key_3 == event_type

        assert message_1["meta"]["id"] == message_2["meta"]["id"]
        assert message_1["meta"]["id"] == message_3["meta"]["id"]

    async def test_derives_tracing_metadata_when_a_handler_emits_a_child_event(
        self, make_client
    ) -> None:
        publisher, _ = await make_client("publisher-service")
        user_service, _ = await make_client("user-service")
        email_service, _ = await make_client("email-service")

        parent_event_type = "user.created"
        child_event_type = "welcome-email.requested"

        stream_id = "user_123"
        correlation_id = "corr_user_signup_123"

        await publisher.start()
        await user_service.start()
        await email_service.start()

        parent_handled = asyncio.get_event_loop().create_future()
        child_received = asyncio.get_event_loop().create_future()

        async def handle_child(message, ctx):
            child_received.set_result((message, ctx.transport.routing_key))

        async def handle_parent(message, ctx):
            parent_handled.set_result((message, ctx.transport.routing_key))

            await ctx.emit(
                child_event_type,
                {
                    "userId": message["data"]["userId"],
                    "email": message["data"]["email"],
                },
            )

        sub1 = email_service.on(child_event_type, handle_child)
        sub2 = user_service.on(parent_event_type, handle_parent)
        await sub1.ready
        await sub2.ready

        await publisher.emit(
            parent_event_type,
            {"userId": "user_123", "email": "janx@example.com"},
            stream_id=stream_id,
            correlation_id=correlation_id,
        )

        (
            (parent_message, parent_routing_key),
            (child_message, child_routing_key),
        ) = await asyncio.gather(
            asyncio.wait_for(parent_handled, timeout=5),
            asyncio.wait_for(child_received, timeout=5),
        )

        assert parent_message["meta"]["type"] == parent_event_type
        assert parent_message["meta"]["source"] == "publisher-service"
        assert parent_message["meta"]["streamId"] == stream_id
        assert parent_message["meta"]["correlationId"] == correlation_id
        assert "causationId" not in parent_message["meta"]

        assert child_message["meta"]["type"] == child_event_type
        assert child_message["meta"]["source"] == "user-service"
        assert child_message["data"] == {
            "userId": "user_123",
            "email": "janx@example.com",
        }

        assert child_message["meta"]["streamId"] == parent_message["meta"]["streamId"]
        assert (
            child_message["meta"]["correlationId"]
            == parent_message["meta"]["correlationId"]
        )
        assert child_message["meta"]["causationId"] == parent_message["meta"]["id"]

        assert isinstance(child_message["meta"]["id"], str)
        assert child_message["meta"]["id"] != parent_message["meta"]["id"]

        assert parent_routing_key == parent_event_type
        assert child_routing_key == child_event_type


class TestClientWireInterop:
    async def test_receives_a_message_published_directly_over_amqp(
        self, make_client
    ) -> None:
        """Bypasses the client's own publish path entirely - proves wire
        compatibility with an independent, spec-compliant producer that
        never touches EnvelopeFactory or Client serialization.
        """
        client, names = await make_client("receiving-service")

        # Starting the client (and subscribing below) is what creates the
        # exchange/queue/binding. From here on, only the *publish* is done
        # outside the client - the point of this test.
        await client.start()

        received = asyncio.get_event_loop().create_future()

        async def handler(message, ctx):
            received.set_result((message, ctx.transport.routing_key))

        subscription = client.on("user.created", handler)
        await subscription.ready

        # Hand-built message: no EnvelopeFactory, no client-side
        # serialization. This is what an independent, spec-compliant
        # producer's wire output would look like.
        raw_message = {
            "meta": {
                "id": "01JZM8FNDNGXARX13W5FQG9B3Z",
                "kind": "event",
                "type": "user.created",
                "version": "1.0.0",
                "streamId": "user_123",
                "correlationId": "01JZM8FNDNGXARX13W5FQG9B3Z",
                "timestamp": "2026-07-19T18:30:00.000Z",
                "source": "external-producer",
            },
            "data": {"userId": "user_123", "email": "user@example.com"},
        }

        # Prove the hand-built message is genuinely schema-conformant using
        # the same validator the client uses internally, rather than
        # eyeballing it.
        SchemaValidator().validate(raw_message)

        raw_connection = await aio_pika.connect(RABBITMQ_URL)
        raw_channel = await raw_connection.channel()

        try:
            exchange = await raw_channel.get_exchange(names.exchange)
            await exchange.publish(
                aio_pika.Message(
                    json.dumps(raw_message).encode("utf-8"),
                    content_type="application/json",
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                ),
                routing_key="user.created",
            )
        finally:
            await raw_channel.close()
            await raw_connection.close()

        message, routing_key = await asyncio.wait_for(received, timeout=5)

        assert message == raw_message
        assert routing_key == "user.created"
