"""Integration tests for RabbitMqTransport - require a real RabbitMQ broker
reachable at RABBITMQ_URL (default amqp://localhost).

The first four scenarios mirror conduit-node-client's own
rabbitmq-transport.integration.test.js, so both clients are held to the same
proven behavioral contract. The rest cover Python-specific additions
(unsubscribe, malformed-body handling, conflicting redeclaration).
"""

import asyncio
import json
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import aio_pika
import pytest

from conduit_python_client.transports.rabbitmq import QueueOptions, RabbitMqTransport


async def noop_handler(_event, _ctx):
    pass


RABBITMQ_URL = "amqp://localhost"


def create_test_id() -> str:
    return f"test_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"


def create_message(message_type: str = "user.created") -> dict:
    event_id = f"evt_{uuid.uuid4().hex[:12]}"
    return {
        "meta": {
            "id": event_id,
            "kind": "event",
            "type": message_type,
            "version": "1.0.0",
            "streamId": "user_123",
            "correlationId": event_id,
            "timestamp": "2026-07-31T12:00:00.000Z",
            "source": "integration-test",
        },
        "data": {"userId": "user_123", "email": "jan@example.com"},
    }


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


async def eventually[T](
    predicate: Callable[[], Awaitable[T | None]],
    *,
    timeout: float = 5.0,
    interval: float = 0.1,
    message: str = "Condition was not met in time",
) -> T:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout

    while True:
        result = await predicate()
        if result:
            return result

        if loop.time() >= deadline:
            raise TimeoutError(message)

        await asyncio.sleep(interval)


@pytest.fixture
async def make_transport():
    created: list[tuple[RabbitMqTransport, ResourceNames]] = []

    async def _make(*, service: str = "email-service"):
        namespace = create_test_id()
        names = names_for(namespace, service)
        transport = RabbitMqTransport(
            namespace=namespace, service=service, url=RABBITMQ_URL
        )
        created.append((transport, names))
        return transport, names

    yield _make

    for transport, names in created:
        await transport.disconnect()
        await cleanup_rabbitmq(names)


class TestRabbitMqTransportIntegration:
    async def test_publishes_and_consumes_an_exact_event_type(self, make_transport):
        transport, names = await make_transport()
        message = create_message("user.created")

        received = asyncio.get_event_loop().create_future()

        async def handler(event, ctx):
            received.set_result((event, ctx))

        await transport.subscribe("user.created", handler)
        await transport.publish(message)

        event, ctx = await asyncio.wait_for(received, timeout=5)

        assert event == message
        assert ctx.routing_key == "user.created"
        assert ctx.exchange == names.exchange

    async def test_publishes_and_consumes_all_events_with_hash_subscription(
        self, make_transport
    ):
        transport, _names = await make_transport()
        message = create_message("payment.received")

        received = asyncio.get_event_loop().create_future()

        async def handler(event, ctx):
            received.set_result((event, ctx))

        await transport.subscribe("#", handler)
        await transport.publish(message)

        event, ctx = await asyncio.wait_for(received, timeout=5)

        assert event == message
        assert ctx.routing_key == "payment.received"

    async def test_dead_letters_a_message_when_the_handler_throws(self, make_transport):
        transport, names = await make_transport()
        message = create_message("user.created")

        async def handler(_event, _ctx):
            raise RuntimeError("Handler failed")

        await transport.subscribe("user.created", handler)
        await transport.publish(message)

        admin_connection = await aio_pika.connect(RABBITMQ_URL)
        admin_channel = await admin_connection.channel()

        try:
            dead_letter_queue = await admin_channel.get_queue(names.dead_letter_queue)

            async def try_get():
                return await dead_letter_queue.get(no_ack=True, fail=False)

            dead_lettered = await eventually(
                try_get, message="Message was not dead-lettered in time"
            )

            body = json.loads(dead_lettered.body.decode("utf-8"))

            assert body == message
            assert "x-death" in (dead_lettered.headers or {})
        finally:
            await admin_channel.close()
            await admin_connection.close()

    async def test_does_not_deliver_a_non_matching_event_to_an_exact_subscription(
        self, make_transport
    ):
        transport, _names = await make_transport()

        handled = False

        async def handler(_event, _ctx):
            nonlocal handled
            handled = True

        await transport.subscribe("user.created", handler)
        await transport.publish(create_message("random.gossip"))

        await asyncio.sleep(0.5)

        assert handled is False

    async def test_unsubscribe_stops_delivery(self, make_transport):
        transport, _names = await make_transport()

        handled = False

        async def handler(_event, _ctx):
            nonlocal handled
            handled = True

        subscription = await transport.subscribe("user.created", handler)
        await subscription["unsubscribe"]()

        await transport.publish(create_message("user.created"))
        await asyncio.sleep(0.5)

        assert handled is False

    async def test_unsubscribe_from_a_shared_queue_does_not_leak_the_pattern_binding(
        self, make_transport
    ):
        """Regression test: unsubscribing "user.created" from a queue that
        also serves "random.gossip" must unbind the now-unused "user.created"
        routing key, not just drop the local subscription bookkeeping. Before
        the fix, the queue binding for "user.created" stayed live, so
        publishing to it kept delivering messages to the queue with no
        matching local subscription - silently acked and dropped instead of
        never reaching the queue at all.
        """
        transport, names = await make_transport()

        handled: list[str] = []

        async def user_handler(_event, _ctx):
            handled.append("user.created")

        async def gossip_handler(_event, _ctx):
            handled.append("random.gossip")

        user_subscription = await transport.subscribe("user.created", user_handler)
        await transport.subscribe("random.gossip", gossip_handler)

        await user_subscription["unsubscribe"]()

        admin_connection = await aio_pika.connect(RABBITMQ_URL)
        admin_channel = await admin_connection.channel()

        try:
            queue = await admin_channel.get_queue(names.queue)
            messages_before = (await queue.declare()).message_count

            await transport.publish(create_message("user.created"))
            await asyncio.sleep(0.5)

            messages_after = (await queue.declare()).message_count
            assert messages_after == messages_before
        finally:
            await admin_channel.close()
            await admin_connection.close()

        await transport.publish(create_message("random.gossip"))
        await asyncio.sleep(0.5)

        assert handled == ["random.gossip"]

    async def test_malformed_body_is_rejected_without_disrupting_later_messages(
        self, make_transport
    ):
        transport, names = await make_transport()

        received = asyncio.get_event_loop().create_future()

        async def handler(event, _ctx):
            received.set_result(event)

        await transport.subscribe("user.created", handler)

        raw_connection = await aio_pika.connect(RABBITMQ_URL)
        raw_channel = await raw_connection.channel()

        try:
            await raw_channel.default_exchange.publish(
                aio_pika.Message(b"not valid json"),
                routing_key=names.queue,
            )
        finally:
            await raw_channel.close()
            await raw_connection.close()

        message = create_message("user.created")
        await transport.publish(message)

        event = await asyncio.wait_for(received, timeout=5)
        assert event == message

    async def test_conflicting_queue_redeclaration_is_rejected(self, make_transport):
        transport, names = await make_transport()

        await transport.subscribe("user.created", noop_handler)

        with pytest.raises(ValueError, match="already been declared"):
            await transport.subscribe(
                "user.deleted",
                noop_handler,
                queue=QueueOptions(name=names.queue, durable=False),
            )
