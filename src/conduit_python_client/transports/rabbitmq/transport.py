"""RabbitMQ transport for the Conduit protocol, built on aio-pika.

Uses `aio_pika.connect` (not `connect_robust`): no silent auto-reconnect,
matching conduit-node-client's transport, which also dies on connection loss
rather than reconnecting transparently. Auto-reconnect can be layered on
deliberately later if needed.
"""

import asyncio
import itertools
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import aio_pika
from aio_pika.abc import (
    AbstractChannel,
    AbstractConnection,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
    ConsumerTag,
)

from ..base import MessageHandler, Transport
from .config import (
    QueueConfig,
    QueueOptions,
    RabbitMqTransportConfig,
    configure_rabbitmq_queue,
    configure_rabbitmq_transport,
)


@dataclass(frozen=True)
class RabbitMqSubscriptionContext:
    routing_key: str
    exchange: str
    redelivered: bool
    properties: Any


ErrorHandler = Callable[
    [BaseException, dict[str, Any], RabbitMqSubscriptionContext], Awaitable[None]
]


@dataclass(frozen=True)
class _Subscription:
    id: int
    queue: str
    pattern: str
    handler: MessageHandler
    on_error: ErrorHandler | None
    requeue_on_error: bool


class RabbitMqTransport(Transport):
    def __init__(self, **config: Any) -> None:
        self._config: RabbitMqTransportConfig = configure_rabbitmq_transport(**config)
        self._connection: AbstractConnection | None = None
        self._channel: AbstractChannel | None = None
        self._exchange: AbstractExchange | None = None
        self._connect_lock = asyncio.Lock()

        self._queues: dict[str, AbstractQueue] = {}
        self._declared_queues: dict[str, str] = {}
        self._subscriptions_by_queue: dict[str, list[_Subscription]] = {}
        self._consumers_by_queue: dict[str, ConsumerTag] = {}
        self._subscription_ids = itertools.count(1)

    async def connect(self) -> None:
        if self._connection is not None:
            return

        async with self._connect_lock:
            if self._connection is not None:
                return

            connection = await aio_pika.connect(self._config.url)
            channel = await connection.channel()

            await channel.set_qos(prefetch_count=self._config.prefetch)
            exchange = await channel.declare_exchange(
                self._config.exchange,
                self._config.exchange_type,
                durable=True,
            )

            self._connection = connection
            self._channel = channel
            self._exchange = exchange

    async def disconnect(self) -> None:
        connection = self._connection
        channel = self._channel

        self._connection = None
        self._channel = None
        self._exchange = None
        self._queues = {}
        self._declared_queues = {}
        self._subscriptions_by_queue = {}
        self._consumers_by_queue = {}

        if channel is not None:
            await channel.close()

        if connection is not None:
            await connection.close()

    async def publish(self, message: dict[str, Any], **options: Any) -> None:
        await self.connect()
        assert self._exchange is not None

        meta = message.get("meta", {})
        routing_key = options.get("routing_key") or meta.get("type")

        if not routing_key:
            raise ValueError(
                "RabbitMQ publish requires routing_key option or "
                "message['meta']['type']"
            )

        persistent = options.get("persistent", True)

        headers = {
            "kind": meta.get("kind"),
            "type": meta.get("type"),
            "version": meta.get("version"),
            "source": meta.get("source"),
            "namespace": self._config.namespace,
            **(options.get("headers") or {}),
        }

        amqp_message = aio_pika.Message(
            json.dumps(message).encode("utf-8"),
            content_type="application/json",
            delivery_mode=(
                aio_pika.DeliveryMode.PERSISTENT
                if persistent
                else aio_pika.DeliveryMode.NOT_PERSISTENT
            ),
            message_id=meta.get("id"),
            correlation_id=meta.get("correlationId"),
            timestamp=datetime.now(UTC),
            headers=headers,
        )

        await self._exchange.publish(amqp_message, routing_key=routing_key)

    async def subscribe(
        self, pattern: str, handler: MessageHandler, **options: Any
    ) -> Any:
        self._validate_pattern(pattern)

        queue_name = await self.ensure_queue(options.get("queue"))
        queue = self._queues[queue_name]

        assert self._exchange is not None
        await queue.bind(self._exchange, routing_key=pattern)

        subscription = _Subscription(
            id=next(self._subscription_ids),
            queue=queue_name,
            pattern=pattern,
            handler=handler,
            on_error=options.get("on_error"),
            requeue_on_error=options.get("requeue_on_error", False),
        )

        self._add_subscription(subscription)
        await self._ensure_consumer(queue_name)

        async def unsubscribe() -> None:
            await self._remove_subscription(subscription)

        return {"queue": queue_name, "pattern": pattern, "unsubscribe": unsubscribe}

    async def ensure_queue(self, queue: str | QueueOptions | None = None) -> str:
        await self.connect()
        queue_options = configure_rabbitmq_queue(self._config, queue)

        await self._assert_queue_infrastructure(queue_options)

        return queue_options.name

    def _add_subscription(self, subscription: _Subscription) -> None:
        subscriptions = self._subscriptions_by_queue.setdefault(subscription.queue, [])
        subscriptions.append(subscription)

    async def _ensure_consumer(self, queue_name: str) -> None:
        if queue_name in self._consumers_by_queue:
            return

        queue = self._queues[queue_name]

        async def callback(raw_message: AbstractIncomingMessage) -> None:
            await self._handle_raw_message(queue_name, raw_message)

        consumer_tag = await queue.consume(callback, no_ack=False)
        self._consumers_by_queue[queue_name] = consumer_tag

    async def _handle_raw_message(
        self, queue_name: str, raw_message: AbstractIncomingMessage
    ) -> None:
        try:
            message = json.loads(raw_message.body.decode("utf-8"))
        except json.JSONDecodeError, UnicodeDecodeError:
            await raw_message.nack(requeue=False)
            return

        routing_key = raw_message.routing_key or ""

        subscriptions = self._subscriptions_by_queue.get(queue_name, [])
        matching = [
            subscription
            for subscription in subscriptions
            if self._matches_pattern(subscription.pattern, routing_key)
        ]

        if not matching:
            await raw_message.ack()
            return

        ctx = RabbitMqSubscriptionContext(
            routing_key=routing_key,
            exchange=raw_message.exchange or "",
            redelivered=bool(raw_message.redelivered),
            properties=raw_message.properties,
        )

        for subscription in matching:
            try:
                await subscription.handler(message, ctx)
            except Exception as error:  # noqa: BLE001 - any handler failure must nack
                if subscription.on_error is not None:
                    await subscription.on_error(error, message, ctx)

                await raw_message.nack(requeue=subscription.requeue_on_error)
                return

        await raw_message.ack()

    async def _remove_subscription(self, subscription: _Subscription) -> None:
        subscriptions = self._subscriptions_by_queue.get(subscription.queue, [])
        remaining = [item for item in subscriptions if item.id != subscription.id]

        if remaining:
            self._subscriptions_by_queue[subscription.queue] = remaining
            return

        self._subscriptions_by_queue.pop(subscription.queue, None)

        consumer_tag = self._consumers_by_queue.pop(subscription.queue, None)

        if consumer_tag is not None:
            queue = self._queues[subscription.queue]
            await queue.cancel(consumer_tag)

    async def _assert_queue_infrastructure(self, queue_options: QueueConfig) -> None:
        assert self._channel is not None

        self._assert_queue_declaration_is_compatible(queue_options)

        if queue_options.dead_letter.enabled:
            dead_letter = queue_options.dead_letter
            assert dead_letter.exchange is not None
            assert dead_letter.queue is not None
            assert dead_letter.routing_key is not None

            await self._channel.declare_exchange(
                dead_letter.exchange,
                dead_letter.exchange_type or "direct",
                durable=True,
            )
            dead_letter_queue = await self._channel.declare_queue(
                dead_letter.queue, durable=True
            )
            await dead_letter_queue.bind(
                dead_letter.exchange, routing_key=dead_letter.routing_key
            )

        queue = await self._channel.declare_queue(
            queue_options.name,
            durable=queue_options.durable,
            exclusive=queue_options.exclusive,
            auto_delete=queue_options.auto_delete,
            arguments=queue_options.arguments,
        )

        self._queues[queue_options.name] = queue

    def _assert_queue_declaration_is_compatible(
        self, queue_options: QueueConfig
    ) -> None:
        signature = self._queue_declaration_signature(queue_options)
        existing_signature = self._declared_queues.get(queue_options.name)

        if existing_signature is not None and existing_signature != signature:
            raise ValueError(
                f'RabbitMQ queue "{queue_options.name}" has already been '
                "declared with different options"
            )

        self._declared_queues[queue_options.name] = signature

    def _queue_declaration_signature(self, queue_options: QueueConfig) -> str:
        return json.dumps(
            {
                "durable": queue_options.durable,
                "exclusive": queue_options.exclusive,
                "autoDelete": queue_options.auto_delete,
                "arguments": queue_options.arguments,
            },
            sort_keys=True,
        )

    def _validate_pattern(self, pattern: str) -> None:
        if not pattern:
            raise ValueError("RabbitMQ subscribe requires a pattern")

        if pattern == "#":
            return

        if "*" in pattern or "#" in pattern:
            raise ValueError(
                'RabbitMQ transport currently supports exact event types or "#" only'
            )

    def _matches_pattern(self, pattern: str, routing_key: str) -> bool:
        return pattern == "#" or pattern == routing_key
