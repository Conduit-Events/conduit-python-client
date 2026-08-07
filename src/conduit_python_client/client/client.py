"""The Conduit client: ties the envelope factory, schema validator and a
transport together behind emit/command/on/subscribe.

Mirrors conduit-node-client's Client class: start/stop lifecycle with
concurrent-call coalescing, subscriptions registered via `on()` but only
activated against the transport once `start()` has run (or immediately, if
registered after start), and a handler context that lets a handler emit/send
child messages with tracing metadata (`stream_id`/`correlation_id`/
`causation_id`) derived from the parent message it's responding to.

One deliberate gap from Node: `stop()` takes no options to forward to the
transport. Node's `Transport.disconnect(options)` accepts (and structurally
ignores) an options bag that nothing consumes yet; Python's `Transport.
disconnect()` takes none. Extending it ahead of an actual need would be
speculative, the same call already made for connection pooling - see the
README.
"""

import asyncio
import itertools
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from ..envelope import EnvelopeFactory, Message
from ..schema import SchemaValidator
from ..transports.base import MessageHandler, Transport
from ..transports.rabbitmq import QueueOptions, RabbitMqTransport

DEFAULT_NAMESPACE = "default"


@dataclass
class HandlerContext:
    client: Client
    emit: Callable[..., Awaitable[Any]]
    command: Callable[..., Awaitable[Any]]
    transport: Any = None


@dataclass
class _Subscription:
    id: int
    type: str
    handler: MessageHandler
    options: dict[str, Any]
    active: bool = False
    cancelled: bool = False
    transport_subscription: Any = None
    ready: asyncio.Task[None] | None = None


async def _noop() -> None:
    return None


class SubscriptionHandle:
    def __init__(self, client: Client, subscription: _Subscription) -> None:
        self.id = subscription.id
        self.type = subscription.type
        self._client = client
        self._subscription = subscription

    @property
    def ready(self) -> Awaitable[None]:
        return (
            self._subscription.ready
            if self._subscription.ready is not None
            else _noop()
        )

    async def unsubscribe(self) -> None:
        await self._client._unsubscribe(self.id)


class Client:
    def __init__(
        self,
        *,
        namespace: str | None = None,
        service: str | None = None,
        source: str | None = None,
        transport: Transport | None = None,
        envelopes: EnvelopeFactory | None = None,
        validator: SchemaValidator | None = None,
        schemas: dict[str, Any] | None = None,
        rabbitmq: dict[str, Any] | None = None,
    ) -> None:
        self._namespace = namespace or DEFAULT_NAMESPACE
        self._service = service
        self._source = source or service

        if transport is not None:
            self.transport: Transport = transport
        else:
            self.transport = RabbitMqTransport(
                **(rabbitmq or {}),
                namespace=self._namespace,
                service=service,
            )

        if envelopes is not None:
            self.envelopes = envelopes
        else:
            if not self._source:
                raise ValueError(
                    "Client requires service or source to construct the "
                    "default EnvelopeFactory"
                )
            self.envelopes = EnvelopeFactory(self._source)

        self.validator = validator or SchemaValidator(schemas)

        self._subscriptions: dict[int, _Subscription] = {}
        self._subscription_ids = itertools.count(1)
        self._state = "stopped"
        self._starting: asyncio.Task[Client] | None = None
        self._stopping: asyncio.Task[Client] | None = None

    async def start(self) -> Client:
        if self._state == "started":
            return self

        if self._starting is not None:
            return await self._starting

        self._state = "starting"
        self._starting = asyncio.ensure_future(self._do_start())

        try:
            return await self._starting
        finally:
            self._starting = None

    async def _do_start(self) -> Client:
        try:
            await self.transport.connect()

            for subscription in self._subscriptions.values():
                subscription.ready = self._activate_subscription(subscription)
                await subscription.ready

            self._state = "started"
            return self
        except Exception as start_error:
            cleanup_error: Exception | None = None

            try:
                await self.transport.disconnect()
            except Exception as error:  # noqa: BLE001 - captured to report alongside start_error
                cleanup_error = error

            self._reset_subscription_runtime_state()

            if cleanup_error is not None:
                self._state = "failed"
                raise ExceptionGroup(
                    "Client startup failed and cleanup also failed",
                    [start_error, cleanup_error],
                ) from start_error

            self._state = "stopped"
            raise

    async def stop(self) -> Client:
        if self._state == "stopped":
            return self

        if self._stopping is not None:
            return await self._stopping

        self._state = "stopping"
        self._stopping = asyncio.ensure_future(self._do_stop())

        try:
            return await self._stopping
        finally:
            self._stopping = None

    async def _do_stop(self) -> Client:
        try:
            await self.transport.disconnect()
        finally:
            self._reset_subscription_runtime_state()
            self._state = "stopped"

        return self

    def _reset_subscription_runtime_state(self) -> None:
        for subscription in self._subscriptions.values():
            subscription.active = False
            subscription.transport_subscription = None
            subscription.ready = None

    async def emit(
        self, type: str, data: dict[str, Any] | None = None, **options: Any
    ) -> Any:
        return await self._publish("event", type, data, **options)

    async def command(
        self, type: str, data: dict[str, Any] | None = None, **options: Any
    ) -> Any:
        return await self._publish("command", type, data, **options)

    async def _publish(
        self,
        kind: str,
        type: str,
        data: dict[str, Any] | None,
        *,
        routing_key: str | None = None,
        stream_id: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        extensions: dict[str, Any] | None = None,
    ) -> Any:
        self._assert_started()

        if not type:
            raise ValueError(
                f"Client {kind if kind == 'command' else 'emit'} requires a type"
            )

        message = self.envelopes.create(
            kind,  # type: ignore[arg-type]
            type,
            data,
            stream_id=stream_id,
            correlation_id=correlation_id,
            causation_id=causation_id,
            extensions=extensions,
        )

        payload = self._to_wire(message)
        self.validator.validate(payload)

        return await self.transport.publish(payload, routing_key=routing_key or type)

    def on(
        self, type: str, handler: MessageHandler, **options: Any
    ) -> SubscriptionHandle:
        self._assert_can_subscribe(type, handler, options)

        subscription = _Subscription(
            id=next(self._subscription_ids),
            type=type,
            handler=handler,
            options=options,
        )

        self._subscriptions[subscription.id] = subscription

        if self._state == "started":
            subscription.ready = self._activate_subscription(subscription)

        return SubscriptionHandle(self, subscription)

    async def subscribe(
        self, type: str, handler: MessageHandler, **options: Any
    ) -> SubscriptionHandle:
        if self._state != "started":
            raise RuntimeError(
                "Client must be started before subscribe() can activate a "
                "subscription. Use on() to register handlers before start()."
            )

        handle = self.on(type, handler, **options)
        await handle.ready
        return handle

    def _activate_subscription(self, subscription: _Subscription) -> asyncio.Task[None]:
        async def activate() -> None:
            if subscription.cancelled or subscription.active:
                return

            transport_subscription = await self.transport.subscribe(
                subscription.type,
                self._wrap_handler(subscription),
                **subscription.options,
            )

            subscription.transport_subscription = transport_subscription
            subscription.active = True

        return asyncio.ensure_future(activate())

    def _wrap_handler(self, subscription: _Subscription) -> MessageHandler:
        async def wrapped(
            message: dict[str, Any], transport_context: Any = None
        ) -> Any:
            self.validator.validate(message)

            context = self._create_handler_context(message, transport_context)

            return await subscription.handler(message, context)

        return wrapped

    def _create_handler_context(
        self, message: dict[str, Any], transport_context: Any
    ) -> HandlerContext:
        async def emit(
            type: str, data: dict[str, Any] | None = None, **options: Any
        ) -> Any:
            return await self.emit(
                type, data, **self._derive_child_options(message, options)
            )

        async def command(
            type: str, data: dict[str, Any] | None = None, **options: Any
        ) -> Any:
            return await self.command(
                type, data, **self._derive_child_options(message, options)
            )

        return HandlerContext(
            client=self, emit=emit, command=command, transport=transport_context
        )

    def _derive_child_options(
        self, parent_message: dict[str, Any], options: dict[str, Any]
    ) -> dict[str, Any]:
        parent_meta = parent_message.get("meta") or {}

        return {
            "stream_id": parent_meta.get("streamId"),
            "correlation_id": parent_meta.get("correlationId") or parent_meta.get("id"),
            **options,
            "causation_id": parent_meta.get("id"),
        }

    def _assert_started(self) -> None:
        if self._state != "started":
            raise RuntimeError("Client must be started before it can broadcast events.")

    def _assert_can_subscribe(
        self, type: str, handler: MessageHandler, options: dict[str, Any]
    ) -> None:
        if not type:
            raise ValueError("Client subscribe requires a type")

        if not callable(handler):
            # ValueError (not TypeError) to keep a uniform exception type across
            # this method's validation failures.
            raise ValueError("Client subscribe requires a handler function")  # noqa: TRY004

        if not self._service and not self._has_explicit_queue_name(options):
            raise ValueError(
                "Client subscribe requires config.service or options.queue.name. "
                "This prevents accidental queue-name collisions."
            )

    @staticmethod
    def _has_explicit_queue_name(options: dict[str, Any]) -> bool:
        queue = options.get("queue")

        if isinstance(queue, QueueOptions):
            return bool(queue.name)

        if isinstance(queue, str):
            return bool(queue)

        return False

    @staticmethod
    def _to_wire(message: Message) -> dict[str, Any]:
        return message.model_dump(mode="json", by_alias=True, exclude_none=True)

    async def _unsubscribe(self, subscription_id: int) -> bool:
        subscription = self._subscriptions.pop(subscription_id, None)

        if subscription is None:
            return False

        subscription.cancelled = True

        if subscription.ready is not None:
            try:
                await subscription.ready
            except Exception:  # noqa: BLE001, S110 - the original activation error
                # already surfaced from start()/ready; unsubscribe just needs
                # to know activation has settled before cleaning up.
                pass

        await self._deactivate_subscription(subscription)
        return True

    async def _deactivate_subscription(self, subscription: _Subscription) -> None:
        transport_subscription = subscription.transport_subscription

        subscription.active = False
        subscription.transport_subscription = None
        subscription.ready = None

        if transport_subscription is not None:
            await transport_subscription["unsubscribe"]()
