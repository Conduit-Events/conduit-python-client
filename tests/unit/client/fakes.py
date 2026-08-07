"""Fake Client dependencies for unit tests - no real transport/broker,
schema, or envelope logic involved. Mirrors conduit-node-client's
test/helpers/fake-client-dependencies.js.
"""

import inspect
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from conduit_python_client.envelope import Message, Meta


def make_runtime_subscription() -> dict[str, Any]:
    state = {"unsubscribe_calls": 0}

    async def unsubscribe() -> None:
        state["unsubscribe_calls"] += 1

    return {"unsubscribe": unsubscribe, "_state": state}


class FakeTransport:
    def __init__(self) -> None:
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.publish_calls: list[dict[str, Any]] = []
        self.subscribe_calls: list[dict[str, Any]] = []
        self.runtime_subscriptions: list[dict[str, Any]] = []

        self.connect_error: Exception | None = None
        self.disconnect_error: Exception | None = None
        self.publish_error: Exception | None = None
        self.subscribe_error: Exception | None = None

        self.connect_impl: Callable[[], Any] | None = None
        self.subscribe_impl: Callable[[dict[str, Any]], Any] | None = None

    async def connect(self) -> None:
        self.connect_calls += 1

        if self.connect_error is not None:
            raise self.connect_error

        if self.connect_impl is not None:
            result = self.connect_impl()
            if inspect.isawaitable(result):
                await result

    async def disconnect(self) -> None:
        self.disconnect_calls += 1

        if self.disconnect_error is not None:
            raise self.disconnect_error

    async def publish(self, message: dict[str, Any], **options: Any) -> Any:
        self.publish_calls.append({"message": message, "options": options})

        if self.publish_error is not None:
            raise self.publish_error

        return None

    async def subscribe(
        self, pattern: str, handler: Callable[..., Awaitable[Any]], **options: Any
    ) -> Any:
        call = {"type": pattern, "handler": handler, "options": options}
        self.subscribe_calls.append(call)

        if self.subscribe_error is not None:
            raise self.subscribe_error

        if self.subscribe_impl is not None:
            result = self.subscribe_impl(call)
            runtime_subscription = (
                await result if inspect.isawaitable(result) else result
            )
        else:
            runtime_subscription = make_runtime_subscription()

        self.runtime_subscriptions.append(runtime_subscription)
        return runtime_subscription


class FakeEnvelopes:
    def __init__(self) -> None:
        self.create_calls: list[dict[str, Any]] = []
        self._next_id = 0

    def create(
        self,
        kind: str,
        type: str,
        data: dict[str, Any] | None = None,
        *,
        stream_id: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        extensions: dict[str, Any] | None = None,
        source: str | None = None,
        version: str | None = None,
    ) -> Message:
        self._next_id += 1
        message_id = f"message-{self._next_id}"

        self.create_calls.append(
            {
                "kind": kind,
                "type": type,
                "data": data,
                "stream_id": stream_id,
                "correlation_id": correlation_id,
                "causation_id": causation_id,
                "extensions": extensions,
            }
        )

        meta = Meta(
            id=message_id,
            kind=kind,  # type: ignore[arg-type]
            type=type,
            version=version or "1.0.0",
            source=source or "test-service",
            streamId=stream_id or f"stream-{self._next_id}",
            correlationId=correlation_id or f"correlation-{self._next_id}",
            causationId=causation_id,
            extensions=extensions,
            timestamp=datetime.now(UTC),
        )

        return Message(meta=meta, data=data or {})


class FakeValidator:
    def __init__(self) -> None:
        self.validate_calls: list[dict[str, Any]] = []
        self.validate_error: Exception | None = None

    def validate(self, data: Any, type: str = "message") -> bool:
        self.validate_calls.append({"data": data, "type": type})

        if self.validate_error is not None:
            raise self.validate_error

        return True
