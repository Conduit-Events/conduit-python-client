"""Envelope factory for constructing new Conduit messages."""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import uuid4

from .models import Message, Meta

DEFAULT_VERSION = "1.0.0"


def _default_id_generator() -> str:
    return str(uuid4())


def _default_clock() -> datetime:
    return datetime.now(UTC)


class EnvelopeFactory:
    def __init__(
        self,
        source: str,
        *,
        default_version: str = DEFAULT_VERSION,
        id_generator: Callable[[], str] = _default_id_generator,
        clock: Callable[[], datetime] = _default_clock,
    ) -> None:
        self._source = source
        self._default_version = default_version
        self._id_generator = id_generator
        self._clock = clock

    def create_event(
        self,
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
        return self.create(
            "event",
            type,
            data,
            stream_id=stream_id,
            correlation_id=correlation_id,
            causation_id=causation_id,
            extensions=extensions,
            source=source,
            version=version,
        )

    def create_command(
        self,
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
        return self.create(
            "command",
            type,
            data,
            stream_id=stream_id,
            correlation_id=correlation_id,
            causation_id=causation_id,
            extensions=extensions,
            source=source,
            version=version,
        )

    def create(
        self,
        kind: Literal["event", "command"],
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
        id_ = self._id_generator()

        meta = Meta(
            id=id_,
            kind=kind,
            type=type,
            version=version or self._default_version,
            source=source or self._source,
            stream_id=stream_id or self._id_generator(),
            correlation_id=correlation_id or id_,
            causation_id=causation_id,
            extensions=extensions,
            timestamp=self._clock(),
        )

        return Message(meta=meta, data=data or {})
