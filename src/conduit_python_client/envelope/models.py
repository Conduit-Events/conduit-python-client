"""Envelope models for the Conduit protocol."""

from datetime import datetime, timezone
from typing import Any
from uuid import UUID
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel
from pydantic import AwareDatetime, field_serializer


class Meta(BaseModel):
    model_config = ConfigDict(
        extra="forbid", alias_generator=to_camel, populate_by_name=True
    )

    id: str
    kind: str
    type: str
    version: str
    stream_id: str
    correlation_id: str
    causation_id: str | None = None
    timestamp: AwareDatetime
    source: str
    extensions: dict[str, Any] | None = None

    @field_serializer("timestamp")
    def _serialize_timestamp(self, dt: datetime) -> str:
        return (
            dt.astimezone(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z")
        )


class Message(BaseModel):
    model_config = ConfigDict(extra="forbid")
    meta: Meta
    data: dict[str, Any]
