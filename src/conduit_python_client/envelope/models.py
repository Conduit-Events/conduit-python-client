"""Envelope models for the Conduit protocol."""

from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel
from pydantic import AwareDatetime, field_serializer


class Meta(BaseModel):
    model_config = ConfigDict(
        extra="forbid", alias_generator=to_camel, populate_by_name=True
    )

    id: str = Field(min_length=1)
    kind: Literal["event", "command"]
    type: str = Field(min_length=1)
    version: str = Field(min_length=1)
    stream_id: str = Field(min_length=1)
    correlation_id: str = Field(min_length=1)
    causation_id: str | None = Field(default=None, min_length=1)
    timestamp: AwareDatetime
    source: str = Field(min_length=1)
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

    def to_wire_json(self) -> str:
        return self.model_dump_json(by_alias=True, exclude_none=True)

    @classmethod
    def from_wire_json(cls, raw: str | bytes) -> Message:
        return cls.model_validate_json(raw)
