"""Envelope models for the Conduit protocol."""

import json
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_serializer
from pydantic.alias_generators import to_camel

from ..schema import SchemaValidator

_protocol_validator = SchemaValidator()


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
            dt.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        )


class Message(BaseModel):
    model_config = ConfigDict(extra="forbid")
    meta: Meta
    data: dict[str, Any]

    def to_wire_json(self) -> str:
        return self.model_dump_json(by_alias=True, exclude_none=True)

    @classmethod
    def from_wire_json(cls, raw: str | bytes) -> Message:
        data = json.loads(raw)
        _protocol_validator.validate_envelope(data)
        return cls.model_validate(data)
