import pytest
import json
from pydantic import ValidationError
from jsonschema import validate
import conduit_protocol
from conduit_python_client.envelope import Meta, Message


def valid_meta_kwargs(**overrides: object) -> dict[str, object]:
    return {
        "kind": "event",
        "type": "user.created",
        "id": "test-message-1",
        "version": "1.0.0",
        "streamId": "stream1",
        "correlationId": "test-message-1",
        "timestamp": "2026-07-27T15:07:44.109374+00:00",
        "source": "test-service",
        **overrides,
    }


class TestMeta:
    def test_extensions_default_to_none(self) -> None:
        meta = Meta(**valid_meta_kwargs())

        assert meta.extensions is None

    def test_accepts_arbitrary_extension_values(self) -> None:
        meta = Meta(
            **valid_meta_kwargs(
                extensions={
                    "trace": {
                        "trace_id": "abc",
                        "sampled": True,
                    }
                }
            )
        )

        assert meta.extensions["trace"]["trace_id"] == "abc"

    def test_rejects_incomplete_meta(self) -> None:
        missing_stream_id = valid_meta_kwargs()
        del missing_stream_id["streamId"]
        with pytest.raises(ValidationError):
            Meta(**missing_stream_id)

        missing_kind = valid_meta_kwargs()
        del missing_kind["kind"]
        with pytest.raises(ValidationError):
            Meta(**missing_kind)

    def test_rejects_unknown_top_level_meta_fields(self) -> None:
        with pytest.raises(ValidationError):
            Meta(**valid_meta_kwargs(trace={"trace_id": "abc"}))

    def test_dumping_to_json(self) -> None:
        meta = Meta(**valid_meta_kwargs())
        payload = json.loads(meta.model_dump_json(by_alias=True, exclude_none=True))

        assert payload["streamId"] == "stream1"
        assert "stream_id" not in payload
        assert "causationId" not in payload
        assert "extensions" not in payload
        assert payload["timestamp"] == "2026-07-27T15:07:44.109Z"


class TestMetaFieldConstraints:
    @pytest.mark.parametrize(
        "field", ["id", "type", "version", "streamId", "correlationId", "source"]
    )
    def test_rejects_empty_required_string_fields(self, field: str) -> None:
        with pytest.raises(ValidationError):
            Meta(**valid_meta_kwargs(**{field: ""}))

    def test_rejects_empty_causation_id(self) -> None:
        with pytest.raises(ValidationError):
            Meta(**valid_meta_kwargs(causationId=""))

    def test_accepts_omitted_causation_id(self) -> None:
        meta = Meta(**valid_meta_kwargs())
        assert meta.causation_id is None

    def test_rejects_invalid_kind(self) -> None:
        with pytest.raises(ValidationError):
            Meta(**valid_meta_kwargs(kind="not-a-real-kind"))

    @pytest.mark.parametrize("kind", ["event", "command"])
    def test_accepts_valid_kind_values(self, kind: str) -> None:
        meta = Meta(**valid_meta_kwargs(kind=kind))
        assert meta.kind == kind


class TestMessage:
    @pytest.fixture
    def meta(self) -> Meta:
        return Meta(**valid_meta_kwargs())

    def test_constructs_message_with_valid_meta_and_data(self, meta: Meta) -> None:
        data = {"username": "test-user1"}
        message = Message(meta=meta, data=data)
        assert message.meta.extensions is None

    def test_dumped_message_is_schema_valid(self, meta: Meta) -> None:
        message = Message(meta=meta, data={"username": "test-user1"})
        payload = json.loads(message.model_dump_json(by_alias=True, exclude_none=True))

        schema = json.loads(conduit_protocol.schema_path().read_text())
        validate(instance=payload, schema=schema)  # raises on failure

    def test_rejects_unknown_top_level_message_fields(self, meta: Meta) -> None:
        with pytest.raises(ValidationError):
            Message(meta=meta, data={"username": "test-user1"}, extraField="oops")


class TestMessageWireRoundTrip:
    @pytest.fixture
    def meta(self) -> Meta:
        # Millisecond-precision timestamp: to_wire_json truncates to milliseconds,
        # so a microsecond-precision input wouldn't round-trip to an equal object.
        return Meta(**valid_meta_kwargs(timestamp="2026-07-27T15:07:44.109+00:00"))

    def test_round_trip_preserves_message(self, meta: Meta) -> None:
        message = Message(meta=meta, data={"username": "test-user1"})

        round_tripped = Message.from_wire_json(message.to_wire_json())

        assert round_tripped == message

    def test_round_trip_preserves_extensions(self) -> None:
        meta = Meta(
            **valid_meta_kwargs(
                timestamp="2026-07-27T15:07:44.109+00:00",
                extensions={"trace": {"trace_id": "abc"}},
            )
        )
        message = Message(meta=meta, data={"username": "test-user1"})

        round_tripped = Message.from_wire_json(message.to_wire_json())

        assert round_tripped.meta.extensions == {"trace": {"trace_id": "abc"}}

    def test_round_trip_omits_causation_id_and_extensions_when_absent(
        self, meta: Meta
    ) -> None:
        message = Message(meta=meta, data={"username": "test-user1"})

        wire = message.to_wire_json()
        assert "causationId" not in wire
        assert "extensions" not in wire

        round_tripped = Message.from_wire_json(wire)
        assert round_tripped.meta.causation_id is None
        assert round_tripped.meta.extensions is None

    def test_from_wire_json_accepts_bytes(self, meta: Meta) -> None:
        message = Message(meta=meta, data={"username": "test-user1"})

        round_tripped = Message.from_wire_json(message.to_wire_json().encode("utf-8"))

        assert round_tripped == message

    def test_from_wire_json_rejects_invalid_payload(self) -> None:
        with pytest.raises(ValidationError):
            Message.from_wire_json('{"meta": {"kind": "event"}, "data": {}}')
