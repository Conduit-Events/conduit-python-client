import pytest
import json
from pydantic import ValidationError
from jsonschema import validate
import conduit_protocol
from conduit_python_client.envelope import Meta, Message


class TestMeta:
    def test_extensions_default_to_none(self) -> None:
        meta = Meta(
            kind="event",
            type="user.created",
            id="test-message 1",
            version="1.0.0",
            streamId="stream1",
            correlationId="test-message 1",
            timestamp="2026-07-27 15:07:44.109374+00:00",
            source="test-service",
        )

        assert meta.extensions == None

    def test_accepts_arbitrary_extension_values(self) -> None:
        meta = Meta(
            kind="event",
            type="user.created",
            id="test-message 1",
            version="1.0.0",
            streamId="stream1",
            correlationId="test-message 1",
            timestamp="2026-07-27 15:07:44.109374+00:00",
            source="test-service",
            extensions={
                "trace": {
                    "trace_id": "abc",
                    "sampled": True,
                }
            },
        )

        assert meta.extensions["trace"]["trace_id"] == "abc"

    def test_rejects_incomplete_meta(self) -> None:
        with pytest.raises(ValidationError):
            meta = Meta(
                kind="event",
                type="user.created",
                id="test-message 1",
                version="1.0.0",
                # streamId="stream1",
                correlationId="test-message 1",
                timestamp="2026-07-27 15:07:44.109374+00:00",
                source="test-service",
            )

        with pytest.raises(ValidationError):
            meta = Meta(
                # kind="event",
                type="user.created",
                id="test-message 1",
                version="1.0.0",
                streamId="stream1",
                correlationId="test-message 1",
                timestamp="2026-07-27 15:07:44.109374+00:00",
                source="test-service",
            )

    def test_rejects_unknown_top_level_meta_fields(self) -> None:
        with pytest.raises(ValidationError):
            Meta(
                kind="event",
                type="user.created",
                id="test-message 1",
                version="1.0.0",
                streamId="stream1",
                correlationId="test-message 1",
                timestamp="2026-07-27 15:07:44.109374+00:00",
                source="test-service",
                trace={"trace_id": "abc"},
            )

    def test_dumping_to_json(self) -> None:
        meta = Meta(
            kind="event",
            type="user.created",
            id="test-message 1",
            version="1.0.0",
            streamId="stream1",
            correlationId="test-message 1",
            timestamp="2026-07-27 15:07:44.109374+00:00",
            source="test-service",
        )
        payload = json.loads(meta.model_dump_json(by_alias=True, exclude_none=True))

        assert payload["streamId"] == "stream1"
        assert "stream_id" not in payload
        assert "causationId" not in payload
        assert "extensions" not in payload
        assert payload["timestamp"] == "2026-07-27T15:07:44.109Z"


class TestMessage:
    @pytest.fixture
    def meta(self) -> Meta:
        return Meta(
            kind="event",
            type="user.created",
            id="test-message-1",
            version="1.0.0",
            streamId="stream1",
            correlationId="test-message-1",
            timestamp="2026-07-27 15:07:44.109374+00:00",
            source="test-service",
        )

    def test_constructs_message_with_valid_meta_and_data(self, meta: Meta) -> None:
        data = {"username": "test-user1"}
        message = Message(meta=meta, data=data)
        assert message.meta.extensions == None

    def test_dumped_message_is_schema_valid(self, meta: Meta) -> None:
        message = Message(meta=meta, data={"username": "test-user1"})
        payload = json.loads(message.model_dump_json(by_alias=True, exclude_none=True))

        schema = json.loads(conduit_protocol.schema_path().read_text())
        validate(instance=payload, schema=schema)  # raises on failure

    def test_rejects_unknown_top_level_message_fields(self, meta: Meta) -> None:

        with pytest.raises(ValidationError):
            message = Message(
                meta=meta, data={"username": "test-user1"}, extraField="oops"
            )
