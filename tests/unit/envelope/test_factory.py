import itertools
import json
from collections.abc import Callable
from datetime import UTC, datetime

import conduit_protocol
import pytest
from jsonschema import validate

from conduit_python_client.envelope import EnvelopeFactory


def make_id_generator() -> Callable[[], str]:
    counter = itertools.count(1)
    return lambda: f"id-{next(counter)}"


FIXED_TIMESTAMP = datetime(2026, 7, 29, 12, 0, 0, tzinfo=UTC)


class TestEnvelopeFactory:
    @pytest.fixture
    def factory(self) -> EnvelopeFactory:
        return EnvelopeFactory(
            source="test-service",
            id_generator=make_id_generator(),
            clock=lambda: FIXED_TIMESTAMP,
        )

    def test_create_event_sets_kind(self, factory: EnvelopeFactory) -> None:
        message = factory.create_event("user.created", {"userId": "user_123"})
        assert message.meta.kind == "event"

    def test_create_command_sets_kind(self, factory: EnvelopeFactory) -> None:
        message = factory.create_command("welcome-email.send", {"userId": "user_123"})
        assert message.meta.kind == "command"

    def test_generates_unique_ids_across_calls(self, factory: EnvelopeFactory) -> None:
        first = factory.create_event("user.created", {})
        second = factory.create_event("user.created", {})
        assert first.meta.id != second.meta.id

    def test_defaults_stream_id_to_a_freshly_generated_id(
        self, factory: EnvelopeFactory
    ) -> None:
        message = factory.create_event("user.created", {})
        assert message.meta.stream_id != message.meta.id

    def test_defaults_correlation_id_to_message_id(
        self, factory: EnvelopeFactory
    ) -> None:
        message = factory.create_event("user.created", {})
        assert message.meta.correlation_id == message.meta.id

    def test_omits_causation_id_for_root_messages(
        self, factory: EnvelopeFactory
    ) -> None:
        message = factory.create_event("user.created", {})
        assert message.meta.causation_id is None
        assert "causationId" not in json.loads(message.to_wire_json())["meta"]

    def test_sets_causation_id_when_provided(self, factory: EnvelopeFactory) -> None:
        message = factory.create_command(
            "welcome-email.send", {}, causation_id="parent-id"
        )
        assert message.meta.causation_id == "parent-id"

    def test_overrides_stream_id_and_correlation_id_when_provided(
        self, factory: EnvelopeFactory
    ) -> None:
        message = factory.create_command(
            "welcome-email.send",
            {},
            stream_id="parent-stream",
            correlation_id="parent-correlation",
            causation_id="parent-id",
        )
        assert message.meta.stream_id == "parent-stream"
        assert message.meta.correlation_id == "parent-correlation"

    def test_omits_extensions_when_not_provided(self, factory: EnvelopeFactory) -> None:
        message = factory.create_event("user.created", {})
        assert message.meta.extensions is None

    def test_includes_extensions_when_provided(self, factory: EnvelopeFactory) -> None:
        message = factory.create_event(
            "user.created", {}, extensions={"trace": {"trace_id": "abc"}}
        )
        assert message.meta.extensions == {"trace": {"trace_id": "abc"}}

    def test_defaults_source_and_version_from_factory_config(
        self, factory: EnvelopeFactory
    ) -> None:
        message = factory.create_event("user.created", {})
        assert message.meta.source == "test-service"
        assert message.meta.version == "1.0.0"

    def test_overrides_source_and_version_per_call(
        self, factory: EnvelopeFactory
    ) -> None:
        message = factory.create_event(
            "user.created", {}, source="other-service", version="2.0.0"
        )
        assert message.meta.source == "other-service"
        assert message.meta.version == "2.0.0"

    def test_defaults_data_to_empty_dict(self, factory: EnvelopeFactory) -> None:
        message = factory.create_event("user.created")
        assert message.data == {}

    def test_uses_injected_clock(self, factory: EnvelopeFactory) -> None:
        message = factory.create_event("user.created", {})
        assert message.meta.timestamp == FIXED_TIMESTAMP

    def test_produced_message_is_schema_valid(self, factory: EnvelopeFactory) -> None:
        message = factory.create_event("user.created", {"userId": "user_123"})

        schema = json.loads(conduit_protocol.schema_path().read_text())
        validate(instance=json.loads(message.to_wire_json()), schema=schema)
