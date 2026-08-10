import jsonschema
import pytest

from conduit_python_client.schema import SchemaValidator

GOOD_EVENT_MESSAGE = {
    "meta": {
        "id": "evt_01JYH7Z7K8N9Q2M3R4S5T6V7W8",
        "kind": "event",
        "type": "user.created",
        "version": "1.0.0",
        "streamId": "user_123",
        "correlationId": "corr_01JYH7Z7K8N9Q2M3R4S5T6V7W8",
        "timestamp": "2026-06-24T19:30:00.000Z",
        "source": "test-suite",
    },
    "data": {
        "userId": "user_123",
        "email": "janx@example.com",
        "name": "Janx",
    },
}

BAD_EVENT_MESSAGE = {"meta": dict(GOOD_EVENT_MESSAGE["meta"])}

GOOD_COMMAND_MESSAGE = {
    "meta": {
        "id": "cmd_01JYH80A1B2C3D4E5F6G7H8J9K",
        "kind": "command",
        "type": "user.create",
        "version": "1.0.0",
        "streamId": "user_123",
        "correlationId": "corr_01JYH80A1B2C3D4E5F6G7H8J9K",
        "timestamp": "2026-06-24T19:31:00.000Z",
        "source": "test-suite",
    },
    "data": {
        "email": "janx@example.com",
        "name": "Janx",
    },
}


class TestSchemaValidator:
    def test_validates_a_valid_message(self) -> None:
        validator = SchemaValidator()
        assert validator.validate(GOOD_EVENT_MESSAGE) is True

    def test_validates_an_event_using_the_default_event_alias(self) -> None:
        validator = SchemaValidator()
        assert validator.validate(GOOD_EVENT_MESSAGE, "event") is True

    def test_validates_a_command_using_the_default_command_alias(self) -> None:
        validator = SchemaValidator()
        assert validator.validate(GOOD_COMMAND_MESSAGE, "command") is True

    def test_raises_value_error_for_an_invalid_message(self) -> None:
        validator = SchemaValidator()

        with pytest.raises(ValueError, match="Invalid message"):
            validator.validate({})

    def test_raises_value_error_when_required_data_is_missing(self) -> None:
        validator = SchemaValidator()

        with pytest.raises(ValueError, match="Invalid event"):
            validator.validate(BAD_EVENT_MESSAGE)

    def test_allows_custom_schemas(self) -> None:
        validator = SchemaValidator(
            {
                "test": {
                    "type": "object",
                    "required": ["flag"],
                    "additionalProperties": True,
                    "properties": {
                        "flag": {"type": "boolean"},
                        "str": {"type": "string"},
                    },
                }
            }
        )

        assert (
            validator.validate(
                {"flag": False, "str": "string", "additional": "string"}, "test"
            )
            is True
        )

    def test_raises_value_error_when_custom_schema_validation_fails(self) -> None:
        validator = SchemaValidator(
            {
                "test": {
                    "type": "object",
                    "required": ["flag"],
                    "properties": {
                        "flag": {"type": "boolean"},
                        "str": {"type": "string"},
                    },
                }
            }
        )

        with pytest.raises(ValueError, match="Invalid test"):
            validator.validate({"flag": False, "str": 23}, "test")

    def test_does_not_leak_the_underlying_jsonschema_exception_type(self) -> None:
        validator = SchemaValidator()

        with pytest.raises(ValueError) as excinfo:
            validator.validate({})

        assert not isinstance(excinfo.value, jsonschema.exceptions.ValidationError)
        assert isinstance(
            excinfo.value.__cause__, jsonschema.exceptions.ValidationError
        )
