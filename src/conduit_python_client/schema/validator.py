"""JSON Schema validation for Conduit messages, built on `jsonschema`.

Mirrors conduit-node-client's SchemaValidator (Ajv-based): the shared
conduit-protocol envelope schema is always registered under "message", with
"event"/"command" aliased to it unless a caller overrides them. Callers may
also register payload schemas per event/command `type`, validated against
`data` when present.
"""

import json
from typing import Any

import conduit_protocol
import jsonschema
from jsonschema.protocols import Validator

DEFAULT_SCHEMA_TYPE = "message"


class SchemaValidator:
    def __init__(self, schemas: dict[str, dict[str, Any] | str] | None = None) -> None:
        self._validators: dict[str, Validator] = {}

        protocol_schema = json.loads(conduit_protocol.schema_path().read_text())
        self.add_schema(DEFAULT_SCHEMA_TYPE, protocol_schema)

        aliases: list[tuple[str, str]] = []

        for type_, schema in (schemas or {}).items():
            if isinstance(schema, str):
                aliases.append((type_, schema))
            else:
                self.add_schema(type_, schema)

        for alias_type, target_type in aliases:
            self.alias(alias_type, target_type)

        if "event" not in self._validators:
            self.alias("event", DEFAULT_SCHEMA_TYPE)

        if "command" not in self._validators:
            self.alias("command", DEFAULT_SCHEMA_TYPE)

    def add_schema(self, type_: str, schema: dict[str, Any]) -> None:
        validator_cls = jsonschema.validators.validator_for(schema)
        validator_cls.check_schema(schema)
        self._validators[type_] = validator_cls(
            schema, format_checker=jsonschema.FormatChecker()
        )

    def alias(self, alias_type: str, target_type: str) -> None:
        target = self._validators.get(target_type)

        if target is None:
            raise ValueError(f"Unknown schema alias target: {target_type}")

        self._validators[alias_type] = target

    def validate_envelope(self, data: dict[str, Any]) -> bool:
        kind = ((data or {}).get("meta") or {}).get("kind", DEFAULT_SCHEMA_TYPE)
        return self._validate_as(data, kind)

    def validate_payload(self, message: dict[str, Any]) -> bool:
        type_ = ((message or {}).get("meta") or {}).get("type")

        if not type_ or type_ not in self._validators:
            return True  # no payload schema registered for this type

        return self._validate_as(message.get("data"), type_)

    def validate_message(self, message: dict[str, Any]) -> bool:
        self.validate_envelope(message)
        self.validate_payload(message)
        return True

    def is_valid(self, data: Any, type_: str = DEFAULT_SCHEMA_TYPE) -> bool:
        validator = self._validators.get(type_)

        if validator is None:
            return False

        return validator.is_valid(data)

    def validate(self, data: Any, type_: str = DEFAULT_SCHEMA_TYPE) -> bool:
        if type_ == DEFAULT_SCHEMA_TYPE:
            return self.validate_message(data)

        return self._validate_as(data, type_)

    def _validate_as(self, data: Any, type_: str) -> bool:
        validator = self._validators.get(type_)

        if validator is None:
            raise ValueError(f"Unknown schema type: {type_}")

        try:
            validator.validate(data)
        except jsonschema.exceptions.ValidationError as error:
            raise ValueError(f"Invalid {type_}: {error.message}") from error

        return True
