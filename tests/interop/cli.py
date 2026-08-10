"""Test-only interoperability CLI: exposes this client's real validation
and message-model path over stdin/stdout so an external cross-language
test runner can treat it as a black box. Not exported as public package
API.

Usage:
    echo '<wire envelope JSON>' | python tests/interop/cli.py validate
    echo '<wire envelope JSON>' | python tests/interop/cli.py roundtrip

Contract:
    - Reads exactly one JSON document from stdin.
    - Writes exactly one JSON document (plus trailing newline) to stdout
      on success - nothing else ever goes to stdout.
    - Diagnostics go to stderr.
    - Exit 0 = operation completed (this includes {"valid": false} - the
      operation successfully answered the question).
    - Exit 1 = runtime/validation failure (malformed input JSON, or an
      invalid envelope passed to roundtrip).
    - Exit 2 = CLI usage error (missing/unknown operation).
"""

from __future__ import annotations

import json
import sys
from typing import Any

from conduit_python_client.envelope import Message
from conduit_python_client.schema import SchemaValidator

KNOWN_OPERATIONS = {"validate", "roundtrip"}

validator = SchemaValidator()


def validate_operation(data: dict[str, Any]) -> dict[str, Any]:
    try:
        validator.validate(data)
        return {"valid": True}
    except ValueError:
        return {"valid": False}


def roundtrip_operation(data: dict[str, Any]) -> dict[str, Any]:
    # Unlike Node, Python's internal message representation genuinely
    # differs from the wire shape (snake_case fields, a real pydantic
    # model) - Message.from_wire_json() is the real production ingress
    # path: schema validation, then model construction. Re-serializing it
    # is the real egress path.
    message = Message.from_wire_json(json.dumps(data))
    result: dict[str, Any] = message.model_dump(
        mode="json", by_alias=True, exclude_none=True
    )
    return result


def write_json(value: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(value) + "\n")


def main() -> int:
    argv = sys.argv[1:]
    operation = argv[0] if argv else None

    if operation is None:
        sys.stderr.write("Usage: cli.py <validate|roundtrip>\n")
        return 2

    if operation not in KNOWN_OPERATIONS:
        sys.stderr.write(f"Unknown operation: {operation}\n")
        return 2

    try:
        data = json.loads(sys.stdin.read())
    except json.JSONDecodeError as error:
        sys.stderr.write(f"Malformed JSON input: {error}\n")
        return 1

    try:
        result = (
            validate_operation(data)
            if operation == "validate"
            else roundtrip_operation(data)
        )
    except Exception as error:  # noqa: BLE001 - any failure here means "invalid envelope", reported uniformly to stderr
        sys.stderr.write(f"Invalid envelope: {error}\n")
        return 1

    write_json(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
