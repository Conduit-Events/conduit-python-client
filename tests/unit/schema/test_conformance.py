"""Runs conduit-protocol's shared validation-fixture family
(conformance/fixtures/validation/{valid,invalid}/*.json) against
SchemaValidator, proving it actually accepts what the schema allows and
rejects what it doesn't - not just that this client's own hand-written
tests agree with its own implementation.

See conduit-protocol/conformance/README.md for the fixture format and the
rules being checked. Unlike the creation fixtures (not yet wired up in this
client - see conduit-node-client's conformance.test.js for that family),
these don't exercise message construction at all: each fixture is already a
complete envelope, and the only question is accept or reject.
"""

import json
from pathlib import Path
from typing import Any

import conduit_protocol
import pytest

from conduit_python_client.schema import SchemaValidator

_VALIDATION_DIR = Path(str(conduit_protocol.conformance_fixtures_dir())) / "validation"


def _load_fixtures(subdir: str) -> list[tuple[str, dict[str, Any]]]:
    directory = _VALIDATION_DIR / subdir

    return sorted(
        (
            path.name,
            json.loads(path.read_text()),
        )
        for path in directory.glob("*.json")
    )


VALID_FIXTURES = _load_fixtures("valid")
INVALID_FIXTURES = _load_fixtures("invalid")


def _fixture_id(param: tuple[str, dict[str, Any]]) -> str:
    _file, fixture = param
    return str(fixture["name"])


class TestValidationFixtures:
    def test_fixture_sets_are_not_empty(self) -> None:
        assert VALID_FIXTURES
        assert INVALID_FIXTURES

    @pytest.mark.parametrize("file_and_fixture", VALID_FIXTURES, ids=_fixture_id)
    def test_valid_fixture_is_accepted(
        self, file_and_fixture: tuple[str, dict[str, Any]]
    ) -> None:
        file, fixture = file_and_fixture
        assert fixture["name"] == file.removesuffix(".json")

        assert SchemaValidator().validate(fixture["message"]) is True

    @pytest.mark.parametrize("file_and_fixture", INVALID_FIXTURES, ids=_fixture_id)
    def test_invalid_fixture_is_rejected(
        self, file_and_fixture: tuple[str, dict[str, Any]]
    ) -> None:
        file, fixture = file_and_fixture
        assert fixture["name"] == file.removesuffix(".json")

        with pytest.raises(ValueError):
            SchemaValidator().validate(fixture["message"])
