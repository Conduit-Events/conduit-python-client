"""Runs conduit-protocol's shared creation-fixture family
(conformance/fixtures/creation/*.json) against this client, proving it
actually implements the streamId/correlationId/causationId derivation
rules the protocol documents - not just that this client's own hand-written
tests agree with its own implementation.

See conduit-protocol/conformance/README.md for the fixture format and the
rules being checked. Mirrors conduit-node-client's own
test/unit/conformance/conformance.test.js, so both clients are held to the
same proven behavioral contract.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import conduit_protocol
import pytest

from conduit_python_client.client import Client
from conduit_python_client.envelope import EnvelopeFactory, Message
from conduit_python_client.schema import SchemaValidator

from ..client.fakes import FakeTransport

_CREATION_DIR = Path(str(conduit_protocol.conformance_fixtures_dir())) / "creation"

FIXTURES = sorted(
    (path.name, json.loads(path.read_text())) for path in _CREATION_DIR.glob("*.json")
)


def _fixture_id(param: tuple[str, dict[str, Any]]) -> str:
    _file, fixture = param
    return str(fixture["name"])


def _to_wire_dict(message: Message) -> dict[str, Any]:
    return message.model_dump(mode="json", by_alias=True, exclude_none=True)


def _forced_envelope_factory(fixture: dict[str, Any]) -> EnvelopeFactory:
    given = fixture["given"]

    return EnvelopeFactory(
        fixture["client"]["source"],
        default_version=fixture["client"]["defaultVersion"],
        id_generator=lambda: given["id"],
        clock=lambda: datetime.fromisoformat(given["timestamp"]),
    )


def _run_root_message_fixture(fixture: dict[str, Any]) -> dict[str, Any]:
    factory = _forced_envelope_factory(fixture)
    input_ = fixture["input"]

    kwargs: dict[str, Any] = {}
    if "streamId" in input_:
        kwargs["stream_id"] = input_["streamId"]
    if "correlationId" in input_:
        kwargs["correlation_id"] = input_["correlationId"]
    if "causationId" in input_:
        kwargs["causation_id"] = input_["causationId"]

    message = factory.create(
        input_["kind"], input_["type"], input_.get("data"), **kwargs
    )
    return _to_wire_dict(message)


async def _run_child_message_fixture(fixture: dict[str, Any]) -> dict[str, Any]:
    transport = FakeTransport()

    client = Client(
        namespace="conformance",
        service=fixture["client"]["source"],
        source=fixture["client"]["source"],
        transport=transport,
        envelopes=_forced_envelope_factory(fixture),
        validator=SchemaValidator(),
    )

    await client.start()

    child_input = fixture["childInput"]

    # Only include keys the fixture's childInput actually supplies - Client
    # only overrides the parent-derived defaults for keys the caller passed
    # at all, so an absent key must stay absent here too.
    child_options: dict[str, Any] = {}
    if "streamId" in child_input:
        child_options["stream_id"] = child_input["streamId"]
    if "correlationId" in child_input:
        child_options["correlation_id"] = child_input["correlationId"]
    if "attemptedCausationId" in fixture:
        child_options["causation_id"] = fixture["attemptedCausationId"]

    async def handler(_message: dict[str, Any], ctx: Any) -> None:
        if child_input["kind"] == "event":
            await ctx.emit(
                child_input["type"], child_input.get("data"), **child_options
            )
        else:
            await ctx.command(
                child_input["type"], child_input.get("data"), **child_options
            )

    subscription = client.on(fixture["parent"]["meta"]["type"], handler)
    await subscription.ready

    # Simulate the parent message being delivered by the transport, driving
    # the handler above through the exact same path a real ctx.emit()/
    # ctx.command() call goes through in production.
    delivery_handler = transport.subscribe_calls[-1]["handler"]
    await delivery_handler(fixture["parent"], {})

    await client.stop()

    result: dict[str, Any] = transport.publish_calls[-1]["message"]
    return result


def _assert_matches_expected_envelope(
    actual: dict[str, Any], expected: dict[str, Any]
) -> None:
    assert set(actual["meta"].keys()) == set(expected["meta"].keys())

    for key, expected_value in expected["meta"].items():
        if expected_value == "<generated>":
            assert isinstance(actual["meta"][key], str)
            assert actual["meta"][key] != ""
            continue

        assert actual["meta"][key] == expected_value, key

    assert actual["data"] == expected["data"]


class TestCreationFixtures:
    def test_fixture_set_is_not_empty(self) -> None:
        assert FIXTURES

    @pytest.mark.parametrize("file_and_fixture", FIXTURES, ids=_fixture_id)
    async def test_fixture(self, file_and_fixture: tuple[str, dict[str, Any]]) -> None:
        file, fixture = file_and_fixture
        assert fixture["name"] == file.removesuffix(".json")

        if fixture["scenario"] == "root-message":
            actual = _run_root_message_fixture(fixture)
        elif fixture["scenario"] == "child-message":
            actual = await _run_child_message_fixture(fixture)
        else:
            raise ValueError(f"Unknown fixture scenario: {fixture['scenario']}")

        _assert_matches_expected_envelope(actual, fixture["expected"])
