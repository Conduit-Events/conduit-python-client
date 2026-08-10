"""Spawns tests/interop/cli.py as a real subprocess and asserts on its
stdin/stdout/stderr/exit-code contract - the CLI is meant to be treated as
a black box by an external cross-language test runner, so these tests
exercise it the same way.
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import conduit_protocol

CLI_PATH = Path(__file__).resolve().parents[2] / "interop" / "cli.py"

_VALIDATION_DIR = Path(str(conduit_protocol.conformance_fixtures_dir())) / "validation"


def _load_fixture_message(subdir: str, file: str) -> dict[str, Any]:
    fixture = json.loads((_VALIDATION_DIR / subdir / file).read_text())
    result: dict[str, Any] = fixture["message"]
    return result


VALID_MESSAGE = _load_fixture_message("valid", "minimal-event.json")
INVALID_MESSAGE = _load_fixture_message("invalid", "missing-id.json")


def run_cli(args: list[str], stdin: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(CLI_PATH), *args],
        input=stdin,
        capture_output=True,
        text=True,
        check=False,
    )


class TestInteropCli:
    def test_validate_reports_a_valid_message_as_valid(self) -> None:
        result = run_cli(["validate"], json.dumps(VALID_MESSAGE))

        assert result.returncode == 0
        assert json.loads(result.stdout) == {"valid": True}

    def test_validate_reports_an_invalid_message_as_invalid_without_failing(
        self,
    ) -> None:
        result = run_cli(["validate"], json.dumps(INVALID_MESSAGE))

        assert result.returncode == 0
        assert json.loads(result.stdout) == {"valid": False}

    def test_roundtrip_returns_a_valid_messages_wire_representation_unchanged(
        self,
    ) -> None:
        result = run_cli(["roundtrip"], json.dumps(VALID_MESSAGE))

        assert result.returncode == 0
        assert json.loads(result.stdout) == VALID_MESSAGE

    def test_roundtrip_fails_on_an_invalid_message_producing_no_stdout_result(
        self,
    ) -> None:
        result = run_cli(["roundtrip"], json.dumps(INVALID_MESSAGE))

        assert result.returncode == 1
        assert result.stdout.strip() == ""

    def test_exits_1_with_no_stdout_and_a_diagnostic_on_stderr_for_malformed_json(
        self,
    ) -> None:
        result = run_cli(["validate"], "{not json")

        assert result.returncode == 1
        assert result.stdout.strip() == ""
        assert result.stderr.strip() != ""

    def test_exits_2_when_no_operation_is_given(self) -> None:
        result = run_cli([])

        assert result.returncode == 2

    def test_exits_2_for_an_unknown_operation(self) -> None:
        result = run_cli(["bogus"])

        assert result.returncode == 2

    def test_writes_only_the_json_result_to_stdout(self) -> None:
        result = run_cli(["validate"], json.dumps(VALID_MESSAGE))

        assert result.stdout == json.dumps({"valid": True}) + "\n"
