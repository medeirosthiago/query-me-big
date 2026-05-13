"""Phase 10C: structured JSON errors + standard exit codes."""

import io
import json

import click
import pytest
from typer.testing import CliRunner

import qmb.cli as cli
from qmb.errors import (
    EXIT_ENGINE_ERROR,
    EXIT_INTERRUPTED,
    EXIT_OK,
    EXIT_USER_ERROR,
    emit_json_error,
)

# ---------------------------------------------------------------------------
# emit_json_error — unit
# ---------------------------------------------------------------------------


def test_emit_json_error_writes_payload_and_exits() -> None:
    buf = io.StringIO()
    with pytest.raises(SystemExit) as excinfo:
        emit_json_error(
            type_="user_error",
            message="oops",
            exit_code=EXIT_USER_ERROR,
            details={"class": "BadParameter"},
            stream=buf,
        )
    assert excinfo.value.code == EXIT_USER_ERROR
    payload = json.loads(buf.getvalue())
    assert payload == {
        "error": {
            "type": "user_error",
            "message": "oops",
            "details": {"class": "BadParameter"},
        }
    }


def test_emit_json_error_omits_details_when_none() -> None:
    buf = io.StringIO()
    with pytest.raises(SystemExit):
        emit_json_error(
            type_="interrupted",
            message="Aborted",
            exit_code=EXIT_INTERRUPTED,
            stream=buf,
        )
    payload = json.loads(buf.getvalue())
    assert "details" not in payload["error"]
    assert payload["error"] == {"type": "interrupted", "message": "Aborted"}


def test_exit_code_constants_have_expected_values() -> None:
    """Pin the codes so scripts can rely on them."""
    assert EXIT_OK == 0
    assert EXIT_USER_ERROR == 1
    assert EXIT_ENGINE_ERROR == 2
    assert EXIT_INTERRUPTED == 130


# ---------------------------------------------------------------------------
# _DefaultRunGroup.main() error catching
# ---------------------------------------------------------------------------


def _runner() -> CliRunner:
    # Click 8.3+ separates stderr from stdout by default.
    return CliRunner()


def test_invalid_format_emits_user_error_json_on_stderr() -> None:
    result = _runner().invoke(cli.app, ["run", "SELECT 1", "--format", "ndjson"])

    assert result.exit_code == EXIT_USER_ERROR
    payload = json.loads(result.stderr.strip())
    assert payload["error"]["type"] == "user_error"
    assert "Invalid format" in payload["error"]["message"]
    assert payload["error"]["details"]["class"] == "BadParameter"


def test_unknown_option_emits_user_error_json() -> None:
    result = _runner().invoke(cli.app, ["run", "SELECT 1", "--definitely-not-a-flag"])

    assert result.exit_code == EXIT_USER_ERROR
    payload = json.loads(result.stderr.strip())
    assert payload["error"]["type"] == "user_error"
    assert "No such option" in payload["error"]["message"] or "Got unexpected" in payload[
        "error"
    ]["message"]


def test_missing_required_argument_emits_user_error_json() -> None:
    # ``qmb describe`` requires a positional target.
    result = _runner().invoke(cli.app, ["describe"])

    assert result.exit_code == EXIT_USER_ERROR
    payload = json.loads(result.stderr.strip())
    assert payload["error"]["type"] == "user_error"


def test_keyboard_interrupt_emits_interrupted_json(monkeypatch) -> None:
    def boom(*a, **kw):
        raise KeyboardInterrupt()

    # Make the command body raise KeyboardInterrupt.
    monkeypatch.setattr(cli, "_execute", boom)

    result = _runner().invoke(cli.app, ["run", "SELECT 1"])

    assert result.exit_code == EXIT_INTERRUPTED
    payload = json.loads(result.stderr.strip())
    assert payload["error"]["type"] == "interrupted"


def test_click_abort_emits_interrupted_json(monkeypatch) -> None:
    def boom(*a, **kw):
        raise click.exceptions.Abort()

    monkeypatch.setattr(cli, "_execute", boom)

    result = _runner().invoke(cli.app, ["run", "SELECT 1"])

    assert result.exit_code == EXIT_INTERRUPTED
    payload = json.loads(result.stderr.strip())
    assert payload["error"]["type"] == "interrupted"


def test_unexpected_exception_emits_internal_error_json(monkeypatch) -> None:
    def boom(*a, **kw):
        raise RuntimeError("boom")

    monkeypatch.setattr(cli, "_execute", boom)

    result = _runner().invoke(cli.app, ["run", "SELECT 1"])

    assert result.exit_code == EXIT_ENGINE_ERROR
    payload = json.loads(result.stderr.strip())
    assert payload["error"]["type"] == "internal_error"
    assert payload["error"]["message"] == "boom"
    assert payload["error"]["details"]["class"] == "RuntimeError"


def test_google_api_error_emits_engine_error_json(monkeypatch) -> None:
    from google.api_core.exceptions import Forbidden

    def boom(*a, **kw):
        raise Forbidden("permission denied")

    monkeypatch.setattr(cli, "_execute", boom)

    result = _runner().invoke(cli.app, ["run", "SELECT 1"])

    assert result.exit_code == EXIT_ENGINE_ERROR
    payload = json.loads(result.stderr.strip())
    assert payload["error"]["type"] == "engine_error"
    assert "permission denied" in payload["error"]["message"]


def test_file_not_found_emits_user_error_json(tmp_path) -> None:
    missing = tmp_path / "definitely-not-there.sql"

    result = _runner().invoke(cli.app, ["run", "--file", str(missing)])

    assert result.exit_code == EXIT_USER_ERROR
    payload = json.loads(result.stderr.strip())
    assert payload["error"]["type"] == "user_error"


def test_ambiguous_job_id_emits_user_error_json(tmp_path, monkeypatch) -> None:
    """`qmb jobs show <prefix>` with an unknown id → user_error."""
    monkeypatch.setenv("QMB_JOBS_DIR", str(tmp_path))

    result = _runner().invoke(cli.app, ["jobs", "show", "nonexistent"])

    assert result.exit_code == EXIT_USER_ERROR
    payload = json.loads(result.stderr.strip())
    assert payload["error"]["type"] == "user_error"


def test_help_still_works_with_zero_exit_and_no_stderr() -> None:
    """`--help` is not an error and must not emit JSON to stderr."""
    result = _runner().invoke(cli.app, ["--help"])
    assert result.exit_code == EXIT_OK
    assert result.stderr == ""
    assert "qmb" in result.output.lower() or "Usage" in result.output


def test_successful_run_does_not_touch_stderr(monkeypatch) -> None:
    """Happy-path runs leave stderr empty."""
    from tests.test_bigquery_flow import FakeBigQueryClient, _rows, _schema

    fake_client = FakeBigQueryClient(_rows(), _schema())
    monkeypatch.setattr("qmb.bigquery.client.get_client", lambda *a, **kw: fake_client)

    result = _runner().invoke(cli.app, ["run", "SELECT * FROM t"])

    assert result.exit_code == EXIT_OK
    assert result.stderr == ""
