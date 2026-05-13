from pathlib import Path

from typer.testing import CliRunner

import qmb.cli as cli
from qmb.types import InputMode


def test_parse_vars_coerces_scalar_values() -> None:
    assert cli._parse_vars(["limit=10", "enabled=true", "ratio=2.5", "note=01", "empty=null"]) == {
        "limit": 10,
        "enabled": True,
        "ratio": 2.5,
        "note": "01",
        "empty": None,
    }


def test_file_mode_resolve_dbt_auto_discovers_manifest(
    monkeypatch, tmp_path: Path
) -> None:
    sql_path = tmp_path / "query.sql"
    sql_path.write_text("select * from {{ ref('orders') }}", encoding="utf-8")
    manifest_path = tmp_path / "target" / "manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text("{}", encoding="utf-8")

    captured: dict[str, object] = {}

    def fake_execute(request, **_kwargs) -> None:
        captured["request"] = request

    monkeypatch.setattr(cli, "_execute", fake_execute)
    monkeypatch.setattr("qmb.dbt.manifest.discover_manifest_path", lambda: manifest_path)

    result = CliRunner().invoke(cli.app, ["run", "--file", str(sql_path), "--resolve-dbt"])

    assert result.exit_code == 0, result.output
    request = captured["request"]
    assert request.mode == InputMode.FILE
    assert request.resolve_dbt is True
    assert request.manifest_path == manifest_path


def test_browse_command_routes_correctly(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeClient:
        project = "proj"

    class FakeApp:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def run(self):
            pass

    monkeypatch.setattr("qmb.bigquery.client.get_client", lambda *a, **kw: FakeClient())
    monkeypatch.setattr("qmb.tui.app.QueryResultApp.__init__", FakeApp.__init__)
    monkeypatch.setattr("qmb.tui.app.QueryResultApp.run", FakeApp.run)

    result = CliRunner().invoke(cli.app, ["browse", "--project", "proj"])

    assert result.exit_code == 0, result.output
    assert captured["browser_only"] is True
    assert captured["source_label"] == "browser"


def test_default_run_group_routes_options_to_run(monkeypatch, tmp_path: Path) -> None:
    sql_path = tmp_path / "foo.sql"
    sql_path.write_text("select 1", encoding="utf-8")

    captured: dict[str, object] = {}

    def fake_execute(request, **_kwargs) -> None:
        captured["request"] = request

    monkeypatch.setattr(cli, "_execute", fake_execute)

    result = CliRunner().invoke(cli.app, ["--file", str(sql_path)])

    assert result.exit_code == 0, result.output
    request = captured["request"]
    assert request.mode == InputMode.FILE


def test_default_run_group_routes_positional_to_run(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_execute(request, **_kwargs) -> None:
        captured["request"] = request

    monkeypatch.setattr(cli, "_execute", fake_execute)

    result = CliRunner().invoke(cli.app, ["select 1"])

    assert result.exit_code == 0, result.output
    request = captured["request"]
    assert request.mode == InputMode.SQL


def test_default_run_group_routes_history_command(monkeypatch) -> None:
    """`qmb history` is headless: prints a JSON array on stdout."""
    import json as _json

    monkeypatch.setattr(
        "qmb.bigquery.client.get_client", lambda project, location: None
    )
    monkeypatch.setattr(
        "qmb.bigquery.history.list_recent_queries", lambda client, days, limit: []
    )

    result = CliRunner().invoke(cli.app, ["history", "--project", "proj"])

    assert result.exit_code == 0, result.output
    assert _json.loads(result.output.strip()) == []


def test_top_level_help_lists_commands() -> None:
    result = CliRunner().invoke(cli.app, ["--help"])

    assert result.exit_code == 0, result.output
    assert "run" in result.output
    assert "browse" in result.output
    assert "history" in result.output
    assert "jobs" in result.output


def test_version_flag_prints_version_and_exits() -> None:
    from qmb import __version__

    result = CliRunner().invoke(cli.app, ["--version"])

    assert result.exit_code == 0, result.output
    assert f"qmb {__version__}" in result.output


def test_version_short_flag_prints_version_and_exits() -> None:
    from qmb import __version__

    result = CliRunner().invoke(cli.app, ["-V"])

    assert result.exit_code == 0, result.output
    assert f"qmb {__version__}" in result.output


def test_explicit_run_still_works(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_execute(request, **_kwargs) -> None:
        captured["request"] = request

    monkeypatch.setattr(cli, "_execute", fake_execute)

    result = CliRunner().invoke(cli.app, ["run", "select 1"])

    assert result.exit_code == 0, result.output
    request = captured["request"]
    assert request.mode == InputMode.SQL


def test_history_json_payload_contains_entry_fields(monkeypatch) -> None:
    """`qmb history` JSON includes every QueryHistoryEntry field."""
    import json as _json
    from datetime import UTC, datetime

    from qmb.bigquery.history import QueryHistoryEntry

    entries = [
        QueryHistoryEntry(
            job_id="job-abc",
            project="proj",
            location="US",
            created=datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC),
            query="SELECT 1",
            bytes_processed=2048,
            state="DONE",
        ),
    ]

    monkeypatch.setattr(
        "qmb.bigquery.client.get_client", lambda project, location: None
    )
    monkeypatch.setattr(
        "qmb.bigquery.history.list_recent_queries", lambda client, days, limit: entries
    )

    result = CliRunner().invoke(cli.app, ["history"])

    assert result.exit_code == 0, result.output
    payload = _json.loads(result.output.strip())
    assert payload == [
        {
            "job_id": "job-abc",
            "project": "proj",
            "location": "US",
            "created": "2026-04-01T12:00:00+00:00",
            "query": "SELECT 1",
            "bytes_processed": 2048,
            "state": "DONE",
        }
    ]


def test_history_tui_flag_opens_the_picker(monkeypatch) -> None:
    """`qmb history -t` opens the Textual picker instead of printing JSON."""
    monkeypatch.setattr(
        "qmb.bigquery.client.get_client",
        lambda project, location: type("FC", (), {"project": "proj"})(),
    )
    from datetime import UTC, datetime

    from qmb.bigquery.history import QueryHistoryEntry

    monkeypatch.setattr(
        "qmb.bigquery.history.list_recent_queries",
        lambda client, days, limit: [
            QueryHistoryEntry(
                job_id="j",
                project="p",
                location="US",
                created=datetime(2026, 1, 1, tzinfo=UTC),
                query="select 1",
            )
        ],
    )

    captured: dict = {}

    def fake_init(self, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("qmb.tui.app.QueryResultApp.__init__", fake_init)
    monkeypatch.setattr("qmb.tui.app.QueryResultApp.run", lambda self: None)

    result = CliRunner().invoke(cli.app, ["history", "-t"])

    assert result.exit_code == 0, result.output
    assert captured["source_label"] == "history"
    assert len(captured["history_entries"]) == 1
