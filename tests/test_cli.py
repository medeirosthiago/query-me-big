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

    def fake_execute(request) -> None:
        captured["request"] = request

    monkeypatch.setattr(cli, "_execute", fake_execute)
    monkeypatch.setattr("qmb.dbt.manifest.discover_manifest_path", lambda: manifest_path)

    result = CliRunner().invoke(cli.app, ["run", "--file", str(sql_path), "--resolve-dbt"])

    assert result.exit_code == 0, result.output
    request = captured["request"]
    assert request.mode == InputMode.FILE
    assert request.resolve_dbt is True
    assert request.manifest_path == manifest_path


def test_browser_only_mode_builds_browser_request(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_execute(request) -> None:
        captured["request"] = request

    monkeypatch.setattr(cli, "_execute", fake_execute)

    result = CliRunner().invoke(cli.app, ["run", "--browser-only", "--project", "proj"])

    assert result.exit_code == 0, result.output
    request = captured["request"]
    assert request.mode == InputMode.BROWSER
    assert request.project == "proj"


def test_browser_only_mode_rejects_query_inputs(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_execute", lambda request: None)

    result = CliRunner().invoke(cli.app, ["run", "select 1", "--browser-only"])

    assert result.exit_code != 0
    assert "cannot be combined" in result.output


def test_default_run_group_routes_options_to_run(monkeypatch, tmp_path: Path) -> None:
    sql_path = tmp_path / "foo.sql"
    sql_path.write_text("select 1", encoding="utf-8")

    captured: dict[str, object] = {}

    def fake_execute(request) -> None:
        captured["request"] = request

    monkeypatch.setattr(cli, "_execute", fake_execute)

    result = CliRunner().invoke(cli.app, ["--file", str(sql_path)])

    assert result.exit_code == 0, result.output
    request = captured["request"]
    assert request.mode == InputMode.FILE


def test_default_run_group_routes_positional_to_run(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_execute(request) -> None:
        captured["request"] = request

    monkeypatch.setattr(cli, "_execute", fake_execute)

    result = CliRunner().invoke(cli.app, ["select 1"])

    assert result.exit_code == 0, result.output
    request = captured["request"]
    assert request.mode == InputMode.SQL


def test_default_run_group_routes_history_command(monkeypatch) -> None:
    monkeypatch.setattr(
        "qmb.bigquery.client.get_client", lambda project, location: None
    )
    monkeypatch.setattr(
        "qmb.bigquery.history.list_recent_queries", lambda client, days, limit: []
    )

    result = CliRunner().invoke(cli.app, ["history", "--project", "proj"])

    assert result.exit_code == 0, result.output
    assert "No recent queries found" in result.output


def test_explicit_run_still_works(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_execute(request) -> None:
        captured["request"] = request

    monkeypatch.setattr(cli, "_execute", fake_execute)

    result = CliRunner().invoke(cli.app, ["run", "select 1"])

    assert result.exit_code == 0, result.output
    request = captured["request"]
    assert request.mode == InputMode.SQL
