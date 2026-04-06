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

    result = CliRunner().invoke(cli.app, ["--file", str(sql_path), "--resolve-dbt"])

    assert result.exit_code == 0, result.output
    request = captured["request"]
    assert request.mode == InputMode.FILE
    assert request.resolve_dbt is True
    assert request.manifest_path == manifest_path
