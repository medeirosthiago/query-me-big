"""Characterization tests for CLI orchestration flows.

These tests pin down the current `cli._execute` orchestration (resolve →
optional --where wrap → execute → optional export → TUI). They mock out
BigQuery and the TUI so the orchestration steps can be observed directly.

They exist to fail loudly if Phase 3 (orchestration extraction) accidentally
changes user-visible behavior.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

import qmb.cli as cli
from qmb.types import ExportFormat, QueryResultHandle, ResolvedQuery


def _handle(total_rows: int = 1, *, bytes_processed: int = 1024) -> QueryResultHandle:
    return QueryResultHandle(
        job_id="job-1",
        project="proj",
        location="US",
        destination_table="proj.ds.tbl",
        schema=[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}],
        total_rows=total_rows,
        bytes_processed=bytes_processed,
    )


class _ExecuteRecorder:
    """Captures execute_query calls and returns a configurable handle."""

    def __init__(self, handle: QueryResultHandle | None = None) -> None:
        self.handle = handle or _handle()
        self.calls: list[dict[str, Any]] = []

    def __call__(
        self,
        client: Any,
        resolved: ResolvedQuery,
        dry_run: bool = False,
        max_bytes_billed: int | None = None,
    ) -> QueryResultHandle:
        self.calls.append({
            "client": client,
            "resolved": resolved,
            "dry_run": dry_run,
            "max_bytes_billed": max_bytes_billed,
        })
        return self.handle


class _ExportRecorder:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def __call__(
        self,
        client: Any,
        handle: QueryResultHandle,
        fmt: ExportFormat,
        path: Path,
    ) -> int:
        self.calls.append({
            "client": client,
            "handle": handle,
            "fmt": fmt,
            "path": path,
        })
        return handle.total_rows


def _install_common_mocks(
    monkeypatch,
    *,
    execute: _ExecuteRecorder,
    export: _ExportRecorder | None = None,
    tui_started: list[dict[str, Any]] | None = None,
) -> object:
    """Patch out BigQuery client, executor, exporter, and TUI inside cli._execute."""

    class FakeClient:
        project = "proj"
        location = "US"

    client = FakeClient()

    monkeypatch.setattr("qmb.bigquery.client.get_client", lambda *a, **kw: client)
    monkeypatch.setattr("qmb.bigquery.executor.execute_query", execute)

    if export is not None:
        monkeypatch.setattr("qmb.bigquery.exporters.export_results", export)

    def fake_init(self: Any, **kwargs: Any) -> None:
        if tui_started is not None:
            tui_started.append(kwargs)

    def fake_run(self: Any) -> None:
        return None

    monkeypatch.setattr("qmb.tui.app.QueryResultApp.__init__", fake_init)
    monkeypatch.setattr("qmb.tui.app.QueryResultApp.run", fake_run)

    return client


def test_ad_hoc_sql_flow_resolves_executes_and_opens_tui(monkeypatch) -> None:
    execute = _ExecuteRecorder()
    tui_started: list[dict[str, Any]] = []
    _install_common_mocks(monkeypatch, execute=execute, tui_started=tui_started)

    result = CliRunner().invoke(cli.app, ["run", "SELECT 1"])

    assert result.exit_code == 0, result.output
    assert len(execute.calls) == 1
    call = execute.calls[0]
    assert call["resolved"].sql == "SELECT 1"
    assert call["resolved"].source_label == "ad-hoc"
    assert call["dry_run"] is False
    assert len(tui_started) == 1
    assert tui_started[0]["source_label"] == "ad-hoc"


def test_no_tui_skips_tui(monkeypatch) -> None:
    execute = _ExecuteRecorder()
    tui_started: list[dict[str, Any]] = []
    _install_common_mocks(monkeypatch, execute=execute, tui_started=tui_started)

    result = CliRunner().invoke(cli.app, ["run", "SELECT 1", "--no-tui"])

    assert result.exit_code == 0, result.output
    assert len(execute.calls) == 1
    assert tui_started == []


def test_file_mode_reads_file_and_resolves(monkeypatch, tmp_path: Path) -> None:
    sql_path = tmp_path / "q.sql"
    sql_path.write_text("SELECT 2;\n", encoding="utf-8")

    execute = _ExecuteRecorder()
    _install_common_mocks(monkeypatch, execute=execute, tui_started=[])

    result = CliRunner().invoke(cli.app, ["run", "--file", str(sql_path), "--no-tui"])

    assert result.exit_code == 0, result.output
    assert len(execute.calls) == 1
    resolved = execute.calls[0]["resolved"]
    assert resolved.sql == "SELECT 2"
    assert "file: q.sql" in resolved.source_label


def test_file_mode_with_resolve_dbt_calls_dbt_resolver(
    monkeypatch, tmp_path: Path
) -> None:
    sql_path = tmp_path / "model.sql"
    sql_path.write_text("select * from {{ ref('orders') }}", encoding="utf-8")
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{}", encoding="utf-8")

    execute = _ExecuteRecorder()
    _install_common_mocks(monkeypatch, execute=execute, tui_started=[])

    # Force the dbt branch: no manifest node match, fall through to
    # resolve_file_sql so we can observe the call without any real manifest.
    monkeypatch.setattr("qmb.dbt.manifest.load_manifest", lambda p: {"_idx": True})
    monkeypatch.setattr(
        "qmb.dbt.resolver.resolve_file_to_model",
        lambda file_path, index: None,
    )

    resolver_calls: list[dict[str, Any]] = []

    def fake_resolve_file_sql(sql, index, variables, source_label):
        resolver_calls.append({
            "sql": sql,
            "index": index,
            "variables": variables,
            "source_label": source_label,
        })
        return ResolvedQuery(sql="SELECT 99", source_label=source_label)

    monkeypatch.setattr("qmb.dbt.resolver.resolve_file_sql", fake_resolve_file_sql)

    result = CliRunner().invoke(
        cli.app,
        [
            "run",
            "--file",
            str(sql_path),
            "--resolve-dbt",
            "--manifest",
            str(manifest_path),
            "--no-tui",
        ],
    )

    assert result.exit_code == 0, result.output
    assert len(resolver_calls) == 1
    assert resolver_calls[0]["index"] == {"_idx": True}
    # The resolver's output is what gets executed.
    assert execute.calls[0]["resolved"].sql == "SELECT 99"


def test_model_mode_resolves_from_manifest(monkeypatch, tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{}", encoding="utf-8")

    execute = _ExecuteRecorder()
    _install_common_mocks(monkeypatch, execute=execute, tui_started=[])

    monkeypatch.setattr("qmb.dbt.manifest.load_manifest", lambda p: {"_idx": "ok"})

    resolve_calls: list[dict[str, Any]] = []

    def fake_resolve_model_query(model_name, index, variables):
        resolve_calls.append({
            "model_name": model_name,
            "index": index,
            "variables": variables,
        })
        return ResolvedQuery(sql="SELECT 42", source_label=f"model: {model_name}")

    monkeypatch.setattr(
        "qmb.dbt.resolver.resolve_model_query", fake_resolve_model_query
    )

    result = CliRunner().invoke(
        cli.app,
        ["run", "--model", "orders", "--manifest", str(manifest_path), "--no-tui"],
    )

    assert result.exit_code == 0, result.output
    assert resolve_calls == [
        {"model_name": "orders", "index": {"_idx": "ok"}, "variables": {}}
    ]
    assert execute.calls[0]["resolved"].sql == "SELECT 42"


def test_where_clause_wraps_resolved_sql_in_subquery(monkeypatch) -> None:
    execute = _ExecuteRecorder()
    _install_common_mocks(monkeypatch, execute=execute, tui_started=[])

    result = CliRunner().invoke(
        cli.app, ["run", "SELECT 1 AS x", "--where", "x = 1", "--no-tui"]
    )

    assert result.exit_code == 0, result.output
    sql = execute.calls[0]["resolved"].sql
    assert sql == "SELECT * FROM (SELECT 1 AS x) __qmb WHERE x = 1"


def test_dry_run_executes_with_dry_run_flag_and_skips_export(monkeypatch) -> None:
    handle = QueryResultHandle(
        job_id="job-dry",
        project="proj",
        location="US",
        destination_table="",
        schema=[],
        total_rows=0,
        bytes_processed=2048,
    )
    execute = _ExecuteRecorder(handle=handle)
    export = _ExportRecorder()
    tui_started: list[dict[str, Any]] = []
    _install_common_mocks(
        monkeypatch, execute=execute, export=export, tui_started=tui_started
    )

    result = CliRunner().invoke(cli.app, ["run", "SELECT 1", "--dry-run"])

    assert result.exit_code == 0, result.output
    assert len(execute.calls) == 1
    assert execute.calls[0]["dry_run"] is True
    # Dry run should not export and should not open the TUI.
    assert export.calls == []
    assert tui_started == []


def test_export_csv_with_no_tui_calls_exporter(monkeypatch, tmp_path: Path) -> None:
    out_path = tmp_path / "out.csv"
    execute = _ExecuteRecorder()
    export = _ExportRecorder()
    tui_started: list[dict[str, Any]] = []
    _install_common_mocks(
        monkeypatch, execute=execute, export=export, tui_started=tui_started
    )

    result = CliRunner().invoke(
        cli.app,
        ["run", "SELECT 1", "--export", "csv", "--out", str(out_path), "--no-tui"],
    )

    assert result.exit_code == 0, result.output
    assert len(execute.calls) == 1
    assert len(export.calls) == 1
    assert export.calls[0]["fmt"] == ExportFormat.CSV
    assert export.calls[0]["path"] == out_path
    assert tui_started == []


def test_max_bytes_billed_is_passed_to_executor(monkeypatch) -> None:
    execute = _ExecuteRecorder()
    _install_common_mocks(monkeypatch, execute=execute, tui_started=[])

    result = CliRunner().invoke(
        cli.app,
        ["run", "SELECT 1", "--max-bytes-billed", "12345", "--no-tui"],
    )

    assert result.exit_code == 0, result.output
    assert execute.calls[0]["max_bytes_billed"] == 12345


def test_cli_run_archives_local_job(monkeypatch, tmp_path: Path) -> None:
    from tests.test_bigquery_flow import FakeBigQueryClient, _rows, _schema

    fake_client = FakeBigQueryClient(_rows(), _schema())
    monkeypatch.setattr(
        "qmb.bigquery.client.get_client", lambda *a, **kw: fake_client
    )
    monkeypatch.setenv("QMB_JOBS_DIR", str(tmp_path / "jobs"))

    result = CliRunner().invoke(
        cli.app,
        ["run", "SELECT * FROM example", "--where", "id = 1", "--no-tui"],
    )

    assert result.exit_code == 0, result.output
    job_dirs = list((tmp_path / "jobs").iterdir())
    assert len(job_dirs) == 1
    job_dir = job_dirs[0]
    assert (job_dir / "query.sql").read_text(encoding="utf-8") == (
        "SELECT * FROM (SELECT * FROM example) __qmb WHERE id = 1"
    )
    assert (job_dir / "preview.jsonl").read_text(encoding="utf-8") == (
        '{"id": 1, "enabled": true, "payload": {"items": [1, 2]}}\n'
        '{"id": 2, "enabled": false, "payload": {"items": [3]}}\n'
        '{"id": 3, "enabled": true, "payload": {"items": []}}\n'
    )


def test_export_json_round_trip_uses_real_exporter(
    monkeypatch, tmp_path: Path
) -> None:
    """One end-to-end test that the export path is wired up correctly.

    Uses the real export_results function with the FakeBigQueryClient pattern
    (mirroring test_bigquery_flow) so we know cli wiring matches the exporter
    contract.
    """
    from tests.test_bigquery_flow import FakeBigQueryClient, _rows, _schema

    rows = _rows()
    fake_client = FakeBigQueryClient(rows, _schema())

    monkeypatch.setattr(
        "qmb.bigquery.client.get_client", lambda *a, **kw: fake_client
    )
    monkeypatch.setenv("QMB_JOBS_DIR", str(tmp_path / "jobs"))

    out_path = tmp_path / "out.json"

    result = CliRunner().invoke(
        cli.app,
        [
            "run",
            "SELECT * FROM example",
            "--export",
            "json",
            "--out",
            str(out_path),
            "--no-tui",
        ],
    )

    assert result.exit_code == 0, result.output
    assert out_path.exists()
    assert json.loads(out_path.read_text(encoding="utf-8")) == rows


# ---------------------------------------------------------------------------
# Phase 10A: --format flag wires through to the formatters package
# ---------------------------------------------------------------------------


def test_format_json_emits_structured_payload_on_stdout(
    monkeypatch, tmp_path: Path
) -> None:
    """`qmb run --format json` prints a single JSON object with rows + stats."""
    from tests.test_bigquery_flow import FakeBigQueryClient, _rows, _schema

    fake_client = FakeBigQueryClient(_rows(), _schema())
    monkeypatch.setattr("qmb.bigquery.client.get_client", lambda *a, **kw: fake_client)
    monkeypatch.setenv("QMB_JOBS_DIR", str(tmp_path / "jobs"))

    # If the TUI somehow launches the test will hang / error; assert it does not.
    def fail_init(*a, **kw):
        raise AssertionError("TUI must not launch when --format json is requested")

    monkeypatch.setattr("qmb.tui.app.QueryResultApp.__init__", fail_init)

    result = CliRunner().invoke(
        cli.app, ["run", "SELECT * FROM example", "--format", "json"]
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output.strip().splitlines()[-1])
    assert payload["dry_run"] is False
    assert payload["stats"]["total_rows"] == 3
    assert payload["stats"]["source_label"] == "ad-hoc"
    assert payload["schema"] == [
        {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "enabled", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "payload", "type": "JSON", "mode": "NULLABLE"},
    ]
    assert payload["rows"] == [
        {"id": 1, "enabled": True, "payload": {"items": [1, 2]}},
        {"id": 2, "enabled": False, "payload": {"items": [3]}},
        {"id": 3, "enabled": True, "payload": {"items": []}},
    ]
    # Phase 9 archive id is surfaced in the JSON payload.
    assert payload["archive"]["qmb_job_id"] is not None


def test_format_csv_emits_header_and_rows_on_stdout(
    monkeypatch, tmp_path: Path
) -> None:
    """`qmb run --format csv` prints a CSV with a header row drawn from schema."""
    import csv as _csv
    import io as _io

    from tests.test_bigquery_flow import FakeBigQueryClient, _rows, _schema

    fake_client = FakeBigQueryClient(_rows(), _schema())
    monkeypatch.setattr("qmb.bigquery.client.get_client", lambda *a, **kw: fake_client)
    monkeypatch.setenv("QMB_JOBS_DIR", str(tmp_path / "jobs"))

    def fail_init(*a, **kw):
        raise AssertionError("TUI must not launch when --format csv is requested")

    monkeypatch.setattr("qmb.tui.app.QueryResultApp.__init__", fail_init)

    result = CliRunner().invoke(
        cli.app, ["run", "SELECT * FROM example", "--format", "csv"]
    )

    assert result.exit_code == 0, result.output
    parsed = list(_csv.reader(_io.StringIO(result.output)))
    # Header.
    assert parsed[0] == ["id", "enabled", "payload"]
    # Three data rows.
    assert len(parsed) == 4
    assert parsed[1][0] == "1"
    assert parsed[1][1] == "True"
    assert parsed[1][2] == '{"items": [1, 2]}'


def test_format_dry_run_json_emits_dry_run_shape(monkeypatch, tmp_path: Path) -> None:
    """`qmb run --dry-run --format json` emits the dry-run schema, not rows."""
    execute = _ExecuteRecorder(handle=_handle(total_rows=0, bytes_processed=99))
    _install_common_mocks(monkeypatch, execute=execute, tui_started=[])
    monkeypatch.setenv("QMB_JOBS_DIR", str(tmp_path / "jobs"))

    result = CliRunner().invoke(
        cli.app, ["run", "SELECT 1", "--dry-run", "--format", "json"]
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output.strip().splitlines()[-1])
    assert payload["dry_run"] is True
    assert payload["sql"] == "SELECT 1"
    assert payload["stats"]["bytes_processed"] == 99
    assert "rows" not in payload


def test_invalid_format_value_is_a_user_error(monkeypatch) -> None:
    """An unknown --format value fails fast with a BadParameter."""
    result = CliRunner().invoke(
        cli.app, ["run", "SELECT 1", "--format", "ndjson", "--no-tui"]
    )
    assert result.exit_code != 0
    assert "Invalid format" in result.output


def test_format_tui_overrides_no_tui(monkeypatch) -> None:
    """`--format tui` opens the TUI even alongside (legacy) --no-tui."""
    execute = _ExecuteRecorder()
    tui_started: list[dict[str, Any]] = []
    _install_common_mocks(monkeypatch, execute=execute, tui_started=tui_started)

    result = CliRunner().invoke(
        cli.app,
        ["run", "SELECT 1", "--format", "tui", "--no-tui"],
    )

    assert result.exit_code == 0, result.output
    assert len(tui_started) == 1
