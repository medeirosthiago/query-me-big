"""Application pipeline tests for Phase 9 job archive persistence."""

from __future__ import annotations

import importlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from qmb.application.pipeline import run_query_pipeline
from qmb.types import ExportFormat, InputMode, QueryRequest, QueryResultHandle

NOW = datetime(2026, 5, 12, 14, 33, 2, tzinfo=UTC)


class FakeRow:
    def __init__(self, values: dict[str, Any]) -> None:
        self._values = values

    def items(self) -> Any:
        return self._values.items()


class FakeClient:
    project = "proj"
    location = "US"

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.list_rows_calls: list[dict[str, Any]] = []

    def list_rows(
        self,
        table_ref: Any,
        start_index: int | None = None,
        max_results: int | None = None,
        page_size: int | None = None,
    ) -> list[FakeRow]:
        self.list_rows_calls.append(
            {
                "table": f"{table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}",
                "start_index": start_index,
                "max_results": max_results,
                "page_size": page_size,
            }
        )
        start = start_index or 0
        end = start + (max_results or len(self.rows))
        return [FakeRow(row) for row in self.rows[start:end]]


def _jobs_modules() -> tuple[Any, Any]:
    try:
        models = importlib.import_module("qmb.jobs.models")
        store = importlib.import_module("qmb.jobs.store")
    except ModuleNotFoundError:  # pragma: no cover - expected until implemented
        pytest.fail("Expected qmb.jobs.models and qmb.jobs.store for Phase 9 archives")
    return models, store


def _store(tmp_path: Path) -> Any:
    _, store_module = _jobs_modules()
    return store_module.JobStore(
        root=tmp_path / "jobs",
        now=lambda: NOW,
        nonce=lambda: "a1b2c3",
    )


def _handle(*, job_id: str = "bq-job-123", total_rows: int = 2) -> QueryResultHandle:
    return QueryResultHandle(
        job_id=job_id,
        project="proj",
        location="US",
        destination_table="proj.ds.tbl",
        schema=[
            {"name": "x", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        ],
        total_rows=total_rows,
        bytes_processed=4096,
        execution_seconds=1.25,
    )


def _patch_pipeline_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    *,
    client: FakeClient,
    handle: QueryResultHandle,
) -> list[dict[str, Any]]:
    execute_calls: list[dict[str, Any]] = []

    def fake_execute_query(
        bq_client: Any,
        resolved: Any,
        dry_run: bool = False,
        max_bytes_billed: int | None = None,
    ) -> QueryResultHandle:
        execute_calls.append(
            {
                "client": bq_client,
                "resolved": resolved,
                "dry_run": dry_run,
                "max_bytes_billed": max_bytes_billed,
            }
        )
        return handle

    monkeypatch.setattr("qmb.bigquery.client.get_client", lambda *a, **kw: client)
    monkeypatch.setattr("qmb.bigquery.executor.execute_query", fake_execute_query)
    return execute_calls


def test_successful_pipeline_archives_resolved_sql_metadata_and_preview(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    store = _store(tmp_path)
    client = FakeClient(rows=[{"x": 1, "name": "one"}, {"x": 2, "name": "two"}])
    _patch_pipeline_dependencies(monkeypatch, client=client, handle=_handle())

    request = QueryRequest(
        mode=InputMode.SQL,
        sql="SELECT 1 AS x",
        where="x = 1",
        project="proj",
        location="US",
        no_tui=True,
    )

    outcome = run_query_pipeline(request, job_store=store)

    assert outcome.archived_job is not None
    record = outcome.archived_job
    assert record.qmb_job_id == "qmb_2026-05-12_14-33-02_a1b2c3"
    assert record.query_path.read_text(encoding="utf-8") == (
        "SELECT * FROM (SELECT 1 AS x) __qmb WHERE x = 1"
    )
    assert record.preview_path.read_text(encoding="utf-8") == (
        '{"x": 1, "name": "one"}\n{"x": 2, "name": "two"}\n'
    )

    metadata = json.loads(record.metadata_path.read_text(encoding="utf-8"))
    assert metadata["created_at"] == "2026-05-12T14:33:02+00:00"
    assert metadata["session_id"] is None
    assert metadata["parent_job_id"] is None
    assert metadata["source"] == {
        "label": "ad-hoc",
        "input_mode": "sql",
        "file_path": None,
        "model_name": None,
        "manifest_path": None,
        "resolver": "plain",
        "matched_node_id": None,
    }
    assert metadata["engine"] == {
        "name": "bigquery",
        "job_id": "bq-job-123",
        "project": "proj",
        "location": "US",
    }
    assert metadata["stats"] == {
        "total_rows": 2,
        "bytes_processed": 4096,
        "execution_seconds": 1.25,
    }
    assert metadata["artifacts"]["result"] is None


def test_dry_run_does_not_create_a_result_archive(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    store = _store(tmp_path)
    client = FakeClient(rows=[])
    _patch_pipeline_dependencies(
        monkeypatch,
        client=client,
        handle=_handle(job_id="dry-run-job", total_rows=0),
    )

    request = QueryRequest(
        mode=InputMode.SQL,
        sql="SELECT 1",
        dry_run=True,
        no_tui=True,
    )

    outcome = run_query_pipeline(request, job_store=store)

    assert outcome.dry_run is True
    assert outcome.archived_job is None
    assert not (tmp_path / "jobs").exists() or list((tmp_path / "jobs").iterdir()) == []
    assert client.list_rows_calls == []


def test_explicit_export_stays_separate_from_history_archive(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    store = _store(tmp_path)
    client = FakeClient(rows=[{"x": 1, "name": "one"}])
    _patch_pipeline_dependencies(monkeypatch, client=client, handle=_handle(total_rows=1))
    export_calls: list[dict[str, Any]] = []

    def fake_export_results(
        bq_client: Any,
        handle: QueryResultHandle,
        fmt: ExportFormat,
        output_path: Path,
    ) -> int:
        export_calls.append(
            {"client": bq_client, "handle": handle, "fmt": fmt, "path": output_path}
        )
        output_path.write_text("x,name\n1,one\n", encoding="utf-8")
        return 1

    monkeypatch.setattr("qmb.bigquery.exporters.export_results", fake_export_results)
    out_path = tmp_path / "user-export.csv"
    request = QueryRequest(
        mode=InputMode.SQL,
        sql="SELECT 1 AS x, 'one' AS name",
        export_format=ExportFormat.CSV,
        export_path=out_path,
        no_tui=True,
    )

    outcome = run_query_pipeline(request, job_store=store)

    assert export_calls == [
        {
            "client": client,
            "handle": outcome.handle,
            "fmt": ExportFormat.CSV,
            "path": out_path,
        }
    ]
    assert out_path.read_text(encoding="utf-8") == "x,name\n1,one\n"
    assert outcome.archived_job is not None
    assert outcome.archived_job.result_path is None
    assert not (outcome.archived_job.directory / "result.jsonl").exists()
