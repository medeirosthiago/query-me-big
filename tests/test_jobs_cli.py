"""CLI tests for local historical qmb jobs."""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

import qmb.cli as cli
from qmb.types import SchemaField


def _jobs_modules() -> tuple[Any, Any]:
    try:
        models = importlib.import_module("qmb.jobs.models")
        store = importlib.import_module("qmb.jobs.store")
    except ModuleNotFoundError:  # pragma: no cover - expected until implemented
        pytest.fail("Expected qmb.jobs.models and qmb.jobs.store for Phase 9 archives")
    return models, store


def _iter_callable(values: list[Any]) -> Callable[[], Any]:
    iterator: Iterator[Any] = iter(values)
    return lambda: next(iterator)


def _seed_jobs(root: Path) -> list[Any]:
    models, store_module = _jobs_modules()
    store = store_module.JobStore(
        root=root,
        now=_iter_callable(
            [
                datetime(2026, 5, 12, 10, 0, tzinfo=UTC),
                datetime(2026, 5, 12, 11, 0, tzinfo=UTC),
            ]
        ),
        nonce=_iter_callable(["abc111", "def222"]),
    )
    first = store.create(
        resolved_sql="SELECT 'first' AS label",
        schema=[SchemaField("label", "STRING")],
        preview_rows=[{"label": "first"}],
        source=models.SourceMetadata(label="ad-hoc", input_mode="sql"),
        engine=models.EngineMetadata(
            name="bigquery",
            job_id="bq-first",
            project="proj",
            location="US",
        ),
        total_rows=1,
        bytes_processed=100,
        execution_seconds=0.5,
    )
    second = store.create(
        resolved_sql="SELECT 'second' AS label",
        schema=[SchemaField("label", "STRING")],
        preview_rows=[{"label": "second"}],
        source=models.SourceMetadata(label="file: second.sql", input_mode="file"),
        engine=models.EngineMetadata(
            name="bigquery",
            job_id="bq-second",
            project="proj",
            location="US",
        ),
        total_rows=1,
        bytes_processed=200,
        execution_seconds=0.75,
    )
    return [first, second]


def test_jobs_list_shows_local_qmb_jobs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    jobs_root = tmp_path / "jobs"
    first, second = _seed_jobs(jobs_root)
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))

    result = CliRunner().invoke(cli.app, ["jobs", "list"])

    assert result.exit_code == 0, result.output
    assert second.qmb_job_id in result.output
    assert first.qmb_job_id in result.output
    assert "file: second.sql" in result.output
    assert "ad-hoc" in result.output


def test_jobs_list_json_returns_machine_readable_records(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    jobs_root = tmp_path / "jobs"
    first, second = _seed_jobs(jobs_root)
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))

    result = CliRunner().invoke(cli.app, ["jobs", "list", "--format", "json"])

    assert result.exit_code == 0, result.output
    records = json.loads(result.output)
    assert [record["qmb_job_id"] for record in records] == [
        second.qmb_job_id,
        first.qmb_job_id,
    ]
    assert records[0]["source"]["label"] == "file: second.sql"
    assert records[0]["engine"] == {
        "name": "bigquery",
        "job_id": "bq-second",
        "project": "proj",
        "location": "US",
    }


def test_jobs_show_json_returns_metadata_for_partial_job_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    jobs_root = tmp_path / "jobs"
    first, _second = _seed_jobs(jobs_root)
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))

    result = CliRunner().invoke(
        cli.app,
        ["jobs", "show", "abc111", "--format", "json"],
    )

    assert result.exit_code == 0, result.output
    metadata = json.loads(result.output)
    assert metadata["qmb_job_id"] == first.qmb_job_id
    assert metadata["engine"]["job_id"] == "bq-first"
    assert metadata["stats"]["bytes_processed"] == 100


def test_jobs_sql_prints_archived_resolved_sql(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    jobs_root = tmp_path / "jobs"
    _first, second = _seed_jobs(jobs_root)
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))

    result = CliRunner().invoke(cli.app, ["jobs", "sql", "def222"])

    assert result.exit_code == 0, result.output
    assert result.output == "SELECT 'second' AS label\n"


def test_jobs_paths_json_returns_artifact_paths_for_nvim(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    jobs_root = tmp_path / "jobs"
    first, _second = _seed_jobs(jobs_root)
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))

    result = CliRunner().invoke(
        cli.app,
        ["jobs", "paths", "abc111", "--format", "json"],
    )

    assert result.exit_code == 0, result.output
    paths = json.loads(result.output)
    assert paths == {
        "metadata": str(first.metadata_path),
        "query": str(first.query_path),
        "schema": str(first.schema_path),
        "preview": str(first.preview_path),
        "result": None,
    }


def test_jobs_open_launches_tui_for_archived_preview(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    jobs_root = tmp_path / "jobs"
    first, _second = _seed_jobs(jobs_root)
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))
    captured: dict[str, Any] = {}

    def fake_init(self: Any, **kwargs: Any) -> None:
        captured.update(kwargs)

    def fake_run(self: Any) -> None:
        captured["ran"] = True

    monkeypatch.setattr("qmb.tui.app.QueryResultApp.__init__", fake_init)
    monkeypatch.setattr("qmb.tui.app.QueryResultApp.run", fake_run)

    result = CliRunner().invoke(
        cli.app,
        ["jobs", "open", "abc111", "--page-size", "50"],
    )

    assert result.exit_code == 0, result.output
    assert captured["ran"] is True
    assert captured["bq_client"] is None
    assert captured["source_label"] == f"archive: {first.qmb_job_id}"
    assert captured["resolved_sql"] == "SELECT 'first' AS label"
    assert captured["page_size"] == 50
    assert captured["handle"].job_id == first.qmb_job_id
    assert captured["handle"].schema == [
        {"name": "label", "type": "STRING", "mode": "NULLABLE"}
    ]
    assert captured["result_source"].__class__.__name__ == "JsonlPreviewResultSource"
