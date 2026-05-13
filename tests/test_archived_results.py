"""Tests for reading archived preview results without BigQuery."""

from __future__ import annotations

import importlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from qmb.types import SchemaField

NOW = datetime(2026, 5, 12, 14, 33, 2, tzinfo=UTC)


def _jobs_modules() -> tuple[Any, Any, Any]:
    try:
        models = importlib.import_module("qmb.jobs.models")
        store = importlib.import_module("qmb.jobs.store")
        result_source = importlib.import_module("qmb.jobs.result_source")
    except ModuleNotFoundError:  # pragma: no cover - expected until implemented
        pytest.fail(
            "Expected qmb.jobs models/store/result_source modules for Phase 9 archives"
        )
    return models, store, result_source


def _seed_job(tmp_path: Path) -> Any:
    models, store_module, _ = _jobs_modules()
    store = store_module.JobStore(
        root=tmp_path / "jobs",
        now=lambda: NOW,
        nonce=lambda: "a1b2c3",
    )
    return store.create(
        resolved_sql="SELECT id, payload FROM example",
        schema=[SchemaField("id", "INTEGER"), SchemaField("payload", "JSON")],
        preview_rows=[
            {"id": 1, "payload": {"items": [1, 2]}},
            {"id": 2, "payload": {"items": [3]}},
            {"id": 3, "payload": {"items": []}},
        ],
        source=models.SourceMetadata(label="ad-hoc", input_mode="sql"),
        engine=models.EngineMetadata(
            name="bigquery",
            job_id="bq-job-123",
            project="proj",
            location="US",
        ),
        total_rows=3,
        bytes_processed=1024,
        execution_seconds=0.25,
    )


def test_jsonl_preview_result_source_pages_archived_rows(tmp_path: Path) -> None:
    _models, _store, result_source = _jobs_modules()
    record = _seed_job(tmp_path)

    source = result_source.JsonlPreviewResultSource.from_job(record)

    assert source.schema == [SchemaField("id", "INTEGER"), SchemaField("payload", "JSON")]
    assert source.total_rows == 3

    first_page = source.page(page=0, page_size=2)
    assert first_page.rows == [
        {"id": 1, "payload": {"items": [1, 2]}},
        {"id": 2, "payload": {"items": [3]}},
    ]
    assert first_page.display_rows == [
        {"id": "1", "payload": '{"items": [1, 2]}'},
        {"id": "2", "payload": '{"items": [3]}'},
    ]
    assert first_page.page == 0
    assert first_page.total_pages == 2
    assert first_page.total_rows == 3

    second_page = source.page(page=1, page_size=2)
    assert second_page.rows == [{"id": 3, "payload": {"items": []}}]
    assert second_page.page == 1
