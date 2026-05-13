"""Tests for the Phase 9 local qmb job archive store.

The desired store is intentionally engine-independent. BigQuery job IDs are
metadata on a qmb-owned job record, not the primary identity.
"""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from qmb.types import AgentContext, SchemaField

NOW = datetime(2026, 5, 12, 14, 33, 2, tzinfo=UTC)


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


def _store(tmp_path: Path, *, now: Callable[[], datetime] | None = None) -> Any:
    _, store_module = _jobs_modules()
    return store_module.JobStore(
        root=tmp_path / "jobs",
        now=now or (lambda: NOW),
        nonce=lambda: "a1b2c3",
    )


def _create_record(store: Any, *, sql: str = "SELECT 1 AS id") -> Any:
    models, _ = _jobs_modules()
    return store.create(
        resolved_sql=sql,
        schema=[SchemaField("id", "INTEGER")],
        preview_rows=[{"id": 1}],
        source=models.SourceMetadata(label="ad-hoc", input_mode="sql"),
        engine=models.EngineMetadata(
            name="bigquery",
            job_id="bq-job-123",
            project="proj",
            location="US",
        ),
        total_rows=1,
        bytes_processed=2048,
        execution_seconds=2.5,
    )


def test_create_job_writes_metadata_sql_schema_and_preview(tmp_path: Path) -> None:
    store = _store(tmp_path)

    record = _create_record(store)

    assert record.qmb_job_id == "qmb_2026-05-12_14-33-02_a1b2c3"
    assert record.directory == tmp_path / "jobs" / record.qmb_job_id
    assert record.metadata_path == record.directory / "metadata.json"
    assert record.query_path == record.directory / "query.sql"
    assert record.schema_path == record.directory / "schema.json"
    assert record.preview_path == record.directory / "preview.jsonl"
    assert record.result_path is None

    assert record.query_path.read_text(encoding="utf-8") == "SELECT 1 AS id"
    assert json.loads(record.schema_path.read_text(encoding="utf-8")) == [
        {"name": "id", "type": "INTEGER", "mode": "NULLABLE"}
    ]
    assert record.preview_path.read_text(encoding="utf-8") == '{"id": 1}\n'

    metadata = json.loads(record.metadata_path.read_text(encoding="utf-8"))
    assert metadata == {
        "version": 2,
        "qmb_job_id": "qmb_2026-05-12_14-33-02_a1b2c3",
        "created_at": "2026-05-12T14:33:02+00:00",
        "session_id": None,
        "parent_job_id": None,
        "agent": None,
        "source": {
            "label": "ad-hoc",
            "input_mode": "sql",
            "file_path": None,
            "model_name": None,
            "manifest_path": None,
            "resolver": None,
            "matched_node_id": None,
        },
        "engine": {
            "name": "bigquery",
            "job_id": "bq-job-123",
            "project": "proj",
            "location": "US",
        },
        "stats": {
            "total_rows": 1,
            "bytes_processed": 2048,
            "execution_seconds": 2.5,
        },
        "artifacts": {
            "metadata": "metadata.json",
            "query": "query.sql",
            "schema": "schema.json",
            "preview": "preview.jsonl",
            "result": None,
        },
    }


def test_read_job_record_round_trips_from_disk(tmp_path: Path) -> None:
    store = _store(tmp_path)
    created = _create_record(store)

    loaded = store.read(created.qmb_job_id)

    assert loaded.qmb_job_id == created.qmb_job_id
    assert loaded.created_at == NOW
    assert loaded.source.label == "ad-hoc"
    assert loaded.source.input_mode == "sql"
    assert loaded.engine.name == "bigquery"
    assert loaded.engine.job_id == "bq-job-123"
    assert loaded.total_rows == 1
    assert loaded.bytes_processed == 2048
    assert loaded.execution_seconds == 2.5
    assert loaded.agent_context is None
    assert loaded.query_path.read_text(encoding="utf-8") == "SELECT 1 AS id"


def test_agent_context_round_trips_from_disk(tmp_path: Path) -> None:
    store = _store(tmp_path)
    models, _ = _jobs_modules()
    agent_context = AgentContext(
        name="pi",
        session_id="pi-session",
        conversation_id="conversation-1",
        run_id="run-1",
        turn_id="turn-2",
        task="debug orders",
        cwd="/repo",
        repo_root="/repo",
        git_branch="main",
        git_sha="abc123",
        git_dirty=True,
        user="mds",
        host="host",
        tags=["orders"],
        metadata={"priority": 1},
    )

    created = store.create(
        resolved_sql="SELECT 1 AS id",
        schema=[SchemaField("id", "INTEGER")],
        preview_rows=[{"id": 1}],
        source=models.SourceMetadata(label="ad-hoc", input_mode="sql"),
        engine=models.EngineMetadata(name="bigquery"),
        total_rows=1,
        session_id="pi-session",
        agent_context=agent_context,
    )

    metadata = json.loads(created.metadata_path.read_text(encoding="utf-8"))
    assert metadata["agent"] == agent_context.to_mapping()

    loaded = store.read(created.qmb_job_id)
    assert loaded.agent_context == agent_context


def test_list_jobs_sorts_newest_first(tmp_path: Path) -> None:
    _, store_module = _jobs_modules()
    times = _iter_callable(
        [
            datetime(2026, 5, 12, 10, 0, tzinfo=UTC),
            datetime(2026, 5, 12, 11, 0, tzinfo=UTC),
        ]
    )
    nonces = _iter_callable(["old111", "new222"])
    store = store_module.JobStore(root=tmp_path / "jobs", now=times, nonce=nonces)

    old = _create_record(store, sql="SELECT 'old'")
    new = _create_record(store, sql="SELECT 'new'")

    assert [record.qmb_job_id for record in store.list()] == [
        new.qmb_job_id,
        old.qmb_job_id,
    ]


def test_resolve_id_accepts_full_or_unambiguous_partial_ids(tmp_path: Path) -> None:
    _, store_module = _jobs_modules()
    times = _iter_callable(
        [
            datetime(2026, 5, 12, 10, 0, tzinfo=UTC),
            datetime(2026, 5, 12, 11, 0, tzinfo=UTC),
        ]
    )
    nonces = _iter_callable(["abc111", "def222"])
    store = store_module.JobStore(root=tmp_path / "jobs", now=times, nonce=nonces)
    first = _create_record(store, sql="SELECT 'first'")
    second = _create_record(store, sql="SELECT 'second'")

    assert store.resolve_id(first.qmb_job_id) == first.qmb_job_id
    assert store.resolve_id("abc111") == first.qmb_job_id
    assert store.resolve_id("def222") == second.qmb_job_id

    with pytest.raises(store_module.AmbiguousJobIdError):
        store.resolve_id("2026-05-12")
    with pytest.raises(store_module.JobNotFoundError):
        store.resolve_id("does-not-exist")


def test_missing_and_corrupt_jobs_raise_predictable_errors(tmp_path: Path) -> None:
    _, store_module = _jobs_modules()
    store = store_module.JobStore(root=tmp_path / "jobs")

    with pytest.raises(store_module.JobNotFoundError):
        store.read("qmb_missing")

    corrupt_dir = tmp_path / "jobs" / "qmb_corrupt"
    corrupt_dir.mkdir(parents=True)
    (corrupt_dir / "metadata.json").write_text("{not json", encoding="utf-8")

    with pytest.raises(store_module.CorruptJobError):
        store.read("qmb_corrupt")
