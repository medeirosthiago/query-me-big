"""Phase 10D: --session-id / --parent-job-id end-to-end."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

import qmb.cli as cli


def _runner() -> CliRunner:
    return CliRunner()


# ---------------------------------------------------------------------------
# `qmb run --session-id X --parent-job-id Y`
# ---------------------------------------------------------------------------


def test_run_session_flags_propagate_into_archive_and_json(
    monkeypatch, tmp_path: Path
) -> None:
    """The CLI flags travel through the request, pipeline, and archive."""
    from tests.test_bigquery_flow import FakeBigQueryClient, _rows, _schema

    fake_client = FakeBigQueryClient(_rows(), _schema())
    monkeypatch.setattr("qmb.bigquery.client.get_client", lambda *a, **kw: fake_client)
    monkeypatch.setenv("QMB_JOBS_DIR", str(tmp_path / "jobs"))

    result = _runner().invoke(
        cli.app,
        [
            "run",
            "SELECT 1",
            "--session-id",
            "agent-42",
            "--parent-job-id",
            "20260101T120000-abc12345",
        ],
    )

    assert result.exit_code == 0, result.output + result.stderr
    payload = json.loads(result.output.strip().splitlines()[-1])
    archive = payload["archive"]
    assert archive["session_id"] == "agent-42"
    assert archive["parent_job_id"] == "20260101T120000-abc12345"
    # And it is also persisted to disk in metadata.json.
    job_dir = next((tmp_path / "jobs").iterdir())
    metadata = json.loads((job_dir / "metadata.json").read_text())
    assert metadata["session_id"] == "agent-42"
    assert metadata["parent_job_id"] == "20260101T120000-abc12345"


def test_run_without_session_flags_persists_null(monkeypatch, tmp_path: Path) -> None:
    """Default: session / parent fields are null in the archive."""
    from tests.test_bigquery_flow import FakeBigQueryClient, _rows, _schema

    fake_client = FakeBigQueryClient(_rows(), _schema())
    monkeypatch.setattr("qmb.bigquery.client.get_client", lambda *a, **kw: fake_client)
    monkeypatch.setenv("QMB_JOBS_DIR", str(tmp_path / "jobs"))

    result = _runner().invoke(cli.app, ["run", "SELECT 1"])

    assert result.exit_code == 0, result.output + result.stderr
    payload = json.loads(result.output.strip().splitlines()[-1])
    assert payload["archive"]["session_id"] is None
    assert payload["archive"]["parent_job_id"] is None


# ---------------------------------------------------------------------------
# `qmb jobs list --session-id X --parent-job-id Y --limit N`
# ---------------------------------------------------------------------------


def _seed_jobs(
    monkeypatch, tmp_path: Path, *, session_ids: list[str | None]
) -> list[str]:
    """Create N archived jobs with the requested session ids; return ids."""
    from qmb.jobs.models import EngineMetadata, SourceMetadata
    from qmb.jobs.store import JobStore
    from qmb.types import SchemaField

    monkeypatch.setenv("QMB_JOBS_DIR", str(tmp_path / "jobs"))
    store = JobStore()
    ids: list[str] = []
    for i, sid in enumerate(session_ids):
        record = store.create(
            resolved_sql=f"SELECT {i}",
            schema=[SchemaField("x", "INTEGER")],
            preview_rows=[{"x": i}],
            source=SourceMetadata(label=f"job-{i}", input_mode="sql"),
            engine=EngineMetadata(name="bigquery"),
            total_rows=1,
            session_id=sid,
        )
        ids.append(record.qmb_job_id)
    return ids


def test_jobs_list_session_id_filter(monkeypatch, tmp_path: Path) -> None:
    _seed_jobs(monkeypatch, tmp_path, session_ids=["a", "b", "a", None])

    result = _runner().invoke(
        cli.app, ["jobs", "list", "--format", "json", "--session-id", "a"]
    )

    assert result.exit_code == 0, result.output + result.stderr
    payload = json.loads(result.output.strip())
    assert len(payload) == 2
    assert all(item["session_id"] == "a" for item in payload)


def test_jobs_list_parent_job_id_filter(monkeypatch, tmp_path: Path) -> None:
    from qmb.jobs.models import EngineMetadata, SourceMetadata
    from qmb.jobs.store import JobStore
    from qmb.types import SchemaField

    monkeypatch.setenv("QMB_JOBS_DIR", str(tmp_path / "jobs"))
    store = JobStore()
    parent_id = "20260101T120000-fffffff0"
    store.create(
        resolved_sql="A",
        schema=[SchemaField("x", "INTEGER")],
        preview_rows=[{"x": 1}],
        source=SourceMetadata(label="a", input_mode="sql"),
        engine=EngineMetadata(name="bigquery"),
        total_rows=1,
        parent_job_id=parent_id,
    )
    store.create(
        resolved_sql="B",
        schema=[SchemaField("x", "INTEGER")],
        preview_rows=[{"x": 2}],
        source=SourceMetadata(label="b", input_mode="sql"),
        engine=EngineMetadata(name="bigquery"),
        total_rows=1,
    )

    result = _runner().invoke(
        cli.app,
        ["jobs", "list", "--format", "json", "--parent-job-id", parent_id],
    )

    assert result.exit_code == 0, result.output + result.stderr
    payload = json.loads(result.output.strip())
    assert len(payload) == 1
    assert payload[0]["parent_job_id"] == parent_id


def test_jobs_list_limit_caps_output(monkeypatch, tmp_path: Path) -> None:
    _seed_jobs(monkeypatch, tmp_path, session_ids=["a"] * 5)

    result = _runner().invoke(
        cli.app, ["jobs", "list", "--format", "json", "--limit", "2"]
    )

    assert result.exit_code == 0, result.output + result.stderr
    payload = json.loads(result.output.strip())
    assert len(payload) == 2


def test_jobs_list_no_matches_returns_empty_json(monkeypatch, tmp_path: Path) -> None:
    _seed_jobs(monkeypatch, tmp_path, session_ids=["a", "b"])

    result = _runner().invoke(
        cli.app, ["jobs", "list", "--format", "json", "--session-id", "nothing"]
    )

    assert result.exit_code == 0, result.output + result.stderr
    assert json.loads(result.output.strip()) == []
