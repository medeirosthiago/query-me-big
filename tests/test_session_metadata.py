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
    job_dir = next(
        child
        for child in (tmp_path / "jobs").iterdir()
        if child.is_dir() and child.name != "sessions"
    )
    metadata = json.loads((job_dir / "metadata.json").read_text())
    assert metadata["session_id"] == "agent-42"
    assert metadata["parent_job_id"] == "20260101T120000-abc12345"


def test_run_publish_exports_archived_job_and_reports_remote_status(
    monkeypatch, tmp_path: Path
) -> None:
    from tests.test_bigquery_flow import FakeBigQueryClient, _rows, _schema

    fake_client = FakeBigQueryClient(_rows(), _schema())
    monkeypatch.setattr("qmb.bigquery.client.get_client", lambda *a, **kw: fake_client)
    monkeypatch.setenv("QMB_JOBS_DIR", str(tmp_path / "jobs"))
    exported: list[str] = []

    class FakeRemoteResult:
        def __init__(self, qmb_job_id: str) -> None:
            self.qmb_job_id = qmb_job_id
            self.status = "exported"

        def to_mapping(self):
            return {
                "qmb_job_id": self.qmb_job_id,
                "status": self.status,
                "uri": f"gs://bucket/qmb/sessions/agent-42/{self.qmb_job_id}/",
                "error": None,
            }

    class FakeRemote:
        def export_job(self, record, *, preview_rows=None):
            exported.append(record.qmb_job_id)
            return FakeRemoteResult(record.qmb_job_id)

    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: FakeRemote())

    result = _runner().invoke(
        cli.app,
        [
            "run",
            "SELECT 1",
            "--session-id",
            "agent-42",
            "--publish",
            "--destination",
            "gs://bucket/qmb/",
        ],
    )

    assert result.exit_code == 0, result.output + result.stderr
    payload = json.loads(result.output.strip().splitlines()[-1])
    assert payload["remote_archive"]["status"] == "exported"
    assert payload["remote_archive"]["destination"] == "gs://bucket/qmb/"
    assert exported == [payload["archive"]["qmb_job_id"]]


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


# ---------------------------------------------------------------------------
# `qmb jobs sessions`
# ---------------------------------------------------------------------------


def test_jobs_sessions_json_groups_sessions_newest_first(
    monkeypatch, tmp_path: Path
) -> None:
    from collections.abc import Iterator
    from datetime import UTC, datetime

    from qmb.jobs.models import EngineMetadata, SourceMetadata
    from qmb.jobs.store import JobStore
    from qmb.types import AgentContext, SchemaField

    def next_value(values: list):
        iterator: Iterator = iter(values)
        return lambda: next(iterator)

    monkeypatch.setenv("QMB_JOBS_DIR", str(tmp_path / "jobs"))
    store = JobStore(
        now=next_value(
            [
                datetime(2026, 5, 13, 10, 0, tzinfo=UTC),
                datetime(2026, 5, 13, 11, 0, tzinfo=UTC),
                datetime(2026, 5, 13, 12, 0, tzinfo=UTC),
                datetime(2026, 5, 13, 13, 0, tzinfo=UTC),
            ]
        ),
        nonce=next_value(["aaaaaa", "bbbbbb", "cccccc", "dddddd"]),
    )
    common = {
        "schema": [SchemaField("x", "INTEGER")],
        "preview_rows": [{"x": 1}],
        "source": SourceMetadata(label="ad-hoc", input_mode="sql"),
        "engine": EngineMetadata(name="bigquery"),
        "total_rows": 1,
    }
    store.create(
        resolved_sql="SELECT 1",
        session_id="alpha",
        agent_context=AgentContext(name="pi", session_id="alpha", task="debug alpha"),
        **common,
    )
    store.create(resolved_sql="SELECT 2", session_id="beta", **common)
    store.create(
        resolved_sql="SELECT 3",
        session_id="alpha",
        agent_context=AgentContext(name="codex", session_id="alpha"),
        **common,
    )
    store.create(resolved_sql="SELECT 4", **common)

    result = _runner().invoke(cli.app, ["jobs", "sessions", "--format", "json"])

    assert result.exit_code == 0, result.output + result.stderr
    payload = json.loads(result.output)
    assert payload == [
        {
            "session_id": "alpha",
            "count": 2,
            "bytes_processed": 0,
            "first": "2026-05-13T10:00:00+00:00",
            "latest": "2026-05-13T12:00:00+00:00",
            "agents": ["codex", "pi"],
            "tasks": ["debug alpha"],
            "cwds": [],
        },
        {
            "session_id": "beta",
            "count": 1,
            "bytes_processed": 0,
            "first": "2026-05-13T11:00:00+00:00",
            "latest": "2026-05-13T11:00:00+00:00",
            "agents": [],
            "tasks": [],
            "cwds": [],
        },
    ]


def test_jobs_sessions_text_and_limit(monkeypatch, tmp_path: Path) -> None:
    _seed_jobs(monkeypatch, tmp_path, session_ids=["a", "b", "a"])

    result = _runner().invoke(cli.app, ["jobs", "sessions", "--limit", "1"])

    assert result.exit_code == 0, result.output + result.stderr
    assert len(result.output.strip().splitlines()) == 1
    assert "jobs" in result.output


# ---------------------------------------------------------------------------
# Agent metadata
# ---------------------------------------------------------------------------


def test_run_env_session_and_agent_metadata_persist(
    monkeypatch, tmp_path: Path
) -> None:
    """Agent/session context is collected from env + flags and archived."""
    from tests.test_bigquery_flow import FakeBigQueryClient, _rows, _schema

    fake_client = FakeBigQueryClient(_rows(), _schema())
    monkeypatch.setattr("qmb.bigquery.client.get_client", lambda *a, **kw: fake_client)
    monkeypatch.setenv("QMB_JOBS_DIR", str(tmp_path / "jobs"))
    monkeypatch.setenv("QMB_SESSION_ID", "pi-session-env")
    monkeypatch.setenv("QMB_AGENT_NAME", "pi")
    monkeypatch.setenv("QMB_AGENT_CONVERSATION_ID", "conversation-1")
    monkeypatch.setenv("QMB_AGENT_TASK", "debug orders discrepancy")
    monkeypatch.setenv("QMB_AGENT_TAGS", "env-tag,orders")
    monkeypatch.setenv("QMB_AGENT_META_JSON", '{"env_key": "env-value", "priority": 1}')
    monkeypatch.chdir(tmp_path)

    result = _runner().invoke(
        cli.app,
        [
            "run",
            "SELECT 1",
            "--agent-run-id",
            "run-1",
            "--agent-turn-id",
            "turn-2",
            "--tag",
            "orders",
            "--tag",
            "investigation",
            "--meta",
            "priority=2",
            "--meta",
            "manual=true",
        ],
    )

    assert result.exit_code == 0, result.output + result.stderr
    payload = json.loads(result.output.strip().splitlines()[-1])
    archive = payload["archive"]
    assert archive["session_id"] == "pi-session-env"
    assert archive["agent"]["name"] == "pi"
    assert archive["agent"]["session_id"] == "pi-session-env"
    assert archive["agent"]["conversation_id"] == "conversation-1"
    assert archive["agent"]["run_id"] == "run-1"
    assert archive["agent"]["turn_id"] == "turn-2"
    assert archive["agent"]["task"] == "debug orders discrepancy"
    assert archive["agent"]["cwd"] == str(tmp_path.resolve())
    assert archive["agent"]["repo_root"] is None
    assert archive["agent"]["git_sha"] is None
    assert archive["agent"]["git_dirty"] is None
    assert archive["agent"]["tags"] == ["env-tag", "orders", "investigation"]
    assert archive["agent"]["metadata"] == {
        "env_key": "env-value",
        "priority": 2,
        "manual": True,
    }

    job_dir = next(
        child
        for child in (tmp_path / "jobs").iterdir()
        if child.is_dir() and child.name != "sessions"
    )
    metadata = json.loads((job_dir / "metadata.json").read_text())
    assert metadata["session_id"] == "pi-session-env"
    assert metadata["agent"] == archive["agent"]
