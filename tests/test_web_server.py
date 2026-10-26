"""Tests for the stdlib-only `qmb web` backend (JSON API + static serving)."""

from __future__ import annotations

import contextlib
import http.client
import json
import threading
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

import qmb.cli as cli
import qmb.web.server as server_module
from qmb.config import RemoteArchiveConfig
from qmb.jobs.models import EngineMetadata, SourceMetadata
from qmb.jobs.remote import RemoteArchiveError
from qmb.jobs.session_manifest import SessionManifest
from qmb.jobs.store import JobStore
from qmb.types import AgentContext, SchemaField
from qmb.web.server import create_server

# -- Helpers -----------------------------------------------------------------


def _seed_job(
    store: JobStore,
    *,
    sql: str = "SELECT 1 AS id",
    session_id: str | None = None,
    total_rows: int = 2,
    preview_rows: list[dict[str, Any]] | None = None,
    agent_context: AgentContext | None = None,
) -> Any:
    rows = preview_rows if preview_rows is not None else [{"id": i} for i in range(total_rows)]
    return store.create(
        resolved_sql=sql,
        schema=[SchemaField("id", "INTEGER")],
        preview_rows=rows,
        source=SourceMetadata(label="ad-hoc", input_mode="sql"),
        engine=EngineMetadata(name="bigquery", job_id="bq-job", project="proj", location="US"),
        total_rows=total_rows,
        bytes_processed=1024,
        session_id=session_id,
        agent_context=agent_context,
    )


@contextlib.contextmanager
def running_server(
    job_store: JobStore,
    *,
    remote_destination: str | None = None,
    remote_archives: list[RemoteArchiveConfig] | None = None,
) -> Iterator[str]:
    srv = create_server(
        "127.0.0.1",
        0,
        job_store=job_store,
        remote_destination=remote_destination,
        remote_archives=remote_archives,
    )
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = srv.server_address[:2]
        yield f"{host}:{port}"
    finally:
        srv.shutdown()
        thread.join(timeout=5)
        srv.server_close()


def _get(netloc: str, path: str) -> tuple[int, bytes]:
    conn = http.client.HTTPConnection(netloc, timeout=5)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


def _get_json(netloc: str, path: str) -> tuple[int, Any]:
    status, body = _get(netloc, path)
    return status, json.loads(body)


class _FakeRemoteArchive:
    """Stand-in for GcsRemoteArchive; never touches the network."""

    def __init__(self) -> None:
        self.index_jobs: list[dict[str, Any]] = []
        self.index_sessions: list[SessionManifest] = []
        self.full_jobs: dict[str, dict[str, Any]] = {}
        self.previews: dict[str, str] = {}
        self.session_manifests: dict[str, SessionManifest] = {}
        self.session_blob_names: list[tuple[str, str | None]] = []
        self.fetch_preview_calls = 0
        self.list_jobs_calls = 0
        self.fetch_session_manifest_calls = 0
        self.list_session_blob_names_calls = 0
        self.list_error: Exception | None = None
        self.list_session_blob_names_error: Exception | None = None

    def list_jobs(self) -> list[dict[str, Any]]:
        self.list_jobs_calls += 1
        if self.list_error:
            raise self.list_error
        return self.index_jobs

    def list_sessions(self) -> list[SessionManifest]:
        if self.list_error:
            raise self.list_error
        return self.index_sessions

    def list_session_blob_names(self) -> list[tuple[str, str | None]]:
        self.list_session_blob_names_calls += 1
        if self.list_session_blob_names_error:
            raise self.list_session_blob_names_error
        return self.session_blob_names

    def fetch_job_artifacts(self, job_id: str) -> dict[str, Any]:
        try:
            return self.full_jobs[job_id]
        except KeyError:
            raise RemoteArchiveError(f"Remote qmb job not found: {job_id}") from None

    def fetch_preview_jsonl(self, job_id: str) -> str:
        self.fetch_preview_calls += 1
        try:
            return self.previews[job_id]
        except KeyError:
            raise RemoteArchiveError(f"Remote qmb job not found: {job_id}") from None

    def fetch_session_manifest(self, session_id: str) -> SessionManifest | None:
        self.fetch_session_manifest_calls += 1
        return self.session_manifests.get(session_id)


class _AlwaysRaisingRemoteArchive:
    """Stand-in that fails any call — proves a scope never touches it."""

    def list_jobs(self) -> list[dict[str, Any]]:
        raise AssertionError("scope=local must never call the remote archive")

    def list_sessions(self) -> list[SessionManifest]:
        raise AssertionError("scope=local must never call the remote archive")

    def list_session_blob_names(self) -> list[tuple[str, str | None]]:
        raise AssertionError("scope=local must never call the remote archive")

    def fetch_session_manifest(self, session_id: str) -> SessionManifest | None:
        raise AssertionError("scope=local must never call the remote archive")


# -- /api/index ----------------------------------------------------------


def test_api_index_returns_local_jobs_and_sessions(tmp_path: Path) -> None:
    store = JobStore(root=tmp_path / "jobs")
    record = _seed_job(store, sql="SELECT 1 AS id", session_id="session-a")

    with running_server(store) as netloc:
        status, payload = _get_json(netloc, "/api/index")

    assert status == 200
    assert "generated_at" in payload
    assert len(payload["jobs"]) == 1
    job = payload["jobs"][0]
    assert job["qmb_job_id"] == record.qmb_job_id
    assert job["origin"] == "local"
    assert job["query_excerpt"] == "SELECT 1 AS id"
    assert len(payload["sessions"]) == 1
    session = payload["sessions"][0]
    assert session["session_id"] == "session-a"
    assert session["origin"] == "local"
    assert "remote_error" not in payload


def test_api_index_without_remote_configured_omits_remote_fields(tmp_path: Path) -> None:
    store = JobStore(root=tmp_path / "jobs")
    _seed_job(store)

    with running_server(store, remote_destination=None) as netloc:
        status, payload = _get_json(netloc, "/api/index")

    assert status == 200
    assert all(job["origin"] == "local" for job in payload["jobs"])
    assert "remote_error" not in payload


def test_api_index_caches_and_refresh_param_rebuilds(tmp_path: Path) -> None:
    store = JobStore(root=tmp_path / "jobs")
    _seed_job(store)

    with running_server(store) as netloc:
        _, first = _get_json(netloc, "/api/index")
        assert len(first["jobs"]) == 1

        _seed_job(store)  # bypasses the server's cache

        _, cached = _get_json(netloc, "/api/index")
        assert len(cached["jobs"]) == 1, "index should be served from cache without refresh"

        _, refreshed = _get_json(netloc, "/api/index?refresh=1")
        assert len(refreshed["jobs"]) == 2


def test_api_index_merges_remote_dedups_and_tags_origin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = JobStore(root=tmp_path / "jobs")
    local_record = _seed_job(store, sql="SELECT 1 AS id")

    fake = _FakeRemoteArchive()
    fake.index_jobs = [
        {
            "qmb_job_id": local_record.qmb_job_id,
            "session_id": None,
            "created_at": local_record.created_at.isoformat(),
            "engine": "bigquery",
            "source_label": "ad-hoc",
            "total_rows": 2,
            "bytes_processed": 1024,
            "query_excerpt": "SELECT 1 AS id",
        },
        {
            "qmb_job_id": "qmb_remote_only",
            "session_id": None,
            "created_at": local_record.created_at.isoformat(),
            "engine": "bigquery",
            "source_label": "remote-ad-hoc",
            "total_rows": 5,
            "bytes_processed": 500,
            "query_excerpt": "SELECT 2",
        },
    ]
    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: fake)

    with running_server(store, remote_destination="gs://bucket/qmb") as netloc:
        status, payload = _get_json(netloc, "/api/index")

    assert status == 200
    by_id = {job["qmb_job_id"]: job for job in payload["jobs"]}
    assert len(by_id) == 2
    assert by_id[local_record.qmb_job_id]["origin"] == "both"
    # Local fields win for the shared job (e.g. the full metadata shape).
    assert "artifacts" in by_id[local_record.qmb_job_id]
    assert by_id["qmb_remote_only"]["origin"] == "remote"


def test_api_index_remote_failure_keeps_local_data_and_sets_remote_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = JobStore(root=tmp_path / "jobs")
    _seed_job(store)

    fake = _FakeRemoteArchive()
    fake.list_error = RuntimeError("no network")
    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: fake)

    with running_server(store, remote_destination="gs://bucket/qmb") as netloc:
        status, payload = _get_json(netloc, "/api/index")

    assert status == 200
    assert len(payload["jobs"]) == 1
    assert payload["jobs"][0]["origin"] == "local"
    assert "no network" in payload["remote_error"]


# -- /api/index?scope=... -----------------------------------------------


def _remote_index_entry(
    qmb_job_id: str,
    *,
    session_id: str | None = None,
    created_at: str = "2024-01-01T00:00:00+00:00",
    bytes_processed: int = 100,
) -> dict[str, Any]:
    return {
        "qmb_job_id": qmb_job_id,
        "session_id": session_id,
        "created_at": created_at,
        "engine": "bigquery",
        "source_label": "ad-hoc",
        "total_rows": 3,
        "bytes_processed": bytes_processed,
        "query_excerpt": "SELECT 1",
    }


def test_api_index_scope_local_never_touches_remote_archive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = JobStore(root=tmp_path / "jobs")
    record = _seed_job(store)
    monkeypatch.setattr(
        "qmb.jobs.remote.get_remote_archive", lambda destination: _AlwaysRaisingRemoteArchive()
    )

    with running_server(store, remote_destination="gs://bucket/qmb") as netloc:
        status, payload = _get_json(netloc, "/api/index?scope=local")

    assert status == 200
    assert "remote_error" not in payload
    assert len(payload["jobs"]) == 1
    assert payload["jobs"][0]["qmb_job_id"] == record.qmb_job_id
    assert payload["jobs"][0]["origin"] == "local"


def test_api_index_scope_remote_returns_only_remote_jobs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = JobStore(root=tmp_path / "jobs")
    _seed_job(store)  # present locally only; scope=remote must not include it

    fake = _FakeRemoteArchive()
    fake.index_jobs = [_remote_index_entry("qmb_remote_only")]
    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: fake)

    with running_server(store, remote_destination="gs://bucket/qmb") as netloc:
        status, payload = _get_json(netloc, "/api/index?scope=remote")

    assert status == 200
    assert [job["qmb_job_id"] for job in payload["jobs"]] == ["qmb_remote_only"]
    assert payload["jobs"][0]["origin"] == "remote"
    assert fake.list_jobs_calls == 1


def test_api_index_scope_remote_derives_sessions_from_index_entries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = JobStore(root=tmp_path / "jobs")

    fake = _FakeRemoteArchive()
    fake.index_jobs = [
        _remote_index_entry(
            "qmb_a", session_id="session-x", created_at="2024-01-01T00:00:00+00:00",
            bytes_processed=100,
        ),
        _remote_index_entry(
            "qmb_b", session_id="session-x", created_at="2024-01-02T00:00:00+00:00",
            bytes_processed=200,
        ),
        _remote_index_entry("qmb_c", session_id=None),  # session-less jobs are excluded
    ]
    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: fake)

    with running_server(store, remote_destination="gs://bucket/qmb") as netloc:
        status, payload = _get_json(netloc, "/api/index?scope=remote")

    assert status == 200
    assert len(payload["sessions"]) == 1
    session = payload["sessions"][0]
    assert session["session_id"] == "session-x"
    assert session["jobs"] == ["qmb_b", "qmb_a"]
    assert session["count"] == 2
    assert session["first"] == "2024-01-01T00:00:00+00:00"
    assert session["latest"] == "2024-01-02T00:00:00+00:00"
    assert session["bytes_processed"] == 300
    assert session["agents"] == []
    assert session["tasks"] == []
    assert session["cwds"] == []
    assert session["derived"] is True
    assert session["origin"] == "remote"
    # This is the whole point: one index.json download, no manifest scan.
    assert fake.list_jobs_calls == 1


def test_api_index_scope_remote_adds_unindexed_stub_for_manifest_missing_from_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A manifest visible via the sessions/ listing but absent from
    index.json (a stale index — e.g. an older qmb export job that never ran
    `qmb jobs reindex --remote`) must still surface as a stub session."""
    store = JobStore(root=tmp_path / "jobs")

    fake = _FakeRemoteArchive()
    fake.index_jobs = [_remote_index_entry("qmb_a", session_id="session-known")]
    fake.session_blob_names = [
        ("session-known", "2024-01-02T00:00:00+00:00"),
        ("example-bot-bq-growth-20260815", "2026-08-15T06:00:00+00:00"),
    ]
    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: fake)

    with running_server(store, remote_destination="gs://bucket/qmb") as netloc:
        status, payload = _get_json(netloc, "/api/index?scope=remote")

    assert status == 200
    assert payload["index_stale"] == 1
    by_id = {s["session_id"]: s for s in payload["sessions"]}
    assert set(by_id) == {"session-known", "example-bot-bq-growth-20260815"}

    known = by_id["session-known"]
    assert "unindexed" not in known or known["unindexed"] is not True

    stub = by_id["example-bot-bq-growth-20260815"]
    assert stub["unindexed"] is True
    assert stub["origin"] == "remote"
    assert stub["jobs"] == []
    assert stub["count"] is None
    assert stub["latest"] == "2026-08-15T06:00:00+00:00"
    assert fake.list_session_blob_names_calls == 1


def test_api_index_scope_remote_omits_index_stale_when_nothing_unindexed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = JobStore(root=tmp_path / "jobs")

    fake = _FakeRemoteArchive()
    fake.index_jobs = [_remote_index_entry("qmb_a", session_id="session-known")]
    fake.session_blob_names = [("session-known", "2024-01-02T00:00:00+00:00")]
    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: fake)

    with running_server(store, remote_destination="gs://bucket/qmb") as netloc:
        status, payload = _get_json(netloc, "/api/index?scope=remote")

    assert status == 200
    assert "index_stale" not in payload


def test_api_index_scope_remote_listing_failure_degrades_gracefully(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failure of the supplementary sessions/ listing must not break the
    (already-succeeded) index.json-derived remote payload."""
    store = JobStore(root=tmp_path / "jobs")

    fake = _FakeRemoteArchive()
    fake.index_jobs = [_remote_index_entry("qmb_a", session_id="session-known")]
    fake.list_session_blob_names_error = RuntimeError("listing unavailable")
    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: fake)

    with running_server(store, remote_destination="gs://bucket/qmb") as netloc:
        status, payload = _get_json(netloc, "/api/index?scope=remote")

    assert status == 200
    assert "index_stale" not in payload
    assert [s["session_id"] for s in payload["sessions"]] == ["session-known"]


def test_api_index_no_scope_combined_includes_index_stale(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = JobStore(root=tmp_path / "jobs")
    _seed_job(store, session_id="session-x")

    fake = _FakeRemoteArchive()
    fake.session_blob_names = [("stale-remote-session", None)]
    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: fake)

    with running_server(store, remote_destination="gs://bucket/qmb") as netloc:
        _, combined = _get_json(netloc, "/api/index")

    assert combined["index_stale"] == 1
    by_session = {s["session_id"]: s for s in combined["sessions"]}
    assert by_session["stale-remote-session"]["unindexed"] is True


def test_api_session_detail_opens_unindexed_stub_via_manifest_fetch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Opening an unindexed stub session must serve full detail from the
    manifest via the existing on-demand session-detail endpoint."""
    store = JobStore(root=tmp_path / "jobs")

    fake = _FakeRemoteArchive()
    fake.session_blob_names = [("example-bot-bq-growth-20260815", "2026-08-15T06:00:00+00:00")]
    fake.session_manifests["example-bot-bq-growth-20260815"] = SessionManifest(
        session_id="example-bot-bq-growth-20260815",
        jobs=("qmb_a", "qmb_b"),
        count=2,
        agents=("example-bot",),
        tasks=("growth backfill",),
        cwds=("/repo",),
    )
    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: fake)

    with running_server(store, remote_destination="gs://bucket/qmb") as netloc:
        status, index_payload = _get_json(netloc, "/api/index?scope=remote")
        assert index_payload["index_stale"] == 1

        status, detail = _get_json(
            netloc, "/api/sessions/example-bot-bq-growth-20260815?scope=remote"
        )

    assert status == 200
    assert detail["session_id"] == "example-bot-bq-growth-20260815"
    assert detail["jobs"] == ["qmb_a", "qmb_b"]
    assert detail["count"] == 2
    assert detail["agents"] == ["example-bot"]
    assert "unindexed" not in detail


def test_api_index_no_scope_matches_scoped_assembly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = JobStore(root=tmp_path / "jobs")
    local_record = _seed_job(store, session_id="session-x")

    fake = _FakeRemoteArchive()
    fake.index_jobs = [_remote_index_entry("qmb_remote_only", session_id="session-y")]
    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: fake)

    with running_server(store, remote_destination="gs://bucket/qmb") as netloc:
        _, combined = _get_json(netloc, "/api/index")

    by_id = {job["qmb_job_id"]: job for job in combined["jobs"]}
    assert by_id[local_record.qmb_job_id]["origin"] == "local"
    assert by_id["qmb_remote_only"]["origin"] == "remote"
    by_session = {s["session_id"]: s for s in combined["sessions"]}
    assert by_session["session-x"]["origin"] == "local"
    assert by_session["session-y"]["origin"] == "remote"
    assert by_session["session-y"]["derived"] is True


def test_api_index_scope_local_and_remote_refresh_independently(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = JobStore(root=tmp_path / "jobs")
    _seed_job(store)

    fake = _FakeRemoteArchive()
    fake.index_jobs = [_remote_index_entry("qmb_remote_1")]
    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: fake)

    with running_server(store, remote_destination="gs://bucket/qmb") as netloc:
        _get_json(netloc, "/api/index?scope=local")
        _get_json(netloc, "/api/index?scope=remote")
        assert fake.list_jobs_calls == 1

        # Refreshing local must not rebuild (or touch) the remote cache.
        _seed_job(store)
        _, local_refreshed = _get_json(netloc, "/api/index?scope=local&refresh=1")
        assert len(local_refreshed["jobs"]) == 2
        assert fake.list_jobs_calls == 1

        # Refreshing remote must not touch local again.
        fake.index_jobs = [_remote_index_entry("qmb_remote_1"), _remote_index_entry("qmb_remote_2")]
        _, remote_refreshed = _get_json(netloc, "/api/index?scope=remote&refresh=1")
        assert len(remote_refreshed["jobs"]) == 2
        assert fake.list_jobs_calls == 2


def test_api_index_invalid_scope_returns_400(tmp_path: Path) -> None:
    store = JobStore(root=tmp_path / "jobs")

    with running_server(store) as netloc:
        status, payload = _get_json(netloc, "/api/index?scope=bogus")

    assert status == 400
    assert "error" in payload


# -- /api/sessions/{id} ----------------------------------------------------


def test_api_session_detail_local_returns_manifest(tmp_path: Path) -> None:
    store = JobStore(root=tmp_path / "jobs")
    _seed_job(store, session_id="session-a")

    with running_server(store) as netloc:
        status, payload = _get_json(netloc, "/api/sessions/session-a")

    assert status == 200
    assert payload["session_id"] == "session-a"
    assert payload["origin"] == "local"


def test_api_session_detail_local_unknown_returns_404(tmp_path: Path) -> None:
    store = JobStore(root=tmp_path / "jobs")

    with running_server(store) as netloc:
        status, payload = _get_json(netloc, "/api/sessions/does-not-exist")

    assert status == 404
    assert "error" in payload


def test_api_session_detail_remote_fetches_manifest_and_caches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = JobStore(root=tmp_path / "jobs")

    fake = _FakeRemoteArchive()
    fake.session_manifests["session-x"] = SessionManifest(
        session_id="session-x",
        jobs=("qmb_a", "qmb_b"),
        count=2,
        agents=("agent-1",),
        tasks=("do the thing",),
        cwds=("/repo",),
    )
    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: fake)

    with running_server(store, remote_destination="gs://bucket/qmb") as netloc:
        status, payload = _get_json(netloc, "/api/sessions/session-x?scope=remote")
        assert status == 200
        assert payload["origin"] == "remote"
        assert payload["agents"] == ["agent-1"]
        assert payload["tasks"] == ["do the thing"]
        assert payload["cwds"] == ["/repo"]
        assert "derived" not in payload or payload.get("derived") is not True

        _get_json(netloc, "/api/sessions/session-x?scope=remote")

    assert fake.fetch_session_manifest_calls == 1, "manifest fetch should be cached per session id"


def test_api_session_detail_remote_falls_back_to_derived_when_manifest_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = JobStore(root=tmp_path / "jobs")

    fake = _FakeRemoteArchive()
    fake.index_jobs = [_remote_index_entry("qmb_a", session_id="session-x")]
    # No manifest registered for "session-x" -> fetch_session_manifest returns None.
    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: fake)

    with running_server(store, remote_destination="gs://bucket/qmb") as netloc:
        status, payload = _get_json(netloc, "/api/sessions/session-x?scope=remote")

    assert status == 200
    assert payload["origin"] == "remote"
    assert payload["derived"] is True
    assert payload["jobs"] == ["qmb_a"]


def test_api_session_detail_remote_unknown_returns_404(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = JobStore(root=tmp_path / "jobs")
    fake = _FakeRemoteArchive()
    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: fake)

    with running_server(store, remote_destination="gs://bucket/qmb") as netloc:
        status, payload = _get_json(netloc, "/api/sessions/does-not-exist?scope=remote")

    assert status == 404
    assert "error" in payload


def test_api_session_detail_remote_without_remote_configured_returns_404(
    tmp_path: Path,
) -> None:
    store = JobStore(root=tmp_path / "jobs")

    with running_server(store, remote_destination=None) as netloc:
        status, payload = _get_json(netloc, "/api/sessions/session-x?scope=remote")

    assert status == 404
    assert "error" in payload


# -- /api/search ------------------------------------------------------------


def test_api_search_stages_recent_sql_then_preview_and_older_jobs(tmp_path: Path) -> None:
    root = tmp_path / "jobs"
    now = datetime.now(UTC)
    recent_nonces = iter(("recent1", "recent2"))
    recent_store = JobStore(
        root=root,
        now=lambda: now - timedelta(days=1),
        nonce=lambda: next(recent_nonces),
    )
    recent_sql = _seed_job(
        recent_store,
        sql=f"SELECT '{'x' * 4100}jane.doe' AS email",
    )
    recent_preview = _seed_job(
        recent_store,
        sql="SELECT email FROM users",
        preview_rows=[{"id": "jane.doe@example.com"}],
    )
    older_store = JobStore(root=root, now=lambda: now - timedelta(days=4), nonce=lambda: "older")
    older_sql = _seed_job(older_store, sql="SELECT * FROM `analytics.jane.doe`")

    with running_server(JobStore(root=root)) as netloc:
        _, sql_payload = _get_json(
            netloc,
            "/api/search?q=jane.doe&period=recent&target=sql",
        )
        _, preview_payload = _get_json(
            netloc,
            "/api/search?q=jane.doe&period=recent&target=preview",
        )
        _, older_payload = _get_json(
            netloc,
            "/api/search?q=jane.doe&period=older&target=sql",
        )

    assert sql_payload["job_ids"] == [recent_sql.qmb_job_id]
    assert preview_payload["job_ids"] == [recent_preview.qmb_job_id]
    assert older_payload["job_ids"] == [older_sql.qmb_job_id]


def test_api_search_is_case_insensitive_and_can_scope_to_session(tmp_path: Path) -> None:
    store = JobStore(root=tmp_path / "jobs")
    matching = _seed_job(store, sql="SELECT * FROM Customers", session_id="wanted")
    _seed_job(store, sql="SELECT * FROM Customers", session_id="other")

    with running_server(store) as netloc:
        status, payload = _get_json(
            netloc,
            "/api/search?q=customers&period=recent&target=sql&session_id=wanted",
        )

    assert status == 200
    assert payload["job_ids"] == [matching.qmb_job_id]


@pytest.mark.parametrize(
    "path",
    [
        "/api/search?period=recent&target=sql",
        "/api/search?q=x&period=all&target=sql",
        "/api/search?q=x&period=recent&target=result",
    ],
)
def test_api_search_rejects_invalid_params(tmp_path: Path, path: str) -> None:
    with running_server(JobStore(root=tmp_path / "jobs")) as netloc:
        status, payload = _get_json(netloc, path)

    assert status == 400
    assert "error" in payload


# -- /api/jobs/{id} --------------------------------------------------------


def test_api_job_detail_returns_metadata_query_and_schema(tmp_path: Path) -> None:
    store = JobStore(root=tmp_path / "jobs")
    record = _seed_job(store, sql="SELECT 42 AS answer")

    with running_server(store) as netloc:
        status, payload = _get_json(netloc, f"/api/jobs/{record.qmb_job_id}")

    assert status == 200
    assert payload["qmb_job_id"] == record.qmb_job_id
    assert payload["query"] == "SELECT 42 AS answer"
    assert payload["schema"] == [{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]


def test_api_job_detail_unknown_id_returns_404_json(tmp_path: Path) -> None:
    store = JobStore(root=tmp_path / "jobs")

    with running_server(store) as netloc:
        status, payload = _get_json(netloc, "/api/jobs/does-not-exist")

    assert status == 404
    assert "error" in payload


def test_api_job_detail_fetches_remote_only_job_without_local_import(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = JobStore(root=tmp_path / "jobs")

    fake = _FakeRemoteArchive()
    fake.full_jobs["qmb_remote_job"] = {
        "metadata": {"qmb_job_id": "qmb_remote_job", "stats": {"total_rows": 3}},
        "query": "SELECT * FROM remote",
        "schema": [{"name": "x", "type": "INTEGER", "mode": "NULLABLE"}],
    }
    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: fake)

    with running_server(store, remote_destination="gs://bucket/qmb") as netloc:
        status, payload = _get_json(netloc, "/api/jobs/qmb_remote_job")

    assert status == 200
    assert payload["query"] == "SELECT * FROM remote"
    assert payload["schema"] == [{"name": "x", "type": "INTEGER", "mode": "NULLABLE"}]
    assert not (store.root / "qmb_remote_job").exists()


# -- /api/jobs/{id}/preview -------------------------------------------------


def test_api_job_preview_pages_local_rows(tmp_path: Path) -> None:
    store = JobStore(root=tmp_path / "jobs")
    record = _seed_job(
        store, total_rows=5, preview_rows=[{"id": i} for i in range(5)]
    )

    with running_server(store) as netloc:
        status, payload = _get_json(
            netloc, f"/api/jobs/{record.qmb_job_id}/preview?page=1&page_size=2"
        )

    assert status == 200
    assert payload["rows"] == [{"id": 0}, {"id": 1}]
    assert payload["total"] == 5
    assert payload["page"] == 1
    assert payload["page_size"] == 2


def test_api_job_preview_out_of_range_page_clamps_to_last(tmp_path: Path) -> None:
    store = JobStore(root=tmp_path / "jobs")
    record = _seed_job(
        store, total_rows=5, preview_rows=[{"id": i} for i in range(5)]
    )

    with running_server(store) as netloc:
        status, payload = _get_json(
            netloc, f"/api/jobs/{record.qmb_job_id}/preview?page=999&page_size=2"
        )

    assert status == 200
    assert payload["page"] == 3  # ceil(5/2) == 3 pages, clamped
    assert payload["rows"] == [{"id": 4}]


def test_api_job_preview_bad_params_return_400(tmp_path: Path) -> None:
    store = JobStore(root=tmp_path / "jobs")
    record = _seed_job(store)

    with running_server(store) as netloc:
        status, payload = _get_json(
            netloc, f"/api/jobs/{record.qmb_job_id}/preview?page=not-a-number"
        )
        assert status == 400
        assert "error" in payload

        status2, payload2 = _get_json(
            netloc, f"/api/jobs/{record.qmb_job_id}/preview?page_size=0"
        )
        assert status2 == 400
        assert "error" in payload2


def test_api_job_preview_remote_only_job_downloads_and_caches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = JobStore(root=tmp_path / "jobs")

    fake = _FakeRemoteArchive()
    fake.full_jobs["qmb_remote_job"] = {
        "metadata": {"qmb_job_id": "qmb_remote_job", "stats": {"total_rows": 3}},
        "query": "SELECT * FROM remote",
        "schema": [],
    }
    fake.previews["qmb_remote_job"] = '{"id": 1}\n{"id": 2}\n{"id": 3}\n'
    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: fake)

    with running_server(store, remote_destination="gs://bucket/qmb") as netloc:
        status, payload = _get_json(
            netloc, "/api/jobs/qmb_remote_job/preview?page=1&page_size=2"
        )
        assert status == 200
        assert payload["rows"] == [{"id": 1}, {"id": 2}]
        assert payload["total"] == 3

        _get_json(netloc, "/api/jobs/qmb_remote_job/preview?page=2&page_size=2")

    assert fake.fetch_preview_calls == 1, "remote preview.jsonl should be cached per job"


# -- Multiple named archives -------------------------------------------------

PRIMARY = RemoteArchiveConfig(name="qmb", uri="gs://bucket/qmb/", primary=True)
STAGING = RemoteArchiveConfig(name="mcp-staging", uri="gs://bucket/mcp/staging/")
PRODUCTION = RemoteArchiveConfig(name="mcp", uri="gs://bucket/mcp/production/")
ALL_ARCHIVES = [PRIMARY, STAGING, PRODUCTION]


def _mcp_entry(
    qmb_job_id: str,
    *,
    session_id: str = "gq-session",
    user: str = "jane.doe@example.com",
    task: str = "How many users signed up yesterday?",
    created_at: str = "2024-01-01T00:00:00+00:00",
) -> dict[str, Any]:
    return {
        **_remote_index_entry(qmb_job_id, session_id=session_id, created_at=created_at),
        "agent_name": "example-mcp",
        "user": user,
        "task": task,
    }


def _mcp_metadata(qmb_job_id: str, *, env: str) -> dict[str, Any]:
    return {
        "qmb_job_id": qmb_job_id,
        "stats": {"total_rows": 1},
        "agent": {
            "name": "example-mcp",
            "user": "jane.doe@example.com",
            "task": f"Signups yesterday ({env})?",
            "tags": ["query_metrics", env],
            "metadata": {"tool": "query_metrics", "env": env, "metric_request": {"m": ["x"]}},
        },
    }


@pytest.fixture
def fake_archives(monkeypatch: pytest.MonkeyPatch) -> dict[str, _FakeRemoteArchive]:
    """One fake per archive URI, wired into ``get_remote_archive``."""
    fakes = {archive.uri: _FakeRemoteArchive() for archive in ALL_ARCHIVES}
    monkeypatch.setattr(
        "qmb.jobs.remote.get_remote_archive", lambda destination: fakes[destination]
    )
    return fakes


def test_api_index_tags_every_job_and_session_with_its_archive(
    tmp_path: Path, fake_archives: dict[str, _FakeRemoteArchive]
) -> None:
    store = JobStore(root=tmp_path / "jobs")
    local = _seed_job(store, session_id="local-session")
    fake_archives[PRIMARY.uri].index_jobs = [_remote_index_entry("qmb_example_bot", session_id="r")]
    fake_archives[STAGING.uri].index_jobs = [_mcp_entry("qmb_staging")]
    fake_archives[PRODUCTION.uri].index_jobs = [_mcp_entry("qmb_prod")]

    with running_server(store, remote_archives=ALL_ARCHIVES) as netloc:
        status, payload = _get_json(netloc, "/api/index")

    assert status == 200
    assert {(job["archive"], job["qmb_job_id"]) for job in payload["jobs"]} == {
        ("local", local.qmb_job_id),
        ("qmb", "qmb_example_bot"),
        ("mcp-staging", "qmb_staging"),
        ("mcp", "qmb_prod"),
    }
    assert {(s["archive"], s["session_id"]) for s in payload["sessions"]} == {
        ("local", "local-session"),
        ("qmb", "r"),
        ("mcp-staging", "gq-session"),
        ("mcp", "gq-session"),
    }
    assert [(a["name"], a["primary"], a["status"]) for a in payload["archives"]] == [
        ("qmb", True, "ok"),
        ("mcp-staging", False, "ok"),
        ("mcp", False, "ok"),
    ]
    assert payload["archives"][1] == {
        "name": "mcp-staging",
        "uri": STAGING.uri,
        "primary": False,
        "status": "ok",
        "job_count": 1,
        "session_count": 1,
    }
    assert "remote_error" not in payload


def test_api_index_one_failing_archive_keeps_the_others(
    tmp_path: Path, fake_archives: dict[str, _FakeRemoteArchive]
) -> None:
    store = JobStore(root=tmp_path / "jobs")
    _seed_job(store)
    fake_archives[PRIMARY.uri].index_jobs = [_remote_index_entry("qmb_example_bot")]
    fake_archives[STAGING.uri].list_error = RuntimeError("403 Forbidden")
    fake_archives[PRODUCTION.uri].index_jobs = [_mcp_entry("qmb_prod")]

    with running_server(store, remote_archives=ALL_ARCHIVES) as netloc:
        _, combined = _get_json(netloc, "/api/index")
        _, staging = _get_json(netloc, "/api/index?archive=mcp-staging")
        _, statuses = _get_json(netloc, "/api/archives")

    assert {job["archive"] for job in combined["jobs"]} == {"local", "qmb", "mcp"}
    failed = combined["archives"][1]
    assert failed["name"] == "mcp-staging"
    assert failed["status"] == "error"
    assert "403 Forbidden" in failed["error"]
    assert failed["job_count"] == 0
    assert combined["archives"][2]["status"] == "ok"
    assert "mcp-staging: RuntimeError: 403 Forbidden" in combined["remote_error"]
    assert staging["jobs"] == []
    assert staging["archives"][0]["status"] == "error"
    assert [a["status"] for a in statuses["archives"]] == ["ok", "error", "ok"]


def test_api_index_per_archive_halves_cache_and_refresh_independently(
    tmp_path: Path, fake_archives: dict[str, _FakeRemoteArchive]
) -> None:
    store = JobStore(root=tmp_path / "jobs")
    fake_archives[STAGING.uri].index_jobs = [_mcp_entry("qmb_staging")]
    fake_archives[PRODUCTION.uri].index_jobs = [_mcp_entry("qmb_prod")]

    with running_server(store, remote_archives=ALL_ARCHIVES) as netloc:
        _, statuses = _get_json(netloc, "/api/archives")
        assert [a["status"] for a in statuses["archives"]] == ["pending"] * 3

        _, staging = _get_json(netloc, "/api/index?archive=mcp-staging")
        assert [job["qmb_job_id"] for job in staging["jobs"]] == ["qmb_staging"]
        assert fake_archives[PRIMARY.uri].list_jobs_calls == 0
        assert fake_archives[PRODUCTION.uri].list_jobs_calls == 0

        _get_json(netloc, "/api/index?archive=mcp-staging")
        assert fake_archives[STAGING.uri].list_jobs_calls == 1

        _get_json(netloc, "/api/index?archive=mcp-staging&refresh=1")
        assert fake_archives[STAGING.uri].list_jobs_calls == 2
        assert fake_archives[PRODUCTION.uri].list_jobs_calls == 0

        status, missing = _get_json(netloc, "/api/index?archive=nope")
        assert status == 404
        assert "Unknown archive" in missing["error"]


def test_api_same_job_id_in_two_archives_is_kept_apart(
    tmp_path: Path, fake_archives: dict[str, _FakeRemoteArchive]
) -> None:
    store = JobStore(root=tmp_path / "jobs")
    for archive, env in ((STAGING, "staging"), (PRODUCTION, "production")):
        fake = fake_archives[archive.uri]
        fake.index_jobs = [_mcp_entry("qmb_shared", task=f"question in {env}")]
        fake.full_jobs["qmb_shared"] = {
            "metadata": _mcp_metadata("qmb_shared", env=env),
            "query": f"SELECT '{env}'",
            "schema": [],
        }
        fake.previews["qmb_shared"] = json.dumps({"env": env}) + "\n"

    with running_server(store, remote_archives=ALL_ARCHIVES) as netloc:
        _, combined = _get_json(netloc, "/api/index")
        _, staging = _get_json(netloc, "/api/jobs/qmb_shared?archive=mcp-staging")
        _, production = _get_json(netloc, "/api/jobs/qmb_shared?archive=mcp")
        _, preview = _get_json(netloc, "/api/jobs/qmb_shared/preview?archive=mcp")
        _, staging_preview = _get_json(
            netloc, "/api/jobs/qmb_shared/preview?archive=mcp-staging"
        )
        # No archive: local first, then only the primary archive (unchanged behaviour).
        status, _ = _get_json(netloc, "/api/jobs/qmb_shared")
        local_status, _ = _get_json(netloc, "/api/jobs/qmb_shared?archive=local")

    shared = [job for job in combined["jobs"] if job["qmb_job_id"] == "qmb_shared"]
    assert sorted((job["archive"], job["task"]) for job in shared) == [
        ("mcp", "question in production"),
        ("mcp-staging", "question in staging"),
    ]
    assert staging["query"] == "SELECT 'staging'"
    assert staging["archive"] == "mcp-staging"
    assert production["query"] == "SELECT 'production'"
    assert production["archive"] == "mcp"
    assert preview["rows"] == [{"env": "production"}]
    assert staging_preview["rows"] == [{"env": "staging"}]
    assert status == 404
    assert local_status == 404


def test_api_job_without_archive_param_still_falls_back_to_primary(
    tmp_path: Path, fake_archives: dict[str, _FakeRemoteArchive]
) -> None:
    store = JobStore(root=tmp_path / "jobs")
    fake_archives[PRIMARY.uri].full_jobs["qmb_example_bot"] = {
        "metadata": {"qmb_job_id": "qmb_example_bot", "agent": {"name": "example-bot"}},
        "query": "SELECT 'example-bot'",
        "schema": [],
    }

    with running_server(store, remote_archives=ALL_ARCHIVES) as netloc:
        status, payload = _get_json(netloc, "/api/jobs/qmb_example_bot")

    assert status == 200
    assert payload["archive"] == "qmb"
    assert payload["origin"] == "remote"
    assert payload["agent_name"] == "example-bot"


def test_api_exposes_agent_user_and_task_on_jobs_and_sessions(
    tmp_path: Path, fake_archives: dict[str, _FakeRemoteArchive]
) -> None:
    store = JobStore(root=tmp_path / "jobs")
    local = _seed_job(
        store,
        session_id="local-session",
        agent_context=AgentContext(
            name="pi", session_id="local-session", task="debug orders", user="mds"
        ),
    )
    fake = fake_archives[PRODUCTION.uri]
    fake.index_jobs = [
        _mcp_entry("qmb_old", user="ana@example.com", created_at="2024-01-01T00:00:00+00:00"),
        _mcp_entry("qmb_new", user="bob@example.com", created_at="2024-01-02T00:00:00+00:00"),
        # Written before 0.11.0: no agent fields in the index entry.
        _remote_index_entry("qmb_legacy", session_id="legacy"),
    ]
    fake.full_jobs["qmb_legacy"] = {
        "metadata": _mcp_metadata("qmb_legacy", env="production"),
        "query": "SELECT 1",
        "schema": [],
    }
    fake.session_manifests["legacy"] = SessionManifest(
        session_id="legacy", jobs=("qmb_legacy",), count=1, agents=("example-mcp",),
        tasks=("Signups?",),
    )

    with running_server(store, remote_archives=ALL_ARCHIVES) as netloc:
        _, payload = _get_json(netloc, "/api/index")
        _, legacy_detail = _get_json(netloc, "/api/jobs/qmb_legacy?archive=mcp")
        _, legacy_session = _get_json(netloc, "/api/sessions/legacy?archive=mcp")
        _, gq_session = _get_json(netloc, "/api/sessions/gq-session?archive=mcp")
        _, local_detail = _get_json(netloc, f"/api/jobs/{local.qmb_job_id}")

    jobs = {(job["archive"], job["qmb_job_id"]): job for job in payload["jobs"]}
    local_job = jobs[("local", local.qmb_job_id)]
    assert (local_job["agent_name"], local_job["user"], local_job["task"]) == (
        "pi", "mds", "debug orders",
    )
    new = jobs[("mcp", "qmb_new")]
    assert (new["agent_name"], new["user"]) == ("example-mcp", "bob@example.com")
    legacy = jobs[("mcp", "qmb_legacy")]
    assert (legacy["agent_name"], legacy["user"], legacy["task"]) == (None, None, None)

    sessions = {(s["archive"], s["session_id"]): s for s in payload["sessions"]}
    local_session = sessions[("local", "local-session")]
    assert (local_session["agent_name"], local_session["user"]) == ("pi", "mds")
    derived = sessions[("mcp", "gq-session")]
    assert derived["agent_name"] == "example-mcp"
    assert derived["user"] == "bob@example.com"  # newest job wins
    assert derived["users"] == ["ana@example.com", "bob@example.com"]
    assert derived["agents"] == ["example-mcp"]
    assert derived["tasks"] == ["How many users signed up yesterday?"]

    # Detail views fall back to the full metadata.json / session manifest.
    assert legacy_detail["agent_name"] == "example-mcp"
    assert legacy_detail["user"] == "jane.doe@example.com"
    assert legacy_detail["agent"]["metadata"]["metric_request"] == {"m": ["x"]}
    assert legacy_session["agent_name"] == "example-mcp"
    assert legacy_session["task"] == "Signups?"
    assert legacy_session["archive"] == "mcp"
    assert gq_session["users"] == ["ana@example.com", "bob@example.com"]
    assert local_detail["agent_name"] == "pi"
    assert local_detail["archive"] == "local"


def test_api_local_job_also_in_an_archive_merges_with_also_in(
    tmp_path: Path, fake_archives: dict[str, _FakeRemoteArchive]
) -> None:
    store = JobStore(root=tmp_path / "jobs")
    local = _seed_job(store)
    fake_archives[PRIMARY.uri].index_jobs = [
        {**_remote_index_entry(local.qmb_job_id), "agent_name": "example-bot"}
    ]

    with running_server(store, remote_archives=ALL_ARCHIVES) as netloc:
        _, payload = _get_json(netloc, "/api/index")

    assert len(payload["jobs"]) == 1
    job = payload["jobs"][0]
    assert (job["archive"], job["origin"], job["also_in"]) == ("local", "both", ["qmb"])
    assert job["agent_name"] == "example-bot"  # filled from the archive copy


def test_api_session_detail_with_archive_param(
    tmp_path: Path, fake_archives: dict[str, _FakeRemoteArchive]
) -> None:
    store = JobStore(root=tmp_path / "jobs")
    for archive in (STAGING, PRODUCTION):
        fake_archives[archive.uri].session_manifests["gq-session"] = SessionManifest(
            session_id="gq-session", jobs=(f"qmb_{archive.name}",), count=1
        )

    with running_server(store, remote_archives=ALL_ARCHIVES) as netloc:
        _, staging = _get_json(netloc, "/api/sessions/gq-session?archive=mcp-staging")
        _, production = _get_json(netloc, "/api/sessions/gq-session?archive=mcp")
        primary_status, _ = _get_json(netloc, "/api/sessions/gq-session?scope=remote")
        unknown_status, _ = _get_json(netloc, "/api/sessions/gq-session?archive=nope")

    assert staging["jobs"] == ["qmb_mcp-staging"]
    assert staging["archive"] == "mcp-staging"
    assert production["jobs"] == ["qmb_mcp"]
    assert primary_status == 404  # scope=remote keeps meaning the primary archive
    assert unknown_status == 404


def test_api_search_matches_remote_user_and_task(
    tmp_path: Path, fake_archives: dict[str, _FakeRemoteArchive]
) -> None:
    store = JobStore(root=tmp_path / "jobs")
    local = _seed_job(
        store,
        sql="SELECT 1",
        agent_context=AgentContext(name="pi", user="carol@example.com", task="weekly churn"),
    )
    recent = datetime.now(UTC).isoformat()
    fake_archives[STAGING.uri].index_jobs = [
        _mcp_entry("qmb_ana", user="ana@example.com", created_at=recent),
        _mcp_entry("qmb_bob", user="bob@example.com", created_at=recent),
    ]
    fake_archives[PRODUCTION.uri].index_jobs = [
        _mcp_entry("qmb_ana_old", user="ana@example.com", task="old question"),
    ]

    with running_server(store, remote_archives=ALL_ARCHIVES) as netloc:
        _, by_user = _get_json(netloc, "/api/search?q=ANA%40example&period=recent&target=sql")
        _, older = _get_json(netloc, "/api/search?q=ana@example&period=older&target=sql")
        _, by_task = _get_json(netloc, "/api/search?q=old+question&period=older&target=sql")
        _, local_user = _get_json(netloc, "/api/search?q=carol&period=recent&target=sql")
        _, local_task = _get_json(netloc, "/api/search?q=weekly+churn&period=recent&target=sql")
        _, preview = _get_json(netloc, "/api/search?q=ana&period=recent&target=preview")

    assert by_user["matches"] == [{"archive": "mcp-staging", "qmb_job_id": "qmb_ana"}]
    assert by_user["job_ids"] == ["qmb_ana"]
    assert older["matches"] == [{"archive": "mcp", "qmb_job_id": "qmb_ana_old"}]
    assert by_task["matches"] == [{"archive": "mcp", "qmb_job_id": "qmb_ana_old"}]
    expected_local = [{"archive": "local", "qmb_job_id": local.qmb_job_id}]
    assert local_user["matches"] == expected_local
    assert local_task["matches"] == expected_local
    # Remote previews are never downloaded for search.
    assert preview["matches"] == []
    assert all(fake.fetch_preview_calls == 0 for fake in fake_archives.values())


def test_serve_reads_extra_archives_from_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from qmb import config

    path = tmp_path / "config.toml"
    path.write_text(
        '[remote_archive]\nuri = "gs://bucket/qmb/"\nname = "qmb"\n'
        '[[remote_archives]]\nname = "mcp"\nuri = "gs://bucket/gq/"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(config, "default_config_path", lambda: path)
    captured: dict[str, Any] = {}

    class _StopServer(Exception):
        pass

    def fake_create_server(host: str, port: int, **kwargs: Any) -> Any:
        captured.update(kwargs)
        raise _StopServer

    monkeypatch.setattr(server_module, "create_server", fake_create_server)

    with pytest.raises(_StopServer):
        server_module.serve(
            host="127.0.0.1", port=0, remote_destination="gs://cli/qmb/", open_browser=False
        )

    assert captured["remote_archives"] == [
        RemoteArchiveConfig(name="qmb", uri="gs://cli/qmb/", primary=True),
        RemoteArchiveConfig(name="mcp", uri="gs://bucket/gq/"),
    ]


def test_web_cli_rejects_invalid_archive_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from qmb import config

    path = tmp_path / "config.toml"
    path.write_text('[[remote_archives]]\nname = "local"\nuri = "gs://b/p/"\n', encoding="utf-8")
    monkeypatch.setattr(config, "default_config_path", lambda: path)
    monkeypatch.setattr(
        "qmb.web.server.serve", lambda **kwargs: pytest.fail("must not serve a bad config")
    )

    result = CliRunner().invoke(cli.app, ["web", "--no-open"])

    assert result.exit_code != 0
    assert "reserved" in result.output + result.stderr


# -- Static serving ----------------------------------------------------------


def test_placeholder_page_served_when_static_index_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    empty_static = tmp_path / "empty-static"
    empty_static.mkdir()
    monkeypatch.setattr(server_module, "STATIC_ROOT", empty_static)
    store = JobStore(root=tmp_path / "jobs")

    with running_server(store) as netloc:
        status, body = _get(netloc, "/")

    assert status == 200
    assert b"/api/index" in body


def test_static_traversal_returns_404_never_leaks_file_contents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    static_root = tmp_path / "static"
    static_root.mkdir()
    (static_root / "index.html").write_text("<html>hello</html>", encoding="utf-8")
    monkeypatch.setattr(server_module, "STATIC_ROOT", static_root)
    secret = tmp_path / "secret.txt"
    secret.write_text("do-not-leak", encoding="utf-8")
    store = JobStore(root=tmp_path / "jobs")

    with running_server(store) as netloc:
        status1, body1 = _get(netloc, "/../secret.txt")
        status2, body2 = _get(netloc, "/%2e%2e/secret.txt")

    assert status1 == 404
    assert b"do-not-leak" not in body1
    assert status2 == 404
    assert b"do-not-leak" not in body2


def test_static_serves_existing_index_html(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    static_root = tmp_path / "static"
    static_root.mkdir()
    (static_root / "index.html").write_text("<html>hello qmb</html>", encoding="utf-8")
    monkeypatch.setattr(server_module, "STATIC_ROOT", static_root)
    store = JobStore(root=tmp_path / "jobs")

    with running_server(store) as netloc:
        status, body = _get(netloc, "/")
        status_spa, body_spa = _get(netloc, "/some/client/route")

    assert status == 200
    assert body == b"<html>hello qmb</html>"
    assert status_spa == 200
    assert body_spa == b"<html>hello qmb</html>"


def test_head_request_returns_headers_without_body(tmp_path: Path) -> None:
    store = JobStore(root=tmp_path / "jobs")
    _seed_job(store)

    with running_server(store) as netloc:
        conn = http.client.HTTPConnection(netloc, timeout=5)
        try:
            conn.request("HEAD", "/api/index")
            resp = conn.getresponse()
            status = resp.status
            content_length = resp.getheader("Content-Length")
            body = resp.read()
        finally:
            conn.close()

    assert status == 200
    assert content_length is not None and int(content_length) > 0
    assert body == b""


# -- Client disconnects (BrokenPipeError/ConnectionResetError) ---------------


class _RaisingWfile:
    """Stand-in for a socket file object that raises on every ``write``."""

    def __init__(self, exc: type[BaseException]) -> None:
        self.exc = exc
        self.write_calls = 0

    def write(self, _data: bytes) -> int:
        self.write_calls += 1
        raise self.exc("client gone")

    def flush(self) -> None:
        pass


class _FakeHandler(server_module.QmbRequestHandler):
    """A handler whose I/O is faked so ``_route`` can be exercised directly."""

    def __init__(self, wfile: Any) -> None:
        self.wfile = wfile
        self.sent_responses: list[tuple[int, ...]] = []

    def send_response(self, code: int, message: str | None = None) -> None:
        self.sent_responses.append((code,))

    def send_header(self, keyword: str, value: str) -> None:
        pass

    def end_headers(self) -> None:
        pass

    def _handle_index(self, query: dict[str, list[str]], *, include_body: bool) -> None:
        self._send_json({"ok": True}, status=HTTPStatus.OK, include_body=include_body)


def _make_fake_request(exc: type[BaseException]) -> tuple[_FakeHandler, _RaisingWfile]:
    wfile = _RaisingWfile(exc)
    handler = _FakeHandler(wfile)
    handler.path = "/api/index"
    return handler, wfile


@pytest.mark.parametrize("exc_type", [BrokenPipeError, ConnectionResetError])
def test_route_swallows_disconnect_during_successful_response(
    exc_type: type[BaseException],
) -> None:
    handler, wfile = _make_fake_request(exc_type)

    handler._route(include_body=True)

    assert wfile.write_calls == 1, "should not retry the write after the client is gone"
    assert handler.sent_responses == [(HTTPStatus.OK,)], (
        "should not attempt a second (error) response on the dead socket"
    )


def test_route_swallows_disconnect_from_generic_exception_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A BrokenPipeError raised while sending a 500 must not propagate either."""

    def _boom(self: Any, query: Any, *, include_body: bool) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(_FakeHandler, "_handle_index", _boom)
    handler, wfile = _make_fake_request(BrokenPipeError)

    handler._route(include_body=True)

    assert wfile.write_calls == 1
    assert handler.sent_responses == [(HTTPStatus.INTERNAL_SERVER_ERROR,)]


def test_route_swallows_disconnect_from_api_error_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A BrokenPipeError raised while sending a 4xx must not propagate either."""

    def _not_found(self: Any, query: Any, *, include_body: bool) -> None:
        raise server_module._ApiError(HTTPStatus.NOT_FOUND, "nope")

    monkeypatch.setattr(_FakeHandler, "_handle_index", _not_found)
    handler, wfile = _make_fake_request(BrokenPipeError)

    handler._route(include_body=True)

    assert wfile.write_calls == 1
    assert handler.sent_responses == [(HTTPStatus.NOT_FOUND,)]


def test_handle_error_is_quiet_for_broken_pipe(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    store = JobStore(root=tmp_path / "jobs")
    srv = create_server("127.0.0.1", 0, job_store=store, remote_destination=None)
    try:
        try:
            raise BrokenPipeError("gone")
        except BrokenPipeError:
            srv.handle_error(None, ("127.0.0.1", 12345))
    finally:
        srv.server_close()

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_handle_error_still_reports_other_exceptions(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    store = JobStore(root=tmp_path / "jobs")
    srv = create_server("127.0.0.1", 0, job_store=store, remote_destination=None)
    try:
        try:
            raise ValueError("something else went wrong")
        except ValueError:
            srv.handle_error(None, ("127.0.0.1", 12345))
    finally:
        srv.server_close()

    captured = capsys.readouterr()
    assert "something else went wrong" in captured.err or "something else went wrong" in (
        captured.out
    )


# -- Config precedence -------------------------------------------------------


def test_web_host_and_port_precedence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from qmb import config

    monkeypatch.delenv("QMB_WEB_HOST", raising=False)
    monkeypatch.delenv("QMB_WEB_PORT", raising=False)
    monkeypatch.setattr(config, "load_config", lambda: {})

    assert config.web_host() == "127.0.0.1"
    assert config.web_port() == 8850

    monkeypatch.setattr(config, "load_config", lambda: {"web": {"host": "0.0.0.0", "port": 9000}})
    assert config.web_host() == "0.0.0.0"
    assert config.web_port() == 9000

    monkeypatch.setenv("QMB_WEB_HOST", "10.0.0.1")
    monkeypatch.setenv("QMB_WEB_PORT", "9100")
    assert config.web_host() == "10.0.0.1"
    assert config.web_port() == 9100

    assert config.web_host("192.168.1.1") == "192.168.1.1"
    assert config.web_port(9200) == 9200


# -- CLI ----------------------------------------------------------------------


def test_web_cli_resolves_options_and_calls_serve(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("QMB_WEB_HOST", raising=False)
    monkeypatch.delenv("QMB_WEB_PORT", raising=False)
    monkeypatch.delenv("QMB_REMOTE_ARCHIVE_URI", raising=False)
    monkeypatch.setattr("qmb.config.load_config", lambda: {})

    captured: dict[str, Any] = {}

    def fake_serve(
        *, host: str, port: int, remote_destination: str | None, open_browser: bool
    ) -> None:
        captured.update(
            host=host,
            port=port,
            remote_destination=remote_destination,
            open_browser=open_browser,
        )

    monkeypatch.setattr("qmb.web.server.serve", fake_serve)

    result = CliRunner().invoke(
        cli.app,
        [
            "web",
            "--host",
            "0.0.0.0",
            "--port",
            "9999",
            "--no-open",
            "--destination",
            "gs://bucket/qmb/",
        ],
    )

    assert result.exit_code == 0, result.output + result.stderr
    assert captured == {
        "host": "0.0.0.0",
        "port": 9999,
        "remote_destination": "gs://bucket/qmb/",
        "open_browser": False,
    }


def test_web_cli_defaults_open_browser_true(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("QMB_WEB_HOST", raising=False)
    monkeypatch.delenv("QMB_WEB_PORT", raising=False)
    monkeypatch.delenv("QMB_REMOTE_ARCHIVE_URI", raising=False)
    monkeypatch.setattr("qmb.config.load_config", lambda: {})

    captured: dict[str, Any] = {}

    def fake_serve(**kwargs: Any) -> None:
        captured.update(kwargs)

    monkeypatch.setattr("qmb.web.server.serve", fake_serve)

    result = CliRunner().invoke(cli.app, ["web"])

    assert result.exit_code == 0, result.output + result.stderr
    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 8850
    assert captured["remote_destination"] is None
    assert captured["open_browser"] is True
