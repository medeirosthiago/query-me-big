"""Tests for the stdlib-only `qmb web` backend (JSON API + static serving)."""

from __future__ import annotations

import contextlib
import http.client
import json
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

import qmb.cli as cli
import qmb.web.server as server_module
from qmb.jobs.models import EngineMetadata, SourceMetadata
from qmb.jobs.remote import RemoteArchiveError
from qmb.jobs.session_manifest import SessionManifest
from qmb.jobs.store import JobStore
from qmb.types import SchemaField
from qmb.web.server import create_server

# -- Helpers -----------------------------------------------------------------


def _seed_job(
    store: JobStore,
    *,
    sql: str = "SELECT 1 AS id",
    session_id: str | None = None,
    total_rows: int = 2,
    preview_rows: list[dict[str, Any]] | None = None,
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
    )


@contextlib.contextmanager
def running_server(
    job_store: JobStore, *, remote_destination: str | None = None
) -> Iterator[str]:
    srv = create_server(
        "127.0.0.1", 0, job_store=job_store, remote_destination=remote_destination
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
        self.fetch_preview_calls = 0
        self.list_error: Exception | None = None

    def list_jobs(self) -> list[dict[str, Any]]:
        if self.list_error:
            raise self.list_error
        return self.index_jobs

    def list_sessions(self) -> list[SessionManifest]:
        if self.list_error:
            raise self.list_error
        return self.index_sessions

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
