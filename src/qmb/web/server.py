"""Lean, read-only, stdlib-only local web server for qmb.

Serves a small JSON API over locally (and optionally remotely, from one or
more named archives) archived qmb jobs/sessions, plus static files for the
(separately built) frontend. No third-party dependencies: everything is
built on ``http.server``.
"""

from __future__ import annotations

import json
import math
import mimetypes
import sys
import threading
import urllib.parse
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from qmb.config import DEFAULT_REMOTE_ARCHIVE_NAME, RemoteArchiveConfig
from qmb.jobs.remote import GcsRemoteArchive, RemoteArchiveError, collapse_excerpt
from qmb.jobs.result_source import JsonlPreviewResultSource
from qmb.jobs.store import AmbiguousJobIdError, JobNotFoundError, JobStore

STATIC_ROOT = Path(__file__).resolve().parent / "static"
DEFAULT_PAGE_SIZE = 200
RECENT_SEARCH_DAYS = 3
LOCAL_ARCHIVE = "local"
PEOPLE_KEYS = ("agent_name", "user", "task", "users")
_QUIET_ERRORS = (BrokenPipeError, ConnectionResetError)

PLACEHOLDER_HTML = """<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>qmb web</title></head>
<body style="font: 14px sans-serif; max-width: 40em; margin: 4em auto;">
<h1>qmb web</h1>
<p>The frontend has not been built yet. The JSON API is live at
<a href="/api/index">/api/index</a>.</p>
</body>
</html>
"""


class _ApiError(Exception):
    """A request-handling error with an HTTP status and JSON message."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


@dataclass
class _ArchiveHalf:
    """One configured remote archive's cached index payload and per-job caches."""

    config: RemoteArchiveConfig
    lock: threading.Lock = field(default_factory=threading.Lock)
    payload: dict[str, Any] | None = None
    backend: GcsRemoteArchive | None = None
    preview_cache: dict[str, tuple[list[dict[str, Any]], int]] = field(default_factory=dict)
    session_cache: dict[str, dict[str, Any]] = field(default_factory=dict)


class JobIndexCache:
    """Assembles and caches the local + remote job/session index in memory.

    The local half and every configured remote archive are cached (and
    refreshed) independently so that ``scope=local`` requests never wait on —
    or even touch — GCS, and one slow or failing archive never blocks the
    others. The ``scope=None`` (combined) view is assembled from whichever
    cached halves are already available, rebuilding any as needed.

    Every job and session carries ``archive`` (``"local"`` or the archive
    name). Ids may repeat across remote archives, so those are keyed by
    ``(archive, id)``; an item present both locally and in an archive is
    still merged into one local entry tagged ``origin: "both"`` with the
    archive names listed in ``also_in``.
    """

    def __init__(
        self,
        job_store: JobStore,
        *,
        remote_destination: str | None = None,
        remote_archives: list[RemoteArchiveConfig] | None = None,
    ) -> None:
        if remote_archives is None:
            remote_archives = _archives_for_destination(remote_destination)
        self._store = job_store
        self._local_lock = threading.Lock()
        self._cache_lock = threading.Lock()
        self._local_payload: dict[str, Any] | None = None
        self._halves = {archive.name: _ArchiveHalf(archive) for archive in remote_archives}
        self._primary = next(
            (half for half in self._halves.values() if half.config.primary), None
        )

    def get(
        self,
        *,
        scope: str | None = None,
        archive: str | None = None,
        refresh: bool = False,
    ) -> dict[str, Any]:
        if archive == LOCAL_ARCHIVE or (archive is None and scope == "local"):
            return self._local(refresh=refresh)
        if archive is not None:
            return self._archive_payload(self._half(archive), refresh=refresh)
        remote_payloads = [
            self._archive_payload(half, refresh=refresh) for half in self._halves.values()
        ]
        if scope == "remote":
            return _combine_remote(remote_payloads)
        return _combine(self._local(refresh=refresh), remote_payloads)

    def archive_statuses(self) -> list[dict[str, Any]]:
        """Return every configured archive, with its status once loaded."""
        statuses: list[dict[str, Any]] = []
        for half in self._halves.values():
            payload = half.payload
            if payload is None:
                statuses.append({**_archive_identity(half.config), "status": "pending"})
            else:
                statuses.append(payload["archives"][0])
        return statuses

    def session_detail(
        self, session_id: str, *, scope: str | None, archive: str | None = None
    ) -> dict[str, Any]:
        if archive is None and scope == "remote":
            if self._primary is None:
                raise _ApiError(HTTPStatus.NOT_FOUND, f"Session not found: {session_id}")
            return self._remote_session_detail(session_id, self._primary)
        if archive is not None and archive != LOCAL_ARCHIVE:
            return self._remote_session_detail(session_id, self._half(archive))
        manifest = self._store.read_session_manifest(session_id)
        if manifest is None:
            raise _ApiError(HTTPStatus.NOT_FOUND, f"Session not found: {session_id}")
        return _with_session_people(
            {**manifest.to_dict(), "origin": "local", "archive": LOCAL_ARCHIVE}, []
        )

    def job_detail(self, job_id: str, *, archive: str | None = None) -> dict[str, Any]:
        if archive is not None and archive != LOCAL_ARCHIVE:
            return self._remote_job_detail(job_id, self._half(archive))
        try:
            record = self._store.read(job_id)
        except AmbiguousJobIdError as exc:
            raise _ApiError(HTTPStatus.BAD_REQUEST, str(exc)) from exc
        except JobNotFoundError:
            if archive == LOCAL_ARCHIVE or self._primary is None:
                raise _ApiError(HTTPStatus.NOT_FOUND, f"Job not found: {job_id}") from None
            return self._remote_job_detail(job_id, self._primary)
        metadata = record.to_metadata()
        return {
            **metadata,
            **_agent_fields(metadata.get("agent")),
            "query": record.query_path.read_text(encoding="utf-8"),
            "schema": [field.to_mapping() for field in record.schema or []],
            "origin": "local",
            "archive": LOCAL_ARCHIVE,
        }

    def job_preview(
        self, job_id: str, *, page: int, page_size: int, archive: str | None = None
    ) -> dict[str, Any]:
        if archive is not None and archive != LOCAL_ARCHIVE:
            return self._remote_preview_page(job_id, self._half(archive), page, page_size)
        try:
            record = self._store.read(job_id)
        except AmbiguousJobIdError as exc:
            raise _ApiError(HTTPStatus.BAD_REQUEST, str(exc)) from exc
        except JobNotFoundError:
            if archive == LOCAL_ARCHIVE or self._primary is None:
                raise _ApiError(HTTPStatus.NOT_FOUND, f"Job not found: {job_id}") from None
            return self._remote_preview_page(job_id, self._primary, page, page_size)
        source = JsonlPreviewResultSource.from_job(record)
        result = source.page(page - 1, page_size)
        return {
            "rows": result.rows,
            "total": result.total_rows,
            "page": result.page + 1,
            "page_size": page_size,
        }

    def search(
        self,
        text: str,
        *,
        period: str,
        target: str,
        session_id: str | None,
    ) -> dict[str, Any]:
        """Search local artifacts, plus cached remote index entries for SQL.

        Remote archives are only searched by ``target=sql`` over what their
        ``index.json`` already holds (SQL excerpt, ids, source, agent, user,
        task) — no per-job downloads. ``matches`` pairs each id with its
        archive; ``job_ids`` keeps the flat id list for older clients.
        """
        local = self.search_local(text, period=period, target=target, session_id=session_id)
        matches = [
            {"archive": LOCAL_ARCHIVE, "qmb_job_id": job_id} for job_id in local["job_ids"]
        ]
        if target == "sql" and self._halves:
            local_ids = {job["qmb_job_id"] for job in self._local()["jobs"]}
            needle = text.casefold()
            cutoff = datetime.now(UTC) - timedelta(days=RECENT_SEARCH_DAYS)
            for half in self._halves.values():
                jobs = self._archive_payload(half)["jobs"]
                for job in sorted(jobs, key=lambda j: j.get("created_at") or "", reverse=True):
                    if job["qmb_job_id"] in local_ids:
                        continue
                    if session_id is not None and job.get("session_id") != session_id:
                        continue
                    if (period == "recent") != _is_recent(job.get("created_at"), cutoff):
                        continue
                    if needle in _remote_job_haystack(job).casefold():
                        matches.append(
                            {"archive": half.config.name, "qmb_job_id": job["qmb_job_id"]}
                        )
        return {
            "job_ids": [match["qmb_job_id"] for match in matches],
            "matches": matches,
            "period": period,
            "target": target,
        }

    def search_local(
        self,
        text: str,
        *,
        period: str,
        target: str,
        session_id: str | None,
    ) -> dict[str, Any]:
        """Search local SQL or archived preview rows, newest jobs first."""
        needle = text.casefold()
        cutoff = datetime.now(UTC) - timedelta(days=RECENT_SEARCH_DAYS)
        matches: list[str] = []
        for record in self._store.list():
            if session_id is not None and record.session_id != session_id:
                continue
            is_recent = record.created_at >= cutoff
            if (period == "recent") != is_recent:
                continue
            path = record.query_path if target == "sql" else record.preview_path
            try:
                haystack = path.read_text(encoding="utf-8")
            except OSError:
                continue
            if target == "sql":
                agent = record.agent_context
                metadata = (
                    record.qmb_job_id,
                    record.session_id,
                    record.source.label,
                    agent.name if agent is not None else None,
                    agent.user if agent is not None else None,
                    agent.task if agent is not None else None,
                    record.created_at.isoformat(),
                )
                haystack += "\n" + "\n".join(value for value in metadata if value)
            if needle in haystack.casefold():
                matches.append(record.qmb_job_id)
        return {"job_ids": matches, "period": period, "target": target}

    def _local(self, *, refresh: bool = False) -> dict[str, Any]:
        with self._local_lock:
            if refresh or self._local_payload is None:
                self._local_payload = self._build_local()
            return self._local_payload

    def _archive_payload(self, half: _ArchiveHalf, *, refresh: bool = False) -> dict[str, Any]:
        with half.lock:
            if refresh or half.payload is None:
                half.payload = self._build_archive(half)
                with self._cache_lock:
                    half.session_cache.clear()
            return half.payload

    def _half(self, name: str) -> _ArchiveHalf:
        half = self._halves.get(name)
        if half is None:
            raise _ApiError(HTTPStatus.NOT_FOUND, f"Unknown archive: {name}")
        return half

    def _remote_job_detail(self, job_id: str, half: _ArchiveHalf) -> dict[str, Any]:
        backend = self._backend_or_none(half)
        if backend is None:
            raise _ApiError(HTTPStatus.NOT_FOUND, f"Job not found: {job_id}")
        try:
            fetched = backend.fetch_job_artifacts(job_id)
        except RemoteArchiveError as exc:
            raise _ApiError(HTTPStatus.NOT_FOUND, f"Job not found: {job_id}") from exc
        metadata = fetched["metadata"]
        return {
            **metadata,
            **_agent_fields(metadata.get("agent")),
            "query": fetched["query"],
            "schema": fetched["schema"],
            "origin": "remote",
            "archive": half.config.name,
        }

    def _remote_preview_page(
        self, job_id: str, half: _ArchiveHalf, page: int, page_size: int
    ) -> dict[str, Any]:
        backend = self._backend_or_none(half)
        if backend is None:
            raise _ApiError(HTTPStatus.NOT_FOUND, f"Job not found: {job_id}")
        rows, total_rows = self._remote_preview_rows(job_id, half, backend)
        page_index, _total_pages = _clamp_page(page - 1, total_rows, page_size)
        start = page_index * page_size
        return {
            "rows": rows[start : start + page_size],
            "total": total_rows,
            "page": page_index + 1,
            "page_size": page_size,
        }

    def _remote_preview_rows(
        self, job_id: str, half: _ArchiveHalf, backend: GcsRemoteArchive
    ) -> tuple[list[dict[str, Any]], int]:
        with self._cache_lock:
            cached = half.preview_cache.get(job_id)
        if cached is not None:
            return cached
        try:
            raw_text = backend.fetch_preview_jsonl(job_id)
            metadata = backend.fetch_job_artifacts(job_id)["metadata"]
        except RemoteArchiveError as exc:
            raise _ApiError(HTTPStatus.NOT_FOUND, f"Job not found: {job_id}") from exc
        rows = [json.loads(line) for line in raw_text.splitlines() if line.strip()]
        total_rows = int((metadata.get("stats") or {}).get("total_rows") or len(rows))
        entry = (rows, total_rows)
        with self._cache_lock:
            half.preview_cache[job_id] = entry
        return entry

    def _remote_session_detail(self, session_id: str, half: _ArchiveHalf) -> dict[str, Any]:
        backend = self._backend_or_none(half)
        if backend is None:
            raise _ApiError(HTTPStatus.NOT_FOUND, f"Session not found: {session_id}")
        with self._cache_lock:
            cached = half.session_cache.get(session_id)
        if cached is not None:
            return cached

        derived = self._derived_remote_session(session_id, half)
        manifest = backend.fetch_session_manifest(session_id)
        if manifest is not None:
            result = {**manifest.to_dict(), "origin": "remote", "archive": half.config.name}
            # Index entries can name the people behind the session even when
            # the manifest (agents/tasks only) cannot.
            people = {key: derived.get(key) for key in PEOPLE_KEYS} if derived else {}
            result = _with_session_people({**result, **people}, [])
        elif derived is not None:
            result = derived
        else:
            raise _ApiError(HTTPStatus.NOT_FOUND, f"Session not found: {session_id}")

        with self._cache_lock:
            half.session_cache[session_id] = result
        return result

    def _derived_remote_session(
        self, session_id: str, half: _ArchiveHalf
    ) -> dict[str, Any] | None:
        for session in self._archive_payload(half).get("sessions", []):
            if session.get("session_id") == session_id:
                return session
        return None

    def _backend_or_none(self, half: _ArchiveHalf) -> GcsRemoteArchive | None:
        try:
            return self._backend(half)
        except RemoteArchiveError:
            return None

    def _backend(self, half: _ArchiveHalf) -> GcsRemoteArchive:
        if half.backend is None:
            from qmb.jobs.remote import get_remote_archive

            half.backend = get_remote_archive(half.config.uri)
        return half.backend

    def _build_local(self) -> dict[str, Any]:
        """Build the local half: one query.sql read per job, no GCS calls."""
        local_jobs = [
            {**_local_job_entry(record), "origin": "local", "archive": LOCAL_ARCHIVE}
            for record in self._store.list()
        ]
        local_sessions = [
            {**manifest.to_dict(), "origin": "local", "archive": LOCAL_ARCHIVE}
            for manifest in self._store.session_manifests()
        ]
        return {
            "generated_at": _utcnow_iso(),
            "jobs": local_jobs,
            "sessions": _attach_session_people(local_sessions, local_jobs),
        }

    def _build_archive(self, half: _ArchiveHalf) -> dict[str, Any]:
        """Build one remote archive's payload from a single ``index.json`` download.

        Session summaries are derived from the index job entries (grouped by
        ``session_id``) instead of a ``sessions/`` prefix scan — cwds are not
        available from index entries (agents/tasks only when the entries
        carry them), so each derived session is tagged ``"derived": True``.

        Machines that export jobs + session manifests but never run
        ``qmb jobs reindex --remote`` leave ``index.json`` stale, so their
        new sessions would otherwise stay invisible. A second, cheap
        names-only listing of the ``sessions/`` prefix (see
        ``GcsRemoteArchive.list_session_blob_names``) catches those: any
        session id present there but absent from the index-derived set above
        is appended as a minimal ``"unindexed"`` stub (see
        ``_unindexed_session_stubs``).
        """
        name = half.config.name
        payload: dict[str, Any] = {"generated_at": _utcnow_iso(), "jobs": [], "sessions": []}
        status = _archive_identity(half.config)
        try:
            backend = self._backend(half)
            remote_jobs = backend.list_jobs()
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            payload["remote_error"] = error
            payload["archives"] = [
                {**status, "status": "error", "error": error, "job_count": 0, "session_count": 0}
            ]
            return payload

        jobs = [_remote_job_entry(job, name) for job in remote_jobs if job.get("qmb_job_id")]
        sessions = _derive_remote_sessions(jobs)
        indexed_ids = {session["session_id"] for session in sessions}
        stubs = _unindexed_session_stubs(backend, indexed_ids)
        sessions.extend(stubs)
        payload["jobs"] = jobs
        payload["sessions"] = [
            {**session, "origin": "remote", "archive": name} for session in sessions
        ]
        status = {
            **status,
            "status": "ok",
            "job_count": len(jobs),
            "session_count": len(sessions),
        }
        if stubs:
            payload["index_stale"] = len(stubs)
            status["index_stale"] = len(stubs)
        payload["archives"] = [status]
        return payload


class QmbHTTPServer(ThreadingHTTPServer):
    """Threaded HTTP server carrying the job store and index cache."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_cls: type[BaseHTTPRequestHandler],
        *,
        job_store: JobStore,
        remote_destination: str | None,
        remote_archives: list[RemoteArchiveConfig] | None = None,
    ) -> None:
        super().__init__(server_address, handler_cls)
        self.job_store = job_store
        self.remote_destination = remote_destination
        self.index_cache = JobIndexCache(
            job_store,
            remote_destination=remote_destination,
            remote_archives=remote_archives,
        )

    def handle_error(self, request: Any, client_address: Any) -> None:
        exc_type = sys.exc_info()[0]
        if exc_type is not None and issubclass(exc_type, _QUIET_ERRORS):
            return
        super().handle_error(request, client_address)


class QmbRequestHandler(BaseHTTPRequestHandler):
    """Routes GET/HEAD requests to the JSON API or static file serving."""

    server: QmbHTTPServer

    def log_message(self, format: str, *args: Any) -> None:
        pass

    def do_GET(self) -> None:
        self._route(include_body=True)

    def do_HEAD(self) -> None:
        self._route(include_body=False)

    def _route(self, *, include_body: bool) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        path = urllib.parse.unquote(parsed.path)
        query = urllib.parse.parse_qs(parsed.query)
        try:
            if path == "/api/index":
                self._handle_index(query, include_body=include_body)
            elif path == "/api/archives":
                payload = {"archives": self.server.index_cache.archive_statuses()}
                self._send_json(payload, status=HTTPStatus.OK, include_body=include_body)
            elif path == "/api/search":
                self._handle_search(query, include_body=include_body)
            elif path.startswith("/api/sessions/"):
                self._handle_session(path, query, include_body=include_body)
            elif path.startswith("/api/jobs/"):
                self._handle_job(path, query, include_body=include_body)
            elif path.startswith("/api/"):
                raise _ApiError(HTTPStatus.NOT_FOUND, "Not found")
            else:
                self._handle_static(path, include_body=include_body)
        except _QUIET_ERRORS:
            # The client disconnected (tab closed/refreshed) mid-response. Nothing
            # to send, nothing worth logging: this is routine browser behavior.
            return
        except _ApiError as exc:
            try:
                self._send_json(
                    {"error": exc.message}, status=exc.status, include_body=include_body
                )
            except _QUIET_ERRORS:
                return
        except Exception as exc:  # pragma: no cover - defensive catch-all
            try:
                self._send_json(
                    {"error": str(exc)},
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    include_body=include_body,
                )
            except _QUIET_ERRORS:
                return

    def _handle_index(self, query: dict[str, list[str]], *, include_body: bool) -> None:
        scope = _parse_scope(query)
        refresh = _first(query, "refresh") == "1"
        payload = self.server.index_cache.get(
            scope=scope, archive=_first(query, "archive"), refresh=refresh
        )
        self._send_json(payload, status=HTTPStatus.OK, include_body=include_body)

    def _handle_search(self, query: dict[str, list[str]], *, include_body: bool) -> None:
        text = (_first(query, "q") or "").strip()
        period = _first(query, "period")
        target = _first(query, "target")
        if not text:
            raise _ApiError(HTTPStatus.BAD_REQUEST, "Missing search query")
        if period not in ("recent", "older"):
            raise _ApiError(HTTPStatus.BAD_REQUEST, f"Invalid period: {period!r}")
        if target not in ("sql", "preview"):
            raise _ApiError(HTTPStatus.BAD_REQUEST, f"Invalid target: {target!r}")
        payload = self.server.index_cache.search(
            text,
            period=period,
            target=target,
            session_id=_first(query, "session_id"),
        )
        self._send_json(payload, status=HTTPStatus.OK, include_body=include_body)

    def _handle_session(
        self, path: str, query: dict[str, list[str]], *, include_body: bool
    ) -> None:
        parts = path.split("/")
        if len(parts) != 4 or not parts[3]:
            raise _ApiError(HTTPStatus.NOT_FOUND, "Not found")
        scope = _parse_scope(query)
        detail = self.server.index_cache.session_detail(
            parts[3], scope=scope, archive=_first(query, "archive")
        )
        self._send_json(detail, status=HTTPStatus.OK, include_body=include_body)

    def _handle_job(
        self, path: str, query: dict[str, list[str]], *, include_body: bool
    ) -> None:
        parts = path.split("/")
        archive = _first(query, "archive")
        if len(parts) == 4 and parts[3]:
            detail = self.server.index_cache.job_detail(parts[3], archive=archive)
            self._send_json(detail, status=HTTPStatus.OK, include_body=include_body)
            return
        if len(parts) == 5 and parts[3] and parts[4] == "preview":
            page = _parse_positive_int(_first(query, "page"), 1, name="page")
            page_size = _parse_positive_int(
                _first(query, "page_size"), DEFAULT_PAGE_SIZE, name="page_size"
            )
            result = self.server.index_cache.job_preview(
                parts[3], page=page, page_size=page_size, archive=archive
            )
            self._send_json(result, status=HTTPStatus.OK, include_body=include_body)
            return
        raise _ApiError(HTTPStatus.NOT_FOUND, "Not found")

    def _handle_static(self, path: str, *, include_body: bool) -> None:
        rel = "index.html" if path == "/" else path.lstrip("/")
        safe_path = _safe_join(STATIC_ROOT, rel)
        if safe_path is None:
            raise _ApiError(HTTPStatus.NOT_FOUND, "Not found")

        if safe_path.is_file():
            self._send_file(safe_path, include_body=include_body)
            return

        is_spa_route = path == "/" or not Path(path).suffix
        if is_spa_route:
            index_path = STATIC_ROOT / "index.html"
            if index_path.is_file():
                self._send_file(index_path, include_body=include_body)
            else:
                self._send_placeholder(include_body=include_body)
            return

        raise _ApiError(HTTPStatus.NOT_FOUND, "Not found")

    def _send_json(self, payload: Any, *, status: int, include_body: bool) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if include_body:
            self.wfile.write(body)

    def _send_file(self, path: Path, *, include_body: bool) -> None:
        data = path.read_bytes()
        content_type, _encoding = mimetypes.guess_type(str(path))
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type or "application/octet-stream")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        if include_body:
            self.wfile.write(data)

    def _send_placeholder(self, *, include_body: bool) -> None:
        body = PLACEHOLDER_HTML.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if include_body:
            self.wfile.write(body)


def create_server(
    host: str,
    port: int,
    *,
    job_store: JobStore | None = None,
    remote_destination: str | None = None,
    remote_archives: list[RemoteArchiveConfig] | None = None,
) -> QmbHTTPServer:
    """Build (but do not start) a :class:`QmbHTTPServer`.

    ``remote_archives`` lists every archive to serve (primary first). When
    omitted, ``remote_destination`` alone becomes the primary archive.
    """
    return QmbHTTPServer(
        (host, port),
        QmbRequestHandler,
        job_store=job_store or JobStore(),
        remote_destination=remote_destination,
        remote_archives=remote_archives,
    )


def serve(
    *,
    host: str,
    port: int,
    remote_destination: str | None,
    open_browser: bool = True,
) -> None:
    """Bind, print the URL, optionally open a browser, and serve until Ctrl-C.

    ``remote_destination`` overrides the primary archive's URI; the extra
    ``[[remote_archives]]`` come from ``~/.qmb/config.toml``.
    """
    from qmb.config import remote_archives

    server = create_server(
        host,
        port,
        remote_destination=remote_destination,
        remote_archives=remote_archives(remote_destination),
    )
    try:
        bound_host, bound_port = server.server_address[:2]
        print(f"qmb web: serving at http://{bound_host}:{bound_port}/ (Ctrl-C to stop)")
        if open_browser:
            import webbrowser

            webbrowser.open(f"http://{bound_host}:{bound_port}/")
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def _archives_for_destination(destination: str | None) -> list[RemoteArchiveConfig]:
    if destination is None:
        return []
    return [RemoteArchiveConfig(name=DEFAULT_REMOTE_ARCHIVE_NAME, uri=destination, primary=True)]


def _archive_identity(config: RemoteArchiveConfig) -> dict[str, Any]:
    return {"name": config.name, "uri": config.uri, "primary": config.primary}


def _agent_fields(agent: Any) -> dict[str, Any]:
    """Return the flat ``agent_name``/``user``/``task`` fields of an ``agent`` mapping."""
    agent = agent if isinstance(agent, dict) else {}
    return {"agent_name": agent.get("name"), "user": agent.get("user"), "task": agent.get("task")}


def _local_job_entry(record: Any) -> dict[str, Any]:
    query_text = record.query_path.read_text(encoding="utf-8")
    metadata = record.to_metadata()
    return {
        **metadata,
        **_agent_fields(metadata.get("agent")),
        "query_excerpt": collapse_excerpt(query_text),
    }


def _remote_job_entry(job: dict[str, Any], archive: str) -> dict[str, Any]:
    """Tag one ``index.json`` entry; entries written before 0.11.0 lack the agent fields."""
    return {
        **job,
        "agent_name": job.get("agent_name"),
        "user": job.get("user"),
        "task": job.get("task"),
        "origin": "remote",
        "archive": archive,
    }


def _job_session_id(job: dict[str, Any]) -> str | None:
    return job.get("session_id") or (job.get("agent") or {}).get("session_id")


def _with_session_people(
    session: dict[str, Any], jobs_newest_first: list[dict[str, Any]]
) -> dict[str, Any]:
    """Fill a session's ``agent_name``/``user``/``task``/``users``.

    Values already on the session win, then the newest job carrying one,
    then the manifest's ``agents``/``tasks`` aggregates.
    """

    def newest(key: str) -> Any:
        return next((job[key] for job in jobs_newest_first if job.get(key)), None)

    agents = session.get("agents") or []
    tasks = session.get("tasks") or []
    users = {job["user"] for job in jobs_newest_first if job.get("user")}
    users.update(session.get("users") or [])
    return {
        **session,
        "agent_name": session.get("agent_name") or newest("agent_name") or next(iter(agents), None),
        "user": session.get("user") or newest("user"),
        "task": session.get("task") or newest("task") or next(iter(tasks), None),
        "users": sorted(users),
    }


def _attach_session_people(
    sessions: list[dict[str, Any]], jobs: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    jobs_by_session: dict[str, list[dict[str, Any]]] = {}
    for job in sorted(jobs, key=lambda j: j.get("created_at") or "", reverse=True):
        if session_id := _job_session_id(job):
            jobs_by_session.setdefault(session_id, []).append(job)
    return [
        _with_session_people(session, jobs_by_session.get(session["session_id"], []))
        for session in sessions
    ]


def _unique_sorted(values: list[Any]) -> list[str]:
    return sorted({value for value in values if value})


def _derive_remote_sessions(remote_jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group remote index job entries into session summaries.

    Only fields present on index entries (``created_at``, ``bytes_processed``,
    and ``agent_name``/``user``/``task`` when the entry carries them) are
    aggregated; ``cwds`` require the full session manifest blob, so they come
    back empty and the summary is marked ``"derived": True`` to signal that
    to callers.
    """
    groups: dict[str, list[dict[str, Any]]] = {}
    for job in remote_jobs:
        session_id = job.get("session_id")
        if not session_id:
            continue
        groups.setdefault(session_id, []).append(job)

    sessions: list[dict[str, Any]] = []
    for session_id, jobs in groups.items():
        ordered = sorted(jobs, key=lambda j: j.get("created_at") or "")
        bytes_total = sum(int(j.get("bytes_processed") or 0) for j in ordered)
        latest = ordered[-1].get("created_at")
        # Newest first, matching the sidebar/UI ordering (first/latest below stay min/max).
        newest_first = list(reversed(ordered))
        session = {
            "session_id": session_id,
            "jobs": [j["qmb_job_id"] for j in newest_first],
            "count": len(ordered),
            "first": ordered[0].get("created_at"),
            "latest": latest,
            "bytes_processed": bytes_total,
            "agents": _unique_sorted([j.get("agent_name") for j in ordered]),
            "tasks": _unique_sorted([j.get("task") for j in ordered]),
            "cwds": [],
            "updated_at": latest,
            "derived": True,
        }
        sessions.append(_with_session_people(session, newest_first))
    return sessions


def _unindexed_session_stubs(
    archive: Any, indexed_ids: set[str]
) -> list[dict[str, Any]]:
    """Return minimal stub sessions for manifests missing from ``index.json``.

    Best-effort: a failure of the (already supplementary) names-only listing
    just means stale-index detection is skipped this time, not that the
    whole remote index request fails.
    """
    try:
        blob_names = archive.list_session_blob_names()
    except Exception:
        return []

    stubs: list[dict[str, Any]] = []
    for session_id, updated_at in blob_names:
        if session_id in indexed_ids:
            continue
        stub = {
            "session_id": session_id,
            "jobs": [],
            "count": None,
            "first": None,
            "latest": updated_at,
            "bytes_processed": None,
            "agents": [],
            "tasks": [],
            "cwds": [],
            "updated_at": updated_at,
            "unindexed": True,
        }
        stubs.append(_with_session_people(stub, []))
    return stubs


def _combine(local: dict[str, Any], remote_payloads: list[dict[str, Any]]) -> dict[str, Any]:
    """Assemble the compatibility (no-scope) payload from cached halves."""
    payload: dict[str, Any] = {
        "generated_at": local["generated_at"],
        "jobs": _merge_into_local(
            local["jobs"], [p["jobs"] for p in remote_payloads], key="qmb_job_id"
        ),
        "sessions": _merge_into_local(
            local["sessions"], [p["sessions"] for p in remote_payloads], key="session_id"
        ),
    }
    return _with_archive_summary(payload, remote_payloads)


def _combine_remote(remote_payloads: list[dict[str, Any]]) -> dict[str, Any]:
    """Concatenate every archive's payload; ids are only unique per archive."""
    payload: dict[str, Any] = {
        "generated_at": _utcnow_iso(),
        "jobs": [job for p in remote_payloads for job in p["jobs"]],
        "sessions": [session for p in remote_payloads for session in p["sessions"]],
    }
    return _with_archive_summary(payload, remote_payloads)


def _with_archive_summary(
    payload: dict[str, Any], remote_payloads: list[dict[str, Any]]
) -> dict[str, Any]:
    """Add per-archive statuses plus the legacy ``remote_error``/``index_stale`` fields."""
    statuses = [p["archives"][0] for p in remote_payloads]
    payload["archives"] = statuses
    errors = [f"{s['name']}: {s['error']}" for s in statuses if s["status"] == "error"]
    if errors:
        payload["remote_error"] = "; ".join(errors)
    stale = sum(s.get("index_stale", 0) for s in statuses)
    if stale:
        payload["index_stale"] = stale
    return payload


def _merge_into_local(
    local_items: list[dict[str, Any]],
    remote_lists: list[list[dict[str, Any]]],
    *,
    key: str,
) -> list[dict[str, Any]]:
    """Union local items with every archive's items.

    A remote item whose ``key`` is also local folds into the local entry
    (local fields win), which becomes ``origin: "both"`` and lists the
    archive in ``also_in``. Remote items are otherwise kept per archive, so
    the same id in two archives yields two entries.
    """
    merged = [dict(item) for item in local_items]
    local_by_key = {item[key]: item for item in merged if item.get(key)}
    for remote_items in remote_lists:
        for item in remote_items:
            local_item = local_by_key.get(item.get(key))
            if local_item is None:
                merged.append(item)
                continue
            local_item["origin"] = "both"
            local_item["also_in"] = [*local_item.get("also_in", []), item["archive"]]
            for people_key in ("agent_name", "user", "task"):
                if not local_item.get(people_key) and item.get(people_key):
                    local_item[people_key] = item[people_key]
    return merged


def _is_recent(created_at: str | None, cutoff: datetime) -> bool:
    try:
        created = datetime.fromisoformat(created_at or "")
    except ValueError:
        return False
    if created.tzinfo is None:
        created = created.replace(tzinfo=UTC)
    return created >= cutoff


def _remote_job_haystack(job: dict[str, Any]) -> str:
    fields = (
        "query_excerpt",
        "qmb_job_id",
        "session_id",
        "source_label",
        "agent_name",
        "user",
        "task",
        "created_at",
    )
    return "\n".join(str(job[name]) for name in fields if job.get(name))


def _clamp_page(page_index: int, total_rows: int, page_size: int) -> tuple[int, int]:
    total_pages = max(1, math.ceil(total_rows / page_size))
    return max(0, min(page_index, total_pages - 1)), total_pages


def _safe_join(root: Path, rel_path: str) -> Path | None:
    """Resolve ``rel_path`` under ``root``, returning ``None`` on traversal."""
    root_resolved = root.resolve()
    candidate = (root_resolved / rel_path).resolve()
    if not candidate.is_relative_to(root_resolved):
        return None
    return candidate


def _parse_positive_int(value: str | None, default: int, *, name: str) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise _ApiError(HTTPStatus.BAD_REQUEST, f"Invalid {name}: {value!r}") from exc
    if parsed < 1:
        raise _ApiError(HTTPStatus.BAD_REQUEST, f"Invalid {name}: {value!r}")
    return parsed


def _parse_scope(query: dict[str, list[str]]) -> str | None:
    scope = _first(query, "scope")
    if scope not in (None, "local", "remote"):
        raise _ApiError(HTTPStatus.BAD_REQUEST, f"Invalid scope: {scope!r}")
    return scope


def _first(query: dict[str, list[str]], key: str) -> str | None:
    values = query.get(key)
    return values[0] if values else None


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()
