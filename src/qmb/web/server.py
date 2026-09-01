"""Lean, read-only, stdlib-only local web server for qmb.

Serves a small JSON API over locally (and optionally remotely) archived qmb
jobs/sessions, plus static files for the (separately built) frontend. No
third-party dependencies: everything is built on ``http.server``.
"""

from __future__ import annotations

import json
import math
import mimetypes
import sys
import threading
import urllib.parse
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from qmb.jobs.remote import GcsRemoteArchive, RemoteArchiveError, collapse_excerpt
from qmb.jobs.result_source import JsonlPreviewResultSource
from qmb.jobs.store import AmbiguousJobIdError, JobNotFoundError, JobStore

STATIC_ROOT = Path(__file__).resolve().parent / "static"
DEFAULT_PAGE_SIZE = 200
RECENT_SEARCH_DAYS = 3
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


class JobIndexCache:
    """Assembles and caches the local + remote job/session index in memory.

    Local and remote halves are cached (and refreshed) independently so that
    ``scope=local`` requests never wait on — or even touch — GCS. The
    ``scope=None`` (combined) view is assembled from whichever cached halves
    are already available, rebuilding either as needed.
    """

    def __init__(self, job_store: JobStore, *, remote_destination: str | None) -> None:
        self._store = job_store
        self._remote_destination = remote_destination
        self._lock = threading.Lock()
        self._local_payload: dict[str, Any] | None = None
        self._remote_payload: dict[str, Any] | None = None
        self._archive: GcsRemoteArchive | None = None
        self._preview_cache: dict[str, tuple[list[dict[str, Any]], int]] = {}
        self._remote_session_cache: dict[str, dict[str, Any]] = {}

    def get(self, *, scope: str | None = None, refresh: bool = False) -> dict[str, Any]:
        with self._lock:
            if scope in (None, "local") and (refresh or self._local_payload is None):
                self._local_payload = self._build_local()
            if scope in (None, "remote") and (refresh or self._remote_payload is None):
                self._remote_payload = self._build_remote()
            if scope == "local":
                return self._local_payload
            if scope == "remote":
                return self._remote_payload
            return self._combine(self._local_payload, self._remote_payload)

    def session_detail(self, session_id: str, *, scope: str | None) -> dict[str, Any]:
        if scope == "remote":
            return self._remote_session_detail(session_id)
        manifest = self._store.read_session_manifest(session_id)
        if manifest is None:
            raise _ApiError(HTTPStatus.NOT_FOUND, f"Session not found: {session_id}")
        return {**manifest.to_dict(), "origin": "local"}

    def job_detail(self, job_id: str) -> dict[str, Any]:
        try:
            record = self._store.read(job_id)
        except AmbiguousJobIdError as exc:
            raise _ApiError(HTTPStatus.BAD_REQUEST, str(exc)) from exc
        except JobNotFoundError:
            archive = self._remote_archive_or_none()
            if archive is None:
                raise _ApiError(HTTPStatus.NOT_FOUND, f"Job not found: {job_id}") from None
            try:
                fetched = archive.fetch_job_artifacts(job_id)
            except RemoteArchiveError as exc:
                raise _ApiError(HTTPStatus.NOT_FOUND, f"Job not found: {job_id}") from exc
            return {
                **fetched["metadata"],
                "query": fetched["query"],
                "schema": fetched["schema"],
            }
        return {
            **record.to_metadata(),
            "query": record.query_path.read_text(encoding="utf-8"),
            "schema": [field.to_mapping() for field in record.schema or []],
        }

    def job_preview(self, job_id: str, *, page: int, page_size: int) -> dict[str, Any]:
        try:
            record = self._store.read(job_id)
        except AmbiguousJobIdError as exc:
            raise _ApiError(HTTPStatus.BAD_REQUEST, str(exc)) from exc
        except JobNotFoundError:
            archive = self._remote_archive_or_none()
            if archive is None:
                raise _ApiError(HTTPStatus.NOT_FOUND, f"Job not found: {job_id}") from None
            rows, total_rows = self._remote_preview_rows(job_id, archive)
            page_index, _total_pages = _clamp_page(page - 1, total_rows, page_size)
            start = page_index * page_size
            return {
                "rows": rows[start : start + page_size],
                "total": total_rows,
                "page": page_index + 1,
                "page_size": page_size,
            }
        source = JsonlPreviewResultSource.from_job(record)
        result = source.page(page - 1, page_size)
        return {
            "rows": result.rows,
            "total": result.total_rows,
            "page": result.page + 1,
            "page_size": page_size,
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
                metadata = (
                    record.qmb_job_id,
                    record.session_id,
                    record.source.label,
                    record.agent_context.name if record.agent_context is not None else None,
                    record.created_at.isoformat(),
                )
                haystack += "\n" + "\n".join(value for value in metadata if value)
            if needle in haystack.casefold():
                matches.append(record.qmb_job_id)
        return {"job_ids": matches, "period": period, "target": target}

    def _remote_preview_rows(
        self, job_id: str, archive: GcsRemoteArchive
    ) -> tuple[list[dict[str, Any]], int]:
        with self._lock:
            cached = self._preview_cache.get(job_id)
        if cached is not None:
            return cached
        try:
            raw_text = archive.fetch_preview_jsonl(job_id)
            metadata = archive.fetch_job_artifacts(job_id)["metadata"]
        except RemoteArchiveError as exc:
            raise _ApiError(HTTPStatus.NOT_FOUND, f"Job not found: {job_id}") from exc
        rows = [json.loads(line) for line in raw_text.splitlines() if line.strip()]
        total_rows = int((metadata.get("stats") or {}).get("total_rows") or len(rows))
        entry = (rows, total_rows)
        with self._lock:
            self._preview_cache[job_id] = entry
        return entry

    def _remote_session_detail(self, session_id: str) -> dict[str, Any]:
        archive = self._remote_archive_or_none()
        if archive is None:
            raise _ApiError(HTTPStatus.NOT_FOUND, f"Session not found: {session_id}")
        with self._lock:
            cached = self._remote_session_cache.get(session_id)
        if cached is not None:
            return cached

        manifest = archive.fetch_session_manifest(session_id)
        if manifest is not None:
            result = {**manifest.to_dict(), "origin": "remote"}
        else:
            derived = self._derived_remote_session(session_id)
            if derived is None:
                raise _ApiError(HTTPStatus.NOT_FOUND, f"Session not found: {session_id}")
            result = derived

        with self._lock:
            self._remote_session_cache[session_id] = result
        return result

    def _derived_remote_session(self, session_id: str) -> dict[str, Any] | None:
        remote_payload = self.get(scope="remote")
        for session in remote_payload.get("sessions", []):
            if session.get("session_id") == session_id:
                return session
        return None

    def _remote_archive_or_none(self) -> GcsRemoteArchive | None:
        if self._remote_destination is None:
            return None
        try:
            return self._remote_archive()
        except RemoteArchiveError:
            return None

    def _remote_archive(self) -> GcsRemoteArchive:
        if self._archive is None:
            from qmb.jobs.remote import get_remote_archive

            self._archive = get_remote_archive(self._remote_destination)  # type: ignore[arg-type]
        return self._archive

    def _build_local(self) -> dict[str, Any]:
        """Build the local half: one query.sql read per job, no GCS calls."""
        local_jobs = [_local_job_entry(record) for record in self._store.list()]
        local_sessions = [manifest.to_dict() for manifest in self._store.session_manifests()]
        return {
            "generated_at": _utcnow_iso(),
            "jobs": [{**job, "origin": "local"} for job in local_jobs],
            "sessions": [{**s, "origin": "local"} for s in local_sessions],
        }

    def _build_remote(self) -> dict[str, Any]:
        """Build the remote half from a single ``index.json`` download.

        Session summaries are derived from the index job entries (grouped by
        ``session_id``) instead of a ``sessions/`` prefix scan — no
        agents/tasks/cwds are available from index entries, so those come
        back empty and each derived session is tagged ``"derived": True``.

        Machines that export jobs + session manifests but never run
        ``qmb jobs reindex --remote`` leave ``index.json`` stale, so their
        new sessions would otherwise stay invisible. A second, cheap
        names-only listing of the ``sessions/`` prefix (see
        ``GcsRemoteArchive.list_session_blob_names``) catches those: any
        session id present there but absent from the index-derived set above
        is appended as a minimal ``"unindexed"`` stub (see
        ``_unindexed_session_stubs``).
        """
        payload: dict[str, Any] = {"generated_at": _utcnow_iso(), "jobs": [], "sessions": []}
        if self._remote_destination is None:
            return payload

        try:
            archive = self._remote_archive()
            remote_jobs = archive.list_jobs()
        except Exception as exc:
            payload["remote_error"] = f"{type(exc).__name__}: {exc}"
            return payload

        payload["jobs"] = [{**job, "origin": "remote"} for job in remote_jobs]
        sessions = _derive_remote_sessions(remote_jobs)
        indexed_ids = {session["session_id"] for session in sessions}
        stubs = _unindexed_session_stubs(archive, indexed_ids)
        sessions.extend(stubs)
        payload["sessions"] = [{**session, "origin": "remote"} for session in sessions]
        if stubs:
            payload["index_stale"] = len(stubs)
        return payload

    def _combine(self, local: dict[str, Any], remote: dict[str, Any]) -> dict[str, Any]:
        """Assemble the compatibility (no-scope) payload from cached halves."""
        payload: dict[str, Any] = {"generated_at": local["generated_at"]}

        if self._remote_destination is None:
            payload["jobs"] = local["jobs"]
            payload["sessions"] = local["sessions"]
            return payload

        if "remote_error" in remote:
            payload["jobs"] = local["jobs"]
            payload["sessions"] = local["sessions"]
            payload["remote_error"] = remote["remote_error"]
            return payload

        payload["jobs"] = _merge_tagged(local["jobs"], remote["jobs"], key="qmb_job_id")
        payload["sessions"] = _merge_tagged(local["sessions"], remote["sessions"], key="session_id")
        if "index_stale" in remote:
            payload["index_stale"] = remote["index_stale"]
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
    ) -> None:
        super().__init__(server_address, handler_cls)
        self.job_store = job_store
        self.remote_destination = remote_destination
        self.index_cache = JobIndexCache(job_store, remote_destination=remote_destination)

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
        payload = self.server.index_cache.get(scope=scope, refresh=refresh)
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
        payload = self.server.index_cache.search_local(
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
        detail = self.server.index_cache.session_detail(parts[3], scope=scope)
        self._send_json(detail, status=HTTPStatus.OK, include_body=include_body)

    def _handle_job(
        self, path: str, query: dict[str, list[str]], *, include_body: bool
    ) -> None:
        parts = path.split("/")
        if len(parts) == 4 and parts[3]:
            detail = self.server.index_cache.job_detail(parts[3])
            self._send_json(detail, status=HTTPStatus.OK, include_body=include_body)
            return
        if len(parts) == 5 and parts[3] and parts[4] == "preview":
            page = _parse_positive_int(_first(query, "page"), 1, name="page")
            page_size = _parse_positive_int(
                _first(query, "page_size"), DEFAULT_PAGE_SIZE, name="page_size"
            )
            result = self.server.index_cache.job_preview(parts[3], page=page, page_size=page_size)
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
) -> QmbHTTPServer:
    """Build (but do not start) a :class:`QmbHTTPServer`."""
    return QmbHTTPServer(
        (host, port),
        QmbRequestHandler,
        job_store=job_store or JobStore(),
        remote_destination=remote_destination,
    )


def serve(
    *,
    host: str,
    port: int,
    remote_destination: str | None,
    open_browser: bool = True,
) -> None:
    """Bind, print the URL, optionally open a browser, and serve until Ctrl-C."""
    server = create_server(host, port, remote_destination=remote_destination)
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


def _local_job_entry(record: Any) -> dict[str, Any]:
    query_text = record.query_path.read_text(encoding="utf-8")
    return {**record.to_metadata(), "query_excerpt": collapse_excerpt(query_text)}


def _derive_remote_sessions(remote_jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group remote index job entries into session summaries.

    Only fields present on index entries (``created_at``, ``bytes_processed``)
    are aggregated; ``agents``/``tasks``/``cwds`` require the full session
    manifest blob, so they come back empty and the summary is marked
    ``"derived": True`` to signal that to callers.
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
        sessions.append(
            {
                "session_id": session_id,
                "jobs": [j["qmb_job_id"] for j in newest_first],
                "count": len(ordered),
                "first": ordered[0].get("created_at"),
                "latest": latest,
                "bytes_processed": bytes_total,
                "agents": [],
                "tasks": [],
                "cwds": [],
                "updated_at": latest,
                "derived": True,
            }
        )
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
        stubs.append(
            {
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
        )
    return stubs


def _merge_tagged(
    local_items: list[dict[str, Any]],
    remote_items: list[dict[str, Any]],
    *,
    key: str,
) -> list[dict[str, Any]]:
    """Union ``local_items``/``remote_items`` by ``key``, tagging each origin.

    Local fields win for entries present in both, tagged ``"both"``.
    """
    remote_by_key = {item[key]: item for item in remote_items if item.get(key)}
    local_keys = {item.get(key) for item in local_items}
    merged = [
        {**item, "origin": "both" if item.get(key) in remote_by_key else "local"}
        for item in local_items
    ]
    merged.extend(
        {**item, "origin": "remote"}
        for remote_key, item in remote_by_key.items()
        if remote_key not in local_keys
    )
    return merged


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
