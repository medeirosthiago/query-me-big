"""Remote qmb job archive backends."""

from __future__ import annotations

import json
import shutil
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from qmb.jobs.session_manifest import (
    SessionManifest,
    effective_session_id,
    safe_path_segment,
    update_manifest_with_job,
)
from qmb.jobs.store import CorruptJobError, JobStore

REMOTE_ARTIFACT_NAMES = ("metadata.json", "query.sql", "schema.json", "preview.jsonl")

INDEX_BLOB_NAME = "index.json"
INDEX_VERSION = 1
INDEX_WRITE_ATTEMPTS = 5
INDEX_EXCERPT_MAX_CHARS = 4000

__all__ = [
    "GcsRemoteArchive",
    "RemoteArchiveError",
    "RemoteArchiveResult",
    "collapse_excerpt",
    "get_remote_archive",
    "safe_path_segment",
]


class RemoteArchiveError(Exception):
    """Raised when a remote archive operation fails."""


@dataclass(frozen=True)
class RemoteArchiveResult:
    """Machine-readable result for a remote archive operation."""

    qmb_job_id: str
    status: str
    uri: str | None = None
    error: str | None = None
    warning: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        return {
            "qmb_job_id": self.qmb_job_id,
            "status": self.status,
            "uri": self.uri,
            "error": self.error,
            "warning": self.warning,
        }


def get_remote_archive(destination: str, *, client: Any | None = None) -> GcsRemoteArchive:
    """Return the remote archive backend for ``destination``."""
    if destination.startswith("gs://"):
        return GcsRemoteArchive(destination, client=client)
    raise RemoteArchiveError(f"Unsupported remote archive destination: {destination}")


class GcsRemoteArchive:
    """GCS-backed qmb remote archive.

    The remote layout mirrors qmb's local flat archive shape exactly:

    .. code-block:: text

        gs://bucket/prefix/<qmb_job_id>/{metadata.json,query.sql,...}
        gs://bucket/prefix/sessions/<session_id>.json   # session manifest

    Job artifacts live at a flat ``<qmb_job_id>/`` prefix (O(1) import by
    job id). Session manifests are a regenerable index from session id to
    job ids + cached aggregates — never the source of truth. ``metadata.json``
    inside each job remains authoritative for that job's ``session_id``.
    """

    def __init__(self, destination: str, *, client: Any | None = None) -> None:
        self.destination = destination
        parsed = urlparse(destination)
        if parsed.scheme != "gs" or not parsed.netloc:
            raise RemoteArchiveError(f"Invalid GCS destination: {destination}")
        self.bucket_name = parsed.netloc
        self.prefix = parsed.path.strip("/")
        self._client = client

    def export_job(self, record: Any, *, preview_rows: int | None = None) -> RemoteArchiveResult:
        """Upload one local qmb job archive to GCS and update its session manifest."""
        remote_prefix = self._job_prefix(record)
        bucket = self._bucket()
        for artifact_name in REMOTE_ARTIFACT_NAMES:
            path = record.directory / artifact_name
            if not path.exists():
                raise RemoteArchiveError(
                    f"Cannot export {record.qmb_job_id}: missing artifact {artifact_name}"
                )
            blob = bucket.blob(f"{remote_prefix}/{artifact_name}")
            if artifact_name == "preview.jsonl" and preview_rows is not None:
                blob.upload_from_string(
                    _limited_jsonl(path, preview_rows),
                    content_type="application/x-ndjson",
                )
            else:
                blob.upload_from_filename(
                    str(path),
                    content_type=_content_type_for_artifact(artifact_name),
                )

        session_id = effective_session_id(record)
        if session_id is not None:
            self._update_remote_manifest(session_id, record)

        warning: str | None = None
        try:
            self._upsert_index_entry(_index_entry_for_record(record))
        except RemoteArchiveError as exc:
            warning = (
                f"Remote index update failed (run `qmb jobs reindex --remote` to repair): {exc}"
            )

        return RemoteArchiveResult(
            qmb_job_id=record.qmb_job_id,
            status="exported",
            uri=f"gs://{self.bucket_name}/{remote_prefix}/",
            warning=warning,
        )

    def import_job(
        self,
        job_id: str,
        store: JobStore,
        *,
        overwrite: bool = False,
    ) -> RemoteArchiveResult:
        """Download one remote qmb job archive into ``store``.

        Tries the flat ``<prefix>/<job_id>/`` path first (O(1)). Falls back
        to a prefix list for partial job-id matching (O(all remote jobs)).
        """
        remote_prefix = self._resolve_job_prefix(job_id)
        qmb_job_id = remote_prefix.rsplit("/", 1)[-1]
        return self._download_job(remote_prefix, qmb_job_id, store, overwrite=overwrite)

    def import_session(
        self,
        session_id: str,
        store: JobStore,
        *,
        overwrite: bool = False,
    ) -> list[RemoteArchiveResult]:
        """Download every job archived under a session id.

        Reads the session manifest for the job-id list (O(1) + N downloads).
        Falls back to a full prefix scan if the manifest is missing.
        """
        job_ids = self._remote_session_job_ids(session_id)
        if not job_ids:
            raise RemoteArchiveError(f"No remote qmb jobs found for session: {session_id}")
        return [
            self._download_job(
                self._join_prefix(job_id),
                job_id,
                store,
                overwrite=overwrite,
            )
            for job_id in job_ids
        ]

    def list_jobs(self) -> list[dict[str, Any]]:
        """Return job index entries from ``index.json`` (empty if missing)."""
        data, _generation = self._read_index()
        jobs = data.get("jobs")
        if not isinstance(jobs, dict):
            return []
        return list(jobs.values())

    def fetch_session_manifest(self, session_id: str) -> SessionManifest | None:
        """Fetch one session's manifest blob directly (single GCS round-trip).

        Returns ``None`` if the manifest is missing or corrupt — callers
        (``qmb web``'s on-demand remote session detail) should fall back to a
        derived summary built from ``index.json`` in that case.
        """
        return self._read_remote_manifest(session_id)

    def list_sessions(self) -> list[SessionManifest]:
        """Return every remote session manifest via a ``sessions/`` prefix scan."""
        list_prefix = f"{self._join_prefix('sessions')}/"
        manifests: list[SessionManifest] = []
        for blob in self._client_or_default().list_blobs(self.bucket_name, prefix=list_prefix):
            if not blob.name.endswith(".json"):
                continue
            try:
                manifests.append(SessionManifest.from_json(blob.download_as_text()))
            except Exception:
                continue
        return manifests

    def list_session_blob_names(self) -> list[tuple[str, str | None]]:
        """Return ``(session_id, updated_iso)`` pairs from a names-only scan.

        Cheap stale-index detection: a single paginated ``list_blobs`` call
        over the ``sessions/`` prefix, returning blob names + timestamps only
        — never downloading manifest content. The session id is the blob's
        filename stem, which is ``safe_path_segment``-sanitized and therefore
        only a provisional session id; callers needing the true id (and the
        rest of the manifest) should fetch it via ``fetch_session_manifest``.
        """
        list_prefix = f"{self._join_prefix('sessions')}/"
        pairs: list[tuple[str, str | None]] = []
        for blob in self._client_or_default().list_blobs(self.bucket_name, prefix=list_prefix):
            if not blob.name.endswith(".json"):
                continue
            stem = blob.name.rsplit("/", 1)[-1][: -len(".json")]
            pairs.append((stem, _blob_updated_iso(blob)))
        return pairs

    def build_index(self) -> dict[str, Any]:
        """Rebuild the full index payload from a scan of every remote job.

        Does not persist the result — used by ``reindex --remote`` and as an
        explicit repair path when the incrementally-maintained index drifts.
        """
        list_prefix = f"{self.prefix}/" if self.prefix else ""
        metadata_suffix = "/metadata.json"
        bucket = self._bucket()
        jobs: dict[str, Any] = {}
        for blob in self._client_or_default().list_blobs(self.bucket_name, prefix=list_prefix):
            if not blob.name.endswith(metadata_suffix) or "/sessions/" in blob.name:
                continue
            job_prefix = blob.name[: -len(metadata_suffix)]
            qmb_job_id = job_prefix.rsplit("/", 1)[-1]
            try:
                metadata = json.loads(bucket.blob(blob.name).download_as_bytes().decode("utf-8"))
            except Exception:
                continue
            try:
                query_text = bucket.blob(f"{job_prefix}/query.sql").download_as_bytes().decode(
                    "utf-8"
                )
            except Exception:
                query_text = ""
            jobs[qmb_job_id] = _index_entry_from_metadata(metadata, query_text)
        return {
            "version": INDEX_VERSION,
            "updated_at": _utcnow_iso(),
            "jobs": jobs,
        }

    def write_index(self, data: dict[str, Any]) -> None:
        """Persist a full index payload to ``index.json``, unconditionally."""
        bucket = self._bucket()
        blob = bucket.blob(self._index_key())
        blob.upload_from_string(json.dumps(data, indent=2), content_type="application/json")

    def fetch_job_artifacts(self, job_id: str) -> dict[str, Any]:
        """Fetch one remote job's metadata/query/schema directly into memory.

        Used by ``qmb web`` to serve remote-only job detail pages without
        importing the job into a local :class:`~qmb.jobs.store.JobStore`.
        """
        remote_prefix = self._resolve_job_prefix(job_id)
        bucket = self._bucket()
        try:
            metadata = json.loads(
                bucket.blob(f"{remote_prefix}/metadata.json").download_as_bytes().decode("utf-8")
            )
            query = bucket.blob(f"{remote_prefix}/query.sql").download_as_bytes().decode("utf-8")
            schema = json.loads(
                bucket.blob(f"{remote_prefix}/schema.json").download_as_bytes().decode("utf-8")
            )
        except Exception as exc:
            raise RemoteArchiveError(f"Cannot fetch remote qmb job: {job_id}") from exc
        return {"metadata": metadata, "query": query, "schema": schema}

    def fetch_preview_jsonl(self, job_id: str) -> str:
        """Fetch the raw ``preview.jsonl`` text for one remote job."""
        remote_prefix = self._resolve_job_prefix(job_id)
        bucket = self._bucket()
        try:
            return bucket.blob(f"{remote_prefix}/preview.jsonl").download_as_bytes().decode(
                "utf-8"
            )
        except Exception as exc:
            raise RemoteArchiveError(f"Cannot fetch remote preview for: {job_id}") from exc

    # -- Internals ---------------------------------------------------------

    def _index_key(self) -> str:
        return self._join_prefix(INDEX_BLOB_NAME)

    def _read_index(self) -> tuple[dict[str, Any], int]:
        """Return ``(index payload, generation)``; generation 0 means absent."""
        bucket = self._bucket()
        blob = bucket.blob(self._index_key())
        try:
            raw = blob.download_as_bytes()
        except Exception:
            return _empty_index(), 0
        generation = getattr(blob, "generation", None) or 0
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return _empty_index(), generation
        if not isinstance(data, dict) or not isinstance(data.get("jobs"), dict):
            return _empty_index(), generation
        return data, generation

    def _upsert_index_entry(self, entry: dict[str, Any]) -> None:
        """Merge one job entry into the remote index, retrying on races.

        Uses GCS generation preconditions for optimistic concurrency: reads
        the current blob + generation, writes with ``if_generation_match``
        (``0`` means "create, only if absent"), and re-reads/retries on a
        ``PreconditionFailed`` from a concurrent writer. Never raises past
        ``export_job`` — callers there degrade this to a warning, since
        ``reindex --remote`` can always rebuild the index from a full scan.
        """
        from google.api_core.exceptions import PreconditionFailed

        for _attempt in range(INDEX_WRITE_ATTEMPTS):
            data, generation = self._read_index()
            data = {
                "version": INDEX_VERSION,
                "updated_at": _utcnow_iso(),
                "jobs": {**data.get("jobs", {}), entry["qmb_job_id"]: entry},
            }
            bucket = self._bucket()
            blob = bucket.blob(self._index_key())
            try:
                blob.upload_from_string(
                    json.dumps(data, indent=2),
                    content_type="application/json",
                    if_generation_match=generation,
                )
                return
            except PreconditionFailed:
                continue
        raise RemoteArchiveError(
            "Failed to update remote index.json after concurrent write retries"
        )

    def _download_job(
        self,
        remote_prefix: str,
        qmb_job_id: str,
        store: JobStore,
        *,
        overwrite: bool,
    ) -> RemoteArchiveResult:
        target_dir = store.root / qmb_job_id
        remote_uri = f"gs://{self.bucket_name}/{remote_prefix}/"
        if target_dir.exists() and not overwrite:
            return RemoteArchiveResult(qmb_job_id=qmb_job_id, status="skipped", uri=remote_uri)

        bucket = self._bucket()
        with tempfile.TemporaryDirectory(prefix="qmb-import-") as tmp:
            tmp_dir = Path(tmp)
            for artifact_name in REMOTE_ARTIFACT_NAMES:
                blob = bucket.blob(f"{remote_prefix}/{artifact_name}")
                destination = tmp_dir / artifact_name
                try:
                    blob.download_to_filename(str(destination))
                except Exception as exc:
                    raise RemoteArchiveError(
                        f"Cannot import {qmb_job_id}: missing artifact {artifact_name}"
                    ) from exc
            _validate_download(tmp_dir, qmb_job_id)
            if target_dir.exists():
                shutil.rmtree(target_dir)
            target_dir.parent.mkdir(parents=True, exist_ok=True)
            shutil.copytree(tmp_dir, target_dir)
        try:
            record = store.read(qmb_job_id)
        except CorruptJobError as exc:
            raise RemoteArchiveError(f"Imported corrupt qmb job: {qmb_job_id}") from exc
        # Keep the local session manifest in sync with the imported job.
        store._update_session_manifest(record)
        return RemoteArchiveResult(qmb_job_id=qmb_job_id, status="imported", uri=remote_uri)

    def _resolve_job_prefix(self, job_id: str) -> str:
        """Resolve a full or partial job id to its remote prefix.

        Full ids are O(1) — direct path check. Partial ids fall back to a
        prefix list and substring match.
        """
        exact_prefix = self._join_prefix(job_id)
        if self._blob_exists(f"{exact_prefix}/metadata.json"):
            return exact_prefix

        # Partial-id fallback: list all job prefixes and substring-match.
        list_prefix = f"{self.prefix}/" if self.prefix else ""
        metadata_suffix = "/metadata.json"
        matches = sorted(
            blob.name[: -len(metadata_suffix)]
            for blob in self._client_or_default().list_blobs(
                self.bucket_name,
                prefix=list_prefix,
            )
            if blob.name.endswith(metadata_suffix)
            and job_id in blob.name.rsplit("/", 2)[-2]
        )
        if not matches:
            raise RemoteArchiveError(f"Remote qmb job not found: {job_id}")
        if len(matches) > 1:
            raise RemoteArchiveError(f"Ambiguous remote qmb job id {job_id!r}")
        return matches[0]

    def _remote_session_job_ids(self, session_id: str) -> list[str]:
        """Return job ids for a session, manifest-first with full-scan fallback."""
        manifest = self._read_remote_manifest(session_id)
        if manifest is not None:
            return list(manifest.jobs)
        # Fallback: scan all job metadata and filter by session_id.
        list_prefix = f"{self.prefix}/" if self.prefix else ""
        metadata_suffix = "/metadata.json"
        job_ids: list[str] = []
        for blob in self._client_or_default().list_blobs(
            self.bucket_name,
            prefix=list_prefix,
        ):
            if not blob.name.endswith(metadata_suffix):
                continue
            if "/sessions/" in blob.name:
                continue
            try:
                raw = blob.download_as_bytes() if hasattr(blob, "download_as_bytes") else None
                if raw is None:
                    bucket = self._bucket()
                    raw = bucket.blob(blob.name).download_as_bytes()
                data = json.loads(raw.decode("utf-8"))
            except Exception:
                continue
            if effective_session_id(_SimpleRecord.from_metadata(data)) == session_id:
                job_ids.append(blob.name[: -len(metadata_suffix)].rsplit("/", 1)[-1])
        return job_ids

    def _read_remote_manifest(self, session_id: str) -> SessionManifest | None:
        key = self._manifest_key(session_id)
        bucket = self._bucket()
        blob = bucket.blob(key)
        try:
            raw = blob.download_as_text()
        except Exception:
            return None
        try:
            return SessionManifest.from_json(raw)
        except (ValueError, json.JSONDecodeError):
            return None

    def _update_remote_manifest(self, session_id: str, record: Any) -> None:
        existing = self._read_remote_manifest(session_id)
        try:
            updated = update_manifest_with_job(existing, record)
        except ValueError:
            return
        key = self._manifest_key(session_id)
        bucket = self._bucket()
        blob = bucket.blob(key)
        blob.upload_from_string(updated.to_json(), content_type="application/json")

    def _manifest_key(self, session_id: str) -> str:
        return self._join_prefix("sessions", f"{safe_path_segment(session_id)}.json")

    def _blob_exists(self, key: str) -> bool:
        bucket = self._bucket()
        blob = bucket.blob(key)
        if hasattr(blob, "exists"):
            try:
                return bool(blob.exists())
            except Exception:
                return False
        # Fakes without .exists(): treat as missing so the partial-id path runs.
        return False

    def _job_prefix(self, record: Any) -> str:
        return self._join_prefix(record.qmb_job_id)

    def _join_prefix(self, *parts: str) -> str:
        all_parts = [self.prefix, *parts] if self.prefix else list(parts)
        return "/".join(part.strip("/") for part in all_parts if part.strip("/"))

    def _bucket(self) -> Any:
        return self._client_or_default().bucket(self.bucket_name)

    def _client_or_default(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            from google.cloud import storage
        except ImportError as exc:  # pragma: no cover - depends on optional install state
            raise RemoteArchiveError(
                "GCS remote archives require the google-cloud-storage package."
            ) from exc
        self._client = storage.Client()
        return self._client


@dataclass(frozen=True)
class _SimpleRecord:
    """A minimal record-like view over a metadata dict for session-id lookup."""

    session_id: str | None
    agent_context: Any | None

    @classmethod
    def from_metadata(cls, data: dict[str, Any]) -> _SimpleRecord:
        agent_data = data.get("agent")
        agent = None
        if isinstance(agent_data, dict):
            agent = type(
                "_Agent",
                (),
                {"session_id": agent_data.get("session_id")},
            )()
        return cls(session_id=data.get("session_id"), agent_context=agent)


def _limited_jsonl(path: Path, row_limit: int) -> str:
    rows = []
    with path.open(encoding="utf-8") as f:
        for index, line in enumerate(f):
            if index >= row_limit:
                break
            rows.append(line)
    return "".join(rows)


def _content_type_for_artifact(name: str) -> str:
    if name.endswith(".json"):
        return "application/json"
    if name.endswith(".jsonl"):
        return "application/x-ndjson"
    return "text/plain"


def _validate_download(directory: Path, qmb_job_id: str) -> None:
    metadata_path = directory / "metadata.json"
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RemoteArchiveError(f"Invalid remote metadata for {qmb_job_id}") from exc
    if metadata.get("qmb_job_id") != qmb_job_id:
        raise RemoteArchiveError(
            f"Remote metadata qmb_job_id mismatch: expected {qmb_job_id}, "
            f"got {metadata.get('qmb_job_id')}"
        )


def _blob_updated_iso(blob: Any) -> str | None:
    """Best-effort ISO timestamp from a blob's ``updated`` attribute, if any."""
    updated = getattr(blob, "updated", None)
    if updated is None:
        return None
    if isinstance(updated, str):
        return updated
    isoformat = getattr(updated, "isoformat", None)
    return isoformat() if isoformat is not None else str(updated)


def _empty_index() -> dict[str, Any]:
    return {"version": INDEX_VERSION, "updated_at": None, "jobs": {}}


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


def collapse_excerpt(text: str) -> str:
    """Return the first ``INDEX_EXCERPT_MAX_CHARS`` chars with whitespace collapsed.

    Shared by the remote index builder and the local ``qmb web`` index
    assembly so both produce excerpts with identical semantics.
    """
    return " ".join(text[:INDEX_EXCERPT_MAX_CHARS].split())


def _index_entry_for_record(record: Any) -> dict[str, Any]:
    return {
        "qmb_job_id": record.qmb_job_id,
        "session_id": effective_session_id(record),
        "created_at": record.created_at.isoformat(),
        "engine": record.engine.name,
        "source_label": record.source.label,
        "total_rows": record.total_rows,
        "bytes_processed": record.bytes_processed,
        "query_excerpt": collapse_excerpt(record.query_path.read_text(encoding="utf-8")),
    }


def _index_entry_from_metadata(metadata: dict[str, Any], query_text: str) -> dict[str, Any]:
    stats = metadata.get("stats") or {}
    source = metadata.get("source") or {}
    engine = metadata.get("engine") or {}
    session_id = metadata.get("session_id") or (metadata.get("agent") or {}).get("session_id")
    return {
        "qmb_job_id": metadata.get("qmb_job_id"),
        "session_id": session_id,
        "created_at": metadata.get("created_at"),
        "engine": engine.get("name"),
        "source_label": source.get("label"),
        "total_rows": stats.get("total_rows", 0),
        "bytes_processed": stats.get("bytes_processed", 0),
        "query_excerpt": collapse_excerpt(query_text),
    }
