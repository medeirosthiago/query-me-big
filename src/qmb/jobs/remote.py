"""Remote qmb job archive backends."""

from __future__ import annotations

import json
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from qmb.jobs.store import CorruptJobError, JobStore

REMOTE_ARTIFACT_NAMES = ("metadata.json", "query.sql", "schema.json", "preview.jsonl")


class RemoteArchiveError(Exception):
    """Raised when a remote archive operation fails."""


@dataclass(frozen=True)
class RemoteArchiveResult:
    """Machine-readable result for a remote archive operation."""

    qmb_job_id: str
    status: str
    uri: str | None = None
    error: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        return {
            "qmb_job_id": self.qmb_job_id,
            "status": self.status,
            "uri": self.uri,
            "error": self.error,
        }


def get_remote_archive(destination: str, *, client: Any | None = None) -> GcsRemoteArchive:
    """Return the remote archive backend for ``destination``."""
    if destination.startswith("gs://"):
        return GcsRemoteArchive(destination, client=client)
    raise RemoteArchiveError(f"Unsupported remote archive destination: {destination}")


def safe_path_segment(value: str | None) -> str:
    """Return a GCS-safe path segment for a session id."""
    if not value:
        return "unknown"
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return safe.strip("._-") or "unknown"


class GcsRemoteArchive:
    """GCS-backed qmb remote archive.

    The remote layout mirrors qmb's local archive shape:

    ``gs://bucket/prefix/sessions/<session_id>/<qmb_job_id>/<artifact>``.
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
        """Upload one local qmb job archive to GCS."""
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
        return RemoteArchiveResult(
            qmb_job_id=record.qmb_job_id,
            status="exported",
            uri=f"gs://{self.bucket_name}/{remote_prefix}/",
        )

    def import_job(
        self,
        job_id: str,
        store: JobStore,
        *,
        overwrite: bool = False,
    ) -> RemoteArchiveResult:
        """Download one remote qmb job archive into ``store``."""
        remote_prefix = self._find_job_prefix(job_id)
        qmb_job_id = remote_prefix.rsplit("/", 1)[-1]
        return self._download_job(remote_prefix, qmb_job_id, store, overwrite=overwrite)

    def import_session(
        self,
        session_id: str,
        store: JobStore,
        *,
        overwrite: bool = False,
    ) -> list[RemoteArchiveResult]:
        """Download every job archived under a session id."""
        session_prefix = self._join_prefix("sessions", safe_path_segment(session_id))
        metadata_suffix = "/metadata.json"
        prefixes = sorted(
            blob.name[: -len(metadata_suffix)]
            for blob in self._client_or_default().list_blobs(
                self.bucket_name,
                prefix=f"{session_prefix}/",
            )
            if blob.name.endswith(metadata_suffix)
        )
        if not prefixes:
            raise RemoteArchiveError(f"No remote qmb jobs found for session: {session_id}")
        return [
            self._download_job(prefix, prefix.rsplit("/", 1)[-1], store, overwrite=overwrite)
            for prefix in prefixes
        ]

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
            store.read(qmb_job_id)
        except CorruptJobError as exc:
            raise RemoteArchiveError(f"Imported corrupt qmb job: {qmb_job_id}") from exc
        return RemoteArchiveResult(qmb_job_id=qmb_job_id, status="imported", uri=remote_uri)

    def _find_job_prefix(self, job_id: str) -> str:
        metadata_suffix = "/metadata.json"
        search_prefix = self._join_prefix("sessions")
        matches = sorted(
            blob.name[: -len(metadata_suffix)]
            for blob in self._client_or_default().list_blobs(
                self.bucket_name,
                prefix=f"{search_prefix}/",
            )
            if blob.name.endswith(f"/{job_id}/metadata.json")
            or (blob.name.endswith(metadata_suffix) and job_id in blob.name.rsplit("/", 2)[-2])
        )
        if not matches:
            raise RemoteArchiveError(f"Remote qmb job not found: {job_id}")
        if len(matches) > 1:
            raise RemoteArchiveError(f"Ambiguous remote qmb job id {job_id!r}")
        return matches[0]

    def _job_prefix(self, record: Any) -> str:
        session_id = getattr(record, "session_id", None)
        if session_id is None and getattr(record, "agent_context", None) is not None:
            session_id = record.agent_context.session_id
        return self._join_prefix("sessions", safe_path_segment(session_id), record.qmb_job_id)

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
