"""Filesystem-backed local qmb job archive store."""

import json
import os
import secrets
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from qmb.jobs.artifacts import write_jsonl_rows
from qmb.jobs.models import EngineMetadata, JobRecord, SourceMetadata
from qmb.types import AgentContext, SchemaField


class JobStoreError(Exception):
    """Base class for job archive errors."""


class JobNotFoundError(JobStoreError):
    """Raised when a qmb job ID or partial ID cannot be found."""


class AmbiguousJobIdError(JobStoreError):
    """Raised when a partial qmb job ID matches multiple jobs."""


class CorruptJobError(JobStoreError):
    """Raised when a job directory exists but cannot be parsed."""


class JobStore:
    """Store qmb job records as immutable directories of local artifacts."""

    def __init__(
        self,
        root: Path | None = None,
        *,
        now: Callable[[], datetime] | None = None,
        nonce: Callable[[], str] | None = None,
    ) -> None:
        self.root = root or default_jobs_dir()
        self._now = now or (lambda: datetime.now(UTC))
        self._nonce = nonce or (lambda: secrets.token_hex(3))

    def create(
        self,
        *,
        resolved_sql: str,
        schema: list[SchemaField],
        preview_rows: Iterable[dict[str, Any]],
        source: SourceMetadata,
        engine: EngineMetadata,
        total_rows: int,
        bytes_processed: int = 0,
        execution_seconds: float = 0.0,
        session_id: str | None = None,
        parent_job_id: str | None = None,
        agent_context: AgentContext | None = None,
        result_path: Path | None = None,
    ) -> JobRecord:
        """Create a new job record and write its archive artifacts."""
        created_at = self._now()
        qmb_job_id = self._new_job_id(created_at)
        directory = self.root / qmb_job_id
        directory.mkdir(parents=True, exist_ok=False)

        metadata_path = directory / "metadata.json"
        query_path = directory / "query.sql"
        schema_path = directory / "schema.json"
        preview_path = directory / "preview.jsonl"

        query_path.write_text(resolved_sql, encoding="utf-8")
        schema_path.write_text(
            json.dumps([field.to_mapping() for field in schema], indent=2),
            encoding="utf-8",
        )
        write_jsonl_rows(
            preview_path,
            preview_rows,
            fieldnames=[field.name for field in schema],
        )

        record = JobRecord(
            qmb_job_id=qmb_job_id,
            created_at=created_at,
            session_id=session_id,
            parent_job_id=parent_job_id,
            agent_context=agent_context,
            source=source,
            engine=engine,
            total_rows=total_rows,
            bytes_processed=bytes_processed,
            execution_seconds=execution_seconds,
            directory=directory,
            metadata_path=metadata_path,
            query_path=query_path,
            schema_path=schema_path,
            preview_path=preview_path,
            result_path=result_path,
            schema=schema,
        )
        metadata_path.write_text(
            json.dumps(record.to_metadata(), indent=2),
            encoding="utf-8",
        )
        return record

    def read(self, job_id: str) -> JobRecord:
        """Read a job record by full or unambiguous partial qmb job ID."""
        resolved_id = self.resolve_id(job_id)
        return self._read_full_id(resolved_id)

    def list(self) -> list[JobRecord]:
        """List valid jobs sorted newest first."""
        if not self.root.exists():
            return []

        records: list[JobRecord] = []
        for child in self.root.iterdir():
            if not child.is_dir():
                continue
            try:
                records.append(self._read_full_id(child.name))
            except (CorruptJobError, JobNotFoundError):
                continue
        records.sort(key=lambda record: record.created_at, reverse=True)
        return records

    def resolve_id(self, job_id: str) -> str:
        """Resolve a full or unambiguous partial qmb job ID to the full ID."""
        exact_dir = self.root / job_id
        if exact_dir.is_dir():
            return job_id

        if not self.root.exists():
            raise JobNotFoundError(f"Job not found: {job_id}")

        matches = [
            child.name
            for child in self.root.iterdir()
            if child.is_dir() and job_id in child.name
        ]
        if not matches:
            raise JobNotFoundError(f"Job not found: {job_id}")
        if len(matches) > 1:
            raise AmbiguousJobIdError(f"Ambiguous job ID {job_id!r}: {', '.join(matches)}")
        return matches[0]

    def _new_job_id(self, created_at: datetime) -> str:
        timestamp = created_at.strftime("%Y-%m-%d_%H-%M-%S")
        return f"qmb_{timestamp}_{self._nonce()}"

    def _read_full_id(self, qmb_job_id: str) -> JobRecord:
        directory = self.root / qmb_job_id
        metadata_path = directory / "metadata.json"
        if not directory.is_dir() or not metadata_path.exists():
            raise JobNotFoundError(f"Job not found: {qmb_job_id}")

        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            artifacts = metadata["artifacts"]
            schema_path = directory / artifacts["schema"]
            schema_data = json.loads(schema_path.read_text(encoding="utf-8"))
            schema = [SchemaField.from_mapping(field) for field in schema_data]
            result_name = artifacts.get("result")
            stats = metadata["stats"]
            agent_data = metadata.get("agent")
            agent_context = (
                AgentContext.from_mapping(agent_data) if isinstance(agent_data, dict) else None
            )
            return JobRecord(
                qmb_job_id=metadata["qmb_job_id"],
                created_at=datetime.fromisoformat(metadata["created_at"]),
                session_id=metadata.get("session_id"),
                parent_job_id=metadata.get("parent_job_id"),
                agent_context=agent_context,
                source=SourceMetadata.from_mapping(metadata["source"]),
                engine=EngineMetadata.from_mapping(metadata["engine"]),
                total_rows=stats["total_rows"],
                bytes_processed=stats["bytes_processed"],
                execution_seconds=stats["execution_seconds"],
                directory=directory,
                metadata_path=directory / artifacts["metadata"],
                query_path=directory / artifacts["query"],
                schema_path=schema_path,
                preview_path=directory / artifacts["preview"],
                result_path=directory / result_name if result_name is not None else None,
                schema=schema,
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError, OSError) as e:
            raise CorruptJobError(f"Corrupt job archive: {qmb_job_id}") from e


def default_jobs_dir() -> Path:
    """Return the default qmb jobs archive directory."""
    if env_dir := os.environ.get("QMB_JOBS_DIR"):
        return Path(env_dir)
    return Path.home() / ".qmb" / "jobs"
