"""Session manifests: a regenerable index from session id to job ids.

The session manifest is a cached aggregate over the jobs that belong to a
session. It is never the source of truth — ``metadata.json`` inside each job
remains authoritative for that job's ``session_id``. Manifests exist only to
make session-level operations (``jobs sessions``, ``jobs list --session-id``,
``jobs import --session-id``) O(1)+N instead of O(all jobs).

Because jobs are immutable once created, the manifest only ever grows on
``create()`` / ``export_job()``; there is no mutation drift. If a manifest is
missing or corrupt, callers fall back to a full scan and rebuild — worst case
is staleness, never data loss.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from qmb.jobs.models import JobRecord

MANIFEST_VERSION = 1


def safe_path_segment(value: str | None) -> str:
    """Return a filesystem/object-safe path segment for a session id."""
    if not value:
        return "unknown"
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return safe.strip("._-") or "unknown"


@dataclass(frozen=True)
class SessionManifest:
    """Cached aggregate over the jobs in one session."""

    session_id: str
    jobs: tuple[str, ...] = ()
    count: int = 0
    first: str | None = None  # ISO 8601, earliest created_at
    latest: str | None = None  # ISO 8601, latest created_at
    bytes_processed: int = 0
    agents: tuple[str, ...] = ()
    tasks: tuple[str, ...] = ()
    cwds: tuple[str, ...] = ()
    updated_at: str | None = None  # ISO 8601

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": MANIFEST_VERSION,
            "session_id": self.session_id,
            "jobs": list(self.jobs),
            "count": self.count,
            "first": self.first,
            "latest": self.latest,
            "bytes_processed": self.bytes_processed,
            "agents": list(self.agents),
            "tasks": list(self.tasks),
            "cwds": list(self.cwds),
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SessionManifest":
        return cls(
            session_id=data["session_id"],
            jobs=tuple(data.get("jobs") or []),
            count=int(data.get("count") or 0),
            first=data.get("first"),
            latest=data.get("latest"),
            bytes_processed=int(data.get("bytes_processed") or 0),
            agents=tuple(data.get("agents") or []),
            tasks=tuple(data.get("tasks") or []),
            cwds=tuple(data.get("cwds") or []),
            updated_at=data.get("updated_at"),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_json(cls, text: str) -> "SessionManifest":
        return cls.from_dict(json.loads(text))


def effective_session_id(record: JobRecord | Any) -> str | None:
    """Return the archived session id, including legacy agent-only records.

    Older qmb archives wrote ``session_id`` only inside ``agent.session_id``;
    newer ones also write the top-level ``session_id`` field. This helper
    centralizes the fallback so callers don't reimplement it.
    """
    if getattr(record, "session_id", None):
        return record.session_id
    agent = getattr(record, "agent_context", None)
    if agent is not None and getattr(agent, "session_id", None):
        return agent.session_id
    return None


def recompute_from_jobs(session_id: str, records: list[JobRecord]) -> SessionManifest:
    """Build a fresh manifest from a list of job records.

    All records are assumed to belong to ``session_id`` (callers filter first
    via :func:`effective_session_id`). Order does not matter; the manifest is
    sorted/derived deterministically.
    """
    sorted_records = sorted(records, key=lambda r: r.created_at)
    jobs: list[str] = []
    agents: set[str] = set()
    tasks: set[str] = set()
    cwds: set[str] = set()
    bytes_total = 0
    first: datetime | None = None
    latest: datetime | None = None
    for record in sorted_records:
        jobs.append(record.qmb_job_id)
        bytes_total += int(getattr(record, "bytes_processed", 0) or 0)
        if first is None or record.created_at < first:
            first = record.created_at
        if latest is None or record.created_at > latest:
            latest = record.created_at
        agent = getattr(record, "agent_context", None)
        if agent is not None:
            if getattr(agent, "name", None):
                agents.add(agent.name)
            if getattr(agent, "task", None):
                tasks.add(agent.task)
            if getattr(agent, "cwd", None):
                cwds.add(agent.cwd)
    return SessionManifest(
        session_id=session_id,
        jobs=tuple(jobs),
        count=len(jobs),
        first=first.isoformat() if first else None,
        latest=latest.isoformat() if latest else None,
        bytes_processed=bytes_total,
        agents=tuple(sorted(agents)),
        tasks=tuple(sorted(tasks)),
        cwds=tuple(sorted(cwds)),
        updated_at=latest.isoformat() if latest else None,
    )


@dataclass
class _ManifestBuilder:
    """Mutable accumulator used while folding job records into a manifest."""

    session_id: str
    jobs: list[str] = field(default_factory=list)
    bytes_processed: int = 0
    first: datetime | None = None
    latest: datetime | None = None
    agents: set[str] = field(default_factory=set)
    tasks: set[str] = field(default_factory=set)
    cwds: set[str] = field(default_factory=set)

    def add(self, record: JobRecord) -> None:
        self.jobs.append(record.qmb_job_id)
        self.bytes_processed += int(getattr(record, "bytes_processed", 0) or 0)
        created = record.created_at
        if self.first is None or created < self.first:
            self.first = created
        if self.latest is None or created > self.latest:
            self.latest = created
        agent = getattr(record, "agent_context", None)
        if agent is not None:
            if getattr(agent, "name", None):
                self.agents.add(agent.name)
            if getattr(agent, "task", None):
                self.tasks.add(agent.task)
            if getattr(agent, "cwd", None):
                self.cwds.add(agent.cwd)

    def build(self, *, updated_at: datetime) -> SessionManifest:
        return SessionManifest(
            session_id=self.session_id,
            jobs=tuple(self.jobs),
            count=len(self.jobs),
            first=self.first.isoformat() if self.first else None,
            latest=self.latest.isoformat() if self.latest else None,
            bytes_processed=self.bytes_processed,
            agents=tuple(sorted(self.agents)),
            tasks=tuple(sorted(self.tasks)),
            cwds=tuple(sorted(self.cwds)),
            updated_at=updated_at.isoformat(),
        )


def update_manifest_with_job(
    manifest: SessionManifest | None,
    record: JobRecord,
    *,
    updated_at: datetime | None = None,
) -> SessionManifest:
    """Return a new manifest with ``record`` folded in.

    If ``manifest`` is None, a fresh manifest for the record's session is
    started. If ``manifest`` already lists the record's job id, it is returned
    unchanged (idempotent on replay).
    """
    session_id = effective_session_id(record)
    if session_id is None:
        raise ValueError("Cannot update a session manifest for a session-less job")
    stamp = updated_at or record.created_at

    if manifest is None:
        builder = _ManifestBuilder(session_id=session_id)
    else:
        if manifest.session_id != session_id:
            raise ValueError(
                f"Manifest session {manifest.session_id!r} does not match "
                f"record session {session_id!r}"
            )
        if record.qmb_job_id in manifest.jobs:
            return manifest
        builder = _ManifestBuilder(
            session_id=manifest.session_id,
            jobs=list(manifest.jobs),
            bytes_processed=manifest.bytes_processed,
            first=_parse_iso(manifest.first) if manifest.first else None,
            latest=_parse_iso(manifest.latest) if manifest.latest else None,
            agents=set(manifest.agents),
            tasks=set(manifest.tasks),
            cwds=set(manifest.cwds),
        )
    builder.add(record)
    return builder.build(updated_at=stamp)


def _parse_iso(text: str) -> datetime:
    return datetime.fromisoformat(text)
