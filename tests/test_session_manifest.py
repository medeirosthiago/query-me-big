from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace

from qmb.jobs.session_manifest import (
    SessionManifest,
    effective_session_id,
    recompute_from_jobs,
    update_manifest_with_job,
)


def _record(
    *,
    qmb_job_id: str,
    created_at: datetime,
    session_id: str | None = "agent-42",
    bytes_processed: int = 100,
    agent_name: str | None = "pi",
    agent_task: str | None = "investigate billing",
    agent_cwd: str | None = "/home/user/src/proj-b",
    agent_session_id: str | None = None,
) -> SimpleNamespace:
    agent = None
    if agent_name or agent_session_id or agent_task or agent_cwd:
        agent = SimpleNamespace(
            name=agent_name,
            session_id=agent_session_id,
            task=agent_task,
            cwd=agent_cwd,
        )
    return SimpleNamespace(
        qmb_job_id=qmb_job_id,
        created_at=created_at,
        session_id=session_id,
        bytes_processed=bytes_processed,
        agent_context=agent,
    )


def test_update_manifest_with_job_starts_a_fresh_manifest() -> None:
    record = _record(
        qmb_job_id="qmb_1",
        created_at=datetime(2026, 7, 1, 12, 0, 0, tzinfo=UTC),
        bytes_processed=500,
    )

    manifest = update_manifest_with_job(None, record)

    assert manifest.session_id == "agent-42"
    assert manifest.jobs == ("qmb_1",)
    assert manifest.count == 1
    assert manifest.bytes_processed == 500
    assert manifest.first == "2026-07-01T12:00:00+00:00"
    assert manifest.latest == "2026-07-01T12:00:00+00:00"
    assert manifest.agents == ("pi",)
    assert manifest.tasks == ("investigate billing",)
    assert manifest.cwds == ("/home/user/src/proj-b",)
    assert manifest.updated_at == "2026-07-01T12:00:00+00:00"


def test_update_manifest_with_job_appends_and_recomputes_aggregates() -> None:
    first = _record(
        qmb_job_id="qmb_1",
        created_at=datetime(2026, 7, 1, 12, 0, 0, tzinfo=UTC),
        bytes_processed=500,
    )
    second = _record(
        qmb_job_id="qmb_2",
        created_at=datetime(2026, 7, 1, 13, 0, 0, tzinfo=UTC),
        bytes_processed=1500,
        agent_name="codex",
        agent_task="build dashboard",
        agent_cwd="/home/user/src/proj-a",
    )

    manifest = update_manifest_with_job(None, first)
    manifest = update_manifest_with_job(manifest, second)

    assert manifest.jobs == ("qmb_2", "qmb_1")
    assert manifest.count == 2
    assert manifest.bytes_processed == 2000
    assert manifest.first == "2026-07-01T12:00:00+00:00"
    assert manifest.latest == "2026-07-01T13:00:00+00:00"
    assert manifest.agents == ("codex", "pi")
    assert manifest.tasks == ("build dashboard", "investigate billing")
    assert manifest.cwds == ("/home/user/src/proj-a", "/home/user/src/proj-b")


def test_update_manifest_with_job_is_idempotent_on_replay() -> None:
    record = _record(
        qmb_job_id="qmb_1",
        created_at=datetime(2026, 7, 1, 12, 0, 0, tzinfo=UTC),
    )
    manifest = update_manifest_with_job(None, record)
    replayed = update_manifest_with_job(manifest, record)

    assert replayed is manifest


def test_update_manifest_with_job_rejects_session_mismatch() -> None:
    manifest = SessionManifest(session_id="agent-42", jobs=("qmb_1",), count=1)
    other = _record(
        qmb_job_id="qmb_2",
        created_at=datetime(2026, 7, 1, 12, 0, 0, tzinfo=UTC),
        session_id="agent-99",
    )

    try:
        update_manifest_with_job(manifest, other)
    except ValueError as exc:
        assert "agent-99" in str(exc)
    else:
        raise AssertionError("Expected ValueError on session mismatch")


def test_update_manifest_with_job_rejects_session_less_job() -> None:
    record = _record(
        qmb_job_id="qmb_1",
        created_at=datetime(2026, 7, 1, 12, 0, 0, tzinfo=UTC),
        session_id=None,
        agent_name=None,
        agent_task=None,
        agent_cwd=None,
    )

    try:
        update_manifest_with_job(None, record)
    except ValueError as exc:
        assert "session-less" in str(exc)
    else:
        raise AssertionError("Expected ValueError for session-less job")


def test_recompute_from_jobs_builds_manifest_from_scratch() -> None:
    records = [
        _record(
            qmb_job_id="qmb_2",
            created_at=datetime(2026, 7, 1, 13, 0, 0, tzinfo=UTC),
            bytes_processed=1500,
            agent_name="codex",
        ),
        _record(
            qmb_job_id="qmb_1",
            created_at=datetime(2026, 7, 1, 12, 0, 0, tzinfo=UTC),
            bytes_processed=500,
            agent_name="pi",
        ),
        _record(
            qmb_job_id="qmb_3",
            created_at=datetime(2026, 7, 1, 14, 0, 0, tzinfo=UTC),
            bytes_processed=3000,
            agent_name="pi",
        ),
    ]

    manifest = recompute_from_jobs("agent-42", records)

    assert manifest.jobs == ("qmb_3", "qmb_2", "qmb_1")
    assert manifest.count == 3
    assert manifest.bytes_processed == 5000
    assert manifest.first == "2026-07-01T12:00:00+00:00"
    assert manifest.latest == "2026-07-01T14:00:00+00:00"
    assert manifest.agents == ("codex", "pi")
    assert manifest.updated_at == "2026-07-01T14:00:00+00:00"


def test_recompute_from_jobs_empty_list_returns_empty_manifest() -> None:
    manifest = recompute_from_jobs("agent-42", [])

    assert manifest.session_id == "agent-42"
    assert manifest.jobs == ()
    assert manifest.count == 0
    assert manifest.first is None
    assert manifest.latest is None
    assert manifest.updated_at is None


def test_manifest_round_trips_through_json() -> None:
    record = _record(
        qmb_job_id="qmb_1",
        created_at=datetime(2026, 7, 1, 12, 0, 0, tzinfo=UTC),
    )
    manifest = update_manifest_with_job(None, record)

    text = manifest.to_json()
    data = json.loads(text)
    assert data["version"] == 1
    assert data["session_id"] == "agent-42"
    assert data["jobs"] == ["qmb_1"]

    rebuilt = SessionManifest.from_json(text)
    assert rebuilt == manifest


def test_effective_session_id_prefers_top_level_field() -> None:
    record = SimpleNamespace(
        session_id="top-level",
        agent_context=SimpleNamespace(name="pi", session_id="agent-only"),
    )
    assert effective_session_id(record) == "top-level"


def test_effective_session_id_falls_back_to_agent_context() -> None:
    record = SimpleNamespace(
        session_id=None,
        agent_context=SimpleNamespace(name="pi", session_id="agent-only"),
    )
    assert effective_session_id(record) == "agent-only"


def test_effective_session_id_returns_none_for_session_less_job() -> None:
    record = SimpleNamespace(session_id=None, agent_context=None)
    assert effective_session_id(record) is None


def test_effective_session_id_handles_missing_agent_context() -> None:
    record = SimpleNamespace(session_id=None, agent_context=None)
    assert effective_session_id(record) is None
