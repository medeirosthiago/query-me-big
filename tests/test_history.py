from datetime import UTC, datetime
from types import SimpleNamespace

from qmb.bigquery.history import QueryHistoryEntry, list_recent_queries


def _make_job(
    *,
    job_type: str = "query",
    parent_job_id: str | None = None,
    error_result: dict | None = None,
    query: str = "SELECT 1",
    job_id: str = "job-1",
    project: str = "proj",
    location: str = "US",
    created: datetime | None = None,
    total_bytes_processed: int = 1024,
    state: str = "DONE",
) -> SimpleNamespace:
    return SimpleNamespace(
        job_type=job_type,
        parent_job_id=parent_job_id,
        error_result=error_result,
        query=query,
        job_id=job_id,
        project=project,
        location=location,
        created=created or datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC),
        total_bytes_processed=total_bytes_processed,
        state=state,
    )


def _make_client(jobs: list[SimpleNamespace]) -> SimpleNamespace:
    return SimpleNamespace(list_jobs=lambda **_kwargs: jobs)


def test_query_history_entry_preview_truncates() -> None:
    long_query = "SELECT\n" + "a, " * 80 + "b\nFROM table"
    entry = QueryHistoryEntry(
        job_id="j",
        project="p",
        location="US",
        created=datetime(2026, 4, 1, tzinfo=UTC),
        query=long_query,
    )

    assert "\n" not in entry.preview
    assert len(entry.preview) == 120
    assert entry.preview.endswith("...")

    short_entry = QueryHistoryEntry(
        job_id="j",
        project="p",
        location="US",
        created=datetime(2026, 4, 1, tzinfo=UTC),
        query="SELECT 1",
    )
    assert short_entry.preview == "SELECT 1"


def test_list_recent_queries_filters_non_query_jobs() -> None:
    jobs = [
        _make_job(job_id="q1", job_type="query"),
        _make_job(job_id="l1", job_type="load"),
        _make_job(job_id="c1", job_type="copy"),
        _make_job(job_id="q2", job_type="query"),
    ]
    client = _make_client(jobs)

    entries = list_recent_queries(client)

    assert [e.job_id for e in entries] == ["q1", "q2"]


def test_list_recent_queries_skips_child_jobs() -> None:
    jobs = [
        _make_job(job_id="parent"),
        _make_job(job_id="child", parent_job_id="parent"),
    ]
    client = _make_client(jobs)

    entries = list_recent_queries(client)

    assert [e.job_id for e in entries] == ["parent"]


def test_list_recent_queries_skips_errored_jobs() -> None:
    jobs = [
        _make_job(job_id="ok"),
        _make_job(job_id="err", error_result={"reason": "invalidQuery"}),
    ]
    client = _make_client(jobs)

    entries = list_recent_queries(client)

    assert [e.job_id for e in entries] == ["ok"]


def test_list_recent_queries_skips_empty_query() -> None:
    jobs = [
        _make_job(job_id="ok"),
        _make_job(job_id="empty", query=""),
    ]
    client = _make_client(jobs)

    entries = list_recent_queries(client)

    assert [e.job_id for e in entries] == ["ok"]


def test_list_recent_queries_respects_limit() -> None:
    jobs = [_make_job(job_id=f"j{i}") for i in range(5)]
    client = _make_client(jobs)

    entries = list_recent_queries(client, limit=3)

    assert len(entries) == 3


def test_list_recent_queries_sorted_by_created_desc() -> None:
    jobs = [
        _make_job(job_id="old", created=datetime(2026, 4, 1, 10, 0, 0, tzinfo=UTC)),
        _make_job(job_id="new", created=datetime(2026, 4, 3, 10, 0, 0, tzinfo=UTC)),
        _make_job(job_id="mid", created=datetime(2026, 4, 2, 10, 0, 0, tzinfo=UTC)),
    ]
    client = _make_client(jobs)

    entries = list_recent_queries(client)

    assert [e.job_id for e in entries] == ["new", "mid", "old"]
