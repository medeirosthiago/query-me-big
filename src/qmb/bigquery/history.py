"""BigQuery query history fetcher."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from google.cloud import bigquery


@dataclass(frozen=True)
class QueryHistoryEntry:
    job_id: str
    project: str
    location: str
    created: datetime
    query: str
    bytes_processed: int = 0
    state: str = "DONE"
    user_email: str = ""

    @property
    def preview(self) -> str:
        """Return a single-line preview of the query, truncated to 120 chars."""
        line = " ".join(self.query.split())
        return line[:120] if len(line) <= 120 else line[:117] + "..."


def list_recent_queries(
    client: bigquery.Client,
    *,
    days: int = 7,
    limit: int = 200,
    user_email: str | None = None,
) -> list[QueryHistoryEntry]:
    """Return recent query jobs, sorted by created descending.

    By default only the current authenticated user's jobs are listed
    (``all_users=False``). When ``user_email`` is set, all users' jobs are
    fetched (``all_users=True``) and filtered client-side to that user, since
    BigQuery's ``jobs.list`` API has no server-side user filter.
    """
    cutoff = datetime.now(UTC) - timedelta(days=days)
    target = user_email.strip().lower() if user_email and user_email.strip() else None
    entries: list[QueryHistoryEntry] = []

    for job in client.list_jobs(
        all_users=target is not None,
        state_filter="done",
        min_creation_time=cutoff,
        max_results=limit * 3,
    ):
        if job.job_type != "query":
            continue
        if job.parent_job_id:
            continue
        if job.error_result:
            continue
        if target:
            job_user = (job.user_email or "").lower()
            if job_user != target:
                continue
        query_text = job.query
        if not query_text:
            continue
        entries.append(
            QueryHistoryEntry(
                job_id=job.job_id,
                project=job.project,
                location=job.location,
                created=job.created,
                query=query_text,
                bytes_processed=job.total_bytes_processed or 0,
                state=job.state,
                user_email=job.user_email or "",
            )
        )
        if len(entries) >= limit:
            break

    entries.sort(key=lambda e: e.created, reverse=True)
    return entries
