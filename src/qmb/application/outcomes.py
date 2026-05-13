"""Result types from running the qmb application pipeline."""

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from qmb.application.resolver import ResolutionTrace
from qmb.types import QueryResultHandle, ResolvedQuery

if TYPE_CHECKING:
    from qmb.jobs.models import JobRecord

__all__ = ["ExecutionOutcome"]


@dataclass(frozen=True)
class ExecutionOutcome:
    """The non-UI result of running the qmb pipeline.

    Carries everything the CLI needs to render status messages and decide
    whether to open the TUI, without the pipeline having to print anything.

    `client` is the live BigQuery client used for execution. It is included
    here so the caller can reuse it (e.g. to hand to the TUI) without
    re-initializing. This is a small infrastructure leak — acceptable
    because the alternative is creating a second client.
    """

    resolved: ResolvedQuery
    handle: QueryResultHandle
    client: Any  # google.cloud.bigquery.Client; typed as Any to avoid the import
    trace: ResolutionTrace = ResolutionTrace()
    exported_path: Path | None = None
    exported_rows: int | None = None
    archived_job: "JobRecord | None" = None
    archive_error: str | None = None
    dry_run: bool = False
