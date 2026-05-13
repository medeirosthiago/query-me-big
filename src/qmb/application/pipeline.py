"""End-to-end query pipeline orchestrator.

The pipeline encapsulates the steps that used to live inline in
``cli._execute``:

    1. Create a BigQuery client.
    2. Resolve the request to SQL.
    3. Apply an optional ``--where`` wrap.
    4. Execute (or dry-run) the query.
    5. Optionally export the results.

It is intentionally free of console output and UI concerns — the CLI
inspects the returned :class:`ExecutionOutcome` and decides what to
print and whether to launch the TUI.
"""

from collections.abc import Sequence
from itertools import islice
from typing import Any

# Import modules (not their members) so test monkeypatches against
# the canonical module paths are picked up at call time.
from qmb.application.outcomes import ExecutionOutcome
from qmb.application.protocols import SqlResolver
from qmb.application.resolver import apply_where, resolve_request_to_sql
from qmb.bigquery import client as _bq_client
from qmb.bigquery import executor as _bq_executor
from qmb.bigquery import exporters as _bq_exporters
from qmb.bigquery import pager as _bq_pager
from qmb.jobs.models import EngineMetadata, JobRecord, SourceMetadata
from qmb.sql.resolver import PlainSqlResolver
from qmb.types import QueryRequest

__all__ = ["run_query_pipeline"]


def run_query_pipeline(
    request: QueryRequest,
    *,
    resolvers: Sequence[SqlResolver] | None = None,
    job_store: Any | None = None,
    ignore_archive_errors: bool = False,
) -> ExecutionOutcome:
    """Run the full qmb pipeline for a request and return the outcome.

    Does no console I/O and does not launch the TUI. The caller is
    expected to render status from the returned :class:`ExecutionOutcome`.

    ``resolvers`` is the ordered list of :class:`SqlResolver` instances
    used to turn the request into SQL. When ``None`` (the default), only
    plain SQL/file resolution is supported — callers that want dbt
    resolution must pass a list that includes
    :class:`qmb.dbt.integration.DbtSqlResolver`. The CLI does this at
    its wiring boundary.
    """
    execution = request.execution
    output = request.output

    if resolvers is None:
        resolvers = [PlainSqlResolver()]

    client = _bq_client.get_client(execution.project, execution.location)

    resolved, trace = resolve_request_to_sql(request, resolvers)
    resolved = apply_where(resolved, execution.where)

    if execution.dry_run:
        handle = _bq_executor.execute_query(
            client,
            resolved,
            dry_run=True,
            max_bytes_billed=execution.max_bytes_billed,
        )
        return ExecutionOutcome(
            resolved=resolved,
            handle=handle,
            client=client,
            trace=trace,
            dry_run=True,
        )

    handle = _bq_executor.execute_query(
        client,
        resolved,
        max_bytes_billed=execution.max_bytes_billed,
    )

    exported_path = None
    exported_rows = None
    if output.export_format and output.export_path:
        exported_rows = _bq_exporters.export_results(
            client, handle, output.export_format, output.export_path
        )
        exported_path = output.export_path

    archived_job = None
    archive_error: str | None = None
    if job_store is not None:
        try:
            archived_job = _archive_job(
                job_store,
                request=request,
                resolved=resolved,
                client=client,
                handle=handle,
                resolver_name=_resolver_name(request),
                matched_node_id=trace.matched_node_id,
            )
        except Exception as exc:
            if not ignore_archive_errors:
                raise
            archive_error = f"{type(exc).__name__}: {exc}"

    return ExecutionOutcome(
        resolved=resolved,
        handle=handle,
        client=client,
        trace=trace,
        exported_path=exported_path,
        exported_rows=exported_rows,
        archived_job=archived_job,
        archive_error=archive_error,
    )


def _archive_job(
    job_store: Any,
    *,
    request: QueryRequest,
    resolved: Any,
    client: Any,
    handle: Any,
    resolver_name: str,
    matched_node_id: str | None,
) -> JobRecord:
    preview_rows = list(islice(_bq_pager.iter_all_rows(client, handle), 500))
    schema = handle.schema_fields
    return job_store.create(
        resolved_sql=resolved.sql,
        schema=schema,
        preview_rows=preview_rows,
        source=SourceMetadata(
            label=resolved.source_label,
            input_mode=request.mode.value,
            file_path=str(request.file_path) if request.file_path is not None else None,
            model_name=request.model_name,
            manifest_path=str(request.manifest_path) if request.manifest_path is not None else None,
            resolver=resolver_name,
            matched_node_id=matched_node_id,
        ),
        engine=EngineMetadata(
            name="bigquery",
            job_id=handle.job_id,
            project=handle.project,
            location=handle.location,
        ),
        total_rows=handle.total_rows,
        bytes_processed=handle.bytes_processed,
        execution_seconds=handle.execution_seconds,
    )


def _resolver_name(request: QueryRequest) -> str:
    if request.mode.value == "model" or request.resolve_dbt:
        return "dbt"
    return "plain"
