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

# Import modules (not their members) so test monkeypatches against
# the canonical module paths are picked up at call time.
from qmb.application.outcomes import ExecutionOutcome
from qmb.application.resolver import apply_where, resolve_request_to_sql
from qmb.bigquery import client as _bq_client
from qmb.bigquery import executor as _bq_executor
from qmb.bigquery import exporters as _bq_exporters
from qmb.types import QueryRequest

__all__ = ["run_query_pipeline"]


def run_query_pipeline(request: QueryRequest) -> ExecutionOutcome:
    """Run the full qmb pipeline for a request and return the outcome.

    Does no console I/O and does not launch the TUI. The caller is
    expected to render status from the returned :class:`ExecutionOutcome`.
    """
    client = _bq_client.get_client(request.project, request.location)

    resolved, trace = resolve_request_to_sql(request)
    resolved = apply_where(resolved, request.where)

    if request.dry_run:
        handle = _bq_executor.execute_query(
            client,
            resolved,
            dry_run=True,
            max_bytes_billed=request.max_bytes_billed,
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
        max_bytes_billed=request.max_bytes_billed,
    )

    exported_path = None
    exported_rows = None
    if request.export_format and request.export_path:
        exported_rows = _bq_exporters.export_results(
            client, handle, request.export_format, request.export_path
        )
        exported_path = request.export_path

    return ExecutionOutcome(
        resolved=resolved,
        handle=handle,
        client=client,
        trace=trace,
        exported_path=exported_path,
        exported_rows=exported_rows,
    )
