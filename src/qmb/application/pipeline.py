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

# Import modules (not their members) so test monkeypatches against
# the canonical module paths are picked up at call time.
from qmb.application.outcomes import ExecutionOutcome
from qmb.application.protocols import SqlResolver
from qmb.application.resolver import apply_where, resolve_request_to_sql
from qmb.bigquery import client as _bq_client
from qmb.bigquery import executor as _bq_executor
from qmb.bigquery import exporters as _bq_exporters
from qmb.sql.resolver import PlainSqlResolver
from qmb.types import QueryRequest

__all__ = ["run_query_pipeline"]


def run_query_pipeline(
    request: QueryRequest,
    *,
    resolvers: Sequence[SqlResolver] | None = None,
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

    return ExecutionOutcome(
        resolved=resolved,
        handle=handle,
        client=client,
        trace=trace,
        exported_path=exported_path,
        exported_rows=exported_rows,
    )
