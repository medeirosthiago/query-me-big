"""Rich console formatter — reproduces the original qmb stdout style."""

from __future__ import annotations

from typing import TYPE_CHECKING

from rich.console import Console
from rich.panel import Panel

from qmb.types import fmt_bytes

if TYPE_CHECKING:
    from qmb.application.outcomes import ExecutionOutcome
    from qmb.types import QueryRequest


class TableFormatter:
    """Pretty-printed status output, matching qmb's pre-Phase-10 behavior.

    This formatter writes status lines (resolver match, source label,
    row/byte/job summary, archive id, export progress) to a Rich
    :class:`Console`. It never launches the TUI. For the historical
    "table+TUI" default, the CLI dispatches separately to the
    :class:`TuiFormatter` after this one runs.
    """

    def __init__(self, console: Console | None = None) -> None:
        self.console = console or Console()

    def render_run(self, outcome: ExecutionOutcome, request: QueryRequest) -> None:
        resolved = outcome.resolved
        handle = outcome.handle
        trace = outcome.trace
        console = self.console

        if trace.matched_node_id:
            if trace.matched_via_raw_code:
                console.print(
                    f"[dim]Matched {trace.matched_node_id} (no compiled_code, "
                    "resolving from raw SQL)[/dim]"
                )
            else:
                console.print(f"[dim]Matched manifest node: {trace.matched_node_id}[/dim]")

        if outcome.dry_run:
            console.print(
                Panel(resolved.sql, title="Resolved SQL (dry run)", border_style="cyan")
            )
            console.print(f"[cyan]Estimated:[/cyan] {fmt_bytes(handle.bytes_processed)}")
            return

        console.print(f"[dim]Source: {resolved.source_label}[/dim]")
        console.print("[dim]Executing query...[/dim]")
        console.print(
            f"[green]✓[/green] {handle.total_rows:,} rows · "
            f"{fmt_bytes(handle.bytes_processed)} processed · "
            f"Job: {handle.job_id}"
        )

        if outcome.archived_job is not None:
            console.print(f"[dim]Archived: {outcome.archived_job.qmb_job_id}[/dim]")

        if outcome.exported_path is not None:
            console.print(f"[dim]Exporting to {outcome.exported_path}...[/dim]")
            console.print(
                f"[green]✓[/green] Exported {outcome.exported_rows:,} rows "
                f"to {outcome.exported_path}"
            )

        if handle.total_rows == 0 and not request.no_tui:
            console.print("[yellow]No rows to display.[/yellow]")
