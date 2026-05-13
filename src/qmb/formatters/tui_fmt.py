"""Interactive Textual TUI formatter."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qmb.application.outcomes import ExecutionOutcome
    from qmb.types import QueryRequest


class TuiFormatter:
    """Launch the Textual :class:`QueryResultApp` on a successful query.

    No-ops for dry runs or empty result sets — those have nothing to
    page through. The CLI is responsible for dispatching a separate
    :class:`TableFormatter` first if it wants the status lines printed
    above the TUI launch (current default behavior).
    """

    def render_run(self, outcome: ExecutionOutcome, request: QueryRequest) -> None:
        if outcome.dry_run:
            return
        handle = outcome.handle
        if handle.total_rows == 0:
            return

        # Deferred import — Textual is heavy and importing it eagerly
        # would slow down headless paths.
        from qmb.tui.app import QueryResultApp

        resolved = outcome.resolved
        tui = QueryResultApp(
            bq_client=outcome.client,
            handle=handle,
            source_label=resolved.source_label,
            resolved_sql=resolved.sql,
            page_size=request.page_size,
        )
        tui.run()
