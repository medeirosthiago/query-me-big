"""History picker controller.

Owns recent-query history state: fetched entries, filtering, selection,
and opening a chosen entry in the editor.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from textual.containers import Vertical
from textual.widgets import Input, OptionList

from qmb.bigquery.history import QueryHistoryEntry
from qmb.types import fmt_bytes

if TYPE_CHECKING:
    from qmb.tui.app import QueryResultApp


class HistoryController:
    """Controls the inline recent-query history picker."""

    def __init__(
        self,
        app: QueryResultApp,
        entries: list[QueryHistoryEntry] | None = None,
    ) -> None:
        self.app = app
        self.entries: list[QueryHistoryEntry] = entries or []
        self.filtered_indices: list[int] = []
        self.loading: bool = False

    # -- load --------------------------------------------------------------

    def load_and_open(self) -> None:
        if self.entries:
            self.open()
            return
        if self.loading:
            return
        self.loading = True
        self.app._info("Loading query history…")
        self.app._fetch_history()

    def on_load_succeeded(self, entries: list[QueryHistoryEntry]) -> None:
        self.loading = False
        self.entries = entries
        if not entries:
            self.app._warn("No recent queries found")
            return
        self.open()

    def on_load_failed(self, error: str) -> None:
        self.loading = False
        self.app._error(f"History load failed: {error}")

    # -- open / populate ---------------------------------------------------

    def open(self) -> None:
        picker = self.app.query_one("#history-picker", Vertical)
        inp = self.app.query_one("#history-filter", Input)
        inp.value = ""
        picker.display = True
        inp.focus()
        self.app.call_after_refresh(self.populate, "")

    def populate(self, query: str) -> None:
        opt = self.app.query_one("#history-list", OptionList)
        opt.clear_options()
        self.filtered_indices.clear()
        q = query.strip().lower()
        avail = (opt.size.width or self.app.size.width) - 6  # border/padding/scrollbar
        for i, entry in enumerate(self.entries):
            date_str = f"{entry.created:%Y-%m-%d %H:%M}"
            if q and not any(q in s for s in (entry.query.lower(), entry.job_id.lower(), date_str)):
                continue
            prefix = f"{date_str} · {fmt_bytes(entry.bytes_processed)} · "
            remaining = max(avail - len(prefix), 20)
            sql_line = " ".join(entry.query.split())
            if len(sql_line) > remaining:
                sql_line = sql_line[: remaining - 3] + "..."
            opt.add_option(f"{prefix}{sql_line}")
            self.filtered_indices.append(i)
        if self.filtered_indices:
            opt.highlighted = 0

    # -- input handlers ----------------------------------------------------

    def on_filter_changed(self, value: str) -> None:
        self.populate(value)

    def on_filter_submitted(self) -> None:
        opt = self.app.query_one("#history-list", OptionList)
        if self.filtered_indices and opt.highlighted is not None:
            self.select(opt.highlighted)

    def on_option_selected(self, option_index: int) -> None:
        self.select(option_index)

    def select(self, option_idx: int) -> None:
        if option_idx < 0 or option_idx >= len(self.filtered_indices):
            return
        entry = self.entries[self.filtered_indices[option_idx]]
        self.app._dismiss_picker()
        self.app._open_in_editor(
            entry.query, suffix=".sql", prefix="qmb_history_", read_only=False
        )
        self.open()

    def on_resize(self) -> None:
        if self.app.query_one("#history-picker", Vertical).display:
            inp = self.app.query_one("#history-filter", Input)
            self.populate(inp.value)
