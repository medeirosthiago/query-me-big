"""Cell-search and column-picker controllers.

Both are small inline pickers that operate over the result table's
columns and rendered cells.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from textual.containers import Vertical
from textual.widgets import DataTable, Input, OptionList

if TYPE_CHECKING:
    from qmb.tui.app import QueryResultApp


class CellSearchController:
    """Find values in the current page and cycle through matches."""

    def __init__(self, app: QueryResultApp) -> None:
        self.app = app
        self.matches: list[tuple[int, int]] = []
        self.match_idx: int = -1

    def clear(self) -> None:
        self.matches.clear()
        self.match_idx = -1

    def on_submitted(self, value: str) -> None:
        query = value.strip().lower()
        self.app._dismiss_picker()
        if not query:
            return

        matches: list[tuple[int, int]] = []
        for row_idx, raw_row in enumerate(self.app._raw_rows):
            for col_idx, col_name in enumerate(self.app._column_names):
                val = str(raw_row.get(col_name, "")).lower()
                if query in val:
                    matches.append((row_idx, col_idx))

        self.matches = matches
        self.match_idx = -1

        if matches:
            self.goto(1)
            self.app._info(
                f"{len(matches)} match{'es' if len(matches) != 1 else ''} · n/N to cycle"
            )
        else:
            self.app._warn("No matches found")

    def goto(self, direction: int) -> None:
        if not self.matches:
            return
        self.match_idx = (self.match_idx + direction) % len(self.matches)
        row_idx, col_idx = self.matches[self.match_idx]
        table = self.app.query_one("#result-table", DataTable)
        table.move_cursor(row=row_idx, column=col_idx + 1)


class ColumnPickerController:
    """Fuzzy column picker — jumps the cursor to the chosen column."""

    def __init__(self, app: QueryResultApp) -> None:
        self.app = app
        self.filtered_indices: list[int] = []

    def open(self) -> None:
        picker = self.app.query_one("#column-picker", Vertical)
        col_filter = self.app.query_one("#column-filter", Input)
        col_filter.value = ""
        picker.display = True
        self.populate("")
        col_filter.focus()

    def populate(self, query: str) -> None:
        col_list = self.app.query_one("#column-list", OptionList)
        col_list.clear_options()
        self.filtered_indices.clear()
        q = query.strip().lower()
        for col_idx, col_name in enumerate(self.app._column_names):
            if not q or q in col_name.lower():
                col_list.add_option(col_name)
                self.filtered_indices.append(col_idx)
        if self.filtered_indices:
            col_list.highlighted = 0

    def on_filter_changed(self, value: str) -> None:
        self.populate(value)

    def on_filter_submitted(self) -> None:
        col_list = self.app.query_one("#column-list", OptionList)
        if self.filtered_indices and col_list.highlighted is not None:
            self.select(col_list.highlighted)
        else:
            self.app._dismiss_picker()

    def on_option_selected(self, option_index: int) -> None:
        self.select(option_index)

    def select(self, option_idx: int) -> None:
        if option_idx < 0 or option_idx >= len(self.filtered_indices):
            return
        col_idx = self.filtered_indices[option_idx]
        self.app._dismiss_picker()
        table = self.app.query_one("#result-table", DataTable)
        table.move_cursor(column=col_idx + 1)
        self.app._info(f"→ {self.app._column_names[col_idx]}")
