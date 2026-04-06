"""Textual TUI application for browsing BigQuery results."""

import csv
import io
import json
import math
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import pyperclip
from google.cloud import bigquery
from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.events import Key
from textual.screen import Screen
from textual.widgets import (
    DataTable,
    Input,
    Label,
    OptionList,
    Static,
)

from qmb.bigquery.exporters import export_results
from qmb.bigquery.pager import fetch_page, get_raw_value, json_default
from qmb.types import ExportFormat, PageResult, QueryResultHandle, fmt_bytes

# ---------------------------------------------------------------------------
# Export format options
# ---------------------------------------------------------------------------

_EXPORT_OPTIONS: list[tuple[ExportFormat, str, str]] = [
    (ExportFormat.CSV, "CSV (.csv)", ".csv"),
    (ExportFormat.JSON, "JSON (.json)", ".json"),
    (ExportFormat.PARQUET, "Parquet (.parquet)", ".parquet"),
]


HELP_TEXT = """\
qmb — Keyboard Shortcuts
========================================

Navigation
  h/j/k/l       Move left/down/up/right
  Arrow keys    Move left/down/up/right
  n             Next page (or next match)
  N             Previous match
  p             Previous page
  Home          First page
  End           Last page

Search
  /             Search cell values
  f             Search column name
  n/N           Next/previous match
  Escape        Clear search

Yank (copy)
  yw            Copy selected cell value
  yc            Copy selected row as CSV
  yj            Copy selected row as JSON

Inspect
  e             Open cell in nvim (read-only)
  s             Open full SQL query in nvim
  d             Open job details in nvim

Export
  x             Open export picker
  xc            Quick export to CSV
  xj            Quick export to JSON

Other
  ?             Show this help
  Ctrl-Q        Quit
"""


class HelpScreen(Screen):
    """Simple scrollable help screen."""

    BINDINGS = [
        Binding("escape", "app.pop_screen", "Back", show=False),

    ]
    DEFAULT_CSS = """
    HelpScreen { padding: 1 2; }
    HelpScreen Static { width: 1fr; }
    """

    def compose(self) -> ComposeResult:
        yield Static(HELP_TEXT)


# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------


class QueryResultApp(App):
    """Textual app for browsing BigQuery query results."""

    ENABLE_COMMAND_PALETTE = False
    ESCAPE_TO_MINIMIZE = False

    CSS = """
    #result-table {
        height: 1fr;
    }
    #page-bar {
        height: 1;
        background: $boost;
        padding: 0 1;
    }
    #column-picker, #export-picker {
        display: none;
        height: auto;
        max-height: 16;
        border: tall $accent;
    }
    #column-filter, #export-filter {
        height: 3;
    }
    #column-list, #export-list {
        height: auto;
        max-height: 12;
    }
    #cell-search {
        display: none;
        height: 3;
        border: tall $accent;
    }
    """

    BINDINGS = [

        Binding("n", "next_page", "Next", show=False),
        Binding("p", "prev_page", "Prev", show=False),
        Binding("e", "vim_cell", "Edit", show=False),
        Binding("s", "vim_query", "SQL", show=False),
        Binding("d", "vim_job_details", "Details", show=False),
        Binding("question_mark", "show_help", "Help", show=False),
        Binding("home", "first_page", "First Page", show=False),
        Binding("end", "last_page", "Last Page", show=False),
    ]

    def __init__(
        self,
        bq_client: bigquery.Client,
        handle: QueryResultHandle,
        source_label: str,
        resolved_sql: str = "",
        page_size: int = 200,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.bq_client = bq_client
        self.handle = handle
        self.source_label = source_label
        self.resolved_sql = resolved_sql
        self.page_size = page_size
        self.current_page = 0
        self._raw_rows: list[dict[str, Any]] = []
        self._column_names: list[str] = []
        self._pending_key: str | None = None
        self._cell_matches: list[tuple[int, int]] = []
        self._match_idx: int = -1
        self._filtered_columns: list[int] = []
        self._filtered_exports: list[int] = []
        self._export_format: ExportFormat | None = None

    def compose(self) -> ComposeResult:
        yield DataTable(id="result-table")
        with Vertical(id="column-picker"):
            yield Input(placeholder="Filter columns…", id="column-filter")
            yield OptionList(id="column-list")
        with Vertical(id="export-picker"):
            yield Input(placeholder="Filter formats…", id="export-filter")
            yield OptionList(id="export-list")
        yield Input(placeholder="Search value…", id="cell-search")
        yield Label("Page: 1/1  ·  ? for help", id="page-bar")

    def on_mount(self) -> None:
        table = self.query_one("#result-table", DataTable)
        table.cursor_type = "cell"
        self._load_page(0)

    @on(DataTable.CellHighlighted)
    def _enforce_min_column(self, event: DataTable.CellHighlighted) -> None:
        if self._column_names and event.coordinate.column == 0:
            self.query_one("#result-table", DataTable).move_cursor(column=1)

    # -- key handling (hjkl + multi-key sequences) --------------------------

    def _picker_active(self) -> bool:
        return (
            self.query_one("#column-picker", Vertical).display
            or self.query_one("#export-picker", Vertical).display
            or self.query_one("#cell-search", Input).display
        )

    def _dismiss_picker(self) -> None:
        self.query_one("#column-picker", Vertical).display = False
        self.query_one("#export-picker", Vertical).display = False
        self.query_one("#export-list", OptionList).display = True
        self.query_one("#export-filter", Input).display = True
        self.query_one("#cell-search", Input).display = False
        self._export_format = None
        self.query_one("#result-table", DataTable).focus()

    def _navigate_option_list(self, list_id: str, event: Key) -> None:
        opt = self.query_one(list_id, OptionList)
        if opt.option_count == 0:
            return
        idx = opt.highlighted or 0
        if event.key == "down":
            opt.highlighted = min(idx + 1, opt.option_count - 1)
            event.prevent_default()
            event.stop()
        elif event.key == "up":
            opt.highlighted = max(idx - 1, 0)
            event.prevent_default()
            event.stop()

    def on_key(self, event: Key) -> None:
        # When a picker is focused, handle escape and arrow navigation
        if self._picker_active():
            if event.key == "escape":
                self._dismiss_picker()
                event.prevent_default()
                event.stop()
            elif self.query_one("#column-picker", Vertical).display:
                self._navigate_option_list("#column-list", event)
            return

        # Escape clears search matches
        if event.key == "escape" and self._cell_matches:
            self._cell_matches.clear()
            self._match_idx = -1
            self.notify("Search cleared")
            event.prevent_default()
            event.stop()
            return

        # Second key of a pending sequence
        if self._pending_key == "y":
            self._clear_pending()
            event.prevent_default()
            event.stop()
            if event.key == "w":
                self._copy_cell()
            elif event.key == "c":
                self._copy_row_csv()
            elif event.key == "j":
                self._copy_row_json()
            return

        if self._pending_key == "x":
            self._clear_pending()
            if event.key == "c":
                event.prevent_default()
                event.stop()
                self._quick_export(ExportFormat.CSV, ".csv")
            elif event.key == "j":
                event.prevent_default()
                event.stop()
                self._quick_export(ExportFormat.JSON, ".json")
            else:
                self._open_export_picker()
            return

        # First key — start sequence, search, or navigate
        if event.key == "y":
            self._pending_key = "y"
            self.set_timer(0.4, self._on_pending_timeout)
            event.prevent_default()
            event.stop()
            return

        if event.key == "x":
            self._pending_key = "x"
            self.set_timer(0.4, self._on_pending_timeout)
            event.prevent_default()
            event.stop()
            return

        if event.key == "slash":
            search = self.query_one("#cell-search", Input)
            search.value = ""
            search.display = True
            search.focus()
            event.prevent_default()
            event.stop()
            return

        if event.key == "f":
            self._open_column_picker()
            event.prevent_default()
            event.stop()
            return

        # n/N — next/prev match when search is active, else page navigation
        if event.key == "n" and self._cell_matches:
            self._goto_match(1)
            event.prevent_default()
            event.stop()
            return

        if event.key == "N" and self._cell_matches:
            self._goto_match(-1)
            event.prevent_default()
            event.stop()
            return

        # vim-style navigation
        table = self.query_one("#result-table", DataTable)
        if event.key == "h":
            table.action_cursor_left()
            event.prevent_default()
        elif event.key == "j":
            table.action_cursor_down()
            event.prevent_default()
        elif event.key == "k":
            table.action_cursor_up()
            event.prevent_default()
        elif event.key == "l":
            table.action_cursor_right()
            event.prevent_default()

    def _on_pending_timeout(self) -> None:
        if self._pending_key == "x":
            self._pending_key = None
            self._open_export_picker()
        elif self._pending_key == "y":
            self._pending_key = None

    def _clear_pending(self) -> None:
        self._pending_key = None

    # -- search -------------------------------------------------------------

    @on(Input.Submitted, "#cell-search")
    def _on_cell_search(self, event: Input.Submitted) -> None:
        query = event.value.strip().lower()
        self._dismiss_picker()
        if not query:
            return

        matches: list[tuple[int, int]] = []
        for row_idx, raw_row in enumerate(self._raw_rows):
            for col_idx, col_name in enumerate(self._column_names):
                val = str(raw_row.get(col_name, "")).lower()
                if query in val:
                    matches.append((row_idx, col_idx))

        self._cell_matches = matches
        self._match_idx = -1

        if matches:
            self._goto_match(1)
            self.notify(
                f"{len(matches)} match{'es' if len(matches) != 1 else ''} · n/N to cycle"
            )
        else:
            self.notify("No matches found", severity="warning")

    def _open_column_picker(self) -> None:
        picker = self.query_one("#column-picker", Vertical)
        col_filter = self.query_one("#column-filter", Input)
        col_filter.value = ""
        picker.display = True
        self._populate_column_list("")
        col_filter.focus()

    def _populate_column_list(self, query: str) -> None:
        col_list = self.query_one("#column-list", OptionList)
        col_list.clear_options()
        self._filtered_columns.clear()
        q = query.strip().lower()
        for col_idx, col_name in enumerate(self._column_names):
            if not q or q in col_name.lower():
                col_list.add_option(col_name)
                self._filtered_columns.append(col_idx)
        if self._filtered_columns:
            col_list.highlighted = 0

    @on(Input.Changed, "#column-filter")
    def _on_column_filter_changed(self, event: Input.Changed) -> None:
        self._populate_column_list(event.value)

    @on(Input.Submitted, "#column-filter")
    def _on_column_filter_submitted(self, event: Input.Submitted) -> None:
        col_list = self.query_one("#column-list", OptionList)
        if self._filtered_columns and col_list.highlighted is not None:
            self._select_column(col_list.highlighted)
        else:
            self._dismiss_picker()

    @on(OptionList.OptionSelected, "#column-list")
    def _on_column_selected(self, event: OptionList.OptionSelected) -> None:
        self._select_column(event.option_index)

    def _select_column(self, option_idx: int) -> None:
        if option_idx < 0 or option_idx >= len(self._filtered_columns):
            return
        col_idx = self._filtered_columns[option_idx]
        self._dismiss_picker()
        table = self.query_one("#result-table", DataTable)
        table.move_cursor(column=col_idx + 1)
        self.notify(f"→ {self._column_names[col_idx]}")

    def _goto_match(self, direction: int) -> None:
        if not self._cell_matches:
            return
        self._match_idx = (self._match_idx + direction) % len(self._cell_matches)
        row_idx, col_idx = self._cell_matches[self._match_idx]
        table = self.query_one("#result-table", DataTable)
        table.move_cursor(row=row_idx, column=col_idx + 1)

    # -- clipboard ----------------------------------------------------------

    def _data_col(self) -> int:
        """Map DataTable column index to data column index (skip row-number col)."""
        return self.query_one("#result-table", DataTable).cursor_coordinate.column - 1

    def _copy_cell(self) -> None:
        table = self.query_one("#result-table", DataTable)
        if not self._raw_rows or not self._column_names:
            self.notify("No data to copy", severity="warning")
            return

        row_idx = table.cursor_coordinate.row
        col_idx = self._data_col()
        if row_idx < 0 or row_idx >= len(self._raw_rows):
            return
        if col_idx < 0 or col_idx >= len(self._column_names):
            return

        col_name = self._column_names[col_idx]
        raw_value = self._raw_rows[row_idx].get(col_name)
        full_text = get_raw_value(raw_value)

        try:
            pyperclip.copy(full_text)
            self.notify(f"Copied {col_name} value", severity="information")
        except pyperclip.PyperclipException:
            self.notify("Clipboard not available", severity="error")

    def _copy_row_json(self) -> None:
        table = self.query_one("#result-table", DataTable)
        if not self._raw_rows:
            self.notify("No data to copy", severity="warning")
            return

        row_idx = table.cursor_coordinate.row
        if row_idx < 0 or row_idx >= len(self._raw_rows):
            return

        raw_row = self._raw_rows[row_idx]

        try:
            pyperclip.copy(json.dumps(raw_row, indent=2, default=json_default))
            self.notify("Copied row as JSON", severity="information")
        except pyperclip.PyperclipException:
            self.notify("Clipboard not available", severity="error")

    def _copy_row_csv(self) -> None:
        table = self.query_one("#result-table", DataTable)
        if not self._raw_rows or not self._column_names:
            self.notify("No data to copy", severity="warning")
            return

        row_idx = table.cursor_coordinate.row
        if row_idx < 0 or row_idx >= len(self._raw_rows):
            return

        raw_row = self._raw_rows[row_idx]
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=self._column_names)
        writer.writeheader()
        writer.writerow({k: get_raw_value(v) for k, v in raw_row.items()})

        try:
            pyperclip.copy(buf.getvalue())
            self.notify("Copied row as CSV", severity="information")
        except pyperclip.PyperclipException:
            self.notify("Clipboard not available", severity="error")

    # -- vim cell / query ---------------------------------------------------

    def action_vim_cell(self) -> None:
        table = self.query_one("#result-table", DataTable)
        if not self._raw_rows or not self._column_names:
            self.notify("No data to inspect", severity="warning")
            return

        row_idx = table.cursor_coordinate.row
        col_idx = self._data_col()
        if row_idx < 0 or row_idx >= len(self._raw_rows):
            return
        if col_idx < 0 or col_idx >= len(self._column_names):
            return

        col_name = self._column_names[col_idx]
        raw_value = self._raw_rows[row_idx].get(col_name)
        full_text = get_raw_value(raw_value)

        ext = ".txt"
        try:
            json.loads(full_text)
            ext = ".json"
        except (json.JSONDecodeError, TypeError):
            pass

        self._open_in_nvim(full_text, suffix=ext, prefix=f"qmb_{col_name}_")

    def action_vim_query(self) -> None:
        self._open_in_nvim(self.resolved_sql, suffix=".sql", prefix="qmb_query_")

    def _open_in_nvim(self, content: str, suffix: str, prefix: str) -> None:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=suffix, prefix=prefix, delete=False
        ) as f:
            f.write(content)
            tmp_path = f.name

        with self.suspend():
            subprocess.run(["nvim", "-R", tmp_path])

        Path(tmp_path).unlink(missing_ok=True)

    # -- export picker ------------------------------------------------------

    def _open_export_picker(self) -> None:
        self._export_format = None
        picker = self.query_one("#export-picker", Vertical)
        inp = self.query_one("#export-filter", Input)
        opt = self.query_one("#export-list", OptionList)
        inp.display = False
        opt.display = True
        picker.display = True
        self._populate_export_list("")
        opt.focus()

    def _populate_export_list(self, query: str) -> None:
        opt = self.query_one("#export-list", OptionList)
        opt.clear_options()
        self._filtered_exports.clear()
        q = query.strip().lower()
        for i, (_, label, _) in enumerate(_EXPORT_OPTIONS):
            if not q or q in label.lower():
                opt.add_option(label)
                self._filtered_exports.append(i)
        if self._filtered_exports:
            opt.highlighted = 0

    @on(Input.Changed, "#export-filter")
    def _on_export_filter_changed(self, event: Input.Changed) -> None:
        if self._export_format is not None:
            return
        self._populate_export_list(event.value)

    @on(Input.Submitted, "#export-filter")
    def _on_export_filter_submitted(self, event: Input.Submitted) -> None:
        if self._export_format is None:
            opt = self.query_one("#export-list", OptionList)
            if self._filtered_exports and opt.highlighted is not None:
                self._select_export_format(opt.highlighted)
            return
        # Phase 2: path submitted — do the export
        inp = self.query_one("#export-filter", Input)
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        ext = next(e for f, _, e in _EXPORT_OPTIONS if f == self._export_format)
        path = Path(inp.value or f"{ts}{ext}")
        self._dismiss_picker()
        try:
            count = export_results(self.bq_client, self.handle, self._export_format, path)
            self.notify(f"Exported {count:,} rows to {path}", severity="information")
        except Exception as exc:
            self.notify(f"Export failed: {exc}", severity="error")

    @on(OptionList.OptionSelected, "#export-list")
    def _on_export_selected(self, event: OptionList.OptionSelected) -> None:
        self._select_export_format(event.option_index)

    def _select_export_format(self, option_idx: int) -> None:
        if option_idx < 0 or option_idx >= len(self._filtered_exports):
            return
        i = self._filtered_exports[option_idx]
        fmt, _, ext = _EXPORT_OPTIONS[i]
        self._export_format = fmt
        # Switch to path entry phase
        opt = self.query_one("#export-list", OptionList)
        opt.display = False
        inp = self.query_one("#export-filter", Input)
        inp.display = True
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        inp.placeholder = "Output path…"
        inp.value = f"{ts}{ext}"
        inp.focus()

    def _quick_export(self, fmt: ExportFormat, ext: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        path = Path(f"{ts}{ext}")
        try:
            count = export_results(self.bq_client, self.handle, fmt, path)
            self.notify(f"Exported {count:,} rows to {path}", severity="information")
        except Exception as e:
            self.notify(f"Export failed: {e}", severity="error")

    # -- job details --------------------------------------------------------

    def action_vim_job_details(self) -> None:
        h = self.handle
        duration = (
            f"{h.execution_seconds:.1f}s"
            if h.execution_seconds < 60
            else f"{h.execution_seconds / 60:.1f}m"
        )
        details = "\n".join([
            "Job Details",
            "=" * 40,
            f"  Source:        {self.source_label}",
            f"  Job ID:        {h.job_id}",
            f"  Project:       {h.project}",
            f"  Location:      {h.location}",
            f"  Destination:   {h.destination_table}",
            f"  Total rows:    {h.total_rows:,}",
            f"  Processed:     {fmt_bytes(h.bytes_processed)}",
            f"  Duration:      {duration}",
        ])
        self._open_in_nvim(details, suffix=".txt", prefix="qmb_job_")

    # -- help ---------------------------------------------------------------

    def action_show_help(self) -> None:
        self.push_screen(HelpScreen())

    # -- pagination ---------------------------------------------------------

    @work(thread=True)
    def _load_page(self, page: int) -> None:
        result = fetch_page(self.bq_client, self.handle, page, self.page_size)
        self.call_from_thread(self._render_page, result)

    def _render_page(self, result: PageResult) -> None:
        table = self.query_one("#result-table", DataTable)
        table.clear(columns=True)

        self.current_page = result.page
        self._raw_rows = result.rows
        self._column_names = []
        self._cell_matches.clear()
        self._match_idx = -1

        if not result.display_rows:
            table.add_column("(no results)")
            self._update_page_bar(result)
            return

        table.add_column("#", key="_row_num")
        for col_info in self.handle.schema:
            col_name = col_info["name"]
            self._column_names.append(col_name)
            table.add_column(col_name, key=col_name)

        row_offset = result.page * self.page_size
        for i, display_row in enumerate(result.display_rows):
            values = [str(row_offset + i + 1)]
            values.extend(display_row.get(col, "") for col in self._column_names)
            table.add_row(*values)

        self._update_page_bar(result)

    def _update_page_bar(self, result: PageResult) -> None:
        self.query_one("#page-bar", Label).update(
            f"Page: {result.page + 1}/{result.total_pages}  ·  ? for help"
        )

    def action_next_page(self) -> None:
        total_pages = max(1, math.ceil(self.handle.total_rows / self.page_size))
        if self.current_page < total_pages - 1:
            self._load_page(self.current_page + 1)

    def action_prev_page(self) -> None:
        if self.current_page > 0:
            self._load_page(self.current_page - 1)

    def action_first_page(self) -> None:
        self._load_page(0)

    def action_last_page(self) -> None:
        total_pages = max(1, math.ceil(self.handle.total_rows / self.page_size))
        self._load_page(total_pages - 1)
