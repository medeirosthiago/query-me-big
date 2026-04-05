"""Textual TUI application for browsing BigQuery results."""

from __future__ import annotations

import csv
import io
import json
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
from textual.screen import ModalScreen
from textual.widgets import (
    DataTable,
    Input,
    Label,
    OptionList,
)

from qmb.bigquery.exporters import export_results
from qmb.bigquery.pager import fetch_page, get_raw_value
from qmb.types import ExportFormat, PageResult, QueryResultHandle


def _fmt_bytes(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    for unit in ("KB", "MB", "GB", "TB"):
        n /= 1024
        if n < 1024:
            return f"{n:,.1f} {unit}"
    return f"{n:,.1f} PB"


# ---------------------------------------------------------------------------
# Modal screens
# ---------------------------------------------------------------------------


class ExportScreen(ModalScreen[tuple[ExportFormat, Path] | None]):
    """Modal for choosing export format and output path."""

    BINDINGS = [Binding("escape", "cancel", "Cancel")]
    DEFAULT_CSS = """
    ExportScreen {
        align: center middle;
    }
    #export-dialog {
        width: 60;
        height: 18;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
    }
    #export-dialog Label {
        margin-bottom: 1;
    }
    #format-list {
        height: 5;
        margin-bottom: 1;
    }
    #path-input {
        margin-bottom: 1;
    }
    """

    def compose(self) -> ComposeResult:
        with Vertical(id="export-dialog"):
            yield Label("Export Results", id="export-title")
            yield Label("Choose format:")
            yield OptionList(
                "CSV (.csv)",
                "JSON (.json)",
                "Parquet (.parquet)",
                id="format-list",
            )
            yield Label("Output path:")
            ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            yield Input(placeholder=f"{ts}.csv", id="path-input")
            yield Label("Press Enter to export, Escape to cancel", id="export-hint")

    def on_mount(self) -> None:
        self.query_one("#format-list", OptionList).highlighted = 0

    @on(OptionList.OptionSelected, "#format-list")
    def format_selected(self, event: OptionList.OptionSelected) -> None:
        extensions = {0: ".csv", 1: ".json", 2: ".parquet"}
        ext = extensions.get(event.option_index, ".csv")
        inp = self.query_one("#path-input", Input)
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        current = inp.value
        is_default = not current or any(
            current.endswith(e) and current[: -len(e)].replace("-", "").replace("_", "").isdigit()
            for e in extensions.values()
        )
        if is_default:
            inp.value = f"{ts}{ext}"

    @on(Input.Submitted, "#path-input")
    def submit_export(self) -> None:
        format_list = self.query_one("#format-list", OptionList)
        idx = format_list.highlighted or 0
        fmt_map = {0: ExportFormat.CSV, 1: ExportFormat.JSON, 2: ExportFormat.PARQUET}
        fmt = fmt_map.get(idx, ExportFormat.CSV)
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        path = Path(self.query_one("#path-input", Input).value or f"{ts}.csv")
        self.dismiss((fmt, path))

    def action_cancel(self) -> None:
        self.dismiss(None)


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
    #column-search, #cell-search {
        display: none;
        height: 3;
        border: tall $accent;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit", show=False),
        Binding("n", "next_page", "Next", show=False),
        Binding("p", "prev_page", "Prev", show=False),
        Binding("v", "vim_cell", "Vim Cell", show=False),
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

    def compose(self) -> ComposeResult:
        yield DataTable(id="result-table")
        yield Input(placeholder="Column name…", id="column-search")
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

    def _search_active(self) -> bool:
        return (
            self.query_one("#column-search", Input).display
            or self.query_one("#cell-search", Input).display
        )

    def _dismiss_search(self) -> None:
        self.query_one("#column-search", Input).display = False
        self.query_one("#cell-search", Input).display = False
        self.query_one("#result-table", DataTable).focus()

    def on_key(self, event: Key) -> None:
        # When a search input is focused, only handle escape
        if self._search_active():
            if event.key == "escape":
                self._dismiss_search()
                event.prevent_default()
                event.stop()
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

        if self._pending_key == "e":
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
                self._open_export_modal()
            return

        # First key — start sequence, search, or navigate
        if event.key == "y":
            self._pending_key = "y"
            self.set_timer(0.4, self._on_pending_timeout)
            event.prevent_default()
            event.stop()
            return

        if event.key == "e":
            self._pending_key = "e"
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
            search = self.query_one("#column-search", Input)
            search.value = ""
            search.display = True
            search.focus()
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
        if self._pending_key == "e":
            self._pending_key = None
            self._open_export_modal()
        elif self._pending_key == "y":
            self._pending_key = None

    def _clear_pending(self) -> None:
        self._pending_key = None

    # -- search -------------------------------------------------------------

    @on(Input.Submitted, "#cell-search")
    def _on_cell_search(self, event: Input.Submitted) -> None:
        query = event.value.strip().lower()
        self._dismiss_search()
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

    @on(Input.Submitted, "#column-search")
    def _on_column_search(self, event: Input.Submitted) -> None:
        query = event.value.strip().lower()
        self._dismiss_search()
        if not query:
            return

        table = self.query_one("#result-table", DataTable)
        for col_idx, col_name in enumerate(self._column_names):
            if query in col_name.lower():
                table.move_cursor(column=col_idx + 1)
                self.notify(f"→ {col_name}")
                return

        self.notify(f"No column matching '{query}'", severity="warning")

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
        row_data = {k: get_raw_value(v) for k, v in raw_row.items()}

        try:
            pyperclip.copy(json.dumps(row_data, indent=2))
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

    # -- export -------------------------------------------------------------

    def _open_export_modal(self) -> None:
        def on_export_result(result: tuple[ExportFormat, Path] | None) -> None:
            if result is None:
                return
            fmt, path = result
            try:
                count = export_results(self.bq_client, self.handle, fmt, path)
                self.notify(f"Exported {count:,} rows to {path}", severity="information")
            except Exception as e:
                self.notify(f"Export failed: {e}", severity="error")

        self.push_screen(ExportScreen(), on_export_result)

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
            f"  Processed:     {_fmt_bytes(h.bytes_processed)}",
            f"  Duration:      {duration}",
        ])
        self._open_in_nvim(details, suffix=".txt", prefix="qmb_job_")

    # -- help ---------------------------------------------------------------

    def action_show_help(self) -> None:
        help_text = "\n".join([
            "qmb — Keyboard Shortcuts",
            "=" * 40,
            "",
            "Navigation",
            "  h/j/k/l       Move left/down/up/right",
            "  Arrow keys    Move left/down/up/right",
            "  n             Next page (or next match)",
            "  N             Previous match",
            "  p             Previous page",
            "  Home          First page",
            "  End           Last page",
            "",
            "Search",
            "  /             Search cell values",
            "  f             Search column name",
            "  n/N           Next/previous match",
            "  Escape        Clear search",
            "",
            "Yank (copy)",
            "  yw            Copy selected cell value",
            "  yc            Copy selected row as CSV",
            "  yj            Copy selected row as JSON",
            "",
            "Inspect",
            "  v             Open cell in nvim (read-only)",
            "  s             Open full SQL query in nvim",
            "  d             Open job details in nvim",
            "",
            "Export",
            "  e             Open export dialog",
            "  ec            Quick export to CSV",
            "  ej            Quick export to JSON",
            "",
            "Other",
            "  ?             Show this help",
            "  q             Quit",
        ])
        self._open_in_nvim(help_text, suffix=".txt", prefix="qmb_help_")

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
        import math

        total_pages = max(1, math.ceil(self.handle.total_rows / self.page_size))
        if self.current_page < total_pages - 1:
            self._load_page(self.current_page + 1)

    def action_prev_page(self) -> None:
        if self.current_page > 0:
            self._load_page(self.current_page - 1)

    def action_first_page(self) -> None:
        self._load_page(0)

    def action_last_page(self) -> None:
        import math

        total_pages = max(1, math.ceil(self.handle.total_rows / self.page_size))
        self._load_page(total_pages - 1)
