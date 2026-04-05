"""Textual TUI application for browsing BigQuery results."""

from __future__ import annotations

import csv
import io
import json
import subprocess
import tempfile
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
    Footer,
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
            yield Input(placeholder="output.csv", id="path-input")
            yield Label("Press Enter to export, Escape to cancel", id="export-hint")

    def on_mount(self) -> None:
        self.query_one("#format-list", OptionList).highlighted = 0

    @on(OptionList.OptionSelected, "#format-list")
    def format_selected(self, event: OptionList.OptionSelected) -> None:
        extensions = {0: ".csv", 1: ".json", 2: ".parquet"}
        ext = extensions.get(event.option_index, ".csv")
        inp = self.query_one("#path-input", Input)
        if not inp.value or inp.value in ("output.csv", "output.json", "output.parquet"):
            inp.value = f"output{ext}"

    @on(Input.Submitted, "#path-input")
    def submit_export(self) -> None:
        format_list = self.query_one("#format-list", OptionList)
        idx = format_list.highlighted or 0
        fmt_map = {0: ExportFormat.CSV, 1: ExportFormat.JSON, 2: ExportFormat.PARQUET}
        fmt = fmt_map.get(idx, ExportFormat.CSV)
        path = Path(self.query_one("#path-input", Input).value or "output.csv")
        self.dismiss((fmt, path))

    def action_cancel(self) -> None:
        self.dismiss(None)


class JobDetailScreen(ModalScreen[None]):
    """Modal showing job execution details."""

    BINDINGS = [Binding("escape,q", "dismiss_screen", "Close")]
    DEFAULT_CSS = """
    JobDetailScreen {
        align: center middle;
    }
    #job-dialog {
        width: 70;
        height: 20;
        border: thick $accent;
        background: $surface;
        padding: 1 2;
    }
    #job-dialog Label {
        margin-bottom: 1;
    }
    """

    def __init__(self, handle: QueryResultHandle, source_label: str) -> None:
        super().__init__()
        self._handle = handle
        self._source_label = source_label

    def compose(self) -> ComposeResult:
        h = self._handle
        duration = (
            f"{h.execution_seconds:.1f}s"
            if h.execution_seconds < 60
            else f"{h.execution_seconds / 60:.1f}m"
        )
        with Vertical(id="job-dialog"):
            yield Label("Job Details", id="job-title")
            yield Label(f"  Source:        {self._source_label}")
            yield Label(f"  Job ID:        {h.job_id}")
            yield Label(f"  Project:       {h.project}")
            yield Label(f"  Location:      {h.location}")
            yield Label(f"  Destination:   {h.destination_table}")
            yield Label(f"  Total rows:    {h.total_rows:,}")
            yield Label(f"  Processed:     {_fmt_bytes(h.bytes_processed)}")
            yield Label(f"  Duration:      {duration}")
            yield Label("")
            yield Label("Press q or Escape to close")

    def action_dismiss_screen(self) -> None:
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
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("n", "next_page", "Next"),
        Binding("p", "prev_page", "Prev"),
        Binding("v", "vim_cell", "Vim Cell"),
        Binding("s", "vim_query", "SQL"),
        Binding("d", "show_job", "Details"),
        Binding("e", "noop", "Export"),
        Binding("y", "noop", "Yank"),
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

    def compose(self) -> ComposeResult:
        yield DataTable(id="result-table")
        yield Label("Page: 1/1", id="page-bar")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#result-table", DataTable)
        table.cursor_type = "cell"
        self._load_page(0)

    def action_noop(self) -> None:
        pass

    # -- key handling (hjkl + multi-key sequences) --------------------------

    def on_key(self, event: Key) -> None:
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

        # First key — start sequence or navigate
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

    # -- clipboard ----------------------------------------------------------

    def _copy_cell(self) -> None:
        table = self.query_one("#result-table", DataTable)
        if not self._raw_rows or not self._column_names:
            self.notify("No data to copy", severity="warning")
            return

        row_idx = table.cursor_coordinate.row
        col_idx = table.cursor_coordinate.column
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
        col_idx = table.cursor_coordinate.column
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
        path = Path(f"output{ext}")
        try:
            count = export_results(self.bq_client, self.handle, fmt, path)
            self.notify(f"Exported {count:,} rows to {path}", severity="information")
        except Exception as e:
            self.notify(f"Export failed: {e}", severity="error")

    # -- job details --------------------------------------------------------

    def action_show_job(self) -> None:
        self.push_screen(JobDetailScreen(self.handle, self.source_label))

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

        if not result.display_rows:
            table.add_column("(no results)")
            self._update_page_bar(result)
            return

        for col_info in self.handle.schema:
            col_name = col_info["name"]
            self._column_names.append(col_name)
            table.add_column(col_name, key=col_name)

        for display_row in result.display_rows:
            values = [display_row.get(col, "") for col in self._column_names]
            table.add_row(*values)

        self._update_page_bar(result)

    def _update_page_bar(self, result: PageResult) -> None:
        self.query_one("#page-bar", Label).update(
            f"Page: {result.page + 1}/{result.total_pages}"
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
