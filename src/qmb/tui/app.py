"""Textual TUI application for browsing BigQuery results."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyperclip
from google.cloud import bigquery
from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import (
    DataTable,
    Footer,
    Input,
    Label,
    OptionList,
    TextArea,
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


class QueryResultApp(App):
    """Textual app for browsing BigQuery query results."""

    ESCAPE_TO_MINIMIZE = False

    CSS = """
    #status-bar {
        height: 3;
        padding: 0 1;
        background: $boost;
    }
    #status-bar Label {
        margin-right: 2;
    }
    #result-table {
        height: 1fr;
    }
    #preview-panel {
        height: 10;
        border-top: solid $accent;
        background: $surface;
    }
    #preview-area {
        height: 1fr;
    }
    """

    BINDINGS = [
        Binding("q,escape", "quit", "Quit"),
        Binding("n", "next_page", "Next Page"),
        Binding("p", "prev_page", "Prev Page"),
        Binding("c", "copy_cell", "Copy Cell"),
        Binding("shift+c", "copy_row", "Copy Row (JSON)"),
        Binding("e", "export", "Export"),
        Binding("home", "first_page", "First Page"),
        Binding("end", "last_page", "Last Page"),
    ]

    def __init__(
        self,
        bq_client: bigquery.Client,
        handle: QueryResultHandle,
        source_label: str,
        page_size: int = 200,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.bq_client = bq_client
        self.handle = handle
        self.source_label = source_label
        self.page_size = page_size
        self.current_page = 0
        self._raw_rows: list[dict[str, Any]] = []
        self._column_names: list[str] = []

    def compose(self) -> ComposeResult:
        with Horizontal(id="status-bar"):
            yield Label(f"Source: {self.source_label}", id="source-label")
            yield Label(f"Rows: {self.handle.total_rows:,}", id="row-count")
            yield Label(f"Processed: {_fmt_bytes(self.handle.bytes_processed)}", id="bytes-info")
            yield Label("Page: 1/1", id="page-info")
            yield Label(f"Job: {self.handle.job_id}", id="job-info")
        yield DataTable(id="result-table")
        with Vertical(id="preview-panel"):
            yield Label("Cell Preview (full value):", id="preview-title")
            yield TextArea(read_only=True, id="preview-area")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#result-table", DataTable)
        table.cursor_type = "cell"
        self._load_page(0)

    @work(thread=True)
    def _load_page(self, page: int) -> None:
        """Fetch and display a page of results."""
        result = fetch_page(self.bq_client, self.handle, page, self.page_size)
        self.call_from_thread(self._render_page, result)

    def _render_page(self, result: PageResult) -> None:
        """Render a page of results in the DataTable."""
        table = self.query_one("#result-table", DataTable)
        table.clear(columns=True)

        self.current_page = result.page
        self._raw_rows = result.rows
        self._column_names = []

        if not result.display_rows:
            table.add_column("(no results)")
            self._update_status(result)
            return

        # Add columns
        for col_info in self.handle.schema:
            col_name = col_info["name"]
            self._column_names.append(col_name)
            table.add_column(col_name, key=col_name)

        # Add rows
        for display_row in result.display_rows:
            values = [display_row.get(col, "") for col in self._column_names]
            table.add_row(*values)

        self._update_status(result)

    def _update_status(self, result: PageResult) -> None:
        """Update the status bar."""
        self.query_one("#page-info", Label).update(
            f"Page: {result.page + 1}/{result.total_pages}"
        )

    @on(DataTable.CellHighlighted)
    def cell_highlighted(self, event: DataTable.CellHighlighted) -> None:
        """Update preview panel when a cell is highlighted."""
        if not self._raw_rows or not self._column_names:
            return

        row_idx = event.coordinate.row
        col_idx = event.coordinate.column

        if row_idx < 0 or row_idx >= len(self._raw_rows):
            return
        if col_idx < 0 or col_idx >= len(self._column_names):
            return

        col_name = self._column_names[col_idx]
        raw_value = self._raw_rows[row_idx].get(col_name)
        full_text = get_raw_value(raw_value)

        preview = self.query_one("#preview-area", TextArea)
        preview.load_text(f"[{col_name}]\n{full_text}")

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

    def action_copy_cell(self) -> None:
        """Copy the full value of the selected cell to clipboard."""
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

    def action_copy_row(self) -> None:
        """Copy the selected row as JSON to clipboard."""
        import json

        table = self.query_one("#result-table", DataTable)
        if not self._raw_rows:
            self.notify("No data to copy", severity="warning")
            return

        row_idx = table.cursor_coordinate.row
        if row_idx < 0 or row_idx >= len(self._raw_rows):
            return

        raw_row = self._raw_rows[row_idx]
        row_json = {k: get_raw_value(v) for k, v in raw_row.items()}

        try:
            pyperclip.copy(json.dumps(row_json, indent=2))
            self.notify("Copied row as JSON", severity="information")
        except pyperclip.PyperclipException:
            self.notify("Clipboard not available", severity="error")

    def action_export(self) -> None:
        """Open export dialog."""

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
