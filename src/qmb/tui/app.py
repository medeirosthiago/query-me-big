"""Textual TUI application for browsing BigQuery results."""

import contextlib
import csv
import io
import json
import math
import subprocess
from typing import Any

from google.cloud import bigquery
from rich.text import Text
from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.coordinate import Coordinate
from textual.events import Key
from textual.widgets import (
    DataTable,
    Input,
    Label,
    OptionList,
    Tree,
)

from qmb.bigquery.browser import (
    build_table_index,
    list_dataset_ids,
    list_dataset_tables,
)
from qmb.bigquery.exporters import export_results  # re-exported for tests/monkeypatch  # noqa: F401
from qmb.bigquery.history import QueryHistoryEntry
from qmb.bigquery.pager import fetch_page, get_raw_value, json_default
from qmb.integrations import clipboard
from qmb.integrations.clipboard import ClipboardUnavailable
from qmb.integrations.editor import build_editor_command, temp_file_for_editor
from qmb.jobs.models import JobRecord
from qmb.tui.browser_pane import BrowserController
from qmb.tui.export_picker import ExportController
from qmb.tui.help_screen import HelpScreen
from qmb.tui.history_picker import HistoryController
from qmb.tui.jobs_picker import JobsController
from qmb.tui.key_router import PendingKeyRouter
from qmb.tui.search import CellSearchController, ColumnPickerController
from qmb.types import ExportFormat, PageResult, QueryResultHandle, fmt_bytes

# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------


class QueryResultApp(App):
    """Textual app for browsing BigQuery query results."""

    ENABLE_COMMAND_PALETTE = False
    ESCAPE_TO_MINIMIZE = False

    CSS = """
    #app-layout {
        height: 1fr;
    }
    #browser-panel {
        display: none;
        width: 25%;
        min-width: 32;
        border: tall $accent;
        padding: 0 1;
    }
    #browser-search {
        display: none;
        height: 3;
        margin: 0 0 1 0;
    }
    #browser-tree {
        height: 1fr;
    }
    #browser-status {
        height: 1;
    }
    #main-pane {
        width: 1fr;
    }
    .browser-only #browser-panel {
        width: 1fr;
        min-width: 0;
    }
    .browser-only #main-pane {
        display: none;
    }
    #result-table {
        height: 1fr;
    }
    #page-bar {
        height: 1;
        background: $boost;
        padding: 0 1;
    }
    #column-picker, #export-picker, #history-picker, #jobs-picker {
        display: none;
        height: auto;
        max-height: 16;
        border: tall $accent;
    }
    #column-filter, #export-filter, #history-filter, #jobs-filter {
        height: 3;
    }
    #column-list, #export-list, #history-list, #jobs-list {
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
        bq_client: bigquery.Client | None,
        handle: QueryResultHandle,
        source_label: str,
        resolved_sql: str = "",
        page_size: int = 200,
        start_in_browser: bool = False,
        browser_only: bool = False,
        history_entries: list[QueryHistoryEntry] | None = None,
        result_source: Any | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.bq_client = bq_client
        self.handle = handle
        self.result_source = result_source
        self.source_label = source_label
        self.resolved_sql = resolved_sql
        self.page_size = page_size
        self.start_in_browser = start_in_browser
        self.browser_only = browser_only
        self.current_page = 0
        self._raw_rows: list[dict[str, Any]] = []
        self._display_rows: list[dict[str, str]] = []
        self._column_names: list[str] = []
        # Visual mode: anchor as (row_idx, data_col_idx) when active.
        self._visual_anchor: tuple[int, int] | None = None
        self._visual_last_rect: tuple[int, int, int, int] | None = None
        self._key_router = PendingKeyRouter(timeout=0.4)
        self._search = CellSearchController(self)
        self._columns = ColumnPickerController(self)
        self._export = ExportController(self)
        self._browser = BrowserController(self)
        self._history = HistoryController(self, history_entries)
        self._jobs = JobsController(self)

    # -- legacy browser-state aliases (kept for backwards-compatible tests) --

    @property
    def _browser_dataset_ids(self) -> list[str]:
        return self._browser.dataset_ids

    @_browser_dataset_ids.setter
    def _browser_dataset_ids(self, value: list[str]) -> None:
        self._browser.dataset_ids = value

    @property
    def _browser_tables_by_dataset(self) -> dict[str, tuple[str, ...]]:
        return self._browser.tables_by_dataset

    @_browser_tables_by_dataset.setter
    def _browser_tables_by_dataset(self, value: dict[str, tuple[str, ...]]) -> None:
        self._browser.tables_by_dataset = value

    @property
    def _browser_index_ready(self) -> bool:
        return self._browser.index_ready

    @_browser_index_ready.setter
    def _browser_index_ready(self, value: bool) -> None:
        self._browser.index_ready = value

    def compose(self) -> ComposeResult:
        with Horizontal(id="app-layout"):
            with Vertical(id="browser-panel"):
                yield Input(placeholder="Search datasets or tables…", id="browser-search")
                yield Tree("datasets", id="browser-tree")
                yield Label("0 datasets", id="browser-status")
            with Vertical(id="main-pane"):
                yield DataTable(id="result-table")
                with Vertical(id="column-picker"):
                    yield Input(placeholder="Filter columns…", id="column-filter")
                    yield OptionList(id="column-list")
                with Vertical(id="export-picker"):
                    yield Input(placeholder="Filter formats…", id="export-filter")
                    yield OptionList(id="export-list")
                with Vertical(id="history-picker"):
                    yield Input(placeholder="Search recent queries…", id="history-filter")
                    yield OptionList(id="history-list")
                with Vertical(id="jobs-picker"):
                    yield Input(placeholder="Search archived qmb jobs…", id="jobs-filter")
                    yield OptionList(id="jobs-list")
                yield Input(placeholder="Search value…", id="cell-search")
                yield Label("Page: 1/1  ·  ? for help", id="page-bar")

    def on_mount(self) -> None:
        if self.browser_only:
            self.add_class("browser-only")
        table = self.query_one("#result-table", DataTable)
        table.cursor_type = "cell"
        browser_tree = self.query_one("#browser-tree", Tree)
        browser_tree.show_root = False
        browser_tree.auto_expand = False
        browser_tree.root.expand()
        self._browser.close_search()
        self._browser.render()
        if self._history.entries:
            self._open_history_picker()
            return
        if self.start_in_browser:
            panel = self.query_one("#browser-panel", Vertical)
            panel.display = True
            self._browser.ensure_datasets()
            self._browser.ensure_index()
            self._browser.focus_tree()
            self._browser.render()
            return
        table.focus()
        if self._can_load_results():
            self._load_page(0)
        else:
            self._render_page(
                PageResult(rows=[], display_rows=[], page=0, total_pages=1, total_rows=0)
            )

    # -- notify helpers -----------------------------------------------------

    def _info(self, msg: str) -> None:
        self.notify(msg, severity="information")

    def _warn(self, msg: str) -> None:
        self.notify(msg, severity="warning")

    def _error(self, msg: str) -> None:
        self.notify(msg, severity="error")

    @on(DataTable.CellHighlighted)
    def _enforce_min_column(self, event: DataTable.CellHighlighted) -> None:
        if self._column_names and event.coordinate.column == 0:
            self.query_one("#result-table", DataTable).move_cursor(column=1)
            return
        if self._visual_anchor is not None:
            self._refresh_visual_highlight()

    # -- key handling (hjkl + multi-key sequences) --------------------------

    def _picker_active(self) -> bool:
        return (
            self.query_one("#column-picker", Vertical).display
            or self.query_one("#export-picker", Vertical).display
            or self.query_one("#history-picker", Vertical).display
            or self.query_one("#jobs-picker", Vertical).display
            or self.query_one("#cell-search", Input).display
        )

    def _dismiss_picker(self) -> None:
        self.query_one("#column-picker", Vertical).display = False
        self.query_one("#export-picker", Vertical).display = False
        self.query_one("#export-list", OptionList).display = True
        self.query_one("#export-filter", Input).display = True
        self.query_one("#history-picker", Vertical).display = False
        self.query_one("#jobs-picker", Vertical).display = False
        self.query_one("#cell-search", Input).display = False
        self._export.format = None
        self.query_one("#result-table", DataTable).focus()

    # -- browser pane delegators -------------------------------------------

    def action_toggle_browser(self) -> None:
        if self.bq_client is None:
            self._warn("Browser unavailable for archived jobs")
            return
        self._browser.toggle()

    def _focus_browser_tree(self) -> None:
        self._browser.focus_tree()

    def _open_browser_search(self) -> None:
        self._browser.open_search()

    def _close_browser_search(self) -> None:
        self._browser.close_search()

    def _select_browser_dataset(self, dataset_id: str) -> None:
        self._browser.select_dataset(dataset_id)

    def _move_browser_cursor_first(self) -> None:
        self._browser.move_cursor_first()

    def _move_browser_cursor_last(self) -> None:
        self._browser.move_cursor_last()

    @work(thread=True)
    def _load_browser_datasets(self) -> None:
        try:
            dataset_ids = list_dataset_ids(self.bq_client)
        except Exception as exc:
            self.call_from_thread(self._browser.on_datasets_failed, str(exc))
            return
        self.call_from_thread(self._browser.on_datasets_loaded, dataset_ids)

    @work(thread=True)
    def _load_browser_index(self, dataset_ids: tuple[str, ...]) -> None:
        try:
            tables_by_dataset = build_table_index(self.bq_client, dataset_ids)
        except Exception as exc:
            self.call_from_thread(self._browser.on_index_failed, str(exc))
            return
        self.call_from_thread(self._browser.on_index_loaded, tables_by_dataset)

    @work(thread=True)
    def _load_browser_dataset_tables(self, dataset_id: str) -> None:
        try:
            table_ids = list_dataset_tables(self.bq_client, dataset_id)
        except Exception as exc:
            self.call_from_thread(
                self._browser.on_dataset_tables_failed, dataset_id, str(exc)
            )
            return
        self.call_from_thread(
            self._browser.on_dataset_tables_loaded, dataset_id, table_ids
        )

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
        if self._browser.handle_key(event):
            return

        if self._browser.focus_active() and getattr(self.focused, "id", None) == "browser-search":
            return

        # When a picker is focused, handle escape and arrow navigation
        if self._picker_active():
            if event.key == "escape":
                self._dismiss_picker()
                event.prevent_default()
                event.stop()
            elif self.query_one("#column-picker", Vertical).display:
                self._navigate_option_list("#column-list", event)
            elif self.query_one("#history-picker", Vertical).display:
                self._navigate_option_list("#history-list", event)
            elif self.query_one("#jobs-picker", Vertical).display:
                self._navigate_option_list("#jobs-list", event)
            return

        if event.key == "b":
            self.action_toggle_browser()
            event.prevent_default()
            event.stop()
            return

        # Second key of a visual-mode yank. In visual mode, bare `y` defaults
        # to TSV after the pending-key timeout, while `yc` / `yj` / `yt` copy
        # immediately as CSV / JSON / TSV.
        if self._key_router.is_pending("visual_y"):
            self._key_router.clear()
            event.prevent_default()
            event.stop()
            if self._visual_anchor is None:
                return
            if event.key == "escape":
                self._exit_visual()
                return
            if event.key == "c":
                self._copy_visual_selection("csv")
            elif event.key == "j":
                self._copy_visual_selection("json")
            else:
                self._copy_visual_selection("tsv")
            self._exit_visual()
            return

        # Visual mode intercepts a small set of keys before falling through
        # to the normal navigation / pending-key logic.
        if self._visual_anchor is not None:
            if event.key == "escape":
                self._exit_visual()
                event.prevent_default()
                event.stop()
                return
            if event.key == "v":
                self._exit_visual()
                event.prevent_default()
                event.stop()
                return
            if event.key == "y":
                self._key_router.start("visual_y")
                self.set_timer(self._key_router.timeout, self._on_pending_timeout)
                event.prevent_default()
                event.stop()
                return

        # Escape clears search matches
        if event.key == "escape" and self._search.matches:
            self._search.clear()
            self._info("Search cleared")
            event.prevent_default()
            event.stop()
            return

        # Second key of a pending sequence
        if self._key_router.is_pending("y"):
            self._key_router.clear()
            event.prevent_default()
            event.stop()
            if event.key == "w":
                self._copy_cell()
            elif event.key == "c":
                self._copy_row_csv()
            elif event.key == "j":
                self._copy_row_json()
            return

        if self._key_router.is_pending("x"):
            self._key_router.clear()
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

        if self._key_router.is_pending("g"):
            self._key_router.clear()
            event.prevent_default()
            event.stop()
            if event.key == "g":
                table = self.query_one("#result-table", DataTable)
                table.move_cursor(row=0)
            return

        # First key — start sequence, search, or navigate
        if event.key in {"y", "x", "g"}:
            self._key_router.start(event.key)
            self.set_timer(self._key_router.timeout, self._on_pending_timeout)
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

        if event.key == "H":
            self._load_and_open_history()
            event.prevent_default()
            event.stop()
            return

        if event.key == "J":
            self._load_and_open_jobs()
            event.prevent_default()
            event.stop()
            return

        # n/N — next/prev match when search is active, else page navigation
        if event.key == "n" and self._search.matches:
            self._search.goto(1)
            event.prevent_default()
            event.stop()
            return

        if event.key == "N" and self._search.matches:
            self._search.goto(-1)
            event.prevent_default()
            event.stop()
            return

        if event.key == "v":
            self._enter_visual()
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
        elif event.key == "G":
            table.move_cursor(row=table.row_count - 1)
            event.prevent_default()
        elif event.key == "dollar_sign":
            table.move_cursor(column=len(table.columns) - 1)
            event.prevent_default()
        elif event.key == "0":
            table.move_cursor(column=1)  # skip # column
            event.prevent_default()

    def _on_pending_timeout(self) -> None:
        if self._key_router.is_pending("x"):
            self._key_router.clear()
            self._open_export_picker()
        elif self._key_router.is_pending("y"):
            self._key_router.clear()
        elif self._key_router.is_pending("visual_y"):
            self._key_router.clear()
            if self._visual_anchor is not None:
                self._copy_visual_selection("tsv")
                self._exit_visual()

    # -- cell search / column picker / browser search ----------------------

    @on(Input.Submitted, "#cell-search")
    def _on_cell_search(self, event: Input.Submitted) -> None:
        self._search.on_submitted(event.value)

    @on(Input.Changed, "#browser-search")
    def _on_browser_search_changed(self, event: Input.Changed) -> None:
        self._browser.on_search_changed(event.value)

    @on(Input.Submitted, "#browser-search")
    def _on_browser_search_submitted(self, event: Input.Submitted) -> None:
        self._browser.on_search_submitted(event.value)

    @on(Tree.NodeHighlighted, "#browser-tree")
    def _on_browser_node_highlighted(self, event: Tree.NodeHighlighted) -> None:
        return None

    @on(Tree.NodeSelected, "#browser-tree")
    def _on_browser_node_selected(self, event: Tree.NodeSelected) -> None:
        return None

    def _open_column_picker(self) -> None:
        self._columns.open()

    @on(Input.Changed, "#column-filter")
    def _on_column_filter_changed(self, event: Input.Changed) -> None:
        self._columns.on_filter_changed(event.value)

    @on(Input.Submitted, "#column-filter")
    def _on_column_filter_submitted(self, event: Input.Submitted) -> None:
        self._columns.on_filter_submitted()

    @on(OptionList.OptionSelected, "#column-list")
    def _on_column_selected(self, event: OptionList.OptionSelected) -> None:
        self._columns.on_option_selected(event.option_index)

    # -- clipboard ----------------------------------------------------------

    def _data_col(self) -> int:
        """Map DataTable column index to data column index (skip row-number col)."""
        return self.query_one("#result-table", DataTable).cursor_coordinate.column - 1

    def _copy_cell(self) -> None:
        table = self.query_one("#result-table", DataTable)
        if not self._raw_rows or not self._column_names:
            self._warn("No data to copy")
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
            clipboard.copy(full_text)
        except ClipboardUnavailable:
            self._error("Clipboard not available")
            return
        self._info(f"Copied {col_name} value")

    def _copy_row_json(self) -> None:
        table = self.query_one("#result-table", DataTable)
        if not self._raw_rows:
            self._warn("No data to copy")
            return

        row_idx = table.cursor_coordinate.row
        if row_idx < 0 or row_idx >= len(self._raw_rows):
            return

        raw_row = self._raw_rows[row_idx]

        try:
            clipboard.copy(json.dumps(raw_row, indent=2, default=json_default))
        except ClipboardUnavailable:
            self._error("Clipboard not available")
            return
        self._info("Copied row as JSON")

    def _copy_row_csv(self) -> None:
        table = self.query_one("#result-table", DataTable)
        if not self._raw_rows or not self._column_names:
            self._warn("No data to copy")
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
            clipboard.copy(buf.getvalue())
        except ClipboardUnavailable:
            self._error("Clipboard not available")
            return
        self._info("Copied row as CSV")

    # -- vim cell / query ---------------------------------------------------

    def action_vim_cell(self) -> None:
        table = self.query_one("#result-table", DataTable)
        if not self._raw_rows or not self._column_names:
            self._warn("No data to inspect")
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

        self._open_in_editor(full_text, suffix=ext, prefix=f"qmb_{col_name}_")

    def action_vim_query(self) -> None:
        self._open_in_editor(self.resolved_sql, suffix=".sql", prefix="qmb_query_")

    def _open_in_editor(
        self,
        content: str,
        *,
        suffix: str,
        prefix: str,
        read_only: bool = True,
    ) -> None:
        with temp_file_for_editor(content, suffix=suffix, prefix=prefix) as tmp_path:
            cmd = build_editor_command(tmp_path, read_only=read_only)
            with self.suspend():
                subprocess.run(cmd, check=False)

    # -- export picker ------------------------------------------------------

    def _open_export_picker(self) -> None:
        self._export.open()

    def _select_export_format(self, option_idx: int) -> None:
        self._export.select_format(option_idx)

    def _quick_export(self, fmt: ExportFormat, ext: str) -> None:
        self._export.quick_export(fmt, ext)

    @on(Input.Changed, "#export-filter")
    def _on_export_filter_changed(self, event: Input.Changed) -> None:
        self._export.on_filter_changed(event.value)

    @on(Input.Submitted, "#export-filter")
    def _on_export_filter_submitted(self, event: Input.Submitted) -> None:
        self._export.on_filter_submitted()

    @on(OptionList.OptionSelected, "#export-list")
    def _on_export_selected(self, event: OptionList.OptionSelected) -> None:
        self._export.on_option_selected(event.option_index)

    # -- history picker -----------------------------------------------------

    def _load_and_open_history(self) -> None:
        if self.bq_client is None:
            self._warn("BigQuery history unavailable for archived jobs")
            return
        self._history.load_and_open()

    def _open_history_picker(self) -> None:
        self._history.open()

    def _select_history_entry(self, option_idx: int) -> None:
        self._history.select(option_idx)

    @work(thread=True)
    def _fetch_history(self) -> None:
        from qmb.bigquery.history import list_recent_queries

        try:
            entries = list_recent_queries(self.bq_client, days=7, limit=200)
        except Exception as exc:
            self.call_from_thread(self._history.on_load_failed, str(exc))
            return
        self.call_from_thread(self._history.on_load_succeeded, entries)

    def on_resize(self) -> None:
        self._history.on_resize()
        self._jobs.on_resize()

    @on(Input.Changed, "#history-filter")
    def _on_history_filter_changed(self, event: Input.Changed) -> None:
        self._history.on_filter_changed(event.value)

    @on(Input.Submitted, "#history-filter")
    def _on_history_filter_submitted(self, event: Input.Submitted) -> None:
        self._history.on_filter_submitted()

    @on(OptionList.OptionSelected, "#history-list")
    def _on_history_selected(self, event: OptionList.OptionSelected) -> None:
        self._history.on_option_selected(event.option_index)

    # -- jobs picker (local qmb archive) -----------------------------------

    def _load_and_open_jobs(self) -> None:
        self._jobs.load_and_open()

    def _open_jobs_picker(self) -> None:
        self._jobs.open()

    def _select_jobs_entry(self, option_idx: int) -> None:
        self._jobs.select(option_idx)

    @on(Input.Changed, "#jobs-filter")
    def _on_jobs_filter_changed(self, event: Input.Changed) -> None:
        self._jobs.on_filter_changed(event.value)

    @on(Input.Submitted, "#jobs-filter")
    def _on_jobs_filter_submitted(self, event: Input.Submitted) -> None:
        self._jobs.on_filter_submitted()

    @on(OptionList.OptionSelected, "#jobs-list")
    def _on_jobs_selected(self, event: OptionList.OptionSelected) -> None:
        self._jobs.on_option_selected(event.option_index)

    def _open_archived_job(self, record: JobRecord) -> None:
        """Swap the current TUI view to an archived qmb job's preview."""
        from qmb.jobs.result_source import JsonlPreviewResultSource

        source = JsonlPreviewResultSource.from_job(record)
        schema = record.schema or []
        self.result_source = source
        self.handle = QueryResultHandle(
            job_id=record.qmb_job_id,
            project=record.engine.project or "",
            location=record.engine.location or "",
            destination_table="",
            schema=[field.to_mapping() for field in schema],
            total_rows=source.total_rows,
            bytes_processed=record.bytes_processed,
            execution_seconds=record.execution_seconds,
        )
        self.source_label = f"archive: {record.qmb_job_id}"
        try:
            self.resolved_sql = record.query_path.read_text(encoding="utf-8")
        except OSError:
            self.resolved_sql = ""
        self.current_page = 0
        self._load_page(0)
        self._info(f"Loaded archive {record.qmb_job_id}")

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
        self._open_in_editor(details, suffix=".txt", prefix="qmb_job_")

    # -- help ---------------------------------------------------------------

    def action_show_help(self) -> None:
        self.push_screen(HelpScreen())

    # -- pagination ---------------------------------------------------------

    def _can_load_results(self) -> bool:
        return self.handle.total_rows > 0 and (
            self.result_source is not None or bool(self.handle.destination_table)
        )

    @work(thread=True)
    def _load_page(self, page: int) -> None:
        if self.result_source is not None:
            result = self.result_source.page(page, self.page_size)
        else:
            result = fetch_page(self.bq_client, self.handle, page, self.page_size)
        self.call_from_thread(self._render_page, result)

    def _render_page(self, result: PageResult) -> None:
        table = self.query_one("#result-table", DataTable)
        table.clear(columns=True)

        self.current_page = result.page
        self._raw_rows = result.rows
        self._display_rows = result.display_rows
        self._column_names = []
        self._visual_anchor = None
        self._visual_last_rect = None
        self._search.clear()

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

    def _update_page_bar(self, result: PageResult | None = None) -> None:
        if result is not None:
            self._page_bar_base = (
                f"Page: {result.page + 1}/{result.total_pages}  ·  ? for help"
            )
        text = getattr(self, "_page_bar_base", "Page: 1/1  ·  ? for help")
        if self._visual_anchor is not None:
            r0, r1, c0, c1 = self._visual_rect()
            text = f"-- VISUAL -- ({r1 - r0 + 1}×{c1 - c0 + 1})  ·  {text}"
        self.query_one("#page-bar", Label).update(text)

    # -- visual mode --------------------------------------------------------

    def _enter_visual(self) -> None:
        if not self._raw_rows or not self._column_names:
            self._warn("No data to select")
            return
        table = self.query_one("#result-table", DataTable)
        row_idx = table.cursor_coordinate.row
        col_idx = self._data_col()
        if row_idx < 0 or col_idx < 0:
            return
        self._visual_anchor = (row_idx, col_idx)
        self._visual_last_rect = None
        self._refresh_visual_highlight()
        self._update_page_bar()

    def _exit_visual(self) -> None:
        if self._key_router.is_pending("visual_y"):
            self._key_router.clear()
        if self._visual_anchor is None:
            return
        self._visual_anchor = None
        self._clear_visual_highlight()
        self._update_page_bar()

    def _visual_rect(self) -> tuple[int, int, int, int]:
        """Return (row_min, row_max, col_min, col_max) inclusive."""
        assert self._visual_anchor is not None
        table = self.query_one("#result-table", DataTable)
        ar, ac = self._visual_anchor
        cr = max(0, min(table.cursor_coordinate.row, len(self._raw_rows) - 1))
        cc = max(0, min(self._data_col(), len(self._column_names) - 1))
        return min(ar, cr), max(ar, cr), min(ac, cc), max(ac, cc)

    def _set_cell_plain(self, row_idx: int, data_col_idx: int) -> None:
        table = self.query_one("#result-table", DataTable)
        col_name = self._column_names[data_col_idx]
        value = (
            self._display_rows[row_idx].get(col_name, "")
            if row_idx < len(self._display_rows)
            else ""
        )
        with contextlib.suppress(Exception):
            table.update_cell_at(Coordinate(row_idx, data_col_idx + 1), value)

    def _set_cell_selected(self, row_idx: int, data_col_idx: int) -> None:
        table = self.query_one("#result-table", DataTable)
        col_name = self._column_names[data_col_idx]
        value = (
            self._display_rows[row_idx].get(col_name, "")
            if row_idx < len(self._display_rows)
            else ""
        )
        with contextlib.suppress(Exception):
            table.update_cell_at(
                Coordinate(row_idx, data_col_idx + 1),
                Text(value, style="reverse"),
            )

    def _refresh_visual_highlight(self) -> None:
        if self._visual_anchor is None:
            return
        r0, r1, c0, c1 = self._visual_rect()
        prev = self._visual_last_rect
        # Clear cells that were selected before but aren't anymore.
        if prev is not None:
            pr0, pr1, pc0, pc1 = prev
            for r in range(pr0, pr1 + 1):
                for c in range(pc0, pc1 + 1):
                    if not (r0 <= r <= r1 and c0 <= c <= c1):
                        self._set_cell_plain(r, c)
        # Mark current rect.
        for r in range(r0, r1 + 1):
            for c in range(c0, c1 + 1):
                self._set_cell_selected(r, c)
        self._visual_last_rect = (r0, r1, c0, c1)
        self._update_page_bar()

    def _clear_visual_highlight(self) -> None:
        if self._visual_last_rect is None:
            return
        r0, r1, c0, c1 = self._visual_last_rect
        for r in range(r0, r1 + 1):
            for c in range(c0, c1 + 1):
                self._set_cell_plain(r, c)
        self._visual_last_rect = None

    def _copy_visual_selection(self, fmt: str = "tsv") -> None:
        if self._visual_anchor is None:
            return
        r0, r1, c0, c1 = self._visual_rect()
        cols = self._column_names[c0 : c1 + 1]
        label = fmt.upper()

        if fmt == "json":
            selected_rows = [
                {col: self._raw_rows[r].get(col) for col in cols}
                for r in range(r0, r1 + 1)
            ]
            text = json.dumps(selected_rows, indent=2, default=json_default)
        else:
            buf = io.StringIO()
            if fmt == "csv":
                writer = csv.DictWriter(buf, fieldnames=cols)
                writer.writeheader()
                for r in range(r0, r1 + 1):
                    row = self._raw_rows[r]
                    writer.writerow({col: get_raw_value(row.get(col)) for col in cols})
            else:
                label = "TSV"
                writer = csv.writer(buf, dialect="excel-tab")
                writer.writerow(cols)
                for r in range(r0, r1 + 1):
                    row = self._raw_rows[r]
                    writer.writerow([get_raw_value(row.get(col)) for col in cols])
            text = buf.getvalue()

        try:
            clipboard.copy(text)
        except ClipboardUnavailable:
            self._error("Clipboard not available")
            return
        n_rows = r1 - r0 + 1
        n_cols = c1 - c0 + 1
        self._info(f"Copied {n_rows}×{n_cols} selection as {label}")

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
