"""Browser pane controller.

Owns dataset/table browser state, async loading workers' result handlers,
tree rendering, search input, key handling, and details opening.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from textual.containers import Vertical
from textual.css.query import NoMatches
from textual.events import Key
from textual.widgets import Input, Label, Tree

from qmb.bigquery.browser import (
    BrowserMatch,
    filter_browser_matches,
    format_dataset_details,
    format_table_details,
    get_dataset_metadata,
    get_table_metadata,
)

if TYPE_CHECKING:
    from qmb.tui.app import QueryResultApp


class BrowserController:
    """Controls the dataset/table browser pane."""

    def __init__(self, app: QueryResultApp) -> None:
        self.app = app
        self.dataset_ids: list[str] = []
        self.tables_by_dataset: dict[str, tuple[str, ...]] = {}
        self.query = ""
        self.selected_dataset: str | None = None
        self.loading_datasets = False
        self.loading_index = False
        self.loading_tables: set[str] = set()
        self.index_ready = False
        self.rendering = False
        self.pending_key: str | None = None

    # -- widget readiness --------------------------------------------------

    def widgets_ready(self) -> bool:
        try:
            self.app.query_one("#browser-tree", Tree)
            self.app.query_one("#browser-status", Label)
        except NoMatches:
            return False
        return True

    def focus_active(self) -> bool:
        if not self.widgets_ready():
            return False
        focused_id = getattr(self.app.focused, "id", None)
        return self.app.query_one("#browser-panel", Vertical).display and focused_id in {
            "browser-search",
            "browser-tree",
        }

    # -- toggle / focus / search -------------------------------------------

    def toggle(self) -> None:
        panel = self.app.query_one("#browser-panel", Vertical)
        panel.display = not panel.display
        if panel.display:
            self.ensure_datasets()
            self.ensure_index()
            self.close_search()
            self.render()
            self.focus_tree()
            return
        self.pending_key = None
        self.close_search()
        from textual.widgets import DataTable

        self.app.query_one("#result-table", DataTable).focus()

    def focus_tree(self) -> None:
        tree = self.app.query_one("#browser-tree", Tree)
        if tree.cursor_node is None and tree.root.children:
            tree.select_node(tree.root.children[0])
        tree.focus()

    def open_search(self) -> None:
        search = self.app.query_one("#browser-search", Input)
        search.display = True
        search.value = self.query
        search.focus()

    def close_search(self) -> None:
        search = self.app.query_one("#browser-search", Input)
        self.query = search.value
        search.display = False

    # -- dataset loading (results handlers) --------------------------------

    def ensure_datasets(self) -> None:
        if self.dataset_ids or self.loading_datasets:
            return
        self.loading_datasets = True
        self.update_status()
        self.app._load_browser_datasets()

    def on_datasets_loaded(self, dataset_ids: list[str]) -> None:
        self.loading_datasets = False
        self.dataset_ids = dataset_ids
        if self.selected_dataset not in dataset_ids:
            self.selected_dataset = None
        self.render()
        if self.app.query_one("#browser-panel", Vertical).display:
            self.ensure_index()

    def on_datasets_failed(self, error: str) -> None:
        self.loading_datasets = False
        self.render()
        self.app._error(f"Browser load failed: {error}")

    # -- table index loading -----------------------------------------------

    def ensure_index(self) -> None:
        if not self.dataset_ids or self.loading_index or self.index_ready:
            return
        self.loading_index = True
        self.update_status()
        self.app._load_browser_index(tuple(self.dataset_ids))

    def on_index_loaded(self, tables_by_dataset: dict[str, tuple[str, ...]]) -> None:
        self.loading_index = False
        self.index_ready = True
        self.tables_by_dataset.update(tables_by_dataset)
        self.render()

    def on_index_failed(self, error: str) -> None:
        self.loading_index = False
        self.render()
        self.app._error(f"Table index failed: {error}")

    # -- per-dataset table loading ----------------------------------------

    def ensure_dataset_tables(self, dataset_id: str) -> None:
        if (
            self.index_ready
            or dataset_id in self.tables_by_dataset
            or dataset_id in self.loading_tables
        ):
            return
        self.loading_tables.add(dataset_id)
        self.update_status()
        self.app._load_browser_dataset_tables(dataset_id)

    def on_dataset_tables_loaded(
        self, dataset_id: str, table_ids: tuple[str, ...]
    ) -> None:
        self.loading_tables.discard(dataset_id)
        self.tables_by_dataset[dataset_id] = table_ids
        self.render()

    def on_dataset_tables_failed(self, dataset_id: str, error: str) -> None:
        self.loading_tables.discard(dataset_id)
        self.render()
        self.app._error(f"Dataset browser failed for {dataset_id}: {error}")

    # -- matching / rendering ---------------------------------------------

    def matches(self) -> list[BrowserMatch]:
        if self.query.strip():
            return filter_browser_matches(
                self.dataset_ids,
                self.tables_by_dataset,
                self.query,
            )

        result: list[BrowserMatch] = []
        for dataset_id in self.dataset_ids:
            tables: tuple[str, ...] = ()
            if dataset_id == self.selected_dataset:
                tables = tuple(
                    f"{dataset_id}.{table_id}"
                    for table_id in self.tables_by_dataset.get(dataset_id, ())
                )
            result.append(BrowserMatch(dataset_id=dataset_id, tables=tables))
        return result

    def render(self, *, sync_search_input: bool = True) -> None:
        if not self.widgets_ready():
            return
        tree = self.app.query_one("#browser-tree", Tree)
        search = self.app.query_one("#browser-search", Input)
        tree.root.remove_children()
        tree.root.expand()

        matches = self.matches()
        dataset_nodes: dict[str, Any] = {}

        self.rendering = True
        try:
            if not self.dataset_ids and not self.loading_datasets:
                tree.root.add_leaf("(no datasets)")
            elif not matches and self.query.strip():
                tree.root.add_leaf("(no matches)")
            else:
                for match in matches:
                    dataset_node = tree.root.add(
                        match.dataset_id,
                        data=("dataset", match.dataset_id),
                        expand=bool(match.tables),
                    )
                    dataset_nodes[match.dataset_id] = dataset_node
                    for table_name in match.tables:
                        _, table_id = table_name.split(".", 1)
                        dataset_node.add_leaf(
                            table_name,
                            data=("table", match.dataset_id, table_id),
                        )

            target_dataset = self.selected_dataset
            if target_dataset is None and self.query.strip() and matches:
                target_dataset = matches[0].dataset_id

            if target_dataset in dataset_nodes:
                tree.select_node(dataset_nodes[target_dataset])
            elif tree.root.children and tree.has_focus:
                tree.select_node(tree.root.children[0])
            else:
                tree.select_node(None)
        finally:
            self.rendering = False

        if sync_search_input and search.display and search.value != self.query:
            search.value = self.query
        self.update_status(len(matches))

    def update_status(self, match_count: int | None = None) -> None:
        if not self.widgets_ready():
            return
        if self.loading_datasets:
            status = "Loading datasets…"
        elif self.loading_tables:
            pending = len(self.loading_tables)
            status = f"Loading {pending} dataset{'s' if pending != 1 else ''}…"
        elif self.loading_index:
            status = f"{len(self.dataset_ids)} datasets · loading tables…"
        elif self.query.strip():
            count = 0 if match_count is None else match_count
            status = f"{count} match{'es' if count != 1 else ''}"
        else:
            count = len(self.dataset_ids)
            status = f"{count} dataset{'s' if count != 1 else ''}"
        self.app.query_one("#browser-status", Label).update(status)

    # -- selection / cursor ------------------------------------------------

    def select_dataset(self, dataset_id: str) -> None:
        if not dataset_id:
            return
        self.selected_dataset = dataset_id
        self.ensure_dataset_tables(dataset_id)
        self.render()

    def collapse_cursor(self) -> None:
        if self.query.strip():
            return
        tree = self.app.query_one("#browser-tree", Tree)
        node = tree.cursor_node
        if node is None or not node.data:
            return
        kind = node.data[0]
        if kind == "table" and node.parent is not None:
            node = node.parent
        if node.data and node.data[0] == "dataset":
            dataset_id = node.data[1]
            if self.selected_dataset == dataset_id:
                self.selected_dataset = None
                self.render()

    def activate_cursor(self) -> None:
        node = self.app.query_one("#browser-tree", Tree).cursor_node
        if node is None or not node.data:
            return
        if node.data[0] == "dataset":
            self.select_dataset(node.data[1])

    def open_details(self) -> None:
        node = self.app.query_one("#browser-tree", Tree).cursor_node
        if node is None or not node.data:
            self.app._warn("No browser item selected")
            return

        try:
            if node.data[0] == "dataset":
                dataset_id = node.data[1]
                dataset = get_dataset_metadata(self.app.bq_client, dataset_id)
                details = format_dataset_details(dataset)
                self.app._open_in_editor(
                    details, suffix=".txt", prefix=f"qmb_dataset_{dataset_id}_"
                )
                return

            if node.data[0] == "table":
                dataset_id = node.data[1]
                table_id = node.data[2]
                table = get_table_metadata(self.app.bq_client, dataset_id, table_id)
                details = format_table_details(table)
                self.app._open_in_editor(
                    details, suffix=".txt", prefix=f"qmb_table_{table_id}_"
                )
                return
        except Exception as exc:
            self.app._error(f"Browser details failed: {exc}")

    def move_cursor_first(self) -> None:
        tree = self.app.query_one("#browser-tree", Tree)
        if tree.root.children:
            tree.move_cursor_to_line(0)

    def move_cursor_last(self) -> None:
        tree = self.app.query_one("#browser-tree", Tree)
        if tree.root.children:
            tree.move_cursor_to_line(tree.last_line)

    # -- key handling ------------------------------------------------------

    def handle_key(self, event: Key) -> bool:
        if not self.focus_active():
            return False

        focused_id = getattr(self.app.focused, "id", None)
        tree = self.app.query_one("#browser-tree", Tree)

        if focused_id == "browser-search":
            if event.key == "enter":
                self.close_search()
                self.focus_tree()
                event.prevent_default()
                event.stop()
                return True
            if event.key == "escape":
                self.close_search()
                self.focus_tree()
                event.prevent_default()
                event.stop()
                return True
            self.pending_key = None
            return False

        if focused_id == "browser-tree":
            if self.pending_key == "g":
                self.pending_key = None
                if event.key == "g":
                    self.move_cursor_first()
                    event.prevent_default()
                    event.stop()
                return True

            if event.key == "g":
                self.pending_key = "g"
                self.app.set_timer(0.4, self.on_pending_timeout)
                event.prevent_default()
                event.stop()
                return True

            if event.key in {"j", "down"}:
                tree.action_cursor_down()
            elif event.key in {"k", "up"}:
                tree.action_cursor_up()
            elif event.key in {"l", "right"}:
                self.activate_cursor()
            elif event.key in {"enter", "d"}:
                self.open_details()
            elif event.key in {"h", "left"}:
                self.collapse_cursor()
            elif event.key == "G":
                self.move_cursor_last()
            elif event.key == "slash":
                self.open_search()
            elif event.key in {"escape", "b"}:
                self.toggle()
            else:
                self.pending_key = None
                return True
            self.pending_key = None
            event.prevent_default()
            event.stop()
            return True

        return False

    def on_pending_timeout(self) -> None:
        self.pending_key = None

    # -- search input event handlers --------------------------------------

    def on_search_changed(self, value: str) -> None:
        self.query = value
        if self.query.strip():
            self.ensure_index()
        self.render(sync_search_input=False)

    def on_search_submitted(self, value: str) -> None:
        self.query = value
        self.render()
        self.close_search()
        self.focus_tree()
