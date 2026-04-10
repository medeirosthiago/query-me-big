import asyncio
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from google.cloud.bigquery.schema import SchemaField
from textual.widgets import Input, Tree

from qmb.tui.app import QueryResultApp
from qmb.types import ExportFormat, PageResult, QueryResultHandle


class DummyBigQueryClient:
    project = "proj"


def _handle() -> QueryResultHandle:
    return QueryResultHandle(
        job_id="job-123",
        project="proj",
        location="US",
        destination_table="proj.ds.tbl",
        schema=[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}],
        total_rows=3,
    )


async def _run_export_picker_flow(app: QueryResultApp) -> str:
    async with app.run_test(headless=True, size=(100, 40), notifications=True) as pilot:
        await pilot.pause()
        app._open_export_picker()
        await pilot.pause()
        app._select_export_format(0)
        await pilot.pause()

        export_path = app.query_one("#export-filter").value

        await pilot.press("enter")
        await pilot.pause()
        return export_path


def _fake_fetch_page(client, handle, page, page_size=200):
    return PageResult(
        rows=[{"id": 1}, {"id": 2}, {"id": 3}],
        display_rows=[{"id": "1"}, {"id": "2"}, {"id": "3"}],
        page=0,
        total_pages=1,
        total_rows=3,
    )


def _browser_snapshot(app: QueryResultApp) -> list[tuple[str, list[str]]]:
    tree = app.query_one("#browser-tree", Tree)
    return [
        (node.label.plain, [child.label.plain for child in node.children])
        for node in tree.root.children
    ]


def _dataset_details_stub() -> SimpleNamespace:
    return SimpleNamespace(
        project="proj",
        dataset_id="dataset1",
        friendly_name="Dataset One",
        created=datetime(2026, 4, 10, 10, 0, tzinfo=UTC),
        modified=datetime(2026, 4, 11, 11, 30, tzinfo=UTC),
        location="US",
        description="Dataset description",
        default_table_expiration_ms=86_400_000,
        default_partition_expiration_ms=None,
        default_rounding_mode="ROUND_HALF_EVEN",
        is_case_insensitive=False,
        max_time_travel_hours=168,
        storage_billing_model="LOGICAL",
        path="/projects/proj/datasets/dataset1",
        etag="dataset-etag",
        labels={"env": "prod"},
        access_entries=[object()],
        _properties={"defaultCollation": "und:ci"},
    )


def _table_details_stub() -> SimpleNamespace:
    return SimpleNamespace(
        project="proj",
        dataset_id="dataset1",
        table_id="table1",
        friendly_name="Table One",
        table_type="TABLE",
        created=datetime(2026, 4, 10, 10, 0, tzinfo=UTC),
        modified=datetime(2026, 4, 11, 12, 0, tzinfo=UTC),
        expires=None,
        location="US",
        description="Table description",
        time_partitioning=None,
        range_partitioning=None,
        partitioning_type="DAY",
        clustering_fields=["id"],
        default_collation=None,
        default_rounding_mode=None,
        path="/projects/proj/datasets/dataset1/tables/table1",
        etag="table-etag",
        labels={"tier": "gold"},
        num_rows=123,
        num_bytes=4096,
        schema=[SchemaField("id", "INTEGER", mode="REQUIRED", description="identifier")],
        view_query=None,
        external_data_configuration=None,
        _properties={
            "defaultCollation": "und:ci",
            "numTotalLogicalBytes": "4096",
            "numActiveLogicalBytes": "2048",
            "numCurrentPhysicalBytes": "1024",
            "numTotalPhysicalBytes": "2048",
            "numTimeTravelPhysicalBytes": "512",
        },
    )


def test_export_picker_preserves_selected_format(monkeypatch) -> None:
    recorded: dict[str, object] = {}

    def fake_export_results(client, handle, fmt, path):
        recorded["fmt"] = fmt
        recorded["path"] = path
        recorded["total_rows"] = handle.total_rows
        return 3

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    monkeypatch.setattr("qmb.tui.app.export_results", fake_export_results)

    app = QueryResultApp(DummyBigQueryClient(), _handle(), "ad-hoc", "select 1")
    export_path = asyncio.run(_run_export_picker_flow(app))

    assert recorded == {
        "fmt": ExportFormat.CSV,
        "path": Path(export_path),
        "total_rows": 3,
    }


def test_table_has_focus_on_startup(monkeypatch) -> None:
    async def run() -> None:
        app = QueryResultApp(DummyBigQueryClient(), _handle(), "ad-hoc", "select 1")

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()

            assert app.query_one("#result-table").has_focus
            assert getattr(app.focused, "id", None) == "result-table"

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())


def test_browser_only_mode_starts_with_browser_open(monkeypatch) -> None:
    async def run() -> None:
        app = QueryResultApp(
            DummyBigQueryClient(),
            QueryResultHandle(
                job_id="",
                project="proj",
                location="US",
                destination_table="",
                schema=[],
                total_rows=0,
            ),
            "browser",
            start_in_browser=True,
            browser_only=True,
        )
        app._browser_dataset_ids = ["dataset1", "dataset2"]
        app._browser_index_ready = True

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()

            assert app.query_one("#browser-panel").display is True
            assert app.query_one("#browser-tree", Tree).has_focus
            assert app.has_class("browser-only")
            assert _browser_snapshot(app) == [("dataset1", []), ("dataset2", [])]

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())


def test_browser_enter_opens_dataset_details(monkeypatch) -> None:
    async def run() -> None:
        client = DummyBigQueryClient()
        client.get_dataset = lambda dataset_ref: _dataset_details_stub()

        captured: dict[str, str] = {}
        app = QueryResultApp(client, _handle(), "ad-hoc", "select 1")
        app._browser_dataset_ids = ["dataset1"]
        app._browser_index_ready = True
        app._open_in_nvim = lambda content, suffix, prefix: captured.update(
            {"content": content, "suffix": suffix, "prefix": prefix}
        )

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()
            app.action_toggle_browser()
            await pilot.pause()
            await pilot.press("enter")
            await pilot.pause()

            assert captured["suffix"] == ".txt"
            assert captured["prefix"] == "qmb_dataset_dataset1_"
            assert "Dataset Details" in captured["content"]
            assert "proj.dataset1" in captured["content"]
            assert "Dataset description" in captured["content"]
            assert "env=prod" in captured["content"]

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())


def test_browser_enter_opens_table_details(monkeypatch) -> None:
    async def run() -> None:
        client = DummyBigQueryClient()
        client.get_table = lambda table_ref: _table_details_stub()

        captured: dict[str, str] = {}
        app = QueryResultApp(client, _handle(), "ad-hoc", "select 1")
        app._browser_dataset_ids = ["dataset1"]
        app._browser_tables_by_dataset = {"dataset1": ("table1",)}
        app._browser_index_ready = True
        app._open_in_nvim = lambda content, suffix, prefix: captured.update(
            {"content": content, "suffix": suffix, "prefix": prefix}
        )

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()
            app.action_toggle_browser()
            await pilot.pause()
            app._select_browser_dataset("dataset1")
            await pilot.pause()

            tree = app.query_one("#browser-tree", Tree)
            tree.select_node(tree.root.children[0].children[0])
            tree.focus()
            await pilot.pause()
            await pilot.press("enter")
            await pilot.pause()

            assert captured["suffix"] == ".txt"
            assert captured["prefix"] == "qmb_table_table1_"
            assert "Table Details" in captured["content"]
            assert "proj.dataset1.table1" in captured["content"]
            assert "Table description" in captured["content"]
            assert "id: INTEGER [REQUIRED]" in captured["content"]

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())


def test_browser_toggle_and_table_search(monkeypatch) -> None:
    async def run() -> None:
        app = QueryResultApp(DummyBigQueryClient(), _handle(), "ad-hoc", "select 1")
        app._browser_dataset_ids = ["dataset1", "dataset2", "dataset3"]
        app._browser_tables_by_dataset = {
            "dataset1": ("table1", "table2"),
            "dataset2": ("table9",),
            "dataset3": ("table1", "table2", "table3"),
        }
        app._browser_index_ready = True

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()
            app.action_toggle_browser()
            await pilot.pause()

            assert app.query_one("#browser-panel").display is True
            assert app.query_one("#browser-tree", Tree).has_focus
            assert app.query_one("#browser-search", Input).display is False
            assert _browser_snapshot(app) == [
                ("dataset1", []),
                ("dataset2", []),
                ("dataset3", []),
            ]

            app._open_browser_search()
            await pilot.pause()

            search = app.query_one("#browser-search", Input)
            assert search.display is True
            assert search.has_focus

            search.value = "table1"
            await pilot.pause()
            app._close_browser_search()
            app._focus_browser_tree()
            await pilot.pause()

            assert search.display is False
            assert app.query_one("#browser-tree", Tree).has_focus

            assert _browser_snapshot(app) == [
                ("dataset1", ["dataset1.table1"]),
                ("dataset3", ["dataset3.table1"]),
            ]

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())


def test_browser_tree_expands_selected_dataset(monkeypatch) -> None:
    async def run() -> None:
        app = QueryResultApp(DummyBigQueryClient(), _handle(), "ad-hoc", "select 1")
        app._browser_dataset_ids = ["dataset1", "dataset2", "dataset3"]
        app._browser_tables_by_dataset = {
            "dataset1": ("table1", "table2"),
            "dataset2": ("table9",),
            "dataset3": ("table1", "table2", "table3"),
        }
        app._browser_index_ready = True

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()
            app.action_toggle_browser()
            await pilot.pause()

            tree = app.query_one("#browser-tree", Tree)
            await pilot.pause()

            app._select_browser_dataset("dataset1")
            await pilot.pause()

            assert tree.has_focus
            assert _browser_snapshot(app) == [
                ("dataset1", ["dataset1.table1", "dataset1.table2"]),
                ("dataset2", []),
                ("dataset3", []),
            ]

            app._move_browser_cursor_last()
            await pilot.pause()
            assert tree.cursor_node is not None
            assert tree.cursor_node.label.plain == "dataset3"

            app._move_browser_cursor_first()
            await pilot.pause()
            assert tree.cursor_node is not None
            assert tree.cursor_node.label.plain == "dataset1"

            app.action_toggle_browser()
            await pilot.pause()
            assert app.query_one("#browser-panel").display is False
            assert app.query_one("#result-table").has_focus

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())


def test_browser_search_escape_returns_to_navigation(monkeypatch) -> None:
    async def run() -> None:
        app = QueryResultApp(DummyBigQueryClient(), _handle(), "ad-hoc", "select 1")
        app._browser_dataset_ids = ["dataset1", "dataset2"]
        app._browser_tables_by_dataset = {
            "dataset1": ("table1",),
            "dataset2": ("table2",),
        }
        app._browser_index_ready = True

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()
            app.action_toggle_browser()
            await pilot.pause()
            app._open_browser_search()
            await pilot.pause()

            search = app.query_one("#browser-search", Input)
            search.value = "data"
            await pilot.pause()

            app._close_browser_search()
            app._focus_browser_tree()
            await pilot.pause()

            assert app.query_one("#browser-search", Input).display is False
            assert app.query_one("#browser-tree", Tree).has_focus
            assert _browser_snapshot(app) == [
                ("dataset1", ["dataset1.table1"]),
                ("dataset2", ["dataset2.table2"]),
            ]

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())
