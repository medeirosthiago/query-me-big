import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from google.cloud.bigquery.schema import SchemaField
from textual.widgets import Input, OptionList, Tree

from qmb.bigquery.history import QueryHistoryEntry
from qmb.jobs.models import EngineMetadata, JobRecord, SourceMetadata
from qmb.jobs.result_source import JsonlPreviewResultSource
from qmb.jobs.store import JobStore
from qmb.tui.app import QueryResultApp
from qmb.types import AgentContext, ExportFormat, PageResult, QueryResultHandle
from qmb.types import SchemaField as QmbSchemaField


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


def _handle_3col() -> QueryResultHandle:
    return QueryResultHandle(
        job_id="job-3col",
        project="proj",
        location="US",
        destination_table="proj.ds.tbl",
        schema=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "city", "type": "STRING", "mode": "NULLABLE"},
        ],
        total_rows=3,
    )


def _fake_fetch_page_3x3(client, handle, page, page_size=200):
    return PageResult(
        rows=[
            {"id": 1, "name": "Alice", "city": "NYC"},
            {"id": 2, "name": "Bob", "city": "LA"},
            {"id": 3, "name": "Carol", "city": "SF"},
        ],
        display_rows=[
            {"id": "1", "name": "Alice", "city": "NYC"},
            {"id": "2", "name": "Bob", "city": "LA"},
            {"id": "3", "name": "Carol", "city": "SF"},
        ],
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


class DummyResultSource:
    def __init__(self) -> None:
        self.calls: list[tuple[int, int]] = []

    def page(self, page: int, page_size: int) -> PageResult:
        self.calls.append((page, page_size))
        return PageResult(
            rows=[{"id": 10}],
            display_rows=[{"id": "10"}],
            page=page,
            total_pages=1,
            total_rows=1,
        )


def test_tui_can_load_page_from_archived_result_source(monkeypatch) -> None:
    async def run() -> None:
        source = DummyResultSource()
        handle = QueryResultHandle(
            job_id="qmb_job",
            project="",
            location="",
            destination_table="",
            schema=[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}],
            total_rows=1,
        )
        app = QueryResultApp(
            None,
            handle,
            "archive: qmb_job",
            "select 10 as id",
            page_size=50,
            result_source=source,
        )

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()

            assert source.calls == [(0, 50)]
            assert app._raw_rows == [{"id": 10}]
            assert app._column_names == ["id"]
            assert app.query_one("#result-table").has_focus

    monkeypatch.setattr(
        "qmb.tui.app.fetch_page",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("used BigQuery")),
    )
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


def test_tui_exports_archived_job_preview_rows(tmp_path: Path) -> None:
    record = JobStore(
        root=tmp_path / "jobs",
        now=lambda: datetime(2026, 5, 12, 14, 33, 2, tzinfo=UTC),
        nonce=lambda: "a1b2c3",
    ).create(
        resolved_sql="select id, name from example",
        schema=[QmbSchemaField("id", "INTEGER"), QmbSchemaField("name", "STRING")],
        preview_rows=[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        source=SourceMetadata(label="ad-hoc", input_mode="sql"),
        engine=EngineMetadata(name="bigquery", job_id="bq-job-123", project="proj", location="US"),
        total_rows=2,
    )

    async def run() -> Path:
        source = JsonlPreviewResultSource.from_job(record)
        app = QueryResultApp(
            None,
            QueryResultHandle(
                job_id=record.qmb_job_id,
                project=record.engine.project or "",
                location=record.engine.location or "",
                destination_table="",
                schema=[field.to_mapping() for field in record.schema],
                total_rows=source.total_rows,
            ),
            f"archive: {record.qmb_job_id}",
            record.query_path.read_text(encoding="utf-8"),
            result_source=source,
        )
        export_path = tmp_path / "archive.csv"

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()
            app._open_export_picker()
            await pilot.pause()
            app._select_export_format(0)
            await pilot.pause()
            app.query_one("#export-filter", Input).value = str(export_path)
            await pilot.press("enter")
            await pilot.pause()

        return export_path

    export_path = asyncio.run(run())

    assert export_path.read_text(encoding="utf-8") == "id,name\n1,Alice\n2,Bob\n"


def test_browser_enter_opens_dataset_details(monkeypatch) -> None:
    async def run() -> None:
        client = DummyBigQueryClient()
        client.get_dataset = lambda dataset_ref: _dataset_details_stub()

        captured: dict[str, str] = {}
        app = QueryResultApp(client, _handle(), "ad-hoc", "select 1")
        app._browser_dataset_ids = ["dataset1"]
        app._browser_index_ready = True
        app._open_in_editor = lambda content, **kw: captured.update(
            {"content": content, **kw}
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
        app._open_in_editor = lambda content, **kw: captured.update(
            {"content": content, **kw}
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


def test_browser_search_change_does_not_rewrite_focused_input(monkeypatch) -> None:
    """Incremental search must not reset the active input during its change cycle."""

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
            app._open_browser_search()
            await pilot.pause()

            search = app.query_one("#browser-search", Input)
            writes: list[str] = []
            original_setattr = search.__class__.__setattr__

            def record_value_writes(self, name, value):
                if self is search and name == "value":
                    writes.append(value)
                original_setattr(self, name, value)

            monkeypatch.setattr(search.__class__, "__setattr__", record_value_writes)

            app._browser.on_search_changed("table1")
            await pilot.pause()

            assert writes == []

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


def _make_history_entry(query="SELECT 1", hours_ago=0):
    return QueryHistoryEntry(
        job_id=f"job-{hours_ago}",
        project="proj",
        location="US",
        created=datetime(2026, 4, 13, 14, 0, tzinfo=UTC) - timedelta(hours=hours_ago),
        query=query,
        bytes_processed=1024,
    )


def test_history_picker_opens_with_entries(monkeypatch) -> None:
    async def run() -> None:
        entries = [_make_history_entry("SELECT 1", 0), _make_history_entry("SELECT 2", 1)]
        app = QueryResultApp(
            DummyBigQueryClient(), _handle(), "ad-hoc", "select 1", history_entries=entries
        )

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()

            assert app.query_one("#history-picker").display is True
            assert app.query_one("#history-filter", Input).has_focus
            assert app.query_one("#history-list", OptionList).option_count == 2

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())


def test_history_picker_filters_by_query_text(monkeypatch) -> None:
    async def run() -> None:
        entries = [
            _make_history_entry("SELECT * FROM orders", 0),
            _make_history_entry("SELECT * FROM users", 1),
            _make_history_entry("INSERT INTO logs", 2),
        ]
        app = QueryResultApp(
            DummyBigQueryClient(), _handle(), "ad-hoc", "select 1", history_entries=entries
        )

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()

            inp = app.query_one("#history-filter", Input)
            inp.value = "orders"
            await pilot.pause()

            assert app.query_one("#history-list", OptionList).option_count == 1

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())


def test_history_picker_filters_by_date(monkeypatch) -> None:
    async def run() -> None:
        entries = [
            _make_history_entry("SELECT 1", 0),  # 2026-04-13
            _make_history_entry("SELECT 2", 72),  # 2026-04-10
        ]
        app = QueryResultApp(
            DummyBigQueryClient(), _handle(), "ad-hoc", "select 1", history_entries=entries
        )

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()

            inp = app.query_one("#history-filter", Input)
            inp.value = "2026-04-10"
            await pilot.pause()

            assert app.query_one("#history-list", OptionList).option_count == 1

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())


def test_history_picker_opens_editor_and_reopens(monkeypatch) -> None:
    async def run() -> None:
        entries = [_make_history_entry("SELECT * FROM orders", 0)]
        app = QueryResultApp(
            DummyBigQueryClient(), _handle(), "ad-hoc", "select 1", history_entries=entries
        )

        captured: dict[str, object] = {}
        app._open_in_editor = lambda content, **kw: captured.update(
            {"content": content, **kw}
        )

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()

            app._select_history_entry(0)
            await pilot.pause()

            assert captured["content"] == "SELECT * FROM orders"
            assert captured["suffix"] == ".sql"
            assert captured["read_only"] is False
            assert app.query_one("#history-picker").display is True

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())


def test_history_picker_dismiss_on_escape(monkeypatch) -> None:
    async def run() -> None:
        entries = [_make_history_entry("SELECT 1", 0)]
        app = QueryResultApp(
            DummyBigQueryClient(), _handle(), "ad-hoc", "select 1", history_entries=entries
        )

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()

            assert app.query_one("#history-picker").display is True

            await pilot.press("escape")
            await pilot.pause()

            assert app.query_one("#history-picker").display is False

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())


def _make_job_record(
    tmp_path: Path,
    *,
    qmb_job_id: str = "qmb_2026-05-13_13-04-32_a1b2c3",
    label: str = "model: orders",
    schema: list[QmbSchemaField] | None = None,
    rows: list[dict] | None = None,
    sql: str = "select 1 as id",
    session_id: str | None = None,
    agent_context: AgentContext | None = None,
) -> JobRecord:
    """Build an on-disk job archive directory and return its JobRecord."""
    from qmb.jobs.artifacts import write_jsonl_rows

    schema = schema or [QmbSchemaField(name="id", type="INTEGER", mode="NULLABLE")]
    rows = rows or [{"id": 1}, {"id": 2}]

    directory = tmp_path / qmb_job_id
    directory.mkdir(parents=True)
    (directory / "query.sql").write_text(sql, encoding="utf-8")
    (directory / "schema.json").write_text("[]", encoding="utf-8")
    write_jsonl_rows(directory / "preview.jsonl", rows, fieldnames=[f.name for f in schema])

    return JobRecord(
        qmb_job_id=qmb_job_id,
        created_at=datetime(2026, 5, 13, 13, 4, 32, tzinfo=UTC),
        source=SourceMetadata(label=label, input_mode="model", model_name="orders"),
        engine=EngineMetadata(name="bigquery", job_id="bq-1", project="proj", location="US"),
        total_rows=len(rows),
        bytes_processed=4096,
        execution_seconds=1.5,
        directory=directory,
        metadata_path=directory / "metadata.json",
        query_path=directory / "query.sql",
        schema_path=directory / "schema.json",
        preview_path=directory / "preview.jsonl",
        session_id=session_id,
        agent_context=agent_context,
        schema=schema,
    )


def test_jobs_picker_opens_via_J(monkeypatch, tmp_path) -> None:
    async def run() -> None:
        record = _make_job_record(tmp_path, session_id="pi-session-42")

        app = QueryResultApp(DummyBigQueryClient(), _handle(), "ad-hoc", "select 1")

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()
            # Inject records so we don't touch JobStore on disk.
            app._jobs.records = [record]
            app._jobs.open()
            await pilot.pause()

            assert app.query_one("#jobs-picker").display is True
            assert app.query_one("#jobs-filter", Input).has_focus
            opt = app.query_one("#jobs-list", OptionList)
            assert opt.option_count == 1
            label = opt.get_option_at_index(0).prompt
            assert "model: orders" in label
            assert "a1b2c3" in label
            assert "2 rows" in label
            assert "session:pi-session-42" in label
            # SQL excerpt appears after the source-label tag.
            assert "select 1 as id" in label

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())


def test_jobs_picker_filters_by_text(monkeypatch, tmp_path) -> None:
    async def run() -> None:
        r1 = _make_job_record(
            tmp_path,
            qmb_job_id="qmb_2026-05-13_13-04-32_a1b2c3",
            label="model: orders",
            session_id="agent-orders-debug",
        )
        r2 = _make_job_record(
            tmp_path,
            qmb_job_id="qmb_2026-05-13_13-05-00_ff9999",
            label="file: queries/users.sql",
        )

        app = QueryResultApp(DummyBigQueryClient(), _handle(), "ad-hoc", "select 1")

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()
            app._jobs.records = [r1, r2]
            app._jobs.open()
            await pilot.pause()

            inp = app.query_one("#jobs-filter", Input)
            inp.value = "users"
            await pilot.pause()
            assert app.query_one("#jobs-list", OptionList).option_count == 1

            inp.value = "a1b2c3"
            await pilot.pause()
            assert app.query_one("#jobs-list", OptionList).option_count == 1

            inp.value = "agent-orders"
            await pilot.pause()
            assert app.query_one("#jobs-list", OptionList).option_count == 1

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())


def test_jobs_picker_select_swaps_to_archived_job(monkeypatch, tmp_path) -> None:
    async def run() -> None:
        record = _make_job_record(
            tmp_path,
            qmb_job_id="qmb_2026-05-13_13-04-32_a1b2c3",
            rows=[{"id": 10}, {"id": 20}, {"id": 30}],
        )

        app = QueryResultApp(DummyBigQueryClient(), _handle(), "ad-hoc", "select 1")

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()
            app._jobs.records = [record]
            app._jobs.open()
            await pilot.pause()

            app._select_jobs_entry(0)
            await pilot.pause()

            assert app.query_one("#jobs-picker").display is False
            assert app.source_label == f"archive: {record.qmb_job_id}"
            assert app.handle.job_id == record.qmb_job_id
            assert app.handle.total_rows == 3
            assert app.resolved_sql == "select 1 as id"
            assert app._raw_rows == [{"id": 10}, {"id": 20}, {"id": 30}]
            assert app._column_names == ["id"]

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())


def test_jobs_picker_dismiss_on_escape(monkeypatch, tmp_path) -> None:
    async def run() -> None:
        record = _make_job_record(tmp_path)

        app = QueryResultApp(DummyBigQueryClient(), _handle(), "ad-hoc", "select 1")

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()
            app._jobs.records = [record]
            app._jobs.open()
            await pilot.pause()

            assert app.query_one("#jobs-picker").display is True

            await pilot.press("escape")
            await pilot.pause()

            assert app.query_one("#jobs-picker").display is False

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())


def test_capital_H_opens_history_and_lowercase_r_does_not(monkeypatch) -> None:
    async def run() -> None:
        entries = [_make_history_entry("SELECT 1", 0)]
        app = QueryResultApp(
            DummyBigQueryClient(),
            _handle(),
            "ad-hoc",
            "select 1",
        )

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()

            # Pre-seed entries so capital H opens the picker immediately.
            app._history.entries = entries

            # Lowercase r should NOT open history anymore.
            await pilot.press("r")
            await pilot.pause()
            assert app.query_one("#history-picker").display is False

            # Capital H opens it.
            await pilot.press("H")
            await pilot.pause()
            assert app.query_one("#history-picker").display is True

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())


def test_jobs_picker_warns_when_archive_empty(monkeypatch, tmp_path) -> None:
    async def run() -> None:
        app = QueryResultApp(DummyBigQueryClient(), _handle(), "ad-hoc", "select 1")

        # Point JobStore at an empty directory so list() returns [].
        monkeypatch.setenv("QMB_JOBS_DIR", str(tmp_path))

        warned: list[str] = []
        app._warn = lambda msg: warned.append(msg)

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()
            app._load_and_open_jobs()
            await pilot.pause()

            assert app.query_one("#jobs-picker").display is False
            assert warned == ["No archived qmb jobs found"]

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())


def test_jobs_picker_shows_sql_excerpt_and_collapses_whitespace(monkeypatch, tmp_path) -> None:
    async def run() -> None:
        record = _make_job_record(
            tmp_path,
            sql="SELECT id,\n       name\nFROM `proj.ds.orders`\nWHERE status = 'shipped'",
        )

        app = QueryResultApp(DummyBigQueryClient(), _handle(), "ad-hoc", "select 1")

        async with app.run_test(headless=True, size=(200, 40), notifications=True) as pilot:
            await pilot.pause()
            app._jobs.records = [record]
            app._jobs.open()
            await pilot.pause()

            opt = app.query_one("#jobs-list", OptionList)
            label = opt.get_option_at_index(0).prompt
            assert "SELECT id, name FROM `proj.ds.orders` WHERE status = 'shipped'" in label

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())


def test_jobs_picker_filters_by_sql_text(monkeypatch, tmp_path) -> None:
    async def run() -> None:
        r1 = _make_job_record(
            tmp_path,
            qmb_job_id="qmb_2026-05-13_13-04-32_a1b2c3",
            label="ad-hoc",
            sql="SELECT * FROM orders",
        )
        r2 = _make_job_record(
            tmp_path,
            qmb_job_id="qmb_2026-05-13_13-05-00_ff9999",
            label="ad-hoc",
            sql="SELECT id, email FROM users",
        )

        app = QueryResultApp(DummyBigQueryClient(), _handle(), "ad-hoc", "select 1")

        async with app.run_test(headless=True, size=(200, 40), notifications=True) as pilot:
            await pilot.pause()
            app._jobs.records = [r1, r2]
            app._jobs.open()
            await pilot.pause()

            inp = app.query_one("#jobs-filter", Input)

            inp.value = "users"  # matches SQL only, not label
            await pilot.pause()
            opt = app.query_one("#jobs-list", OptionList)
            assert opt.option_count == 1
            assert "users" in opt.get_option_at_index(0).prompt

            inp.value = "email"  # also SQL-only
            await pilot.pause()
            assert app.query_one("#jobs-list", OptionList).option_count == 1

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())


def test_jobs_picker_caches_sql_reads(monkeypatch, tmp_path) -> None:
    """populate() should only read each job's query.sql once."""
    async def run() -> None:
        record = _make_job_record(tmp_path, sql="select 1 as id")
        original_read_text = Path.read_text
        read_counts: dict[str, int] = {}

        def counting_read_text(self, *args, **kwargs):
            if self.name == "query.sql":
                read_counts[str(self)] = read_counts.get(str(self), 0) + 1
            return original_read_text(self, *args, **kwargs)

        monkeypatch.setattr(Path, "read_text", counting_read_text)

        app = QueryResultApp(DummyBigQueryClient(), _handle(), "ad-hoc", "select 1")

        async with app.run_test(headless=True, size=(200, 40), notifications=True) as pilot:
            await pilot.pause()
            app._jobs.records = [record]
            app._jobs.open()
            await pilot.pause()

            inp = app.query_one("#jobs-filter", Input)
            inp.value = "s"
            await pilot.pause()
            inp.value = "se"
            await pilot.pause()
            inp.value = "sel"
            await pilot.pause()

            reads = read_counts.get(str(record.query_path), 0)
            assert reads == 1, f"expected 1 read, got {reads}"

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page)
    asyncio.run(run())


# ---------------------------------------------------------------------------
# Visual mode
# ---------------------------------------------------------------------------


def test_visual_mode_enter_shows_indicator_in_page_bar(monkeypatch) -> None:
    async def run() -> None:
        app = QueryResultApp(DummyBigQueryClient(), _handle_3col(), "ad-hoc", "select 1")

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()
            assert app._visual_anchor is None

            await pilot.press("v")
            await pilot.pause()

            assert app._visual_anchor is not None
            bar = str(app.query_one("#page-bar").render())
            assert "VISUAL" in bar
            assert "1×1" in bar

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page_3x3)
    asyncio.run(run())


def test_visual_mode_escape_exits(monkeypatch) -> None:
    async def run() -> None:
        app = QueryResultApp(DummyBigQueryClient(), _handle_3col(), "ad-hoc", "select 1")

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()
            await pilot.press("v")
            await pilot.pause()
            assert app._visual_anchor is not None

            await pilot.press("escape")
            await pilot.pause()
            assert app._visual_anchor is None
            bar = str(app.query_one("#page-bar").render())
            assert "VISUAL" not in bar

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page_3x3)
    asyncio.run(run())


def test_visual_mode_v_again_exits(monkeypatch) -> None:
    async def run() -> None:
        app = QueryResultApp(DummyBigQueryClient(), _handle_3col(), "ad-hoc", "select 1")

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()
            await pilot.press("v")
            await pilot.pause()
            assert app._visual_anchor is not None

            await pilot.press("v")
            await pilot.pause()
            assert app._visual_anchor is None

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page_3x3)
    asyncio.run(run())


def test_visual_mode_y_copies_rectangle_as_tsv(monkeypatch) -> None:
    captured: dict[str, str] = {}

    async def run() -> None:
        app = QueryResultApp(DummyBigQueryClient(), _handle_3col(), "ad-hoc", "select 1")

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()
            # Cursor starts at (row=0, data_col=0). Enter visual, extend to 2x2.
            await pilot.press("v")
            await pilot.pause()
            await pilot.press("j")
            await pilot.press("l")
            await pilot.pause()

            bar = str(app.query_one("#page-bar").render())
            assert "2×2" in bar

            await pilot.press("y")
            await pilot.pause(0.5)

            assert "text" in captured
            # excel-tab dialect uses \r\n; selection is 2 rows x 2 cols (id, name).
            # First row is the column header so the data round-trips with names.
            assert captured["text"] == "id\tname\r\n1\tAlice\r\n2\tBob\r\n"
            # Visual mode exits after copy.
            assert app._visual_anchor is None

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page_3x3)
    monkeypatch.setattr(
        "qmb.tui.app.clipboard.copy", lambda text: captured.update(text=text)
    )
    asyncio.run(run())


def test_visual_mode_y_copies_uses_raw_values(monkeypatch) -> None:
    """Visual yank should use raw row values, not display strings."""
    captured: dict[str, str] = {}

    def fake_fetch_page(client, handle, page, page_size=200):
        return PageResult(
            rows=[{"id": 1, "name": "Alice", "city": "NYC"}],
            display_rows=[{"id": "1", "name": "Alice (truncated…)", "city": "NYC"}],
            page=0,
            total_pages=1,
            total_rows=1,
        )

    async def run() -> None:
        app = QueryResultApp(DummyBigQueryClient(), _handle_3col(), "ad-hoc", "select 1")

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()
            await pilot.press("v")
            await pilot.press("l")  # extend to id+name
            await pilot.pause()
            await pilot.press("y")
            await pilot.pause(0.5)

            assert captured["text"] == "id\tname\r\n1\tAlice\r\n"

    monkeypatch.setattr("qmb.tui.app.fetch_page", fake_fetch_page)
    monkeypatch.setattr(
        "qmb.tui.app.clipboard.copy", lambda text: captured.update(text=text)
    )
    asyncio.run(run())


def test_visual_mode_extend_backwards(monkeypatch) -> None:
    """Anchor can be at the bottom-right; selection is the bounding rect."""
    captured: dict[str, str] = {}

    async def run() -> None:
        app = QueryResultApp(DummyBigQueryClient(), _handle_3col(), "ad-hoc", "select 1")

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()
            # Move cursor to (row=2, data_col=2) first.
            await pilot.press("j")
            await pilot.press("j")
            await pilot.press("l")
            await pilot.press("l")
            await pilot.pause()
            # Now enter visual and extend up-left to (0,1).
            await pilot.press("v")
            await pilot.press("k")
            await pilot.press("k")
            await pilot.press("h")
            await pilot.pause()
            await pilot.press("y")
            await pilot.pause(0.5)

            # Selection: rows 0..2, cols name..city.
            assert captured["text"] == (
                "name\tcity\r\n"
                "Alice\tNYC\r\n"
                "Bob\tLA\r\n"
                "Carol\tSF\r\n"
            )

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page_3x3)
    monkeypatch.setattr(
        "qmb.tui.app.clipboard.copy", lambda text: captured.update(text=text)
    )
    asyncio.run(run())


def test_visual_mode_yc_copies_rectangle_as_csv(monkeypatch) -> None:
    captured: dict[str, str] = {}

    async def run() -> None:
        app = QueryResultApp(DummyBigQueryClient(), _handle_3col(), "ad-hoc", "select 1")

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()
            await pilot.press("v")
            await pilot.press("j")
            await pilot.press("l")
            await pilot.pause()

            await pilot.press("y")
            await pilot.press("c")
            await pilot.pause()

            assert captured["text"] == "id,name\r\n1,Alice\r\n2,Bob\r\n"
            assert app._visual_anchor is None

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page_3x3)
    monkeypatch.setattr(
        "qmb.tui.app.clipboard.copy", lambda text: captured.update(text=text)
    )
    asyncio.run(run())


def test_visual_mode_yj_copies_rectangle_as_json(monkeypatch) -> None:
    captured: dict[str, str] = {}

    async def run() -> None:
        app = QueryResultApp(DummyBigQueryClient(), _handle_3col(), "ad-hoc", "select 1")

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()
            await pilot.press("v")
            await pilot.press("j")
            await pilot.press("l")
            await pilot.pause()

            await pilot.press("y")
            await pilot.press("j")
            await pilot.pause()

            assert json.loads(captured["text"]) == [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ]
            assert app._visual_anchor is None

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page_3x3)
    monkeypatch.setattr(
        "qmb.tui.app.clipboard.copy", lambda text: captured.update(text=text)
    )
    asyncio.run(run())


def test_visual_mode_yt_copies_rectangle_as_tsv_without_waiting_for_timeout(monkeypatch) -> None:
    captured: dict[str, str] = {}

    async def run() -> None:
        app = QueryResultApp(DummyBigQueryClient(), _handle_3col(), "ad-hoc", "select 1")

        async with app.run_test(headless=True, size=(120, 40), notifications=True) as pilot:
            await pilot.pause()
            await pilot.press("v")
            await pilot.press("j")
            await pilot.press("l")
            await pilot.pause()

            await pilot.press("y")
            await pilot.press("t")
            await pilot.pause()

            assert captured["text"] == "id\tname\r\n1\tAlice\r\n2\tBob\r\n"
            assert app._visual_anchor is None

    monkeypatch.setattr("qmb.tui.app.fetch_page", _fake_fetch_page_3x3)
    monkeypatch.setattr(
        "qmb.tui.app.clipboard.copy", lambda text: captured.update(text=text)
    )
    asyncio.run(run())
