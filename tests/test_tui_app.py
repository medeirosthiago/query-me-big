import asyncio
from pathlib import Path

from qmb.tui.app import QueryResultApp
from qmb.types import ExportFormat, PageResult, QueryResultHandle


class DummyBigQueryClient:
    pass


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
        await pilot.press("x")
        await pilot.pause(0.5)
        await pilot.press("enter")
        await pilot.pause()

        export_path = app.query_one("#export-filter").value

        await pilot.press("enter")
        await pilot.pause()
        return export_path


def test_export_picker_preserves_selected_format(monkeypatch) -> None:
    recorded: dict[str, object] = {}

    def fake_fetch_page(client, handle, page, page_size=200):
        return PageResult(
            rows=[{"id": 1}, {"id": 2}, {"id": 3}],
            display_rows=[{"id": "1"}, {"id": "2"}, {"id": "3"}],
            page=0,
            total_pages=1,
            total_rows=3,
        )

    def fake_export_results(client, handle, fmt, path):
        recorded["fmt"] = fmt
        recorded["path"] = path
        recorded["total_rows"] = handle.total_rows
        return 3

    monkeypatch.setattr("qmb.tui.app.fetch_page", fake_fetch_page)
    monkeypatch.setattr("qmb.tui.app.export_results", fake_export_results)

    app = QueryResultApp(DummyBigQueryClient(), _handle(), "ad-hoc", "select 1")
    export_path = asyncio.run(_run_export_picker_flow(app))

    assert recorded == {
        "fmt": ExportFormat.CSV,
        "path": Path(export_path),
        "total_rows": 3,
    }
