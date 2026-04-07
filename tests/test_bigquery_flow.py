import json
from datetime import datetime
from typing import Any

from qmb.bigquery.executor import execute_query
from qmb.bigquery.exporters import export_results
from qmb.bigquery.pager import fetch_page, iter_all_rows
from qmb.types import ExportFormat, QueryResultHandle, ResolvedQuery


class FakeRow:
    def __init__(self, values: dict[str, Any]) -> None:
        self._values = values

    def items(self):
        return self._values.items()


class FakeSchemaField:
    def __init__(self, name: str, field_type: str, mode: str = "NULLABLE") -> None:
        self.name = name
        self.field_type = field_type
        self.mode = mode


class FakeDestination:
    project = "proj"
    dataset_id = "ds"
    table_id = "tbl"


class FakeQueryResult:
    def __init__(self, rows: list[dict[str, Any]], schema: list[FakeSchemaField]) -> None:
        self.total_rows = len(rows)
        self.schema = schema


class FakeQueryJob:
    def __init__(self, rows: list[dict[str, Any]], schema: list[FakeSchemaField]) -> None:
        self.job_id = "job-123"
        self.project = "proj"
        self.location = "US"
        self.destination = FakeDestination()
        self.total_bytes_processed = 2048
        self.started = datetime(2026, 4, 1, 12, 0, 0)
        self.ended = datetime(2026, 4, 1, 12, 0, 2)
        self._result = FakeQueryResult(rows, schema)

    def result(self) -> FakeQueryResult:
        return self._result


class FakeBigQueryClient:
    def __init__(self, rows: list[dict[str, Any]], schema: list[FakeSchemaField]) -> None:
        self.rows = rows
        self.schema = schema
        self.list_rows_calls: list[dict[str, Any]] = []

    def query(self, sql: str, job_config: Any) -> FakeQueryJob:
        self.sql = sql
        self.job_config = job_config
        return FakeQueryJob(self.rows, self.schema)

    def list_rows(
        self,
        table_ref: Any,
        start_index: int | None = None,
        max_results: int | None = None,
        page_size: int | None = None,
    ) -> list[FakeRow]:
        self.list_rows_calls.append(
            {
                "project": table_ref.project,
                "dataset_id": table_ref.dataset_id,
                "table_id": table_ref.table_id,
                "start_index": start_index,
                "max_results": max_results,
                "page_size": page_size,
            }
        )

        if max_results is not None:
            start = start_index or 0
            end = start + max_results
            return [FakeRow(row) for row in self.rows[start:end]]

        return []


def _rows() -> list[dict[str, Any]]:
    return [
        {"id": 1, "enabled": True, "payload": {"items": [1, 2]}},
        {"id": 2, "enabled": False, "payload": {"items": [3]}},
        {"id": 3, "enabled": True, "payload": {"items": []}},
    ]


def _schema() -> list[FakeSchemaField]:
    return [
        FakeSchemaField("id", "INTEGER"),
        FakeSchemaField("enabled", "BOOLEAN"),
        FakeSchemaField("payload", "JSON"),
    ]


def _handle(total_rows: int) -> QueryResultHandle:
    return QueryResultHandle(
        job_id="job-123",
        project="proj",
        location="US",
        destination_table="proj.ds.tbl",
        schema=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "enabled", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "payload", "type": "JSON", "mode": "NULLABLE"},
        ],
        total_rows=total_rows,
    )


def test_execute_page_and_export_share_the_same_query_results(tmp_path) -> None:
    rows = _rows()
    client = FakeBigQueryClient(rows, _schema())

    handle = execute_query(
        client,
        ResolvedQuery(sql="select * from example", source_label="ad-hoc"),
    )

    page = fetch_page(client, handle, page=0, page_size=2)
    assert page.rows == rows[:2]
    assert handle.destination_table == "proj.ds.tbl"
    assert handle.total_rows == 3

    out = tmp_path / "rows.json"
    count = export_results(client, handle, ExportFormat.JSON, out)

    assert count == 3
    assert json.loads(out.read_text(encoding="utf-8")) == rows


def test_iter_all_rows_matches_the_explicit_paging_path() -> None:
    rows = _rows()
    handle = _handle(total_rows=len(rows))
    client = FakeBigQueryClient(rows, _schema())

    page = fetch_page(client, handle, page=0, page_size=2)
    assert page.rows == rows[:2]

    assert list(iter_all_rows(client, handle, chunk_size=2)) == rows
