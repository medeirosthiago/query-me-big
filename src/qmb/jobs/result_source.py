"""Result-source adapters for local archived job artifacts."""

import math

from qmb.bigquery.pager import _format_display
from qmb.jobs.artifacts import read_jsonl_rows
from qmb.jobs.models import JobRecord
from qmb.types import PageResult, SchemaField


class JsonlPreviewResultSource:
    """Page rows from a job's preview.jsonl artifact without calling BigQuery."""

    def __init__(self, record: JobRecord, schema: list[SchemaField]) -> None:
        self._record = record
        self.schema = schema
        self.total_rows = record.total_rows

    @classmethod
    def from_job(cls, record: JobRecord) -> "JsonlPreviewResultSource":
        schema = record.schema
        if schema is None:
            # JobStore.read() normally populates this. The fallback keeps the
            # adapter robust for records assembled directly in tests/tools.
            import json

            schema = [
                SchemaField.from_mapping(field)
                for field in json.loads(record.schema_path.read_text(encoding="utf-8"))
            ]
        return cls(record, schema)

    def page(self, page: int, page_size: int = 200) -> PageResult:
        total_pages = max(1, math.ceil(self.total_rows / page_size))
        page = max(0, min(page, total_pages - 1))
        start = page * page_size
        end = start + page_size

        rows = list(self.iter_rows())
        page_rows = rows[start:end]
        display_rows = [
            {key: _format_display(value) for key, value in row.items()} for row in page_rows
        ]
        return PageResult(
            rows=page_rows,
            display_rows=display_rows,
            page=page,
            total_pages=total_pages,
            total_rows=self.total_rows,
        )

    def iter_rows(self):
        return read_jsonl_rows(self._record.preview_path)
