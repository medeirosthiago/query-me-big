"""Paginate through BigQuery query results."""

import json
import math
from collections.abc import Iterator
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

from google.cloud import bigquery

from qmb.types import PageResult, QueryResultHandle

MAX_DISPLAY_WIDTH = 60


def json_default(obj: Any) -> Any:
    """Serialize BigQuery values to JSON-compatible types."""
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        return obj.hex()
    return str(obj)


def fetch_page(
    client: bigquery.Client,
    handle: QueryResultHandle,
    page: int,
    page_size: int = 200,
) -> PageResult:
    """Fetch a single page of results from the result table."""
    total_pages = max(1, math.ceil(handle.total_rows / page_size))
    page = max(0, min(page, total_pages - 1))

    start_index = page * page_size

    table_ref = _table_ref_from_handle(handle)

    rows_iter = client.list_rows(
        table_ref,
        start_index=start_index,
        max_results=page_size,
    )

    raw_rows: list[dict[str, Any]] = []
    display_rows: list[dict[str, str]] = []

    for row in rows_iter:
        raw = dict(row.items())
        raw_rows.append(raw)
        display_rows.append({k: _format_display(v) for k, v in raw.items()})

    return PageResult(
        rows=raw_rows,
        display_rows=display_rows,
        page=page,
        total_pages=total_pages,
        total_rows=handle.total_rows,
    )


def iter_all_rows(
    client: bigquery.Client,
    handle: QueryResultHandle,
    chunk_size: int = 5000,
) -> Iterator[dict[str, Any]]:
    """Iterate all rows from the result table without materializing them in memory."""
    table_ref = _table_ref_from_handle(handle)
    for start_index in range(0, handle.total_rows, chunk_size):
        rows_iter = client.list_rows(
            table_ref,
            start_index=start_index,
            max_results=chunk_size,
        )
        for row in rows_iter:
            yield dict(row.items())


def get_raw_value(value: Any) -> str:
    """Get the full string representation of a value for clipboard copy."""
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, indent=2, default=json_default)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def _format_display(value: Any) -> str:
    """Format a value for table display (truncated)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, (dict, list)):
        s = json.dumps(value, default=str)
        return _truncate(s)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return _truncate(value.hex())
    s = str(value)
    return _truncate(s)


def _truncate(s: str, max_len: int = MAX_DISPLAY_WIDTH) -> str:
    """Truncate a string for display."""
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def _table_ref_from_handle(handle: QueryResultHandle) -> bigquery.TableReference:
    dest_parts = handle.destination_table.split(".")
    return bigquery.TableReference(
        bigquery.DatasetReference(dest_parts[0], dest_parts[1]),
        dest_parts[2],
    )
