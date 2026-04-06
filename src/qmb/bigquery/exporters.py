"""Export query results to CSV, JSON, or Parquet."""

import csv
import json
from collections.abc import Iterable, Iterator
from itertools import chain
from pathlib import Path
from typing import Any

from google.cloud import bigquery

from qmb.bigquery.pager import get_raw_value, iter_all_rows, json_default
from qmb.types import ExportFormat, QueryResultHandle


def export_results(
    client: bigquery.Client,
    handle: QueryResultHandle,
    fmt: ExportFormat,
    output_path: Path,
) -> int:
    """Export all query results to the specified format. Returns row count."""
    rows = iter_all_rows(client, handle)

    if fmt == ExportFormat.CSV:
        return _export_csv(rows, handle.schema, output_path)
    if fmt == ExportFormat.JSON:
        return _export_json(rows, handle.schema, output_path)
    if fmt == ExportFormat.PARQUET:
        return _export_parquet(rows, handle.schema, output_path)
    return 0


def _export_csv(rows: Iterable[dict[str, Any]], schema: list[dict], output_path: Path) -> int:
    """Export to CSV."""
    fieldnames = [col["name"] for col in schema]
    rows_iter = iter(rows)
    first_row = next(rows_iter, None)
    if first_row is None:
        output_path.write_text("", encoding="utf-8")
        return 0

    count = 0
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in chain([first_row], rows_iter):
            writer.writerow({name: _csv_value(row.get(name)) for name in fieldnames})
            count += 1
    return count


def _export_json(rows: Iterable[dict[str, Any]], schema: list[dict], output_path: Path) -> int:
    """Export to a streamed JSON array."""
    fieldnames = [col["name"] for col in schema]
    rows_iter = iter(rows)
    first_row = next(rows_iter, None)
    if first_row is None:
        output_path.write_text("[]", encoding="utf-8")
        return 0

    count = 0
    with output_path.open("w", encoding="utf-8") as f:
        f.write("[\n")
        for i, row in enumerate(chain([first_row], rows_iter)):
            if i:
                f.write(",\n")
            f.write(json.dumps(_ordered_row(row, fieldnames), indent=2, default=json_default))
            count += 1
        f.write("\n]")
    return count


def _export_parquet(rows: Iterable[dict[str, Any]], schema: list[dict], output_path: Path) -> int:
    """Export to Parquet via pyarrow without loading all rows into memory."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    fieldnames = [col["name"] for col in schema]
    writer: pq.ParquetWriter | None = None
    count = 0

    for batch in _iter_row_batches(rows):
        ordered_batch = [_ordered_row(row, fieldnames) for row in batch]
        table = pa.Table.from_pylist(
            ordered_batch,
            schema=writer.schema if writer is not None else None,
        )
        if writer is None:
            writer = pq.ParquetWriter(str(output_path), table.schema)
        writer.write_table(table)
        count += len(batch)

    if writer is None:
        columns = {col["name"]: [] for col in schema}
        table = pa.table(columns)
        pq.write_table(table, str(output_path))
        return 0

    writer.close()
    return count


def _csv_value(value: Any) -> str:
    """Convert a value for CSV output."""
    return get_raw_value(value)


def _iter_row_batches(
    rows: Iterable[dict[str, Any]], batch_size: int = 5000
) -> Iterator[list[dict[str, Any]]]:
    batch: list[dict[str, Any]] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _ordered_row(row: dict[str, Any], fieldnames: list[str]) -> dict[str, Any]:
    return {name: row.get(name) for name in fieldnames}
