"""Export query results to CSV, JSON, or Parquet."""

import csv
import json
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any

from google.cloud import bigquery

from qmb.bigquery.pager import fetch_all_rows, get_raw_value
from qmb.types import ExportFormat, QueryResultHandle


def export_results(
    client: bigquery.Client,
    handle: QueryResultHandle,
    fmt: ExportFormat,
    output_path: Path,
) -> int:
    """Export all query results to the specified format. Returns row count."""
    rows = fetch_all_rows(client, handle)

    if fmt == ExportFormat.CSV:
        _export_csv(rows, handle.schema, output_path)
    elif fmt == ExportFormat.JSON:
        _export_json(rows, output_path)
    elif fmt == ExportFormat.PARQUET:
        _export_parquet(rows, handle.schema, output_path)

    return len(rows)


def _export_csv(rows: list[dict[str, Any]], schema: list[dict], output_path: Path) -> None:
    """Export to CSV."""
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return

    fieldnames = [col["name"] for col in schema]
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _csv_value(v) for k, v in row.items()})


def _export_json(rows: list[dict[str, Any]], output_path: Path) -> None:
    """Export to JSON array."""
    output_path.write_text(
        json.dumps(rows, indent=2, default=_json_serializer),
        encoding="utf-8",
    )


def _export_parquet(rows: list[dict[str, Any]], schema: list[dict], output_path: Path) -> None:
    """Export to Parquet via pyarrow."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not rows:
        # Write empty parquet with schema
        columns = {col["name"]: [] for col in schema}
        table = pa.table(columns)
        pq.write_table(table, str(output_path))
        return

    # Let pyarrow infer types from the data
    columns: dict[str, list] = {col["name"]: [] for col in schema}
    for row in rows:
        for col in schema:
            name = col["name"]
            columns[name].append(row.get(name))

    table = pa.table(columns)
    pq.write_table(table, str(output_path))


def _csv_value(value: Any) -> str:
    """Convert a value for CSV output."""
    return get_raw_value(value)


def _json_serializer(obj: Any) -> Any:
    """Custom JSON serializer for types not handled by default."""
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        return obj.hex()
    return str(obj)
