"""Helpers for browsing BigQuery datasets and tables."""

from collections.abc import Sequence
from datetime import datetime
from typing import Any

from google.cloud import bigquery

from qmb.bigquery.catalog import (
    build_table_index,
    get_dataset_metadata,
    get_table_metadata,
    list_dataset_ids,
    list_dataset_tables,
)
from qmb.bigquery.catalog_search import BrowserMatch, filter_browser_matches
from qmb.types import fmt_bytes

__all__ = [
    "BrowserMatch",
    "build_table_index",
    "filter_browser_matches",
    "format_dataset_details",
    "format_table_details",
    "get_dataset_metadata",
    "get_table_metadata",
    "list_dataset_ids",
    "list_dataset_tables",
]


def format_dataset_details(dataset: bigquery.Dataset) -> str:
    """Format dataset metadata for read-only inspection in nvim."""
    lines = ["Dataset Details", "=" * 40, "", "Dataset Info", "-" * 40]
    dataset_id = _fq_dataset_id(dataset)
    details = [
        ("Dataset ID", dataset_id),
        ("Project", getattr(dataset, "project", None)),
        ("Dataset", getattr(dataset, "dataset_id", None)),
        ("Friendly name", getattr(dataset, "friendly_name", None)),
        ("Created", _format_datetime(getattr(dataset, "created", None))),
        ("Last modified", _format_datetime(getattr(dataset, "modified", None))),
        ("Location", getattr(dataset, "location", None)),
        ("Description", getattr(dataset, "description", None)),
        (
            "Default table expiration",
            _format_millis(getattr(dataset, "default_table_expiration_ms", None)),
        ),
        (
            "Default partition expiration",
            _format_millis(getattr(dataset, "default_partition_expiration_ms", None)),
        ),
        (
            "Default rounding mode",
            _value_or_raw(dataset, "default_rounding_mode", "defaultRoundingMode"),
        ),
        ("Default collation", _raw_property(dataset, "defaultCollation")),
        ("Case insensitive", getattr(dataset, "is_case_insensitive", None)),
        ("Max time travel", _format_hours(getattr(dataset, "max_time_travel_hours", None))),
        (
            "Storage billing model",
            _value_or_raw(dataset, "storage_billing_model", "storageBillingModel"),
        ),
        ("Path", getattr(dataset, "path", None)),
        ("ETag", getattr(dataset, "etag", None)),
    ]
    lines.extend(_format_fields(details))

    labels = getattr(dataset, "labels", None) or {}
    if labels:
        lines.extend(["", "Labels", "-" * 40])
        for key, value in sorted(labels.items()):
            lines.append(f"  {key}={value}")

    access_entries = getattr(dataset, "access_entries", None)
    if access_entries:
        lines.extend(["", "Access", "-" * 40])
        lines.append(f"  Entries: {len(access_entries):,}")

    return "\n".join(lines)


def format_table_details(table: bigquery.Table) -> str:
    """Format table metadata for read-only inspection in nvim."""
    lines = ["Table Details", "=" * 40, "", "Table Info", "-" * 40]
    table_id = _fq_table_id(table)
    details = [
        ("Table ID", table_id),
        ("Project", getattr(table, "project", None)),
        ("Dataset", getattr(table, "dataset_id", None)),
        ("Table", getattr(table, "table_id", None)),
        ("Friendly name", getattr(table, "friendly_name", None)),
        ("Type", _value_or_raw(table, "table_type", "type")),
        ("Created", _format_datetime(getattr(table, "created", None))),
        ("Last modified", _format_datetime(getattr(table, "modified", None))),
        ("Expires", _format_datetime(getattr(table, "expires", None))),
        ("Location", getattr(table, "location", None)),
        ("Description", getattr(table, "description", None)),
        (
            "Partitioning",
            _format_partitioning(
                getattr(table, "time_partitioning", None),
                getattr(table, "range_partitioning", None),
                getattr(table, "partitioning_type", None),
            ),
        ),
        ("Clustering", _format_list(getattr(table, "clustering_fields", None))),
        (
            "Default collation",
            _value_or_raw(table, "default_collation", "defaultCollation"),
        ),
        (
            "Default rounding mode",
            _value_or_raw(table, "default_rounding_mode", "defaultRoundingMode"),
        ),
        ("Path", getattr(table, "path", None)),
        ("ETag", getattr(table, "etag", None)),
    ]
    lines.extend(_format_fields(details))

    labels = getattr(table, "labels", None) or {}
    if labels:
        lines.extend(["", "Labels", "-" * 40])
        for key, value in sorted(labels.items()):
            lines.append(f"  {key}={value}")

    storage_details = [
        ("Rows", _format_number(getattr(table, "num_rows", None))),
        ("Logical bytes", _format_bytes(getattr(table, "num_bytes", None))),
        ("Total logical bytes", _format_bytes(_raw_property(table, "numTotalLogicalBytes"))),
        ("Active logical bytes", _format_bytes(_raw_property(table, "numActiveLogicalBytes"))),
        (
            "Long-term logical bytes",
            _format_bytes(_raw_property(table, "numLongTermLogicalBytes")),
        ),
        (
            "Current physical bytes",
            _format_bytes(_raw_property(table, "numCurrentPhysicalBytes")),
        ),
        (
            "Total physical bytes",
            _format_bytes(_raw_property(table, "numTotalPhysicalBytes")),
        ),
        (
            "Active physical bytes",
            _format_bytes(_raw_property(table, "numActivePhysicalBytes")),
        ),
        (
            "Long-term physical bytes",
            _format_bytes(_raw_property(table, "numLongTermPhysicalBytes")),
        ),
        (
            "Time travel physical bytes",
            _format_bytes(_raw_property(table, "numTimeTravelPhysicalBytes")),
        ),
    ]
    rendered_storage = _format_fields(storage_details)
    if rendered_storage:
        lines.extend(["", "Storage Info", "-" * 40, *rendered_storage])

    schema = getattr(table, "schema", None) or []
    if schema:
        lines.extend(["", "Schema", "-" * 40])
        lines.extend(_format_schema(schema))

    view_query = getattr(table, "view_query", None)
    materialized_view_query = _raw_property(table, "materializedView", "query")
    external_config = getattr(table, "external_data_configuration", None)
    if view_query or materialized_view_query or external_config:
        lines.extend(["", "Advanced", "-" * 40])
        if view_query:
            lines.append("  View query:")
            lines.extend(f"    {line}" for line in view_query.splitlines())
        if materialized_view_query:
            lines.append("  Materialized view query:")
            lines.extend(f"    {line}" for line in materialized_view_query.splitlines())
        if external_config:
            lines.append(f"  External source format: {external_config.source_format}")

    return "\n".join(lines)


def _format_fields(fields: Sequence[tuple[str, Any]]) -> list[str]:
    rendered: list[str] = []
    for label, value in fields:
        if value in (None, "", [], {}):
            continue
        rendered.append(f"  {label:<26} {value}")
    return rendered


def _format_schema(fields: Sequence[Any], indent: int = 2) -> list[str]:
    lines: list[str] = []
    prefix = " " * indent
    for field in fields:
        line = f"{prefix}- {field.name}: {field.field_type}"
        if field.mode and field.mode != "NULLABLE":
            line += f" [{field.mode}]"
        if field.description:
            line += f" -- {field.description}"
        lines.append(line)
        if field.fields:
            lines.extend(_format_schema(field.fields, indent + 2))
    return lines


def _format_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    return str(value)


def _format_millis(value: Any) -> str | None:
    if value is None:
        return None
    milliseconds = int(value)
    seconds = milliseconds // 1000
    days, seconds = divmod(seconds, 86_400)
    hours, seconds = divmod(seconds, 3_600)
    minutes, seconds = divmod(seconds, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds and not parts:
        parts.append(f"{seconds}s")
    formatted = " ".join(parts) if parts else "0s"
    return f"{formatted} ({milliseconds:,} ms)"


def _format_hours(value: Any) -> str | None:
    if value is None:
        return None
    return f"{int(value)} hours"


def _format_bytes(value: Any) -> str | None:
    if value in (None, ""):
        return None
    size = int(value)
    return f"{fmt_bytes(size)} ({size:,} bytes)"


def _format_number(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return f"{int(value):,}"


def _format_list(values: Sequence[Any] | None) -> str | None:
    if not values:
        return None
    return ", ".join(str(value) for value in values)


def _format_partitioning(
    time_partitioning: Any, range_partitioning: Any, partitioning_type: Any
) -> str | None:
    if time_partitioning is not None:
        parts = [partitioning_type or getattr(time_partitioning, "type_", None) or "TIME"]
        field = getattr(time_partitioning, "field", None)
        if field:
            parts.append(f"field={field}")
        expiration_ms = getattr(time_partitioning, "expiration_ms", None)
        if expiration_ms:
            parts.append(f"expires={_format_millis(expiration_ms)}")
        if getattr(time_partitioning, "require_partition_filter", False):
            parts.append("require_partition_filter=true")
        return " · ".join(parts)
    if range_partitioning is not None:
        field = getattr(range_partitioning, "field", None)
        range_ = getattr(range_partitioning, "range_", None)
        parts = ["RANGE"]
        if field:
            parts.append(f"field={field}")
        if range_ is not None:
            parts.append(f"start={range_.start}, end={range_.end}, interval={range_.interval}")
        return " · ".join(parts)
    return partitioning_type


def _fq_dataset_id(dataset: Any) -> str:
    project = getattr(dataset, "project", None)
    dataset_id = getattr(dataset, "dataset_id", None)
    if project and dataset_id:
        return f"{project}.{dataset_id}"
    return str(dataset_id or "")


def _fq_table_id(table: Any) -> str:
    project = getattr(table, "project", None)
    dataset_id = getattr(table, "dataset_id", None)
    table_id = getattr(table, "table_id", None)
    if project and dataset_id and table_id:
        return f"{project}.{dataset_id}.{table_id}"
    return str(table_id or "")


def _value_or_raw(resource: Any, attr_name: str, *property_path: str) -> Any:
    value = getattr(resource, attr_name, None)
    if value not in (None, ""):
        return value
    return _raw_property(resource, *property_path)


def _raw_property(resource: Any, *path: str) -> Any:
    properties = getattr(resource, "_properties", None) or {}
    current: Any = properties
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current
