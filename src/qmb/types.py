"""Core types for qmb."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


def fmt_bytes(n: int) -> str:
    """Format bytes as a human-readable string."""
    if n < 1024:
        return f"{n} B"
    for unit in ("KB", "MB", "GB", "TB"):
        n /= 1024
        if n < 1024:
            return f"{n:,.1f} {unit}"
    return f"{n:,.1f} PB"


class InputMode(enum.Enum):
    SQL = "sql"
    FILE = "file"
    MODEL = "model"


class ExportFormat(enum.Enum):
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"


@dataclass(frozen=True)
class QueryRequest:
    mode: InputMode
    sql: str | None = None
    file_path: Path | None = None
    model_name: str | None = None
    manifest_path: Path | None = None
    resolve_dbt: bool = False
    variables: dict[str, str] = field(default_factory=dict)
    project: str | None = None
    location: str | None = None
    page_size: int = 200
    export_format: ExportFormat | None = None
    export_path: Path | None = None
    no_tui: bool = False
    dry_run: bool = False
    max_bytes_billed: int | None = None


@dataclass
class ResolvedQuery:
    sql: str
    source_label: str  # e.g. "ad-hoc", "file: x.sql", "model: orders"


@dataclass
class QueryResultHandle:
    job_id: str
    project: str
    location: str
    destination_table: str  # "project.dataset.table"
    schema: list[dict[str, Any]]  # [{name, type, mode}]
    total_rows: int
    bytes_processed: int = 0
    execution_seconds: float = 0.0


@dataclass
class PageResult:
    rows: list[dict[str, Any]]  # raw values
    display_rows: list[dict[str, str]]  # truncated for display
    page: int
    total_pages: int
    total_rows: int
