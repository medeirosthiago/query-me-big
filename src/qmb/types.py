"""Core types for qmb."""

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
class InputSpec:
    """Describes *what* query to run (source of the SQL)."""

    mode: InputMode
    sql: str | None = None
    file_path: Path | None = None
    model_name: str | None = None


@dataclass(frozen=True)
class DbtOptions:
    """dbt-specific resolution options."""

    resolve_dbt: bool = False
    manifest_path: Path | None = None
    variables: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutionOptions:
    """BigQuery execution options."""

    project: str | None = None
    location: str | None = None
    dry_run: bool = False
    max_bytes_billed: int | None = None
    where: str | None = None


@dataclass(frozen=True)
class OutputOptions:
    """What to do with the results (export, TUI, paging)."""

    export_format: ExportFormat | None = None
    export_path: Path | None = None
    no_tui: bool = False
    page_size: int = 200


@dataclass(frozen=True)
class QueryRequest:
    mode: InputMode
    sql: str | None = None
    file_path: Path | None = None
    model_name: str | None = None
    manifest_path: Path | None = None
    resolve_dbt: bool = False
    variables: dict[str, Any] = field(default_factory=dict)
    project: str | None = None
    location: str | None = None
    page_size: int = 200
    export_format: ExportFormat | None = None
    export_path: Path | None = None
    no_tui: bool = False
    dry_run: bool = False
    max_bytes_billed: int | None = None
    where: str | None = None
    session_id: str | None = None
    parent_job_id: str | None = None

    @property
    def input(self) -> InputSpec:
        return InputSpec(
            mode=self.mode,
            sql=self.sql,
            file_path=self.file_path,
            model_name=self.model_name,
        )

    @property
    def dbt(self) -> DbtOptions:
        return DbtOptions(
            resolve_dbt=self.resolve_dbt,
            manifest_path=self.manifest_path,
            variables=self.variables,
        )

    @property
    def execution(self) -> ExecutionOptions:
        return ExecutionOptions(
            project=self.project,
            location=self.location,
            dry_run=self.dry_run,
            max_bytes_billed=self.max_bytes_billed,
            where=self.where,
        )

    @property
    def output(self) -> OutputOptions:
        return OutputOptions(
            export_format=self.export_format,
            export_path=self.export_path,
            no_tui=self.no_tui,
            page_size=self.page_size,
        )


@dataclass
class ResolvedQuery:
    sql: str
    source_label: str  # e.g. "ad-hoc", "file: x.sql", "model: orders"


@dataclass(frozen=True)
class TableRef:
    """A fully-qualified BigQuery table reference."""

    project: str
    dataset: str
    table: str

    @classmethod
    def parse(cls, s: str) -> "TableRef":
        """Parse ``'project.dataset.table'`` (or empty string → empty ref)."""
        if not s:
            return cls("", "", "")
        parts = s.split(".")
        if len(parts) != 3:
            raise ValueError(f"Invalid table reference: {s!r}")
        return cls(*parts)

    def __str__(self) -> str:
        if not (self.project or self.dataset or self.table):
            return ""
        return f"{self.project}.{self.dataset}.{self.table}"

    @property
    def is_empty(self) -> bool:
        return not (self.project or self.dataset or self.table)


@dataclass(frozen=True)
class SchemaField:
    """A single column in a query result schema."""

    name: str
    type: str
    mode: str = "NULLABLE"

    @classmethod
    def from_mapping(cls, d: dict[str, Any]) -> "SchemaField":
        return cls(name=d["name"], type=d["type"], mode=d.get("mode", "NULLABLE"))

    def to_mapping(self) -> dict[str, Any]:
        return {"name": self.name, "type": self.type, "mode": self.mode}


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

    @property
    def destination(self) -> TableRef:
        """Typed view over :attr:`destination_table`."""
        return TableRef.parse(self.destination_table)

    @property
    def schema_fields(self) -> list[SchemaField]:
        """Typed view over :attr:`schema`."""
        return [SchemaField.from_mapping(d) for d in self.schema]


@dataclass
class PageResult:
    rows: list[dict[str, Any]]  # raw values
    display_rows: list[dict[str, str]]  # truncated for display
    page: int
    total_pages: int
    total_rows: int
