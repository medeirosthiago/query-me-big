"""Load SQL from string or file."""

from pathlib import Path

from qmb.types import InputMode, QueryRequest, ResolvedQuery


def load_sql(request: QueryRequest) -> ResolvedQuery:
    """Load raw SQL text from the request source (before any dbt resolution)."""
    if request.mode == InputMode.SQL:
        if request.sql is None:
            raise ValueError("InputMode.SQL requires request.sql to be set")
        return ResolvedQuery(sql=normalize_sql(request.sql), source_label="ad-hoc")

    if request.mode == InputMode.FILE:
        if request.file_path is None:
            raise ValueError("InputMode.FILE requires request.file_path to be set")
        path = Path(request.file_path)
        text = path.read_text(encoding="utf-8")
        return ResolvedQuery(sql=normalize_sql(text), source_label=f"file: {path.name}")

    # MODEL mode is handled by the dbt resolver, but we still need a stub
    raise ValueError("Use the dbt resolver for model mode")


def normalize_sql(sql: str) -> str:
    """Strip trailing semicolons and excess whitespace."""
    return sql.strip().rstrip(";").strip()
