"""Load SQL from string or file."""

from __future__ import annotations

from pathlib import Path

from qmb.types import InputMode, QueryRequest, ResolvedQuery


def load_sql(request: QueryRequest) -> ResolvedQuery:
    """Load raw SQL text from the request source (before any dbt resolution)."""
    if request.mode == InputMode.SQL:
        assert request.sql is not None
        return ResolvedQuery(sql=normalize_sql(request.sql), source_label="ad-hoc")

    if request.mode == InputMode.FILE:
        assert request.file_path is not None
        path = Path(request.file_path)
        text = path.read_text(encoding="utf-8")
        return ResolvedQuery(sql=normalize_sql(text), source_label=f"file: {path.name}")

    # MODEL mode is handled by the dbt resolver, but we still need a stub
    raise ValueError("Use the dbt resolver for model mode")


def normalize_sql(sql: str) -> str:
    """Strip trailing semicolons and excess whitespace."""
    return sql.strip().rstrip(";").strip()
