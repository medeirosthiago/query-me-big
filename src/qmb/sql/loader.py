"""Load SQL from string or file."""

from pathlib import Path

from qmb.types import InputMode, InputSpec, ResolvedQuery


def load_sql(spec: InputSpec) -> ResolvedQuery:
    """Load raw SQL text from the input source (before any dbt resolution)."""
    if spec.mode == InputMode.SQL:
        if spec.sql is None:
            raise ValueError("InputMode.SQL requires spec.sql to be set")
        return ResolvedQuery(sql=normalize_sql(spec.sql), source_label="ad-hoc")

    if spec.mode == InputMode.FILE:
        if spec.file_path is None:
            raise ValueError("InputMode.FILE requires spec.file_path to be set")
        path = Path(spec.file_path)
        text = path.read_text(encoding="utf-8")
        return ResolvedQuery(sql=normalize_sql(text), source_label=f"file: {path.name}")

    # MODEL mode is handled by the dbt resolver, but we still need a stub
    raise ValueError("Use the dbt resolver for model mode")


def normalize_sql(sql: str) -> str:
    """Strip trailing semicolons and excess whitespace."""
    return sql.strip().rstrip(";").strip()
