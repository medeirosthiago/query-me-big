"""Resolve dbt Jinja patterns (ref, source, var) in raw SQL."""

from __future__ import annotations

import re
from typing import Any

from qmb.dbt.manifest import ManifestIndex
from qmb.dbt.selector import resolve_model
from qmb.sql.loader import normalize_sql
from qmb.types import ResolvedQuery

# Patterns for ref(), source(), var()
REF_PATTERN = re.compile(
    r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}",
    re.IGNORECASE,
)
SOURCE_PATTERN = re.compile(
    r"\{\{\s*source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}",
    re.IGNORECASE,
)
VAR_WITH_DEFAULT_PATTERN = re.compile(
    r"\{\{\s*var\(\s*['\"]([^'\"]+)['\"]\s*,\s*(.+?)\s*\)\s*\}\}",
    re.IGNORECASE,
)
VAR_NO_DEFAULT_PATTERN = re.compile(
    r"\{\{\s*var\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}",
    re.IGNORECASE,
)

# Detect unsupported Jinja
UNSUPPORTED_JINJA = re.compile(r"\{[%{].*?[%}]\}")


def resolve_model_query(
    model_selector: str,
    index: ManifestIndex,
    variables: dict[str, str] | None = None,
) -> ResolvedQuery:
    """Resolve a --model selector to executable SQL using compiled_code."""
    node = resolve_model(model_selector, index)

    sql = node.compiled_code
    if not sql:
        raise ValueError(
            f"Model '{model_selector}' ({node.unique_id}) has no compiled_code. "
            "Run `dbt compile` first."
        )

    return ResolvedQuery(
        sql=normalize_sql(sql),
        source_label=f"model: {node.name} ({node.unique_id})",
    )


def resolve_file_sql(
    raw_sql: str,
    index: ManifestIndex,
    variables: dict[str, str] | None = None,
    source_label: str = "file",
) -> ResolvedQuery:
    """Resolve ref/source/var patterns in a raw SQL file."""
    variables = variables or {}
    sql = raw_sql

    # Resolve ref()
    sql = REF_PATTERN.sub(lambda m: _resolve_ref(m.group(1), index), sql)

    # Resolve source()
    sql = SOURCE_PATTERN.sub(lambda m: _resolve_source(m.group(1), m.group(2), index), sql)

    # Resolve var() with default
    sql = VAR_WITH_DEFAULT_PATTERN.sub(
        lambda m: _resolve_var(m.group(1), m.group(2).strip(), index, variables),
        sql,
    )

    # Resolve var() without default
    sql = VAR_NO_DEFAULT_PATTERN.sub(
        lambda m: _resolve_var_required(m.group(1), index, variables),
        sql,
    )

    # Check for remaining unsupported Jinja
    remaining = UNSUPPORTED_JINJA.findall(sql)
    if remaining:
        samples = remaining[:3]
        raise ValueError(
            f"Unsupported Jinja found: {samples}. "
            "This tool supports only ref(), source(), and var(). "
            "Use --model with compiled dbt SQL for full dbt semantics."
        )

    return ResolvedQuery(sql=normalize_sql(sql), source_label=source_label)


def _resolve_ref(model_name: str, index: ManifestIndex) -> str:
    """Resolve {{ ref('model_name') }} to a fully-qualified table reference."""
    node = resolve_model(model_name, index)
    return _fq_table(node.database, node.schema_name, node.alias or node.name)


def _resolve_source(source_name: str, table_name: str, index: ManifestIndex) -> str:
    """Resolve {{ source('source', 'table') }} to a fully-qualified table reference."""
    key = (source_name, table_name)
    source = index.sources_by_key.get(key)
    if not source:
        available = sorted(index.sources_by_key.keys())[:5]
        raise ValueError(
            f"Source ('{source_name}', '{table_name}') not found. "
            f"Available: {available}"
        )
    return _fq_table(source.database, source.schema_name, source.identifier or source.name)


def _resolve_var(
    var_name: str,
    default_raw: str,
    index: ManifestIndex,
    variables: dict[str, str],
) -> str:
    """Resolve {{ var('name', default) }}."""
    # CLI vars take highest priority
    if var_name in variables:
        return _to_sql_literal(variables[var_name])

    # Then manifest project vars
    if var_name in index.project_vars:
        return _to_sql_literal(index.project_vars[var_name])

    # Then use the default from the template
    return _parse_default(default_raw)


def _resolve_var_required(
    var_name: str,
    index: ManifestIndex,
    variables: dict[str, str],
) -> str:
    """Resolve {{ var('name') }} — no default, must be provided."""
    if var_name in variables:
        return _to_sql_literal(variables[var_name])

    if var_name in index.project_vars:
        return _to_sql_literal(index.project_vars[var_name])

    raise ValueError(
        f"Required dbt variable '{var_name}' not provided. "
        "Use --var name=value or set QMB_VAR_NAME env var."
    )


def _fq_table(database: str | None, schema: str | None, table: str) -> str:
    """Build a fully-qualified BigQuery table reference."""
    parts = [p for p in (database, schema, table) if p]
    return ".".join(f"`{p}`" for p in parts)


def _to_sql_literal(value: Any) -> str:
    """Convert a Python value to a SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return _quote_sql_string(str(value))


def _quote_sql_string(value: str) -> str:
    """Quote a string as a BigQuery SQL literal."""
    return "'" + value.replace("'", "''") + "'"


def _parse_default(raw: str) -> str:
    """Parse a default value from a Jinja var() call."""
    raw = raw.strip()
    is_quoted = (raw.startswith("'") and raw.endswith("'")) or (
        raw.startswith('"') and raw.endswith('"')
    )
    stripped = raw.strip("'\"")

    if is_quoted:
        return _quote_sql_string(stripped)

    # Boolean-ish
    if stripped.lower() == "true":
        return "TRUE"
    if stripped.lower() == "false":
        return "FALSE"
    if stripped.lower() in ("none", "null"):
        return "NULL"

    # Numeric
    try:
        int(stripped)
        return stripped
    except ValueError:
        pass
    try:
        float(stripped)
        return stripped
    except ValueError:
        pass

    # Fall back to string
    return _quote_sql_string(stripped)
