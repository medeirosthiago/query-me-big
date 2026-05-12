"""Unit tests for the application/orchestration layer.

These tests exercise `resolve_request_to_sql` and `apply_where` directly,
without going through Typer. They complement the CLI characterization
tests in `test_cli_flow.py`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from qmb.application.resolver import (
    ResolutionTrace,
    apply_where,
    resolve_request_to_sql,
)
from qmb.dbt.integration import DbtSqlResolver
from qmb.dbt.manifest import ManifestIndex, ManifestNode
from qmb.sql.resolver import PlainSqlResolver
from qmb.types import InputMode, QueryRequest, ResolvedQuery

# Default resolver list mirroring the CLI's wiring — dbt first, then plain.
_DEFAULT_RESOLVERS = [DbtSqlResolver(), PlainSqlResolver()]


def _request(**overrides: Any) -> QueryRequest:
    base: dict[str, Any] = {"mode": InputMode.SQL, "sql": "SELECT 1"}
    base.update(overrides)
    return QueryRequest(**base)


def _resolve(request: QueryRequest) -> tuple[ResolvedQuery, ResolutionTrace]:
    return resolve_request_to_sql(request, _DEFAULT_RESOLVERS)


# ---------------------------------------------------------------------------
# apply_where
# ---------------------------------------------------------------------------


def test_apply_where_returns_input_unchanged_when_where_is_none() -> None:
    resolved = ResolvedQuery(sql="SELECT 1", source_label="ad-hoc")
    out = apply_where(resolved, None)
    assert out is resolved


def test_apply_where_returns_input_unchanged_when_where_is_empty() -> None:
    resolved = ResolvedQuery(sql="SELECT 1", source_label="ad-hoc")
    assert apply_where(resolved, "") is resolved


def test_apply_where_wraps_sql_in_subquery_and_preserves_label() -> None:
    resolved = ResolvedQuery(sql="SELECT 1 AS x", source_label="file: q.sql")
    out = apply_where(resolved, "x = 1")
    assert out.sql == "SELECT * FROM (SELECT 1 AS x) __qmb WHERE x = 1"
    assert out.source_label == "file: q.sql"


# ---------------------------------------------------------------------------
# resolve_request_to_sql
# ---------------------------------------------------------------------------


def test_resolve_request_ad_hoc_returns_normalized_sql() -> None:
    request = _request(sql="SELECT 7;\n\n")
    resolved, trace = _resolve(request)
    assert resolved.sql == "SELECT 7"
    assert resolved.source_label == "ad-hoc"
    assert trace == ResolutionTrace()


def test_resolve_request_file_without_dbt_loads_file(tmp_path: Path) -> None:
    sql_path = tmp_path / "q.sql"
    sql_path.write_text("SELECT 2;\n", encoding="utf-8")
    request = _request(mode=InputMode.FILE, sql=None, file_path=sql_path)

    resolved, trace = _resolve(request)

    assert resolved.sql == "SELECT 2"
    assert "file: q.sql" in resolved.source_label
    assert trace == ResolutionTrace()


def test_resolve_request_model_uses_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{}", encoding="utf-8")

    fake_index = ManifestIndex()
    monkeypatch.setattr("qmb.dbt.manifest.load_manifest", lambda p: fake_index)

    seen: dict[str, Any] = {}

    def fake_resolve_model_query(
        model_name: str, index: ManifestIndex, variables: dict[str, Any]
    ) -> ResolvedQuery:
        seen["model_name"] = model_name
        seen["index"] = index
        seen["variables"] = variables
        return ResolvedQuery(sql="SELECT 42", source_label=f"model: {model_name}")

    monkeypatch.setattr(
        "qmb.dbt.resolver.resolve_model_query", fake_resolve_model_query
    )

    request = _request(
        mode=InputMode.MODEL,
        sql=None,
        model_name="orders",
        manifest_path=manifest_path,
        variables={"k": "v"},
    )

    resolved, trace = _resolve(request)

    assert resolved.sql == "SELECT 42"
    assert resolved.source_label == "model: orders"
    assert trace == ResolutionTrace()
    assert seen == {
        "model_name": "orders",
        "index": fake_index,
        "variables": {"k": "v"},
    }


def test_resolve_request_file_with_dbt_match_returns_compiled_trace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sql_path = tmp_path / "orders.sql"
    sql_path.write_text("select * from {{ ref('x') }}", encoding="utf-8")
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{}", encoding="utf-8")

    fake_index = ManifestIndex()
    monkeypatch.setattr("qmb.dbt.manifest.load_manifest", lambda p: fake_index)

    matched_node = ManifestNode(
        unique_id="model.proj.orders",
        name="orders",
        resource_type="model",
        package_name="proj",
        database=None,
        schema_name=None,
        alias=None,
        compiled_code="SELECT 1 AS compiled;\n",
        raw_code="select * from {{ ref('x') }}",
        original_file_path="models/orders.sql",
    )
    monkeypatch.setattr(
        "qmb.dbt.resolver.resolve_file_to_model",
        lambda file_path, index: matched_node,
    )

    request = _request(
        mode=InputMode.FILE,
        sql=None,
        file_path=sql_path,
        resolve_dbt=True,
        manifest_path=manifest_path,
    )

    resolved, trace = _resolve(request)

    assert resolved.sql == "SELECT 1 AS compiled"
    assert resolved.source_label == "model: orders (model.proj.orders)"
    assert trace == ResolutionTrace(matched_node_id="model.proj.orders")


def test_resolve_request_file_with_dbt_no_match_falls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sql_path = tmp_path / "q.sql"
    sql_path.write_text("SELECT 3;\n", encoding="utf-8")
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{}", encoding="utf-8")

    fake_index = ManifestIndex()
    monkeypatch.setattr("qmb.dbt.manifest.load_manifest", lambda p: fake_index)
    monkeypatch.setattr(
        "qmb.dbt.resolver.resolve_file_to_model",
        lambda file_path, index: None,
    )

    captured: dict[str, Any] = {}

    def fake_resolve_file_sql(
        sql: str,
        index: ManifestIndex,
        variables: dict[str, Any],
        source_label: str,
    ) -> ResolvedQuery:
        captured["sql"] = sql
        captured["source_label"] = source_label
        return ResolvedQuery(sql="SELECT 999", source_label=source_label)

    monkeypatch.setattr("qmb.dbt.resolver.resolve_file_sql", fake_resolve_file_sql)

    request = _request(
        mode=InputMode.FILE,
        sql=None,
        file_path=sql_path,
        resolve_dbt=True,
        manifest_path=manifest_path,
    )

    resolved, trace = _resolve(request)

    assert resolved.sql == "SELECT 999"
    assert "file: q.sql" in resolved.source_label
    assert trace == ResolutionTrace()
    assert captured["sql"] == "SELECT 3"
