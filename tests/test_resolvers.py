"""Unit tests for SqlResolver implementations."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from qmb.application.protocols import ResolutionTrace
from qmb.dbt.integration import DbtSqlResolver
from qmb.dbt.manifest import ManifestIndex, ManifestNode
from qmb.sql.resolver import PlainSqlResolver
from qmb.types import InputMode, QueryRequest, ResolvedQuery


def _request(**overrides: Any) -> QueryRequest:
    base: dict[str, Any] = {"mode": InputMode.SQL, "sql": "SELECT 1"}
    base.update(overrides)
    return QueryRequest(**base)


# ---------------------------------------------------------------------------
# PlainSqlResolver
# ---------------------------------------------------------------------------


def test_plain_resolver_handles_sql_mode() -> None:
    resolver = PlainSqlResolver()
    assert resolver.can_resolve(_request())


def test_plain_resolver_handles_file_mode_without_dbt(tmp_path: Path) -> None:
    sql_path = tmp_path / "q.sql"
    sql_path.write_text("SELECT 1", encoding="utf-8")
    resolver = PlainSqlResolver()
    request = _request(
        mode=InputMode.FILE, sql=None, file_path=sql_path, resolve_dbt=False
    )
    assert resolver.can_resolve(request)


def test_plain_resolver_rejects_file_mode_with_dbt(tmp_path: Path) -> None:
    sql_path = tmp_path / "q.sql"
    sql_path.write_text("SELECT 1", encoding="utf-8")
    resolver = PlainSqlResolver()
    request = _request(
        mode=InputMode.FILE,
        sql=None,
        file_path=sql_path,
        resolve_dbt=True,
        manifest_path=tmp_path / "manifest.json",
    )
    assert not resolver.can_resolve(request)


def test_plain_resolver_rejects_model_mode() -> None:
    resolver = PlainSqlResolver()
    request = _request(mode=InputMode.MODEL, sql=None, model_name="orders")
    assert not resolver.can_resolve(request)


def test_plain_resolver_resolves_sql() -> None:
    resolver = PlainSqlResolver()
    resolved, trace = resolver.resolve(_request(sql="SELECT 7;\n"))
    assert resolved.sql == "SELECT 7"
    assert resolved.source_label == "ad-hoc"
    assert trace == ResolutionTrace()


def test_plain_resolver_resolves_file(tmp_path: Path) -> None:
    sql_path = tmp_path / "q.sql"
    sql_path.write_text("SELECT 2;\n", encoding="utf-8")
    resolver = PlainSqlResolver()
    resolved, trace = resolver.resolve(
        _request(mode=InputMode.FILE, sql=None, file_path=sql_path)
    )
    assert resolved.sql == "SELECT 2"
    assert "file: q.sql" in resolved.source_label
    assert trace == ResolutionTrace()


# ---------------------------------------------------------------------------
# DbtSqlResolver
# ---------------------------------------------------------------------------


def test_dbt_resolver_handles_model_mode() -> None:
    resolver = DbtSqlResolver()
    request = _request(mode=InputMode.MODEL, sql=None, model_name="orders")
    assert resolver.can_resolve(request)


def test_dbt_resolver_handles_file_mode_with_dbt(tmp_path: Path) -> None:
    sql_path = tmp_path / "q.sql"
    sql_path.write_text("SELECT 1", encoding="utf-8")
    resolver = DbtSqlResolver()
    request = _request(
        mode=InputMode.FILE,
        sql=None,
        file_path=sql_path,
        resolve_dbt=True,
        manifest_path=tmp_path / "manifest.json",
    )
    assert resolver.can_resolve(request)


def test_dbt_resolver_rejects_file_mode_without_dbt(tmp_path: Path) -> None:
    sql_path = tmp_path / "q.sql"
    sql_path.write_text("SELECT 1", encoding="utf-8")
    resolver = DbtSqlResolver()
    request = _request(
        mode=InputMode.FILE, sql=None, file_path=sql_path, resolve_dbt=False
    )
    assert not resolver.can_resolve(request)


def test_dbt_resolver_rejects_sql_mode() -> None:
    resolver = DbtSqlResolver()
    assert not resolver.can_resolve(_request())


def test_dbt_resolver_resolves_model(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{}", encoding="utf-8")

    fake_index = ManifestIndex()
    monkeypatch.setattr("qmb.dbt.manifest.load_manifest", lambda p: fake_index)

    def fake_resolve_model_query(
        model_name: str, index: ManifestIndex, variables: dict[str, Any]
    ) -> ResolvedQuery:
        return ResolvedQuery(sql="SELECT 42", source_label=f"model: {model_name}")

    monkeypatch.setattr(
        "qmb.dbt.resolver.resolve_model_query", fake_resolve_model_query
    )

    resolver = DbtSqlResolver()
    request = _request(
        mode=InputMode.MODEL,
        sql=None,
        model_name="orders",
        manifest_path=manifest_path,
    )

    resolved, trace = resolver.resolve(request)

    assert resolved.sql == "SELECT 42"
    assert resolved.source_label == "model: orders"
    assert trace == ResolutionTrace()


def test_dbt_resolver_resolves_file_with_compiled_match(
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

    resolver = DbtSqlResolver()
    request = _request(
        mode=InputMode.FILE,
        sql=None,
        file_path=sql_path,
        resolve_dbt=True,
        manifest_path=manifest_path,
    )

    resolved, trace = resolver.resolve(request)

    assert resolved.sql == "SELECT 1 AS compiled"
    assert resolved.source_label == "model: orders (model.proj.orders)"
    assert trace == ResolutionTrace(matched_node_id="model.proj.orders")


