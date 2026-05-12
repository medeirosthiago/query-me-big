"""Unit tests for SqlResolver implementations."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from qmb.application.protocols import ResolutionTrace
from qmb.sql.resolver import PlainSqlResolver
from qmb.types import InputMode, QueryRequest


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


