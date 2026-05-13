"""Tests for local job archive JSONL artifacts.

These tests describe the internal row-artifact format for Phase 9. The
implementation is intentionally not present yet; this is the red step of the
TDD cycle.
"""

from __future__ import annotations

import importlib
import json
from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest


def _artifact_module() -> Any:
    try:
        return importlib.import_module("qmb.jobs.artifacts")
    except ModuleNotFoundError:  # pragma: no cover - expected until implemented
        pytest.fail("Expected qmb.jobs.artifacts to exist for Phase 9 job archives")


def test_write_jsonl_rows_streams_one_json_object_per_line(tmp_path: Path) -> None:
    artifacts = _artifact_module()
    out = tmp_path / "preview.jsonl"
    consumed: list[int] = []

    def rows() -> Any:
        for i in range(3):
            consumed.append(i)
            yield {"id": i, "name": f"row-{i}"}

    count = artifacts.write_jsonl_rows(out, rows())

    assert count == 3
    assert consumed == [0, 1, 2]
    raw = out.read_text(encoding="utf-8")
    assert not raw.lstrip().startswith("[")
    assert raw.endswith("\n")
    assert raw.splitlines() == [
        '{"id": 0, "name": "row-0"}',
        '{"id": 1, "name": "row-1"}',
        '{"id": 2, "name": "row-2"}',
    ]


def test_write_jsonl_rows_uses_qmb_json_coercion(tmp_path: Path) -> None:
    artifacts = _artifact_module()
    out = tmp_path / "preview.jsonl"

    artifacts.write_jsonl_rows(
        out,
        [
            {
                "as_date": date(2026, 5, 12),
                "as_datetime": datetime(2026, 5, 12, 14, 33, 2, tzinfo=UTC),
                "as_time": time(14, 33, 2),
                "as_decimal": Decimal("12.34"),
                "as_bytes": b"qmb",
            }
        ],
    )

    assert json.loads(out.read_text(encoding="utf-8")) == {
        "as_date": "2026-05-12",
        "as_datetime": "2026-05-12T14:33:02+00:00",
        "as_time": "14:33:02",
        "as_decimal": 12.34,
        "as_bytes": "716d62",
    }


def test_write_jsonl_rows_preserves_schema_column_order(tmp_path: Path) -> None:
    artifacts = _artifact_module()
    out = tmp_path / "preview.jsonl"

    artifacts.write_jsonl_rows(
        out,
        [{"a": 1, "b": 2, "c": 3}],
        fieldnames=["b", "a", "c"],
    )

    row = json.loads(out.read_text(encoding="utf-8"))
    assert list(row) == ["b", "a", "c"]
    assert row == {"b": 2, "a": 1, "c": 3}


def test_read_jsonl_rows_returns_preview_rows(tmp_path: Path) -> None:
    artifacts = _artifact_module()
    preview = tmp_path / "preview.jsonl"
    preview.write_text(
        '{"id": 1, "name": "alpha"}\n{"id": 2, "name": "beta"}\n',
        encoding="utf-8",
    )

    assert list(artifacts.read_jsonl_rows(preview)) == [
        {"id": 1, "name": "alpha"},
        {"id": 2, "name": "beta"},
    ]
