"""Tests for the TUI browser pane's table-index loader.

We test the pure helper that ``_load_browser_index`` delegates to —
the ``@work(thread=True)`` wrapper is just plumbing.
"""

from __future__ import annotations

from qmb.tui import app as tui_app


def test_prefers_information_schema_when_it_succeeds(monkeypatch) -> None:
    """The helper must call the INFORMATION_SCHEMA path first and skip the
    legacy ``build_table_index`` entirely on success."""
    info_schema_calls = []
    list_tables_calls = []

    def fake_info_schema(client):
        info_schema_calls.append(client)
        return {"ds1": ("t1", "t2")}

    def fake_build_index(client, dataset_ids):  # pragma: no cover - defensive
        list_tables_calls.append((client, dataset_ids))
        raise AssertionError("legacy path must not run on successful INFO_SCHEMA")

    monkeypatch.setattr(tui_app, "list_tables_via_information_schema", fake_info_schema)
    monkeypatch.setattr(tui_app, "build_table_index", fake_build_index)

    result = tui_app._load_browser_index_impl(object(), ("ds1", "empty_ds"))

    assert info_schema_calls, "INFORMATION_SCHEMA path must be attempted first"
    assert list_tables_calls == []
    # Empty datasets are backfilled so the tree shows every dataset the
    # user has already seen via list_dataset_ids.
    assert result == {"ds1": ("t1", "t2"), "empty_ds": ()}


def test_falls_back_to_list_tables_when_information_schema_fails(monkeypatch) -> None:
    """If INFO_SCHEMA raises (e.g. no ``bigquery.jobs.create``), the
    helper must fall back to per-dataset ``list_tables`` so the browser
    still works."""

    def fake_info_schema(client):
        raise PermissionError("403: User lacks bigquery.jobs.create")

    list_tables_calls = []

    def fake_build_index(client, dataset_ids):
        list_tables_calls.append(tuple(dataset_ids))
        return {"ds1": ("t1",)}

    monkeypatch.setattr(tui_app, "list_tables_via_information_schema", fake_info_schema)
    monkeypatch.setattr(tui_app, "build_table_index", fake_build_index)

    result = tui_app._load_browser_index_impl(object(), ("ds1",))

    assert list_tables_calls == [("ds1",)]
    assert result == {"ds1": ("t1",)}


def test_propagates_error_when_both_paths_fail(monkeypatch) -> None:
    """If both INFO_SCHEMA and the legacy path raise, the helper must
    propagate the error so the worker can surface it to the user."""

    def fake_info_schema(client):
        raise RuntimeError("info_schema failed")

    def fake_build_index(client, dataset_ids):
        raise RuntimeError("list_tables failed")

    monkeypatch.setattr(tui_app, "list_tables_via_information_schema", fake_info_schema)
    monkeypatch.setattr(tui_app, "build_table_index", fake_build_index)

    try:
        tui_app._load_browser_index_impl(object(), ("ds1",))
    except RuntimeError as exc:
        assert "list_tables failed" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("expected RuntimeError when both paths fail")
