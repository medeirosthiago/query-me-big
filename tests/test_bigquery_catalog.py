"""Project selection in the ``qmb.bigquery.catalog`` helpers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from qmb.bigquery import catalog


class FakeClient:
    def __init__(self, project: str | None = "proj") -> None:
        self.project = project
        self.calls: list[tuple[str, Any]] = []

    def list_datasets(self, project: str | None = None) -> list[SimpleNamespace]:
        self.calls.append(("list_datasets", project))
        return [SimpleNamespace(dataset_id="b"), SimpleNamespace(dataset_id="A")]

    def list_tables(self, ref: str) -> list[SimpleNamespace]:
        self.calls.append(("list_tables", ref))
        return [SimpleNamespace(table_id="t2"), SimpleNamespace(table_id="T1")]

    def get_dataset(self, ref: str) -> str:
        self.calls.append(("get_dataset", ref))
        return ref

    def get_table(self, ref: str) -> str:
        self.calls.append(("get_table", ref))
        return ref


def test_list_dataset_ids_uses_explicit_project_then_client_project() -> None:
    client = FakeClient()

    assert catalog.list_dataset_ids(client, "other") == ["A", "b"]
    assert catalog.list_dataset_ids(client) == ["A", "b"]

    assert client.calls == [("list_datasets", "other"), ("list_datasets", "proj")]


def test_list_dataset_tables_uses_explicit_project_then_client_project() -> None:
    client = FakeClient()

    assert catalog.list_dataset_tables(client, "ds", "other") == ("T1", "t2")
    catalog.list_dataset_tables(client, "ds")

    assert client.calls == [("list_tables", "other.ds"), ("list_tables", "proj.ds")]


def test_get_dataset_metadata_uses_explicit_project_then_client_project() -> None:
    client = FakeClient()

    catalog.get_dataset_metadata(client, "ds", "other")
    catalog.get_dataset_metadata(client, "ds")

    assert client.calls == [("get_dataset", "other.ds"), ("get_dataset", "proj.ds")]


def test_get_table_metadata_uses_explicit_project_then_client_project() -> None:
    client = FakeClient()

    catalog.get_table_metadata(client, "ds", "t", "other")
    catalog.get_table_metadata(client, "ds", "t")

    assert client.calls == [("get_table", "other.ds.t"), ("get_table", "proj.ds.t")]


def test_helpers_without_any_project_use_bare_references() -> None:
    client = FakeClient(project=None)

    catalog.list_dataset_ids(client)
    catalog.list_dataset_tables(client, "ds")
    catalog.get_dataset_metadata(client, "ds")
    catalog.get_table_metadata(client, "ds", "t")

    assert client.calls == [
        ("list_datasets", None),
        ("list_tables", "ds"),
        ("get_dataset", "ds"),
        ("get_table", "ds.t"),
    ]


@pytest.mark.parametrize(("project", "expected"), [("other", "other"), (None, "proj")])
def test_build_table_index_passes_project_through(project: str | None, expected: str) -> None:
    client = FakeClient()

    index = catalog.build_table_index(client, ["x", "y"], max_workers=1, project=project)

    assert index == {"x": ("T1", "t2"), "y": ("T1", "t2")}
    assert sorted(ref for _, ref in client.calls) == [f"{expected}.x", f"{expected}.y"]
