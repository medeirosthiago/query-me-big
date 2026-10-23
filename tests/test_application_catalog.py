"""Library catalog functions behind ``qmb describe`` / ``qmb browse``."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from qmb.application.catalog import browse_catalog, describe_target


class FakeApiObject:
    def __init__(self, api_repr: dict[str, Any]) -> None:
        self._api_repr = api_repr

    def to_api_repr(self) -> dict[str, Any]:
        return self._api_repr


class FakeDatasetListItem:
    def __init__(self, dataset_id: str, location: str = "us-west1") -> None:
        self.dataset_id = dataset_id
        self._properties = {"location": location}


class FakeJob:
    def __init__(self, rows: list[dict[str, str]]) -> None:
        self._rows = rows

    def result(self) -> list[dict[str, str]]:
        return self._rows


class FakeCatalogClient:
    """Answers both the list_tables fan-out and INFORMATION_SCHEMA paths."""

    project = "proj"

    def __init__(self, *, location: str | None = None) -> None:
        self.location = location
        self.tables = {
            "analytics_prod": ["orders", "users"],
            "analytics_dev": ["orders"],
            "raw": ["events"],
        }
        self.calls: list[tuple[str, Any]] = []

    def list_datasets(self, project: str | None = None) -> list[FakeDatasetListItem]:
        self.calls.append(("list_datasets", project))
        return [FakeDatasetListItem(dataset_id) for dataset_id in self.tables]

    def list_tables(self, ref: str) -> list[SimpleNamespace]:
        self.calls.append(("list_tables", ref))
        dataset_id = ref.split(".")[-1]
        return [SimpleNamespace(table_id=table_id) for table_id in self.tables[dataset_id]]

    def query(self, sql: str, location: str | None = None) -> FakeJob:
        self.calls.append(("query", location))
        return FakeJob(
            [
                {"table_schema": dataset_id, "table_name": table_id}
                for dataset_id, table_ids in self.tables.items()
                for table_id in table_ids
            ]
        )

    def get_dataset(self, ref: str) -> FakeApiObject:
        self.calls.append(("get_dataset", ref))
        return FakeApiObject({"datasetReference": {"datasetId": ref.split(".")[-1]}})

    def get_table(self, ref: str) -> FakeApiObject:
        self.calls.append(("get_table", ref))
        return FakeApiObject({"id": ref})


def _recorder() -> tuple[list[tuple[str, float, str]], Any]:
    steps: list[tuple[str, float, str]] = []

    def on_step(label: str, seconds: float, extra: str) -> None:
        steps.append((label, seconds, extra))

    return steps, on_step


def test_describe_target_dataset_returns_api_repr_and_sorted_tables() -> None:
    client = FakeCatalogClient()
    client.tables["analytics_prod"] = ["users", "Orders", "events"]

    payload = describe_target(client, "analytics_prod")

    assert payload == {
        "kind": "dataset",
        "dataset": {"datasetReference": {"datasetId": "analytics_prod"}},
        "tables": ["events", "Orders", "users"],
    }
    assert ("get_dataset", "proj.analytics_prod") in client.calls


def test_describe_target_dataset_table() -> None:
    client = FakeCatalogClient()

    payload = describe_target(client, "analytics.orders")

    assert payload == {"kind": "table", "table": {"id": "proj.analytics.orders"}}


@pytest.mark.parametrize("target", ["other.analytics.orders", "other:analytics.orders"])
def test_describe_target_three_parts_uses_client_project(target: str) -> None:
    client = FakeCatalogClient()

    payload = describe_target(client, target)

    assert payload == {"kind": "table", "table": {"id": "proj.analytics.orders"}}


def test_describe_target_rejects_more_than_three_parts() -> None:
    with pytest.raises(ValueError, match=r"Cannot parse target 'a\.b\.c\.d'\. Use 'dataset'"):
        describe_target(FakeCatalogClient(), "a.b.c.d")


def test_browse_catalog_without_pattern_lists_datasets() -> None:
    client = FakeCatalogClient()
    steps, on_step = _recorder()

    payload = browse_catalog(client, None, on_step=on_step)

    assert payload == {
        "project": "proj",
        "datasets": ["analytics_dev", "analytics_prod", "raw"],
    }
    assert [(label, extra) for label, _seconds, extra in steps] == [("list_dataset_ids", "n=3")]


def test_browse_catalog_project_falls_back_to_argument() -> None:
    client = FakeCatalogClient()
    client.project = None  # type: ignore[assignment]

    payload = browse_catalog(client, None, project="explicit")

    assert payload["project"] == "explicit"


def test_browse_catalog_legacy_list_tables_fans_out_per_dataset() -> None:
    client = FakeCatalogClient()
    steps, on_step = _recorder()

    payload = browse_catalog(
        client, "analytics_*", legacy_list_tables=True, workers=2, on_step=on_step
    )

    assert payload == {
        "project": "proj",
        "pattern": "analytics_*",
        "matches": [
            {"dataset_id": "analytics_dev", "tables": ["analytics_dev.orders"]},
            {
                "dataset_id": "analytics_prod",
                "tables": ["analytics_prod.orders", "analytics_prod.users"],
            },
        ],
    }
    assert not any(name == "query" for name, _ in client.calls)
    assert [label for label, _seconds, _extra in steps] == [
        "list_dataset_ids",
        "build_table_index",
        "filter_browser_matches",
        "total",
    ]
    assert steps[1][2] == "datasets=3 tables=4 workers=2"


def test_browse_catalog_information_schema_pins_explicit_location() -> None:
    client = FakeCatalogClient()
    steps, on_step = _recorder()

    payload = browse_catalog(client, "raw", location="EU", on_step=on_step)

    assert payload == {
        "project": "proj",
        "pattern": "raw",
        "matches": [{"dataset_id": "raw", "tables": ["raw.events"]}],
    }
    assert client.calls == [("query", "EU")]
    assert [label for label, _seconds, _extra in steps] == [
        "list_tables_via_information_schema",
        "filter_browser_matches",
        "total",
    ]
    assert steps[0][2] == "datasets=3 tables=4 locations=['EU']"


def test_browse_catalog_information_schema_uses_client_location() -> None:
    client = FakeCatalogClient(location="US")

    browse_catalog(client, "raw")

    assert client.calls == [("query", "US")]


def test_browse_catalog_information_schema_discovers_regions(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setenv("QMB_REGIONS_CACHE_DIR", str(tmp_path / "regions"))
    client = FakeCatalogClient()
    steps, on_step = _recorder()

    payload = browse_catalog(client, "analytics_prod", refresh_regions=True, on_step=on_step)

    assert payload["matches"][0]["dataset_id"] == "analytics_prod"
    assert client.calls == [("list_datasets", "proj"), ("query", "us-west1")]
    assert steps[0][2].endswith("locations=auto")


def test_browse_catalog_without_on_step_is_silent(capsys: pytest.CaptureFixture[str]) -> None:
    browse_catalog(FakeCatalogClient(location="US"), "raw")

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
