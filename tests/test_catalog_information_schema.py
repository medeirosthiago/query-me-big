"""Tests for ``qmb.bigquery.catalog_information_schema``.

These exercise the INFORMATION_SCHEMA-based catalog path that powers
``qmb browse`` and the TUI browser pane without contacting BigQuery.
"""

from __future__ import annotations

from qmb.bigquery import catalog_information_schema as cis

# --- minimal fakes ------------------------------------------------------


class _DatasetListItem:
    """Mimic ``google.cloud.bigquery.dataset.DatasetListItem``: stores raw
    REST response data on ``_properties`` and exposes ``dataset_id``."""

    def __init__(self, dataset_id: str, location: str | None) -> None:
        self.dataset_id = dataset_id
        # The real library puts location on ``_properties`` (not as a
        # typed attribute); the production code reads it from there.
        self._properties: dict[str, str] = {}
        if location is not None:
            self._properties["location"] = location


class _FakeJob:
    def __init__(self, rows: list[dict[str, str]]) -> None:
        self._rows = rows

    def result(self) -> list[dict[str, str]]:
        return self._rows


class _FakeClient:
    """Records ``query`` calls so tests can assert which regions were hit."""

    def __init__(
        self,
        datasets: list[_DatasetListItem] | None = None,
        rows_by_location: dict[str, list[dict[str, str]]] | None = None,
        project: str = "proj",
    ) -> None:
        self.project = project
        self._datasets = datasets or []
        self._rows_by_location = rows_by_location or {}
        self.query_calls: list[tuple[str, str | None]] = []

    def list_datasets(self, project: str | None = None):  # noqa: ARG002 - signature parity
        return list(self._datasets)

    def query(self, sql: str, location: str | None = None) -> _FakeJob:
        self.query_calls.append((sql, location))
        return _FakeJob(self._rows_by_location.get(location or "", []))


# --- _region_qualifier --------------------------------------------------


def test_region_qualifier_lowercases_multi_region() -> None:
    assert cis._region_qualifier("US") == "region-us"
    assert cis._region_qualifier("EU") == "region-eu"


def test_region_qualifier_lowercases_specific_region() -> None:
    assert cis._region_qualifier("us-central1") == "region-us-central1"
    assert cis._region_qualifier("US-WEST1") == "region-us-west1"


# --- discover_dataset_locations -----------------------------------------


def test_discover_dataset_locations_reads_location_from_properties() -> None:
    client = _FakeClient(
        datasets=[
            _DatasetListItem("a", "us-west1"),
            _DatasetListItem("b", "US"),
            _DatasetListItem("c", "us-west1"),
        ],
    )

    locations = cis.discover_dataset_locations(client)

    assert locations == {"a": "us-west1", "b": "US", "c": "us-west1"}


def test_discover_dataset_locations_skips_items_without_location() -> None:
    # Simulate ancient list_datasets responses without a location field.
    client = _FakeClient(
        datasets=[
            _DatasetListItem("a", location=None),
            _DatasetListItem("b", "us-west1"),
        ],
    )

    assert cis.discover_dataset_locations(client) == {"b": "us-west1"}


# --- list_tables_via_information_schema ---------------------------------


def test_pinned_single_region_emits_one_query(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("QMB_REGIONS_CACHE_DIR", str(tmp_path))
    client = _FakeClient(
        rows_by_location={
            "us-west1": [
                {"table_schema": "ds1", "table_name": "t1"},
                {"table_schema": "ds1", "table_name": "t2"},
                {"table_schema": "ds2", "table_name": "t9"},
            ],
        },
    )

    result = cis.list_tables_via_information_schema(client, locations=["us-west1"])

    assert result == {"ds1": ("t1", "t2"), "ds2": ("t9",)}
    assert [loc for _, loc in client.query_calls] == ["us-west1"]


def test_auto_discovery_runs_one_query_per_unique_region(
    tmp_path, monkeypatch
) -> None:
    """When ``locations`` is None, the regions come from ``list_datasets``
    deduplicated; one query is fired per region."""
    monkeypatch.setenv("QMB_REGIONS_CACHE_DIR", str(tmp_path))
    client = _FakeClient(
        datasets=[
            _DatasetListItem("ds1", "us-west1"),
            _DatasetListItem("ds2", "us-west1"),
            _DatasetListItem("ds3", "US"),
        ],
        rows_by_location={
            "us-west1": [
                {"table_schema": "ds1", "table_name": "t1"},
                {"table_schema": "ds2", "table_name": "t9"},
            ],
            "US": [{"table_schema": "ds3", "table_name": "tx"}],
        },
    )

    result = cis.list_tables_via_information_schema(client)

    assert result == {"ds1": ("t1",), "ds2": ("t9",), "ds3": ("tx",)}
    assert sorted(loc for _, loc in client.query_calls) == ["US", "us-west1"]


def test_empty_datasets_are_preserved_after_discovery(
    tmp_path, monkeypatch
) -> None:
    """Datasets that exist in ``list_datasets`` but have no rows in
    INFORMATION_SCHEMA must still appear in the result with empty tuples."""
    monkeypatch.setenv("QMB_REGIONS_CACHE_DIR", str(tmp_path))
    client = _FakeClient(
        datasets=[
            _DatasetListItem("with_tables", "us-west1"),
            _DatasetListItem("empty_dataset", "us-west1"),
        ],
        rows_by_location={
            "us-west1": [{"table_schema": "with_tables", "table_name": "t1"}],
        },
    )

    result = cis.list_tables_via_information_schema(client)

    assert result == {"with_tables": ("t1",), "empty_dataset": ()}


def test_cache_hit_skips_list_datasets(tmp_path, monkeypatch) -> None:
    """A fresh on-disk cache must short-circuit the discovery step entirely
    — that's the whole point of the cache."""
    from qmb.bigquery import regions_cache

    monkeypatch.setenv("QMB_REGIONS_CACHE_DIR", str(tmp_path))
    regions_cache.save_regions("proj", ["us-west1"])

    client = _FakeClient(
        # No datasets attached: if we accidentally hit list_datasets we'd
        # silently skip discovery — but the assertion below catches it.
        datasets=[],
        rows_by_location={
            "us-west1": [{"table_schema": "ds1", "table_name": "t1"}],
        },
    )
    list_calls = []
    orig_list_datasets = client.list_datasets

    def _spy_list_datasets(*args, **kwargs):
        list_calls.append((args, kwargs))
        return orig_list_datasets(*args, **kwargs)

    client.list_datasets = _spy_list_datasets  # type: ignore[assignment]

    result = cis.list_tables_via_information_schema(client)

    assert result == {"ds1": ("t1",)}
    assert list_calls == [], "cache hit must not trigger list_datasets"


def test_refresh_cache_rediscovers_and_overwrites(tmp_path, monkeypatch) -> None:
    """``refresh_cache=True`` must ignore the existing cache *and* write
    the fresh value back, so the next run is warm again."""
    from qmb.bigquery import regions_cache

    monkeypatch.setenv("QMB_REGIONS_CACHE_DIR", str(tmp_path))
    # Seed the cache with a stale region — refresh must ignore it.
    regions_cache.save_regions("proj", ["EU"])

    client = _FakeClient(
        datasets=[_DatasetListItem("ds1", "us-west1")],
        rows_by_location={
            "us-west1": [{"table_schema": "ds1", "table_name": "t1"}],
            "EU": [{"table_schema": "should_not_appear", "table_name": "x"}],
        },
    )

    result = cis.list_tables_via_information_schema(client, refresh_cache=True)

    assert "should_not_appear" not in result
    assert result["ds1"] == ("t1",)
    # The fresh value must be persisted so the next call is warm.
    assert regions_cache.load_regions("proj") == ["us-west1"]


def test_cold_discovery_writes_cache(tmp_path, monkeypatch) -> None:
    """A cold run (no cache) must populate the cache for next time."""
    from qmb.bigquery import regions_cache

    monkeypatch.setenv("QMB_REGIONS_CACHE_DIR", str(tmp_path))

    client = _FakeClient(
        datasets=[
            _DatasetListItem("ds1", "us-west1"),
            _DatasetListItem("ds2", "US"),
        ],
        rows_by_location={
            "us-west1": [{"table_schema": "ds1", "table_name": "t1"}],
            "US": [{"table_schema": "ds2", "table_name": "tx"}],
        },
    )

    cis.list_tables_via_information_schema(client)

    cached = regions_cache.load_regions("proj")
    assert cached == ["US", "us-west1"]


def test_explicit_locations_skip_discovery_and_cache_writes(
    tmp_path, monkeypatch
) -> None:
    """When the caller passes ``locations`` explicitly, no discovery should
    happen and we must NOT pollute the cache (the caller is opting out of
    project-wide semantics)."""
    from qmb.bigquery import regions_cache

    monkeypatch.setenv("QMB_REGIONS_CACHE_DIR", str(tmp_path))

    client = _FakeClient(
        datasets=[],
        rows_by_location={"us-west1": []},
    )

    cis.list_tables_via_information_schema(client, locations=["us-west1"])

    assert regions_cache.load_regions("proj") is None


def test_raises_without_project() -> None:
    client = _FakeClient()
    client.project = ""

    try:
        cis.list_tables_via_information_schema(client)
    except ValueError as exc:
        assert "project" in str(exc).lower()
    else:  # pragma: no cover - defensive
        raise AssertionError("expected ValueError when project is missing")
