"""Alternative catalog listing via ``INFORMATION_SCHEMA.TABLES``.

This module is an experimental fast path for ``qmb browse``: instead of
issuing one ``list_tables`` REST call per dataset (which scales as
``O(N_datasets)`` paginated round-trips, and is bounded by the *single
slowest* dataset's pagination), it asks BigQuery's catalog directly with
one SQL query per region.

Trade-offs vs. ``catalog.build_table_index``:

- Pros: typically 10×+ faster on large projects with many tables.
- Pros: ``INFORMATION_SCHEMA`` queries are free of slot/bytes charges.
- Cons: requires a region qualifier (``region-us``, ``region-eu``,
  ``region-us-central1``, …). A project spread across N regions needs N
  queries (run in parallel here).
- Cons: each query spins up a BigQuery job (~0.5–2 s of fixed overhead).
"""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import bigquery


def _region_qualifier(location: str) -> str:
    """Return the ``region-<x>`` qualifier used in INFORMATION_SCHEMA paths.

    BigQuery accepts both multi-regions (``US``, ``EU``) and specific
    regions (``us-central1``). The qualifier is always lowercased and
    prefixed with ``region-``.
    """
    return f"region-{location.lower()}"


def discover_dataset_locations(
    client: bigquery.Client, *, project: str | None = None
) -> dict[str, str]:
    """Return ``{dataset_id: location}`` for every dataset in the project.

    ``DatasetListItem`` does not expose ``location`` as a typed property,
    but the underlying REST response (``_properties``) includes it. We
    treat it as the authoritative source for which regions to query.
    """
    project_id = project or getattr(client, "project", None)
    items = client.list_datasets(project=project_id) if project_id else client.list_datasets()
    locations: dict[str, str] = {}
    for item in items:
        location = item._properties.get("location")
        if location:
            locations[item.dataset_id] = location
    return locations


def list_tables_via_information_schema(
    client: bigquery.Client,
    *,
    project: str | None = None,
    locations: Iterable[str] | None = None,
    max_workers: int = 4,
    refresh_cache: bool = False,
) -> dict[str, tuple[str, ...]]:
    """Return ``{dataset_id: (table_id, ...)}`` for the project.

    If ``locations`` is not provided, discover regions by listing
    datasets first. One ``INFORMATION_SCHEMA.TABLES`` query is issued
    per region, in parallel, and results are merged.

    When ``locations`` is None, the on-disk regions cache at
    ``~/.qmb/cache/regions/<project>.json`` short-circuits the
    ``list_datasets`` discovery step. Pass ``refresh_cache=True`` to
    ignore the existing cache entry and re-discover (the new value is
    written back so subsequent runs are warm again).

    When ``locations`` is provided explicitly, the cache is neither
    read nor written — the caller is pinning regions for one call.

    Setting ``QMB_TRACE_INFO_SCHEMA=1`` prints per-region wall-clock
    timing to stderr.
    """
    from qmb.bigquery import regions_cache

    project_id = project or getattr(client, "project", None)
    if not project_id:
        raise ValueError("project is required for INFORMATION_SCHEMA queries")

    trace = os.environ.get("QMB_TRACE_INFO_SCHEMA") == "1"

    location_map: dict[str, str] | None = None
    if locations is None:
        cached = regions_cache.load_regions(project_id) if not refresh_cache else None
        if cached is not None:
            unique_locations = cached
            if trace:
                print(
                    f"[trace] regions: cache hit  {unique_locations}",
                    file=sys.stderr,
                )
        else:
            t = time.perf_counter()
            location_map = discover_dataset_locations(client, project=project_id)
            unique_locations = sorted(set(location_map.values()))
            if trace:
                print(
                    f"[trace] discover_dataset_locations: "
                    f"{(time.perf_counter() - t) * 1000:.1f} ms  "
                    f"datasets={len(location_map)} regions={unique_locations}",
                    file=sys.stderr,
                )
            if unique_locations:
                regions_cache.save_regions(project_id, unique_locations)
    else:
        unique_locations = sorted({loc for loc in locations})

    if not unique_locations:
        return {}

    merged: dict[str, tuple[str, ...]] = {}
    worker_count = max(1, min(max_workers, len(unique_locations)))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(
                _query_region_with_timing, client, project_id, location, trace
            ): location
            for location in unique_locations
        }
        for future in as_completed(futures):
            region_tables = future.result()
            # A dataset lives in exactly one location, so there's no
            # cross-region overlap to merge. Plain dict.update is safe.
            merged.update(region_tables)

    # Empty datasets exist in ``list_datasets`` but have no rows in
    # ``INFORMATION_SCHEMA.TABLES``. Preserve them with empty tuples so
    # we stay equivalent to the ``build_table_index`` baseline (which
    # always emits one entry per dataset).
    if location_map is not None:
        for dataset_id in location_map:
            merged.setdefault(dataset_id, ())
    return merged


def _query_region_with_timing(
    client: bigquery.Client, project: str, location: str, trace: bool
) -> dict[str, tuple[str, ...]]:
    t = time.perf_counter()
    result = _query_region(client, project, location)
    if trace:
        total_tables = sum(len(v) for v in result.values())
        print(
            f"[trace]   region={location:<14} "
            f"{(time.perf_counter() - t) * 1000:8.1f} ms  "
            f"datasets={len(result):4d} tables={total_tables:6d}",
            file=sys.stderr,
        )
    return result


def _query_region(
    client: bigquery.Client, project: str, location: str
) -> dict[str, tuple[str, ...]]:
    qualifier = _region_qualifier(location)
    sql = f"""
        SELECT table_schema, table_name
        FROM `{project}.{qualifier}.INFORMATION_SCHEMA.TABLES`
        ORDER BY table_schema, table_name
    """
    job = client.query(sql, location=location)
    rows = job.result()
    return _group_rows(rows)


def _group_rows(rows: Iterable[bigquery.Row]) -> dict[str, tuple[str, ...]]:
    grouped: dict[str, list[str]] = {}
    for row in rows:
        grouped.setdefault(row["table_schema"], []).append(row["table_name"])
    return {dataset_id: tuple(table_ids) for dataset_id, table_ids in grouped.items()}
