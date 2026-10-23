"""Catalog inspection payloads (``describe`` / ``browse``) as library functions.

These build the exact JSON-ready payloads the ``qmb describe`` and
``qmb browse`` commands print, so non-CLI callers (e.g. a server holding a
per-user BigQuery client) can reuse them. No console I/O happens here.
"""

import time
from collections.abc import Callable
from typing import Any

# Import modules (not their members) so test monkeypatches against
# the canonical module paths are picked up at call time.
from qmb.bigquery import catalog as _catalog
from qmb.bigquery import catalog_information_schema as _catalog_info_schema
from qmb.bigquery import catalog_search as _catalog_search

__all__ = ["browse_catalog", "describe_target"]

StepCallback = Callable[[str, float, str], None]


def describe_target(client: Any, target: str) -> dict[str, Any]:
    """Return dataset or table metadata for ``target``.

    ``target`` is ``dataset``, ``dataset.table`` or ``project.dataset.table``
    (``project:dataset.table`` is accepted too). Raises ``ValueError`` when
    the target has more than three parts.
    """
    # ``dataset`` vs ``dataset.table`` — colons are also accepted as a
    # convenience (``project:dataset.table`` is BigQuery's own shorthand).
    normalized = target.replace(":", ".")
    parts = normalized.split(".")
    if len(parts) == 1:
        dataset = _catalog.get_dataset_metadata(client, parts[0])
        return {
            "kind": "dataset",
            "dataset": dataset.to_api_repr(),
            "tables": list(_catalog.list_dataset_tables(client, parts[0])),
        }
    if len(parts) == 2:
        table = _catalog.get_table_metadata(client, parts[0], parts[1])
        return {"kind": "table", "table": table.to_api_repr()}
    if len(parts) == 3:
        # project.dataset.table — honor it but require the configured
        # client's project for now; cross-project describe is out of scope.
        table = _catalog.get_table_metadata(client, parts[1], parts[2])
        return {"kind": "table", "table": table.to_api_repr()}
    raise ValueError(f"Cannot parse target {target!r}. Use 'dataset' or 'dataset.table'.")


def browse_catalog(
    client: Any,
    pattern: str | None,
    *,
    project: str | None = None,
    location: str | None = None,
    workers: int = 8,
    legacy_list_tables: bool = False,
    refresh_regions: bool = False,
    on_step: StepCallback | None = None,
) -> dict[str, Any]:
    """Return the catalog listing, optionally filtered by ``pattern``.

    Without a pattern, returns ``{project, datasets}``. With one, returns
    ``{project, pattern, matches}`` where tables come from
    ``INFORMATION_SCHEMA.TABLES`` (default) or a per-dataset
    ``list_tables`` fan-out (``legacy_list_tables=True``).

    ``on_step(label, seconds, extra)`` is called after each timed step.
    """

    def _stamp(label: str, seconds: float, extra: str = "") -> None:
        if on_step is not None:
            on_step(label, seconds, extra)

    t0 = time.perf_counter()

    if pattern is None:
        # Cheap path: just the dataset list, no per-dataset table fetch.
        dataset_ids = _catalog.list_dataset_ids(client)
        _stamp("list_dataset_ids", time.perf_counter() - t0, f"n={len(dataset_ids)}")
        return {
            "project": getattr(client, "project", None) or project,
            "datasets": dataset_ids,
        }

    if legacy_list_tables:
        t_ds = time.perf_counter()
        dataset_ids = _catalog.list_dataset_ids(client)
        _stamp("list_dataset_ids", time.perf_counter() - t_ds, f"n={len(dataset_ids)}")

        t1 = time.perf_counter()
        tables_by_dataset = _catalog.build_table_index(client, dataset_ids, max_workers=workers)
        total_tables = sum(len(v) for v in tables_by_dataset.values())
        _stamp(
            "build_table_index",
            time.perf_counter() - t1,
            f"datasets={len(tables_by_dataset)} tables={total_tables} workers={workers}",
        )
    else:
        # If a location is given, pin to a single region (skip
        # auto-discovery). Otherwise discover regions from list_datasets
        # so multi-region projects are handled correctly.
        explicit_location = location or getattr(client, "location", None)
        pinned_locations = [explicit_location] if explicit_location else None

        t1 = time.perf_counter()
        tables_by_dataset = _catalog_info_schema.list_tables_via_information_schema(
            client,
            project=project,
            locations=pinned_locations,
            refresh_cache=refresh_regions,
        )
        total_tables = sum(len(v) for v in tables_by_dataset.values())
        _stamp(
            "list_tables_via_information_schema",
            time.perf_counter() - t1,
            f"datasets={len(tables_by_dataset)} tables={total_tables} "
            f"locations={pinned_locations or 'auto'}",
        )
        dataset_ids = sorted(tables_by_dataset.keys(), key=str.lower)

    t2 = time.perf_counter()
    matches = _catalog_search.filter_browser_matches(dataset_ids, tables_by_dataset, pattern)
    _stamp(
        "filter_browser_matches",
        time.perf_counter() - t2,
        f"matches={len(matches)}",
    )

    payload = {
        "project": getattr(client, "project", None) or project,
        "pattern": pattern,
        "matches": [{"dataset_id": m.dataset_id, "tables": list(m.tables)} for m in matches],
    }
    _stamp("total", time.perf_counter() - t0)
    return payload
