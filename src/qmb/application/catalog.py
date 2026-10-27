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


def _parse_target(target: str) -> tuple[str | None, str, str | None]:
    """Split ``target`` into ``(project, dataset, table)``.

    ``project`` is ``None`` when the target does not name one. Raises
    ``ValueError`` when the target cannot be parsed.
    """
    error = ValueError(f"Cannot parse target {target!r}. Use 'dataset' or 'dataset.table'.")
    project: str | None = None
    rest = target
    # ``project:dataset[.table]`` is BigQuery's own shorthand.
    if ":" in target:
        project, _, rest = target.partition(":")
        if not project or ":" in rest:
            raise error
    parts = rest.split(".")
    if project is None and len(parts) == 3:
        project, parts = parts[0], parts[1:]
    if len(parts) > 2:
        raise error
    return project, parts[0], parts[1] if len(parts) == 2 else None


def describe_target(client: Any, target: str) -> dict[str, Any]:
    """Return dataset or table metadata for ``target``.

    ``target`` is ``dataset``, ``dataset.table``, ``project.dataset.table``,
    ``project:dataset`` or ``project:dataset.table``. Without a project the
    client's project is used. Raises ``ValueError`` when the target has more
    parts than that.
    """
    project, dataset_id, table_id = _parse_target(target)
    if table_id is None:
        dataset = _catalog.get_dataset_metadata(client, dataset_id, project)
        return {
            "kind": "dataset",
            "dataset": dataset.to_api_repr(),
            "tables": list(_catalog.list_dataset_tables(client, dataset_id, project)),
        }
    table = _catalog.get_table_metadata(client, dataset_id, table_id, project)
    return {"kind": "table", "table": table.to_api_repr()}


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

    ``project`` selects the project to inspect (default: the client's).
    Jobs still run, and are billed, in the client's project.

    ``on_step(label, seconds, extra)`` is called after each timed step.
    """
    project = project or getattr(client, "project", None)

    def _stamp(label: str, seconds: float, extra: str = "") -> None:
        if on_step is not None:
            on_step(label, seconds, extra)

    t0 = time.perf_counter()

    if pattern is None:
        # Cheap path: just the dataset list, no per-dataset table fetch.
        dataset_ids = _catalog.list_dataset_ids(client, project)
        _stamp("list_dataset_ids", time.perf_counter() - t0, f"n={len(dataset_ids)}")
        return {
            "project": project,
            "datasets": dataset_ids,
        }

    if legacy_list_tables:
        t_ds = time.perf_counter()
        dataset_ids = _catalog.list_dataset_ids(client, project)
        _stamp("list_dataset_ids", time.perf_counter() - t_ds, f"n={len(dataset_ids)}")

        t1 = time.perf_counter()
        tables_by_dataset = _catalog.build_table_index(
            client, dataset_ids, max_workers=workers, project=project
        )
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
        "project": project,
        "pattern": pattern,
        "matches": [{"dataset_id": m.dataset_id, "tables": list(m.tables)} for m in matches],
    }
    _stamp("total", time.perf_counter() - t0)
    return payload
