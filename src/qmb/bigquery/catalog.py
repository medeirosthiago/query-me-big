"""BigQuery catalog metadata listing and fetching."""

import os
import sys
import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import bigquery


def list_dataset_ids(client: bigquery.Client) -> list[str]:
    """Return dataset ids for the active project in a stable order."""
    project_id = getattr(client, "project", None)
    datasets = client.list_datasets(project=project_id) if project_id else client.list_datasets()
    return sorted((dataset.dataset_id for dataset in datasets), key=str.lower)


def list_dataset_tables(client: bigquery.Client, dataset_id: str) -> tuple[str, ...]:
    """Return table ids for a dataset in a stable order."""
    project_id = getattr(client, "project", None)
    dataset_ref = f"{project_id}.{dataset_id}" if project_id else dataset_id
    tables = client.list_tables(dataset_ref)
    return tuple(sorted((table.table_id for table in tables), key=str.lower))


def build_table_index(
    client: bigquery.Client, dataset_ids: Sequence[str], max_workers: int = 8
) -> dict[str, tuple[str, ...]]:
    """Fetch tables for many datasets concurrently.

    Setting ``QMB_TRACE_CATALOG=1`` prints per-dataset wall-clock timing
    and a top-10 "slowest datasets" report to stderr. This is a diagnostic
    aid for the browse performance work; it is not part of the public API.
    """
    if not dataset_ids:
        return {}

    trace = os.environ.get("QMB_TRACE_CATALOG") == "1"

    def _timed(dataset_id: str) -> tuple[str, tuple[str, ...], float]:
        t = time.perf_counter()
        result = list_dataset_tables(client, dataset_id)
        return dataset_id, result, time.perf_counter() - t

    table_index: dict[str, tuple[str, ...]] = {}
    timings: list[tuple[str, int, float]] = []
    worker_count = max(1, min(max_workers, len(dataset_ids)))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [executor.submit(_timed, dataset_id) for dataset_id in dataset_ids]
        for future in as_completed(futures):
            dataset_id, tables, elapsed = future.result()
            table_index[dataset_id] = tables
            timings.append((dataset_id, len(tables), elapsed))

    if trace:
        timings.sort(key=lambda row: -row[2])
        total_seconds = sum(elapsed for _, _, elapsed in timings)
        print(
            f"[trace] catalog total per-dataset wall-time (summed): "
            f"{total_seconds * 1000:.1f} ms across {len(timings)} datasets",
            file=sys.stderr,
        )
        print(
            f"[trace] top 10 slowest datasets (out of {len(timings)}):",
            file=sys.stderr,
        )
        for dataset_id, table_count, elapsed in timings[:10]:
            print(
                f"[trace]   {elapsed * 1000:8.1f} ms  tables={table_count:5d}  "
                f"{dataset_id}",
                file=sys.stderr,
            )
    return table_index


def get_dataset_metadata(client: bigquery.Client, dataset_id: str) -> bigquery.Dataset:
    """Fetch dataset metadata for details inspection."""
    project_id = getattr(client, "project", None)
    dataset_ref = f"{project_id}.{dataset_id}" if project_id else dataset_id
    return client.get_dataset(dataset_ref)


def get_table_metadata(
    client: bigquery.Client, dataset_id: str, table_id: str
) -> bigquery.Table:
    """Fetch table metadata for details inspection."""
    project_id = getattr(client, "project", None)
    if project_id:
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
    else:
        table_ref = f"{dataset_id}.{table_id}"
    return client.get_table(table_ref)
