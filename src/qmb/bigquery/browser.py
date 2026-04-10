"""Helpers for browsing BigQuery datasets and tables."""

import fnmatch
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from google.cloud import bigquery


@dataclass(frozen=True)
class BrowserMatch:
    dataset_id: str
    tables: tuple[str, ...]


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
    """Fetch tables for many datasets concurrently."""
    if not dataset_ids:
        return {}

    table_index: dict[str, tuple[str, ...]] = {}
    worker_count = max(1, min(max_workers, len(dataset_ids)))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(list_dataset_tables, client, dataset_id): dataset_id
            for dataset_id in dataset_ids
        }
        for future in as_completed(futures):
            dataset_id = futures[future]
            table_index[dataset_id] = future.result()
    return table_index


def filter_browser_matches(
    dataset_ids: Sequence[str],
    tables_by_dataset: dict[str, tuple[str, ...]],
    query: str,
) -> list[BrowserMatch]:
    """Filter datasets and tables using a lightweight fuzzy matcher."""
    normalized_query = _normalize(query)
    if not normalized_query:
        return [BrowserMatch(dataset_id=dataset_id, tables=()) for dataset_id in dataset_ids]

    if _is_glob_query(normalized_query):
        return _glob_browser_matches(dataset_ids, tables_by_dataset, normalized_query)

    matches: list[tuple[int, BrowserMatch]] = []
    for dataset_id in dataset_ids:
        dataset_score = _fuzzy_score(normalized_query, dataset_id)
        tables = tables_by_dataset.get(dataset_id, ())

        if dataset_score is not None:
            matches.append(
                (
                    2_000 + dataset_score,
                    BrowserMatch(
                        dataset_id=dataset_id,
                        tables=tuple(f"{dataset_id}.{table_id}" for table_id in tables),
                    ),
                )
            )
            continue

        matched_tables: list[str] = []
        best_table_score: int | None = None
        for table_id in tables:
            full_name = f"{dataset_id}.{table_id}"
            table_score = _best_score(normalized_query, table_id, full_name)
            if table_score is None:
                continue
            matched_tables.append(full_name)
            if best_table_score is None:
                best_table_score = table_score
            else:
                best_table_score = max(best_table_score, table_score)

        if matched_tables and best_table_score is not None:
            matches.append(
                (
                    1_000 + best_table_score,
                    BrowserMatch(dataset_id=dataset_id, tables=tuple(matched_tables)),
                )
            )

    matches.sort(key=lambda item: (-item[0], item[1].dataset_id.lower()))
    return [match for _, match in matches]


def _best_score(query: str, *candidates: str) -> int | None:
    scores = [_fuzzy_score(query, candidate) for candidate in candidates]
    valid_scores = [score for score in scores if score is not None]
    return max(valid_scores) if valid_scores else None


def _fuzzy_score(query: str, candidate: str) -> int | None:
    normalized_candidate = _normalize(candidate)
    if query in normalized_candidate:
        return 100 + len(query) * 4 - (len(normalized_candidate) - len(query))

    query_index = 0
    score = 0
    consecutive = 0
    for char in normalized_candidate:
        if query_index >= len(query):
            break
        if char != query[query_index]:
            consecutive = 0
            continue
        query_index += 1
        consecutive += 1
        score += 4 + consecutive * 2

    if query_index != len(query):
        return None
    return score - len(normalized_candidate)


def _normalize(value: str) -> str:
    return value.strip().lower().replace(":", ".")


def _is_glob_query(query: str) -> bool:
    return any(char in query for char in "*?[")


def _glob_browser_matches(
    dataset_ids: Sequence[str],
    tables_by_dataset: dict[str, tuple[str, ...]],
    query: str,
) -> list[BrowserMatch]:
    matches: list[BrowserMatch] = []
    for dataset_id in dataset_ids:
        normalized_dataset = _normalize(dataset_id)
        tables = tables_by_dataset.get(dataset_id, ())
        if fnmatch.fnmatch(normalized_dataset, query):
            matches.append(
                BrowserMatch(
                    dataset_id=dataset_id,
                    tables=tuple(f"{dataset_id}.{table_id}" for table_id in tables),
                )
            )
            continue

        matched_tables = [
            f"{dataset_id}.{table_id}"
            for table_id in tables
            if fnmatch.fnmatch(_normalize(table_id), query)
            or fnmatch.fnmatch(_normalize(f"{dataset_id}.{table_id}"), query)
        ]
        if matched_tables:
            matches.append(BrowserMatch(dataset_id=dataset_id, tables=tuple(matched_tables)))
    return matches
