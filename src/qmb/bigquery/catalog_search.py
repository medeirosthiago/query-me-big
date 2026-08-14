"""Fuzzy and glob filtering for catalog browser matches."""

import fnmatch
from collections.abc import Sequence
from dataclasses import dataclass

from qmb.search.fuzzy import fuzzy_score as _fuzzy_score
from qmb.search.fuzzy import normalize as _normalize


@dataclass(frozen=True)
class BrowserMatch:
    dataset_id: str
    tables: tuple[str, ...]


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
