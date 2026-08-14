"""Shared fuzzy-matching scorer used by the catalog browser and job search."""


def fuzzy_score(query: str, candidate: str) -> int | None:
    """Score ``candidate`` against a pre-normalized ``query``.

    Substring matches score highest (biased toward shorter candidates and
    longer queries). Otherwise falls back to a subsequence match, scoring
    consecutive runs higher, and returns ``None`` if ``query`` is not a
    subsequence of ``candidate``.
    """
    normalized_candidate = normalize(candidate)
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


def normalize(value: str) -> str:
    """Normalize a string for case/whitespace/separator-insensitive matching."""
    return value.strip().lower().replace(":", ".")
