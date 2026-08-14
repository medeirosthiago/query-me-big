/**
 * Port of `src/qmb/search/fuzzy.py`. Keep semantics identical: substring
 * matches score highest (biased toward shorter candidates and longer
 * queries); otherwise fall back to a subsequence match, scoring consecutive
 * runs higher, and return `null` when `query` is not a subsequence.
 *
 * `query` must already be normalized by the caller (see `normalize`).
 */
export function fuzzyScore(query: string, candidate: string): number | null {
  const normalizedCandidate = normalize(candidate);
  if (normalizedCandidate.includes(query)) {
    return 100 + query.length * 4 - (normalizedCandidate.length - query.length);
  }

  let queryIndex = 0;
  let score = 0;
  let consecutive = 0;
  for (const char of normalizedCandidate) {
    if (queryIndex >= query.length) break;
    if (char !== query[queryIndex]) {
      consecutive = 0;
      continue;
    }
    queryIndex += 1;
    consecutive += 1;
    score += 4 + consecutive * 2;
  }

  if (queryIndex !== query.length) return null;
  return score - normalizedCandidate.length;
}

/** Normalize a string for case/whitespace/separator-insensitive matching. */
export function normalize(value: string): string {
  return value.trim().toLowerCase().replaceAll(":", ".");
}

/** Score `candidate` fields against `query`, returning the max score (or null). */
export function bestScore(query: string, candidates: (string | null | undefined)[]): number | null {
  let best: number | null = null;
  for (const candidate of candidates) {
    if (!candidate) continue;
    const score = fuzzyScore(query, candidate);
    if (score !== null && (best === null || score > best)) best = score;
  }
  return best;
}
