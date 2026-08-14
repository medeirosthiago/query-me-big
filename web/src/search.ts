import { bestScore, normalize } from "./fuzzy";
import { fmtDate } from "./format";
import type { JobSummary, SessionSummary } from "./types";

const SESSION_TOKEN_RE = /session:(\S+)/i;

/**
 * Pulls a `session:<id>` token out of a raw query (before fuzzy-normalization
 * mangles the `:`), returning the exact session id to hard-filter on and the
 * remaining free text to fuzzy-match. No token -> `sessionId` is null and
 * `rest` is the query unchanged.
 */
export function extractSessionToken(query: string): { sessionId: string | null; rest: string } {
  const match = SESSION_TOKEN_RE.exec(query);
  if (!match) return { sessionId: null, rest: query };
  const sessionId = match[1].trim();
  const rest = query.slice(0, match.index) + query.slice(match.index + match[0].length);
  return { sessionId, rest };
}

function jobCandidates(job: JobSummary): (string | null | undefined)[] {
  return [
    job.query_excerpt,
    job.qmb_job_id,
    job.session_id,
    job.source.label,
    job.agent?.name,
    job.created_at,
    fmtDate(job.created_at),
  ];
}

function sessionCandidates(session: SessionSummary): (string | null | undefined)[] {
  return [
    session.session_id,
    ...session.agents,
    ...session.tasks,
    ...session.cwds,
    session.first,
    session.latest,
  ];
}

/**
 * Fuzzy-filter + sort jobs. Empty query -> chronological (newest first).
 * A `session:<id>` token first hard-filters to that session's jobs; any
 * remaining text then fuzzy-matches within that subset.
 */
export function searchJobs(jobs: JobSummary[], query: string): JobSummary[] {
  const { sessionId, rest } = extractSessionToken(query);
  const scoped = sessionId === null ? jobs : jobs.filter((job) => job.session_id === sessionId);
  const normalizedQuery = normalize(rest);
  if (!normalizedQuery) {
    return [...scoped].sort((a, b) => b.created_at.localeCompare(a.created_at));
  }
  return scoreAndSort(scoped, (job) => bestScore(normalizedQuery, jobCandidates(job)));
}

/**
 * Fuzzy-filter + sort sessions. Empty query -> most recently updated first.
 * A `session:<id>` token hard-filters to that exact session id.
 */
export function searchSessions(sessions: SessionSummary[], query: string): SessionSummary[] {
  const { sessionId, rest } = extractSessionToken(query);
  const scoped =
    sessionId === null ? sessions : sessions.filter((session) => session.session_id === sessionId);
  const normalizedQuery = normalize(rest);
  if (!normalizedQuery) {
    return [...scoped].sort((a, b) => (b.updated_at ?? "").localeCompare(a.updated_at ?? ""));
  }
  return scoreAndSort(scoped, (session) => bestScore(normalizedQuery, sessionCandidates(session)));
}

function scoreAndSort<T>(items: T[], score: (item: T) => number | null): T[] {
  const scored: { item: T; score: number }[] = [];
  for (const item of items) {
    const s = score(item);
    if (s !== null) scored.push({ item, score: s });
  }
  scored.sort((a, b) => b.score - a.score);
  return scored.map((entry) => entry.item);
}
