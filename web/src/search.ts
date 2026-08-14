import { bestScore, normalize } from "./fuzzy";
import { fmtDate } from "./format";
import type { JobSummary, SessionSummary } from "./types";

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

/** Fuzzy-filter + sort jobs. Empty query -> chronological (newest first). */
export function searchJobs(jobs: JobSummary[], query: string): JobSummary[] {
  const normalizedQuery = normalize(query);
  if (!normalizedQuery) {
    return [...jobs].sort((a, b) => b.created_at.localeCompare(a.created_at));
  }
  return scoreAndSort(jobs, (job) => bestScore(normalizedQuery, jobCandidates(job)));
}

/** Fuzzy-filter + sort sessions. Empty query -> most recently updated first. */
export function searchSessions(sessions: SessionSummary[], query: string): SessionSummary[] {
  const normalizedQuery = normalize(query);
  if (!normalizedQuery) {
    return [...sessions].sort((a, b) => (b.updated_at ?? "").localeCompare(a.updated_at ?? ""));
  }
  return scoreAndSort(sessions, (session) => bestScore(normalizedQuery, sessionCandidates(session)));
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
