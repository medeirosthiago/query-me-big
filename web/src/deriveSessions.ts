import type { JobSummary, Origin, SessionSummary } from "./types";

/**
 * Synthesize session summaries for `session_id`s referenced by jobs but
 * missing from the manifest-backed `/api/index` `sessions` array (jobs that
 * predate manifest support, a failed manifest write, or a remote job whose
 * manifest wasn't exported). Marked `derived: true` so the UI can flag them
 * and suggest `qmb jobs reindex`.
 */
export function deriveMissingSessions(
  jobs: JobSummary[],
  knownSessionIds: ReadonlySet<string>,
): SessionSummary[] {
  const jobsBySession = new Map<string, JobSummary[]>();
  for (const job of jobs) {
    if (!job.session_id || knownSessionIds.has(job.session_id)) continue;
    const list = jobsBySession.get(job.session_id);
    if (list) list.push(job);
    else jobsBySession.set(job.session_id, [job]);
  }
  return [...jobsBySession.entries()].map(([sessionId, sessionJobs]) =>
    deriveSession(sessionId, sessionJobs),
  );
}

function deriveSession(sessionId: string, jobs: JobSummary[]): SessionSummary {
  const sorted = [...jobs].sort((a, b) => b.created_at.localeCompare(a.created_at));
  const bytesProcessed = sorted.reduce((sum, job) => sum + (job.stats.bytes_processed || 0), 0);
  return {
    session_id: sessionId,
    jobs: sorted.map((job) => job.qmb_job_id),
    count: sorted.length,
    first: sorted[sorted.length - 1]?.created_at ?? null,
    latest: sorted[0]?.created_at ?? null,
    bytes_processed: bytesProcessed,
    agents: uniqueSorted(sorted.map((job) => job.agent?.name)),
    tasks: uniqueSorted(sorted.map((job) => job.agent?.task)),
    cwds: uniqueSorted(sorted.map((job) => job.agent?.cwd)),
    updated_at: sorted[0]?.created_at ?? null,
    origin: mergeOrigins(sorted.map((job) => job.origin)),
    derived: true,
  };
}

function mergeOrigins(origins: Origin[]): Origin {
  const first = origins[0];
  return origins.every((o) => o === first) ? first : "both";
}

function uniqueSorted(values: (string | null | undefined)[]): string[] {
  return [...new Set(values.filter((v): v is string => !!v))].sort();
}
