import { archivesOf, itemKey } from "./archives";
import type { JobSummary, Origin, SessionSummary } from "./types";

/**
 * Synthesize session summaries for `session_id`s referenced by jobs but
 * missing from the manifest-backed `/api/index` `sessions` array (jobs that
 * predate manifest support, a failed manifest write, or a remote job whose
 * manifest wasn't exported). Marked `derived: true` so the UI can flag them
 * and suggest `qmb jobs reindex`.
 *
 * Sessions are per archive: `knownKeys` holds `itemKey(archive, id)` for
 * every archive a known session lives in (see `knownSessionKeys`).
 */
export function deriveMissingSessions(
  jobs: JobSummary[],
  knownKeys: ReadonlySet<string>,
): SessionSummary[] {
  const jobsBySession = new Map<string, JobSummary[]>();
  for (const job of jobs) {
    const sessionId = job.session_id;
    if (!sessionId) continue;
    if (archivesOf(job).some((archive) => knownKeys.has(itemKey(archive, sessionId)))) {
      continue;
    }
    const key = itemKey(job.archive, sessionId);
    const list = jobsBySession.get(key);
    if (list) list.push(job);
    else jobsBySession.set(key, [job]);
  }
  return [...jobsBySession.values()].map((sessionJobs) =>
    deriveSession(sessionJobs[0].session_id!, sessionJobs),
  );
}

/** Keys for every archive each session lives in, for `deriveMissingSessions`. */
export function knownSessionKeys(sessions: SessionSummary[]): Set<string> {
  return new Set(
    sessions.flatMap((session) =>
      archivesOf(session).map((archive) => itemKey(archive, session.session_id)),
    ),
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
    users: uniqueSorted(sorted.map((job) => job.user)),
    updated_at: sorted[0]?.created_at ?? null,
    origin: mergeOrigins(sorted.map((job) => job.origin)),
    archive: sorted[0].archive,
    also_in: sorted[0].also_in,
    agent_name: sorted.find((job) => job.agent_name)?.agent_name ?? null,
    user: sorted.find((job) => job.user)?.user ?? null,
    task: sorted.find((job) => job.task)?.task ?? null,
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
