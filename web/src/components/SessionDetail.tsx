import { fmtBytes, fmtDate } from "../format";
import type { JobSummary, SessionSummary } from "../types";
import { ArchiveBadges } from "./ArchiveBadges";
import { CopyLine } from "./CopyLine";

interface Props {
  session: SessionSummary;
  jobs: JobSummary[]; // jobs belonging to this session, in manifest order
  /** True if the on-demand remote manifest fetch failed and this is still the index-derived summary. */
  fetchFailed?: boolean;
  primaryArchive: string | null;
  /** Command that rebuilds this session's remote index (see `unindexed`). */
  reindexCommand: string;
  onSelectJob: (jobId: string) => void;
}

export function SessionDetail({
  session,
  jobs,
  fetchFailed,
  primaryArchive,
  reindexCommand,
  onSelectJob,
}: Props) {
  const jobById = new Map(jobs.map((j) => [j.qmb_job_id, j]));
  // Newest first, matching the sidebar ordering — independent of manifest/derived source order.
  const orderedJobIds = [...session.jobs].sort((a, b) => {
    const aDate = jobById.get(a)?.created_at ?? "";
    const bDate = jobById.get(b)?.created_at ?? "";
    return bDate.localeCompare(aDate);
  });

  // Remote sessions summarized from index.json are the normal two-phase-load
  // case (one GCS download, no per-session manifest scan) — not a warning.
  // Only a session with no manifest anywhere (local fallback) gets the
  // no-manifest badge and the full reindex notice.
  const isNoManifest = session.derived && session.origin !== "remote";
  const isRemoteFetchFailed = session.derived && session.origin === "remote" && fetchFailed;
  const agents = session.agents.length > 0 ? session.agents : session.agent_name ? [session.agent_name] : [];
  const users = session.users?.length ? session.users : session.user ? [session.user] : [];

  return (
    <div class="detail session-detail">
      <header class="detail__header">
        <h2>{session.session_id}</h2>
        <span class="row__badges">
          <ArchiveBadges item={session} primary={primaryArchive} />
          {isNoManifest && <span class="badge badge--derived">no manifest</span>}
          {session.unindexed && <span class="badge badge--derived">unindexed</span>}
        </span>
      </header>

      {isNoManifest && (
        <div class="notice">
          No session manifest was found for this session — these fields were computed from its
          jobs in the index. Run <code>qmb jobs reindex</code> to persist a manifest.
          <CopyLine command="qmb jobs reindex" />
        </div>
      )}

      {session.unindexed && (
        <div class="notice">
          This session's manifest exists remotely but hasn't been picked up by the remote index
          yet — its jobs stay absent from the Jobs tab until reindexed. Fetching its full detail
          now…
          <CopyLine command={reindexCommand} />
        </div>
      )}

      {isRemoteFetchFailed && (
        <div class="subtle-note">
          Summary derived from index — fetching the full session manifest failed, showing
          agents/tasks/cwds as unavailable.
        </div>
      )}

      <section class="detail__grid">
        <div class="field">
          <span class="field__label">Jobs</span>
          <span class="field__value">{session.count ?? "?"}</span>
        </div>
        <div class="field">
          <span class="field__label">Range</span>
          <span class="field__value">{fmtDate(session.first)} → {fmtDate(session.latest)}</span>
        </div>
        <div class="field">
          <span class="field__label">Bytes processed</span>
          <span class="field__value">{fmtBytes(session.bytes_processed)}</span>
        </div>
        {agents.length > 0 && (
          <div class="field field--wide">
            <span class="field__label">Agents</span>
            <span class="field__value">{agents.join(", ")}</span>
          </div>
        )}
        {users.length > 0 && (
          <div class="field field--wide">
            <span class="field__label">People</span>
            <span class="field__value">{users.join(", ")}</span>
          </div>
        )}
        {session.tasks.length > 0 && (
          <div class="field field--wide">
            <span class="field__label">Questions / tasks</span>
            <ul class="field__value field__list">
              {session.tasks.map((task) => <li key={task}>{task}</li>)}
            </ul>
          </div>
        )}
        {session.cwds.length > 0 && (
          <div class="field field--wide">
            <span class="field__label">Cwds</span>
            <span class="field__value">{session.cwds.join(", ")}</span>
          </div>
        )}
      </section>

      <section class="detail__section">
        <h3>Jobs in this session</h3>
        <ul class="session-job-list">
          {orderedJobIds.map((jobId) => {
            const job = jobById.get(jobId);
            return (
              <li key={jobId}>
                <a href="#" onClick={(e) => { e.preventDefault(); onSelectJob(jobId); }}>
                  {jobId}
                </a>
                {job && (
                  <span class="session-job-list__meta">
                    {" "}— {fmtDate(job.created_at)} — {job.query_excerpt.slice(0, 80)}
                  </span>
                )}
              </li>
            );
          })}
        </ul>
      </section>
    </div>
  );
}
