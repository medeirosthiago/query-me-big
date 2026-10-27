import type { AgentFields, ArchiveTagged, JobSummary, SessionSummary } from "./types";

export const LOCAL_ARCHIVE = "local";
// Filter sentinels; `*` can't appear in an archive name and is unlikely in an agent name.
export const ALL = "*all*";
export const UNKNOWN_AGENT = "*unknown*";

/** Number of `.badge--hue-N` classes in `index.css` for extra archives. */
const EXTRA_HUES = 6;

/**
 * Ids are only unique per archive, so list items are keyed by both.
 * Archive names can't contain `:` (see `qmb.config.ARCHIVE_NAME_PATTERN`).
 */
export function itemKey(archive: string, id: string): string {
  return `${archive}:${id}`;
}

/** Every archive an item lives in: its own, then any it was merged with. */
export function archivesOf(item: ArchiveTagged): string[] {
  return [item.archive, ...(item.also_in ?? [])];
}

/**
 * Badge class for an archive: green for local, the primary archive's own
 * colour, and a stable per-name hue for each extra archive.
 */
export function archiveBadgeClass(archive: string, primary: string | null): string {
  if (archive === LOCAL_ARCHIVE) return "badge badge--local";
  if (archive === primary) return "badge badge--remote";
  let hash = 0;
  for (const char of archive) hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
  return `badge badge--hue-${hash % EXTRA_HUES}`;
}

/** `jane.doe@example.com` -> `jane.doe@…` for compact list rows. */
export function shortUser(user: string): string {
  const at = user.indexOf("@");
  return at > 0 ? `${user.slice(0, at)}@…` : user;
}

/** `example-mcp · jane.doe@…`, or whichever half is known, or null. */
export function agentLabel(item: AgentFields): string | null {
  const parts = [item.agent_name, item.user ? shortUser(item.user) : null].filter(
    (part): part is string => !!part,
  );
  return parts.length > 0 ? parts.join(" · ") : null;
}

function jobAgents(job: JobSummary): string[] {
  const name = job.agent_name ?? job.agent?.name;
  return name ? [name] : [];
}

function sessionAgents(session: SessionSummary): string[] {
  return [...new Set([...session.agents, ...(session.agent_name ? [session.agent_name] : [])])];
}

/** Every agent name seen across jobs and sessions, sorted. */
export function agentOptions(jobs: JobSummary[], sessions: SessionSummary[]): string[] {
  const names = new Set<string>();
  for (const job of jobs) for (const name of jobAgents(job)) names.add(name);
  for (const session of sessions) for (const name of sessionAgents(session)) names.add(name);
  return [...names].sort();
}

function matchesFilters(
  archives: string[],
  agents: string[],
  archiveFilter: string,
  agentFilter: string,
): boolean {
  if (archiveFilter !== ALL && !archives.includes(archiveFilter)) return false;
  if (agentFilter === ALL) return true;
  if (agentFilter === UNKNOWN_AGENT) return agents.length === 0;
  return agents.includes(agentFilter);
}

export function filterJobs(jobs: JobSummary[], archive: string, agent: string): JobSummary[] {
  if (archive === ALL && agent === ALL) return jobs;
  return jobs.filter((job) => matchesFilters(archivesOf(job), jobAgents(job), archive, agent));
}

export function filterSessions(
  sessions: SessionSummary[],
  archive: string,
  agent: string,
): SessionSummary[] {
  if (archive === ALL && agent === ALL) return sessions;
  return sessions.filter((session) =>
    matchesFilters(archivesOf(session), sessionAgents(session), archive, agent),
  );
}

/** True when two items share an archive (a merged local item counts for each of its archives). */
export function sharesArchive(a: ArchiveTagged, b: ArchiveTagged): boolean {
  const archives = archivesOf(b);
  return archivesOf(a).some((archive) => archives.includes(archive));
}
