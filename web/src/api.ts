import type {
  AgentContext,
  AgentFields,
  ArchiveStatus,
  ArchiveTagged,
  EngineMetadata,
  IndexResponse,
  JobDetail,
  JobSummary,
  Origin,
  PreviewResponse,
  SearchPeriod,
  SearchResponse,
  SearchTarget,
  SessionSummary,
  SourceMetadata,
} from "./types";

/**
 * Raw `/api/index` job entries come in two shapes (see `src/qmb/web/server.py`
 * and `src/qmb/jobs/remote.py::_index_entry_from_metadata`):
 *
 * - local (and "both", which wins with local fields): the full
 *   `JobRecord.to_metadata()` shape plus `query_excerpt` and `origin`.
 * - remote-only: a flatter shape with `engine` as a plain string and
 *   `source_label` instead of a nested `source` object.
 *
 * Both carry `archive` and the flat `agent_name`/`user`/`task` fields
 * (null on remote index entries written before qmb 0.11.0).
 */
interface RawLocalJobEntry extends ArchiveTagged, AgentFields {
  qmb_job_id: string;
  created_at: string;
  session_id: string | null;
  parent_job_id: string | null;
  agent: AgentContext | null;
  source: SourceMetadata;
  engine: EngineMetadata;
  stats: { total_rows: number; bytes_processed: number; execution_seconds: number };
  query_excerpt: string;
  origin: Origin;
}

interface RawRemoteJobEntry extends ArchiveTagged, AgentFields {
  qmb_job_id: string;
  session_id: string | null;
  created_at: string;
  engine: string;
  source_label: string;
  total_rows: number;
  bytes_processed: number;
  query_excerpt: string;
  origin: Origin;
}

type RawJobEntry = RawLocalJobEntry | RawRemoteJobEntry;

function isRemoteShape(raw: RawJobEntry): raw is RawRemoteJobEntry {
  return typeof (raw as RawRemoteJobEntry).engine === "string";
}

/** Normalize a raw `/api/index` job entry into the uniform `JobSummary` shape. */
export function normalizeJob(raw: RawJobEntry): JobSummary {
  if (isRemoteShape(raw)) {
    return {
      qmb_job_id: raw.qmb_job_id,
      created_at: raw.created_at,
      session_id: raw.session_id,
      parent_job_id: null,
      agent: null,
      source: {
        label: raw.source_label,
        input_mode: null,
        file_path: null,
        model_name: null,
        manifest_path: null,
        resolver: null,
        matched_node_id: null,
      },
      engine: { name: raw.engine, job_id: null, project: null, location: null },
      stats: {
        total_rows: raw.total_rows,
        bytes_processed: raw.bytes_processed,
        execution_seconds: null,
      },
      query_excerpt: raw.query_excerpt,
      origin: raw.origin,
      archive: raw.archive,
      also_in: raw.also_in,
      agent_name: raw.agent_name ?? null,
      user: raw.user ?? null,
      task: raw.task ?? null,
    };
  }
  return {
    qmb_job_id: raw.qmb_job_id,
    created_at: raw.created_at,
    session_id: raw.session_id,
    parent_job_id: raw.parent_job_id,
    agent: raw.agent,
    source: raw.source,
    engine: raw.engine,
    stats: raw.stats,
    query_excerpt: raw.query_excerpt,
    origin: raw.origin,
    archive: raw.archive,
    also_in: raw.also_in,
    agent_name: raw.agent_name ?? raw.agent?.name ?? null,
    user: raw.user ?? raw.agent?.user ?? null,
    task: raw.task ?? raw.agent?.task ?? null,
  };
}

interface RawIndexResponse extends Omit<IndexResponse, "jobs"> {
  jobs: RawJobEntry[];
}

async function getJson<T>(url: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(url, { signal });
  if (!response.ok) {
    const body = await response.json().catch(() => null);
    const message = (body && (body as { error?: string }).error) || response.statusText;
    throw new Error(`${response.status}: ${message}`);
  }
  return response.json() as Promise<T>;
}

export type IndexScope = "local" | "remote";

/**
 * Fetch the local half of the index (see `src/qmb/web/server.py::JobIndexCache`).
 * It resolves in local-scan time (no GCS calls), so the UI renders it
 * immediately; each remote archive is fetched separately via
 * `fetchArchiveIndex` and merged in client-side (see `mergeIndex.ts`).
 */
export async function fetchIndex(
  scope: IndexScope,
  options: { refresh?: boolean } = {},
): Promise<IndexResponse> {
  const params = new URLSearchParams({ scope });
  if (options.refresh) params.set("refresh", "1");
  return normalizeIndex(await getJson<RawIndexResponse>(`/api/index?${params}`));
}

/** The configured remote archives, primary first (no GCS calls). */
export async function fetchArchives(): Promise<ArchiveStatus[]> {
  const payload = await getJson<{ archives: ArchiveStatus[] }>("/api/archives");
  return payload.archives;
}

/**
 * Fetch one remote archive's half of the index — a single `index.json`
 * download, cached server-side per archive, so a slow or failing archive
 * never holds up the others.
 */
export async function fetchArchiveIndex(
  archive: string,
  options: { refresh?: boolean } = {},
): Promise<IndexResponse> {
  const params = new URLSearchParams({ archive });
  if (options.refresh) params.set("refresh", "1");
  return normalizeIndex(await getJson<RawIndexResponse>(`/api/index?${params}`));
}

function normalizeIndex(raw: RawIndexResponse): IndexResponse {
  return { ...raw, jobs: raw.jobs.map(normalizeJob) };
}

/**
 * On-demand full session detail (agents/tasks/cwds) for a remote session
 * that was only available as a derived summary from `index.json` entries.
 * Cached server-side per archive and session id.
 */
export function fetchSessionDetail(sessionId: string, archive: string): Promise<SessionSummary> {
  const params = new URLSearchParams({ archive });
  return getJson<SessionSummary>(`/api/sessions/${encodeURIComponent(sessionId)}?${params}`);
}

export function fetchJobDetail(jobId: string, archive: string): Promise<JobDetail> {
  const params = new URLSearchParams({ archive });
  return getJson<JobDetail>(`/api/jobs/${encodeURIComponent(jobId)}?${params}`);
}

export function fetchJobPreview(
  jobId: string,
  archive: string,
  page: number,
  pageSize = 200,
): Promise<PreviewResponse> {
  const params = new URLSearchParams({
    archive,
    page: String(page),
    page_size: String(pageSize),
  });
  return getJson<PreviewResponse>(`/api/jobs/${encodeURIComponent(jobId)}/preview?${params}`);
}

export function fetchArchiveSearch(
  query: string,
  period: SearchPeriod,
  target: SearchTarget,
  options: { sessionId?: string | null; signal?: AbortSignal } = {},
): Promise<SearchResponse> {
  const params = new URLSearchParams({ q: query, period, target });
  if (options.sessionId) params.set("session_id", options.sessionId);
  return getJson<SearchResponse>(`/api/search?${params}`, options.signal);
}
