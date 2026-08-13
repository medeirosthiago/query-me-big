import type {
  AgentContext,
  EngineMetadata,
  IndexResponse,
  JobDetail,
  JobSummary,
  Origin,
  PreviewResponse,
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
 */
interface RawLocalJobEntry {
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

interface RawRemoteJobEntry {
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
  };
}

interface RawIndexResponse extends Omit<IndexResponse, "jobs"> {
  jobs: RawJobEntry[];
}

async function getJson<T>(url: string): Promise<T> {
  const response = await fetch(url);
  if (!response.ok) {
    const body = await response.json().catch(() => null);
    const message = (body && (body as { error?: string }).error) || response.statusText;
    throw new Error(`${response.status}: ${message}`);
  }
  return response.json() as Promise<T>;
}

export async function fetchIndex(options: { refresh?: boolean } = {}): Promise<IndexResponse> {
  const url = options.refresh ? "/api/index?refresh=1" : "/api/index";
  const raw = await getJson<RawIndexResponse>(url);
  return { ...raw, jobs: raw.jobs.map(normalizeJob) };
}

export function fetchJobDetail(jobId: string): Promise<JobDetail> {
  return getJson<JobDetail>(`/api/jobs/${encodeURIComponent(jobId)}`);
}

export function fetchJobPreview(
  jobId: string,
  page: number,
  pageSize = 200,
): Promise<PreviewResponse> {
  return getJson<PreviewResponse>(
    `/api/jobs/${encodeURIComponent(jobId)}/preview?page=${page}&page_size=${pageSize}`,
  );
}
