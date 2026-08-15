export type Origin = "local" | "remote" | "both";

export interface AgentContext {
  name: string | null;
  session_id: string | null;
  conversation_id: string | null;
  run_id: string | null;
  turn_id: string | null;
  task: string | null;
  cwd: string | null;
  repo_root: string | null;
  git_branch: string | null;
  git_sha: string | null;
  git_dirty: boolean | null;
  user: string | null;
  host: string | null;
  tags: string[];
  metadata: Record<string, unknown>;
}

export interface SourceMetadata {
  label: string;
  input_mode: string | null;
  file_path: string | null;
  model_name: string | null;
  manifest_path: string | null;
  resolver: string | null;
  matched_node_id: string | null;
}

export interface EngineMetadata {
  name: string;
  job_id: string | null;
  project: string | null;
  location: string | null;
}

export interface JobStats {
  total_rows: number;
  bytes_processed: number;
  execution_seconds: number | null;
}

export interface SchemaField {
  name: string;
  type: string;
  mode: string;
}

/** Normalized job index entry, after adapting both local and remote raw shapes. */
export interface JobSummary {
  qmb_job_id: string;
  created_at: string;
  session_id: string | null;
  parent_job_id: string | null;
  agent: AgentContext | null;
  source: SourceMetadata;
  engine: EngineMetadata;
  stats: JobStats;
  query_excerpt: string;
  origin: Origin;
}

export interface SessionSummary {
  session_id: string;
  jobs: string[];
  count: number | null;
  first: string | null;
  latest: string | null;
  bytes_processed: number | null;
  agents: string[];
  tasks: string[];
  cwds: string[];
  updated_at: string | null;
  origin: Origin;
  /** True when synthesized client-side from jobs because no manifest entry exists. */
  derived?: boolean;
  /**
   * True for a minimal stub built from a cheap sessions/ prefix listing when
   * a remote session manifest exists but `index.json` doesn't know about it
   * yet (a stale index — see `qmb jobs reindex --remote`). Counts/dates are
   * unknown until the on-demand session-detail fetch resolves the manifest.
   */
  unindexed?: boolean;
}

export interface IndexResponse {
  generated_at: string;
  jobs: JobSummary[];
  sessions: SessionSummary[];
  remote_error?: string;
  /** Count of unindexed remote sessions detected this build (see `unindexed`). */
  index_stale?: number;
}

export interface JobDetail {
  version?: number;
  qmb_job_id: string;
  created_at: string;
  session_id: string | null;
  parent_job_id: string | null;
  agent: AgentContext | null;
  source: SourceMetadata;
  engine: EngineMetadata;
  stats: JobStats;
  artifacts?: Record<string, string | null>;
  query: string;
  schema: SchemaField[];
  origin?: Origin;
}

export interface PreviewResponse {
  rows: Record<string, unknown>[];
  total: number;
  page: number;
  page_size: number;
}
