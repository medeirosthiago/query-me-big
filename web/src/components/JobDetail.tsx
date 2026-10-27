import { useEffect, useState } from "preact/hooks";
import { fetchJobDetail, fetchJobPreview } from "../api";
import { LOCAL_ARCHIVE } from "../archives";
import { fmtBytes, fmtDate, fmtNumber, fmtSeconds } from "../format";
import type { ArchiveTagged, JobDetail as JobDetailData, PreviewResponse } from "../types";
import { ArchiveBadges } from "./ArchiveBadges";
import { CopyLine } from "./CopyLine";
import { SqlView } from "./SqlView";

const PAGE_SIZE = 200;

interface Props {
  jobId: string;
  /** Archive to read the job from: `"local"` or a remote archive name. */
  archive: string;
  /** The job's index entry, when listed, for its archive badges. */
  tagged: ArchiveTagged | null;
  primaryArchive: string | null;
  onSelectSession: (sessionId: string) => void;
  onSelectJob: (jobId: string) => void;
}

export function JobDetail({
  jobId,
  archive,
  tagged,
  primaryArchive,
  onSelectSession,
  onSelectJob,
}: Props) {
  const [detail, setDetail] = useState<JobDetailData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(1);
  const [preview, setPreview] = useState<PreviewResponse | null>(null);
  const [previewError, setPreviewError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setDetail(null);
    setError(null);
    setPage(1);
    setPreview(null);
    fetchJobDetail(jobId, archive)
      .then((data) => {
        if (!cancelled) setDetail(data);
      })
      .catch((err: Error) => {
        if (!cancelled) setError(err.message);
      });
    return () => {
      cancelled = true;
    };
  }, [jobId, archive]);

  useEffect(() => {
    let cancelled = false;
    setPreviewError(null);
    fetchJobPreview(jobId, archive, page, PAGE_SIZE)
      .then((data) => {
        if (!cancelled) setPreview(data);
      })
      .catch((err: Error) => {
        if (!cancelled) setPreviewError(err.message);
      });
    return () => {
      cancelled = true;
    };
  }, [jobId, archive, page]);

  if (error) return <div class="pane-error">Failed to load job {jobId}: {error}</div>;
  if (!detail) return <div class="pane-loading">Loading job {jobId}…</div>;

  const columns = preview?.rows.length ? Object.keys(preview.rows[0]) : detail.schema.map((f) => f.name);
  const totalPages = preview ? Math.max(1, Math.ceil(preview.total / preview.page_size)) : 1;
  const agent = detail.agent;
  const agentName = detail.agent_name ?? agent?.name ?? null;
  const user = detail.user ?? agent?.user ?? null;
  const task = detail.task ?? agent?.task ?? null;
  const tags = agent?.tags ?? [];
  const metricRequest = agent?.metadata?.["metric_request"];
  const isRemote = archive !== LOCAL_ARCHIVE;
  // Extra archives are read-only and never the default lookup target.
  const archiveFlag = isRemote && archive !== primaryArchive ? ` --archive ${archive}` : "";

  return (
    <div class="detail job-detail">
      <header class="detail__header">
        <h2>{detail.qmb_job_id}</h2>
        <span class="row__badges">
          <ArchiveBadges item={tagged ?? { archive }} primary={primaryArchive} />
        </span>
      </header>

      <section class="detail__section">
        <h3>
          Preview
          {preview && ` — ${fmtNumber(preview.total)} rows`}
        </h3>
        {previewError && <div class="pane-error">Failed to load preview: {previewError}</div>}
        {preview && (
          <>
            <div class="table-scroll">
              <table class="preview-table">
                <thead>
                  <tr>{columns.map((c) => <th key={c}>{c}</th>)}</tr>
                </thead>
                <tbody>
                  {preview.rows.map((row, i) => (
                    <tr key={i}>
                      {columns.map((c) => <td key={c}>{formatCell(row[c])}</td>)}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {totalPages > 1 && (
              <div class="pager">
                <button type="button" disabled={page <= 1} onClick={() => setPage((p) => p - 1)}>
                  Prev
                </button>
                <span>Page {preview.page} of {totalPages}</span>
                <button type="button" disabled={page >= totalPages} onClick={() => setPage((p) => p + 1)}>
                  Next
                </button>
              </div>
            )}
          </>
        )}
      </section>

      <SqlView sql={detail.query} />

      <section class="detail__grid">
        <div class="field">
          <span class="field__label">Created</span>
          <span class="field__value">{fmtDate(detail.created_at)}</span>
        </div>
        <div class="field">
          <span class="field__label">Engine</span>
          <span class="field__value">
            {detail.engine.name}
            {detail.engine.job_id ? ` · ${detail.engine.job_id}` : ""}
          </span>
        </div>
        <div class="field">
          <span class="field__label">Project / Location</span>
          <span class="field__value">
            {detail.engine.project ?? "\u2014"} / {detail.engine.location ?? "\u2014"}
          </span>
        </div>
        <div class="field">
          <span class="field__label">Source</span>
          <span class="field__value">{detail.source.label}</span>
        </div>
        <div class="field">
          <span class="field__label">Rows / Bytes</span>
          <span class="field__value">
            {fmtNumber(detail.stats.total_rows)} rows · {fmtBytes(detail.stats.bytes_processed)}
          </span>
        </div>
        <div class="field">
          <span class="field__label">Execution time</span>
          <span class="field__value">{fmtSeconds(detail.stats.execution_seconds)}</span>
        </div>
        {detail.session_id && (
          <div class="field">
            <span class="field__label">Session</span>
            <span class="field__value">
              <a href="#" onClick={(e) => { e.preventDefault(); onSelectSession(detail.session_id!); }}>
                {detail.session_id}
              </a>
            </span>
          </div>
        )}
        {detail.parent_job_id && (
          <div class="field">
            <span class="field__label">Parent job</span>
            <span class="field__value">
              <a href="#" onClick={(e) => { e.preventDefault(); onSelectJob(detail.parent_job_id!); }}>
                {detail.parent_job_id}
              </a>
            </span>
          </div>
        )}
        {agentName && (
          <div class="field">
            <span class="field__label">Agent</span>
            <span class="field__value">{agentName}</span>
          </div>
        )}
        {user && (
          <div class="field">
            <span class="field__label">Person</span>
            <span class="field__value">{user}</span>
          </div>
        )}
        {task && (
          <div class="field field--wide">
            <span class="field__label">Question / task</span>
            <span class="field__value">{task}</span>
          </div>
        )}
        {tags.length > 0 && (
          <div class="field">
            <span class="field__label">Tags</span>
            <span class="field__value">{tags.join(", ")}</span>
          </div>
        )}
        {agent?.cwd && (
          <div class="field">
            <span class="field__label">Cwd</span>
            <span class="field__value">{agent.cwd}</span>
          </div>
        )}
        {metricRequest !== undefined && metricRequest !== null && (
          <div class="field field--wide">
            <span class="field__label">Metric request</span>
            <pre class="field__value json-block">{JSON.stringify(metricRequest, null, 2)}</pre>
          </div>
        )}
      </section>

      {detail.schema.length > 0 && (
        <section class="detail__section">
          <h3>Schema</h3>
          <table class="schema-table">
            <thead>
              <tr><th>Name</th><th>Type</th><th>Mode</th></tr>
            </thead>
            <tbody>
              {detail.schema.map((field) => (
                <tr key={field.name}>
                  <td>{field.name}</td>
                  <td>{field.type}</td>
                  <td>{field.mode}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}

      <section class="detail__section">
        <h3>Reproduce</h3>
        <CopyLine command={`qmb jobs sql ${detail.qmb_job_id}${archiveFlag}`} />
        <CopyLine command={`qmb jobs open ${detail.qmb_job_id}${archiveFlag}`} />
        {isRemote && <CopyLine command={`qmb jobs import ${detail.qmb_job_id}${archiveFlag}`} />}
      </section>
    </div>
  );
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "\u2014";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}
