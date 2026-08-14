import { useEffect, useState } from "preact/hooks";
import { fetchJobDetail, fetchJobPreview } from "../api";
import { fmtBytes, fmtDate, fmtNumber, fmtSeconds } from "../format";
import type { JobDetail as JobDetailData, Origin, PreviewResponse } from "../types";
import { CopyLine } from "./CopyLine";
import { SqlView } from "./SqlView";

const PAGE_SIZE = 200;

interface Props {
  jobId: string;
  origin: Origin;
  onSelectSession: (sessionId: string) => void;
  onSelectJob: (jobId: string) => void;
}

export function JobDetail({ jobId, origin, onSelectSession, onSelectJob }: Props) {
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
    fetchJobDetail(jobId)
      .then((data) => {
        if (!cancelled) setDetail(data);
      })
      .catch((err: Error) => {
        if (!cancelled) setError(err.message);
      });
    return () => {
      cancelled = true;
    };
  }, [jobId]);

  useEffect(() => {
    let cancelled = false;
    setPreviewError(null);
    fetchJobPreview(jobId, page, PAGE_SIZE)
      .then((data) => {
        if (!cancelled) setPreview(data);
      })
      .catch((err: Error) => {
        if (!cancelled) setPreviewError(err.message);
      });
    return () => {
      cancelled = true;
    };
  }, [jobId, page]);

  if (error) return <div class="pane-error">Failed to load job {jobId}: {error}</div>;
  if (!detail) return <div class="pane-loading">Loading job {jobId}…</div>;

  const columns = preview?.rows.length ? Object.keys(preview.rows[0]) : detail.schema.map((f) => f.name);
  const totalPages = preview ? Math.max(1, Math.ceil(preview.total / preview.page_size)) : 1;

  return (
    <div class="detail job-detail">
      <header class="detail__header">
        <h2>{detail.qmb_job_id}</h2>
        <span class={`badge badge--${origin}`}>{origin}</span>
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
        {detail.agent && (
          <div class="field field--wide">
            <span class="field__label">Agent</span>
            <span class="field__value">
              {detail.agent.name ?? "\u2014"}
              {detail.agent.task ? ` — ${detail.agent.task}` : ""}
              {detail.agent.cwd ? ` (${detail.agent.cwd})` : ""}
            </span>
          </div>
        )}
      </section>

      <section class="detail__section">
        <h3>Reproduce</h3>
        <CopyLine command={`qmb jobs sql ${detail.qmb_job_id}`} />
        <CopyLine command={`qmb jobs open ${detail.qmb_job_id}`} />
        {origin === "remote" && <CopyLine command={`qmb jobs import ${detail.qmb_job_id}`} />}
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
    </div>
  );
}

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "\u2014";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}
