import { useEffect, useMemo, useRef, useState } from "preact/hooks";
import { fetchIndex } from "./api";
import { Banner } from "./components/Banner";
import { JobDetail } from "./components/JobDetail";
import { SessionDetail } from "./components/SessionDetail";
import { deriveMissingSessions } from "./deriveSessions";
import { fmtBytes, fmtDate } from "./format";
import { searchJobs, searchSessions } from "./search";
import type { IndexResponse, JobSummary } from "./types";

const MAX_RENDERED_ROWS = 200;
type Tab = "jobs" | "sessions";
type Selected = { type: "job"; id: string } | { type: "session"; id: string } | null;

type Theme = "auto" | "latte" | "mocha";
const THEME_KEY = "qmb-theme";
const THEME_LABEL: Record<Theme, string> = { auto: "Auto", latte: "Latte", mocha: "Mocha" };

function ThemeIcon({ theme }: { theme: Theme }) {
  const common = {
    width: 14,
    height: 14,
    viewBox: "0 0 14 14",
    fill: "none",
    stroke: "currentColor",
    "stroke-width": 1.3,
    "stroke-linecap": "round" as const,
    "stroke-linejoin": "round" as const,
    "aria-hidden": true,
  };
  if (theme === "latte") {
    // sun
    return (
      <svg {...common}>
        <circle cx="7" cy="7" r="2.6" />
        <path d="M7 0.8v1.6M7 11.6v1.6M0.8 7h1.6M11.6 7h1.6M2.5 2.5l1.1 1.1M10.4 10.4l1.1 1.1M11.5 2.5l-1.1 1.1M3.6 10.4l-1.1 1.1" />
      </svg>
    );
  }
  if (theme === "mocha") {
    // moon
    return (
      <svg {...common}>
        <path d="M9.6 1.6a5.6 5.6 0 1 0 2.8 8.7A5.9 5.9 0 0 1 9.6 1.6Z" />
      </svg>
    );
  }
  // auto: half-filled circle
  return (
    <svg {...common}>
      <circle cx="7" cy="7" r="5.4" />
      <path d="M7 1.6a5.4 5.4 0 0 1 0 10.8Z" fill="currentColor" stroke="none" />
    </svg>
  );
}

const HOLD_MS = 600;

function systemPrefersDark() {
  return window.matchMedia?.("(prefers-color-scheme: dark)").matches ?? false;
}

function useTheme() {
  const [theme, setTheme] = useState<Theme>(() => {
    const stored = localStorage.getItem(THEME_KEY);
    return stored === "latte" || stored === "mocha" ? stored : "auto";
  });

  useEffect(() => {
    if (theme === "auto") {
      document.documentElement.removeAttribute("data-theme");
      localStorage.removeItem(THEME_KEY);
    } else {
      document.documentElement.setAttribute("data-theme", theme);
      localStorage.setItem(THEME_KEY, theme);
    }
  }, [theme]);

  // A click always toggles between the two explicit themes. From "auto" it
  // picks the opposite of whatever is currently in effect, so the click
  // always visibly changes the theme instead of just "locking in" the
  // current appearance.
  function toggle() {
    setTheme((t) => {
      if (t === "auto") return systemPrefersDark() ? "latte" : "mocha";
      return t === "latte" ? "mocha" : "latte";
    });
  }

  function setAuto() {
    setTheme("auto");
  }

  return { theme, toggle, setAuto };
}

function useHoldToggle(onClick: () => void, onHold: () => void, holdMs = HOLD_MS) {
  const timerRef = useRef<number | null>(null);
  const heldRef = useRef(false);

  function clearTimer() {
    if (timerRef.current !== null) {
      window.clearTimeout(timerRef.current);
      timerRef.current = null;
    }
  }

  function onPointerDown() {
    heldRef.current = false;
    clearTimer();
    timerRef.current = window.setTimeout(() => {
      heldRef.current = true;
      timerRef.current = null;
      onHold();
    }, holdMs);
  }

  function onPointerUp() {
    clearTimer();
  }

  function onPointerLeave() {
    clearTimer();
  }

  function onClickHandler(e: MouseEvent) {
    if (heldRef.current) {
      // Suppress the synthetic click that follows a completed hold so it
      // doesn't immediately toggle away from auto.
      e.preventDefault();
      e.stopPropagation();
      heldRef.current = false;
      return;
    }
    onClick();
  }

  return {
    onPointerDown,
    onPointerUp,
    onPointerLeave,
    onPointerCancel: onPointerUp,
    onClick: onClickHandler,
  };
}

export function App() {
  const { theme, toggle: toggleTheme, setAuto } = useTheme();
  const holdToggle = useHoldToggle(toggleTheme, setAuto);
  const [index, setIndex] = useState<IndexResponse | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [dismissedRemoteError, setDismissedRemoteError] = useState<string | null>(null);

  const [tab, setTab] = useState<Tab>("jobs");
  const [query, setQuery] = useState("");
  const [cursor, setCursor] = useState(0);
  const [selected, setSelected] = useState<Selected>(null);

  const searchRef = useRef<HTMLInputElement>(null);

  function load(refresh: boolean) {
    setRefreshing(refresh);
    fetchIndex({ refresh })
      .then((data) => {
        setIndex(data);
        setLoadError(null);
        setDismissedRemoteError(null);
      })
      .catch((err: Error) => setLoadError(err.message))
      .finally(() => setRefreshing(false));
  }

  useEffect(() => load(false), []);

  // Jobs can reference a `session_id` absent from the manifest-backed
  // `sessions` array (pre-manifest jobs, a failed manifest write, or an
  // un-exported remote manifest). Synthesize a "derived" session for those
  // so every job's session is reachable and Sessions-tab counts don't
  // silently diverge from the Jobs tab.
  const allSessions = useMemo(() => {
    if (!index) return [];
    const knownIds = new Set(index.sessions.map((s) => s.session_id));
    const derived = deriveMissingSessions(index.jobs, knownIds);
    return derived.length > 0 ? [...index.sessions, ...derived] : index.sessions;
  }, [index]);

  const filteredJobs = useMemo(
    () => (index ? searchJobs(index.jobs, query) : []),
    [index, query],
  );
  const filteredSessions = useMemo(
    () => searchSessions(allSessions, query),
    [allSessions, query],
  );
  const visibleJobs = filteredJobs.slice(0, MAX_RENDERED_ROWS);
  const visibleSessions = filteredSessions.slice(0, MAX_RENDERED_ROWS);
  const visibleCount = tab === "jobs" ? visibleJobs.length : visibleSessions.length;
  const totalCount = tab === "jobs" ? filteredJobs.length : filteredSessions.length;

  useEffect(() => setCursor(0), [query, tab]);

  function openJob(id: string) {
    setSelected({ type: "job", id });
    setTab("jobs");
    const i = visibleJobs.findIndex((j) => j.qmb_job_id === id);
    if (i >= 0) setCursor(i);
  }

  function openSession(id: string) {
    setSelected({ type: "session", id });
    setTab("sessions");
    const i = visibleSessions.findIndex((s) => s.session_id === id);
    if (i >= 0) setCursor(i);
  }

  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      const isSearchFocused = document.activeElement === searchRef.current;

      if (e.key === "/" && !isSearchFocused) {
        e.preventDefault();
        searchRef.current?.focus();
        return;
      }
      if (e.key === "Escape") {
        setQuery("");
        return;
      }
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setCursor((c) => Math.min(c + 1, Math.max(0, visibleCount - 1)));
        return;
      }
      if (e.key === "ArrowUp") {
        e.preventDefault();
        setCursor((c) => Math.max(c - 1, 0));
        return;
      }
      if (e.key === "Enter") {
        if (tab === "jobs" && visibleJobs[cursor]) {
          e.preventDefault();
          openJob(visibleJobs[cursor].qmb_job_id);
        } else if (tab === "sessions" && visibleSessions[cursor]) {
          e.preventDefault();
          openSession(visibleSessions[cursor].session_id);
        }
      }
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, cursor, visibleJobs, visibleSessions]);

  const sessionJobs: JobSummary[] = useMemo(() => {
    if (!index || selected?.type !== "session") return [];
    const session = allSessions.find((s) => s.session_id === selected.id);
    if (!session) return [];
    const ids = new Set(session.jobs);
    return index.jobs.filter((j) => ids.has(j.qmb_job_id));
  }, [index, allSessions, selected]);

  const selectedJobOrigin = useMemo(() => {
    if (!index || selected?.type !== "job") return "local" as const;
    return index.jobs.find((j) => j.qmb_job_id === selected.id)?.origin ?? "local";
  }, [index, selected]);

  const remoteErrorToShow =
    index?.remote_error && index.remote_error !== dismissedRemoteError ? index.remote_error : null;

  return (
    <div class="app">
      <div class="left-pane">
        <div class="toolbar">
          <input
            ref={searchRef}
            class="search-input"
            type="text"
            placeholder="Search jobs & sessions…  (/)"
            value={query}
            onInput={(e) => setQuery((e.target as HTMLInputElement).value)}
          />
          <button type="button" class="refresh-btn" onClick={() => load(true)} disabled={refreshing}>
            {refreshing ? "…" : "Refresh"}
          </button>
          <button
            type="button"
            class="theme-toggle"
            title={`Theme: ${theme} - click to toggle light/dark, press and hold for auto (system)`}
            {...holdToggle}
          >
            <ThemeIcon theme={theme} />
            <span>{THEME_LABEL[theme]}</span>
          </button>
        </div>

        <div class="tabs">
          <button
            type="button"
            class={`tab ${tab === "jobs" ? "tab--active" : ""}`}
            onClick={() => setTab("jobs")}
          >
            Jobs ({index?.jobs.length ?? 0})
          </button>
          <button
            type="button"
            class={`tab ${tab === "sessions" ? "tab--active" : ""}`}
            onClick={() => setTab("sessions")}
          >
            Sessions ({allSessions.length})
          </button>
        </div>

        {loadError && <div class="pane-error">Failed to load index: {loadError}</div>}

        <div class="list">
          {tab === "jobs"
            ? visibleJobs.map((job, i) => (
                <button
                  type="button"
                  key={job.qmb_job_id}
                  class={`row ${i === cursor ? "row--cursor" : ""} ${
                    selected?.type === "job" && selected.id === job.qmb_job_id ? "row--selected" : ""
                  }`}
                  onClick={() => openJob(job.qmb_job_id)}
                >
                  <div class="row__top">
                    <span class="row__id">{job.qmb_job_id}</span>
                    <span class={`badge badge--${job.origin}`}>{job.origin}</span>
                  </div>
                  <div class="row__excerpt">{job.query_excerpt || "(empty query)"}</div>
                  <div class="row__meta">
                    <span>{fmtDate(job.created_at)}</span>
                    {job.session_id && <span class="row__session">{job.session_id}</span>}
                    <span>{fmtBytes(job.stats.bytes_processed)}</span>
                  </div>
                </button>
              ))
            : visibleSessions.map((session, i) => (
                <button
                  type="button"
                  key={session.session_id}
                  class={`row ${i === cursor ? "row--cursor" : ""} ${
                    selected?.type === "session" && selected.id === session.session_id
                      ? "row--selected"
                      : ""
                  }`}
                  onClick={() => openSession(session.session_id)}
                >
                  <div class="row__top">
                    <span class="row__id">{session.session_id}</span>
                    <span class={`badge badge--${session.origin}`}>{session.origin}</span>
                    {session.derived && <span class="badge badge--derived">derived</span>}
                  </div>
                  <div class="row__excerpt">
                    {session.count} jobs · {fmtDate(session.first)} → {fmtDate(session.latest)}
                  </div>
                  {session.agents.length > 0 && (
                    <div class="row__meta">
                      <span>{session.agents.join(", ")}</span>
                    </div>
                  )}
                </button>
              ))}
          {!index && !loadError && <div class="pane-loading">Loading…</div>}
        </div>

        <div class="list-footer">
          Showing {visibleCount} of {totalCount}
        </div>
      </div>

      <div class="right-pane">
        {remoteErrorToShow && (
          <Banner
            message={`Remote archive error: ${remoteErrorToShow}`}
            onDismiss={() => setDismissedRemoteError(remoteErrorToShow)}
          />
        )}
        {!selected && <div class="empty-state">Select a job or session to see details.</div>}
        {selected?.type === "job" && (
          <JobDetail
            jobId={selected.id}
            origin={selectedJobOrigin}
            onSelectSession={openSession}
            onSelectJob={openJob}
          />
        )}
        {selected?.type === "session" &&
          index &&
          (() => {
            const session = allSessions.find((s) => s.session_id === selected.id);
            return session ? (
              <SessionDetail session={session} jobs={sessionJobs} onSelectJob={openJob} />
            ) : (
              <div class="pane-error">Session not found: {selected.id}</div>
            );
          })()}
      </div>
    </div>
  );
}
