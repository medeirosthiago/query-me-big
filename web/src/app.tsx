import type { JSX } from "preact";
import { useEffect, useMemo, useRef, useState } from "preact/hooks";
import { fetchIndex, fetchSessionDetail } from "./api";
import { Banner } from "./components/Banner";
import { Icon } from "./components/Icon";
import { JobDetail } from "./components/JobDetail";
import { SessionDetail } from "./components/SessionDetail";
import { deriveMissingSessions } from "./deriveSessions";
import { fmtBytes, fmtDate } from "./format";
import { mergeIndex } from "./mergeIndex";
import { searchJobs, searchSessions } from "./search";
import type { IndexResponse, JobSummary, SessionSummary } from "./types";

// Lazy incremental rendering: render this many rows initially, then append
// another page each time the list scrolls near the bottom. All data is
// already in memory (fetched in full), so this is purely a render-cost cap.
const ROWS_PER_PAGE = 100;
const SCROLL_LOAD_THRESHOLD_PX = 200;
type Tab = "jobs" | "sessions";
type Selected = { type: "job"; id: string } | { type: "session"; id: string } | null;

type Theme = "auto" | "latte" | "mocha";
const THEME_KEY = "qmb-theme";

function ThemeIcon({ theme }: { theme: Theme }) {
  if (theme === "latte") return <Icon name="sun" />;
  if (theme === "mocha") return <Icon name="moon" />;
  return <Icon name="sun-moon" />;
}

function RefreshIcon({ spinning }: { spinning: boolean }) {
  return <Icon name="refresh" class={spinning ? "refresh-icon--spinning" : ""} />;
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

  // Two-phase load: `localIndex` arrives fast (local-scan time only) and
  // renders immediately; `remoteIndex` arrives later in the background and
  // is merged in client-side once it lands (see `mergeIndex`).
  const [localIndex, setLocalIndex] = useState<IndexResponse | null>(null);
  const [remoteIndex, setRemoteIndex] = useState<IndexResponse | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [remoteLoading, setRemoteLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [dismissedRemoteError, setDismissedRemoteError] = useState<string | null>(null);
  // Full session detail (agents/tasks/cwds) fetched on-demand for remote
  // sessions, keyed by session id, overriding the derived summary once it
  // resolves. See the effect below and `openSession`.
  const [sessionOverrides, setSessionOverrides] = useState<Record<string, SessionSummary>>({});
  // Session ids whose on-demand remote detail fetch failed — the derived
  // placeholder stays displayed, but the detail view notes it's a fallback.
  const [sessionFetchFailed, setSessionFetchFailed] = useState<Record<string, boolean>>({});

  const [tab, setTab] = useState<Tab>("sessions");
  const [query, setQuery] = useState("");
  const [cursor, setCursor] = useState(0);
  const [selected, setSelected] = useState<Selected>(null);
  const [renderCount, setRenderCount] = useState(ROWS_PER_PAGE);

  const searchRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<HTMLDivElement>(null);

  function load(refresh: boolean) {
    if (refresh) setRefreshing(true);
    setRemoteLoading(true);
    setSessionOverrides({});
    setSessionFetchFailed({});

    const localDone = fetchIndex("local", { refresh })
      .then((data) => {
        setLocalIndex(data);
        setLoadError(null);
      })
      .catch((err: Error) => setLoadError(err.message));

    const remoteDone = fetchIndex("remote", { refresh })
      .then((data) => {
        setRemoteIndex(data);
        setDismissedRemoteError(null);
      })
      .catch((err: Error) =>
        setRemoteIndex((prev) => ({
          generated_at: prev?.generated_at ?? new Date().toISOString(),
          jobs: prev?.jobs ?? [],
          sessions: prev?.sessions ?? [],
          remote_error: err.message,
        })),
      )
      .finally(() => setRemoteLoading(false));

    if (refresh) Promise.allSettled([localDone, remoteDone]).finally(() => setRefreshing(false));
  }

  useEffect(() => load(false), []);

  const index = useMemo(() => mergeIndex(localIndex, remoteIndex), [localIndex, remoteIndex]);

  // Jobs can reference a `session_id` absent from the manifest-backed
  // `sessions` array (pre-manifest jobs, a failed manifest write, or an
  // un-exported remote manifest). Synthesize a "derived" session for those
  // so every job's session is reachable and Sessions-tab counts don't
  // silently diverge from the Jobs tab.
  const allSessions = useMemo(() => {
    if (!index) return [];
    const knownIds = new Set(index.sessions.map((s) => s.session_id));
    const derived = deriveMissingSessions(index.jobs, knownIds);
    const base = derived.length > 0 ? [...index.sessions, ...derived] : index.sessions;
    if (Object.keys(sessionOverrides).length === 0) return base;
    return base.map((s) => sessionOverrides[s.session_id] ?? s);
  }, [index, sessionOverrides]);

  const filteredJobs = useMemo(
    () => (index ? searchJobs(index.jobs, query) : []),
    [index, query],
  );
  const filteredSessions = useMemo(
    () => searchSessions(allSessions, query),
    [allSessions, query],
  );
  const totalCount = tab === "jobs" ? filteredJobs.length : filteredSessions.length;
  // `renderCount` caps how many rows are actually mounted; growing it (via
  // scroll, keyboard nav, or opening an off-screen item) reveals more of the
  // already-in-memory `filtered*` list without re-fetching anything.
  const visibleJobs = filteredJobs.slice(0, renderCount);
  const visibleSessions = filteredSessions.slice(0, renderCount);
  const visibleCount = tab === "jobs" ? visibleJobs.length : visibleSessions.length;

  // `max` is passed explicitly (rather than derived from `totalCount`)
  // because callers like `openSession` may run while a *different* tab is
  // still active (e.g. following a session link from a job detail view),
  // when `totalCount` reflects the wrong list.
  function growRenderCount(toAtLeast: number, max: number) {
    setRenderCount((c) => Math.max(c, Math.min(toAtLeast, max)));
  }

  function handleListScroll(e: JSX.TargetedEvent<HTMLDivElement>) {
    const el = e.currentTarget;
    const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
    if (distanceFromBottom < SCROLL_LOAD_THRESHOLD_PX) {
      setRenderCount((c) => Math.min(c + ROWS_PER_PAGE, totalCount));
    }
  }

  // Resync the cursor whenever the query or tab changes: if the currently
  // selected item is still in the (possibly re-filtered) list, keep the
  // cursor on it — growing the render window to reveal it if it's beyond
  // the current page — otherwise fall back to the top with a fresh window.
  // This runs after the click handlers below re-render with the new list,
  // so it correctly picks up cases where a click changes both the query and
  // the tab at once (see `openJobFromSession`).
  useEffect(() => {
    if (tab === "jobs" && selected?.type === "job") {
      const i = filteredJobs.findIndex((j) => j.qmb_job_id === selected.id);
      if (i >= 0) {
        setCursor(i);
        growRenderCount(Math.max(ROWS_PER_PAGE, i + 1), filteredJobs.length);
        return;
      }
    }
    if (tab === "sessions" && selected?.type === "session") {
      const i = filteredSessions.findIndex((s) => s.session_id === selected.id);
      if (i >= 0) {
        setCursor(i);
        growRenderCount(Math.max(ROWS_PER_PAGE, i + 1), filteredSessions.length);
        return;
      }
    }
    setCursor(0);
    setRenderCount(ROWS_PER_PAGE);
    listRef.current?.scrollTo({ top: 0 });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [query, tab]);

  function openJob(id: string) {
    setSelected({ type: "job", id });
    setTab("jobs");
    const i = filteredJobs.findIndex((j) => j.qmb_job_id === id);
    if (i < 0) return;
    growRenderCount(i + 1, filteredJobs.length);
    setCursor(i);
  }

  function openSession(id: string) {
    setSelected({ type: "session", id });
    setTab("sessions");
    const i = filteredSessions.findIndex((s) => s.session_id === id);
    if (i < 0) return;
    growRenderCount(i + 1, filteredSessions.length);
    setCursor(i);
  }

  /**
   * Clicking a job inside a session's job list: scope the Jobs tab to that
   * session via a `session:<id>` search token (instead of showing every
   * job), then select the clicked job. The token is the only filter state —
   * erasing it from the search bar restores the full jobs list.
   */
  function openJobFromSession(id: string, sessionId: string) {
    setSelected({ type: "job", id });
    setTab("jobs");
    setQuery(`session:${sessionId} `);
  }

  // Opening a remote-only session shows the derived summary (already in
  // hand from the merged index) as an instant placeholder, then fetches the
  // full on-demand detail (agents/tasks/cwds) in the background.
  useEffect(() => {
    if (selected?.type !== "session") return;
    const sessionId = selected.id;
    const session = allSessions.find((s) => s.session_id === sessionId);
    if (!session || session.origin !== "remote" || sessionOverrides[sessionId]) return;

    let cancelled = false;
    fetchSessionDetail(sessionId, "remote")
      .then((detail) => {
        if (cancelled) return;
        setSessionOverrides((prev) => ({ ...prev, [sessionId]: detail }));
      })
      .catch(() => {
        // Keep the derived placeholder on failure — non-fatal, but flag it
        // so the detail view can note the summary is index-derived, not a
        // manifest fetch that hasn't happened yet.
        if (!cancelled) setSessionFetchFailed((prev) => ({ ...prev, [sessionId]: true }));
      });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selected, allSessions]);

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
        setCursor((c) => {
          const next = Math.min(c + 1, Math.max(0, totalCount - 1));
          growRenderCount(next + 1, totalCount);
          return next;
        });
        return;
      }
      if (e.key === "ArrowUp") {
        e.preventDefault();
        setCursor((c) => Math.max(c - 1, 0));
        return;
      }
      if (e.key === "Enter") {
        if (tab === "jobs" && filteredJobs[cursor]) {
          e.preventDefault();
          openJob(filteredJobs[cursor].qmb_job_id);
        } else if (tab === "sessions" && filteredSessions[cursor]) {
          e.preventDefault();
          openSession(filteredSessions[cursor].session_id);
        }
      }
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, cursor, filteredJobs, filteredSessions, totalCount]);

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
          <button
            type="button"
            class="icon-btn refresh-btn"
            title="Refresh"
            aria-label="Refresh"
            onClick={() => load(true)}
            disabled={refreshing}
          >
            <RefreshIcon spinning={refreshing} />
          </button>
          <button
            type="button"
            class="icon-btn theme-toggle"
            title={`Theme: ${theme} - click to toggle light/dark, press and hold for auto (system)`}
            aria-label={`Theme: ${theme} - click to toggle light/dark, press and hold for auto (system)`}
            {...holdToggle}
          >
            <ThemeIcon theme={theme} />
          </button>
        </div>

        <div class="tabs">
          <button
            type="button"
            class={`tab ${tab === "sessions" ? "tab--active" : ""}`}
            onClick={() => setTab("sessions")}
          >
            Sessions ({allSessions.length})
          </button>
          <button
            type="button"
            class={`tab ${tab === "jobs" ? "tab--active" : ""}`}
            onClick={() => setTab("jobs")}
          >
            Jobs ({index?.jobs.length ?? 0})
          </button>
        </div>

        {loadError && <div class="pane-error">Failed to load index: {loadError}</div>}

        <div class="list" ref={listRef} onScroll={handleListScroll}>
          {tab === "sessions"
            ? visibleSessions.map((session, i) => (
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
                    <span class="row__badges">
                      <span class={`badge badge--${session.origin}`}>{session.origin}</span>
                      {/* Remote sessions summarized from index.json are the normal
                          case now (one GCS download, no manifest scan) — only flag
                          the rarer case of a session with no manifest anywhere. */}
                      {session.derived && session.origin !== "remote" && (
                        <span class="badge badge--derived">no manifest</span>
                      )}
                    </span>
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
              ))
            : visibleJobs.map((job, i) => (
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
                    <span class="row__badges">
                      <span class={`badge badge--${job.origin}`}>{job.origin}</span>
                    </span>
                  </div>
                  <div class="row__excerpt">{job.query_excerpt || "(empty query)"}</div>
                  <div class="row__meta">
                    <span>{fmtDate(job.created_at)}</span>
                    {job.session_id && <span class="row__session">{job.session_id}</span>}
                    <span>{fmtBytes(job.stats.bytes_processed)}</span>
                  </div>
                </button>
              ))}
          {!index && !loadError && <div class="pane-loading">Loading…</div>}
        </div>

        <div class="list-footer">
          Showing {visibleCount} of {totalCount}
          {remoteLoading && !index?.remote_error && (
            <span class="list-footer__remote-status"> · loading remote…</span>
          )}
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
              <SessionDetail
                session={session}
                jobs={sessionJobs}
                fetchFailed={!!sessionFetchFailed[session.session_id]}
                onSelectJob={(jobId) => openJobFromSession(jobId, session.session_id)}
              />
            ) : (
              <div class="pane-error">Session not found: {selected.id}</div>
            );
          })()}
      </div>
    </div>
  );
}
