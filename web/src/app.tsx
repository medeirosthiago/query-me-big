import type { JSX } from "preact";
import { useEffect, useMemo, useRef, useState } from "preact/hooks";
import {
  fetchArchiveIndex,
  fetchArchives,
  fetchArchiveSearch,
  fetchIndex,
  fetchSessionDetail,
} from "./api";
import {
  ALL,
  LOCAL_ARCHIVE,
  UNKNOWN_AGENT,
  agentLabel,
  agentOptions,
  archivesOf,
  filterJobs,
  filterSessions,
  itemKey,
  sharesArchive,
} from "./archives";
import { ArchiveBadges } from "./components/ArchiveBadges";
import { Banner } from "./components/Banner";
import { Icon } from "./components/Icon";
import { JobDetail } from "./components/JobDetail";
import { SessionDetail } from "./components/SessionDetail";
import { deriveMissingSessions, knownSessionKeys } from "./deriveSessions";
import { fmtBytes, fmtDate } from "./format";
import { mergeIndex } from "./mergeIndex";
import { extractSessionToken, searchJobs, searchSessions } from "./search";
import type {
  ArchiveStatus,
  IndexResponse,
  JobSummary,
  SearchMatch,
  SearchPeriod,
  SearchTarget,
  SessionSummary,
} from "./types";

// Lazy incremental rendering: render this many rows initially, then append
// another page each time the list scrolls near the bottom. All data is
// already in memory (fetched in full), so this is purely a render-cost cap.
const ROWS_PER_PAGE = 100;
const SCROLL_LOAD_THRESHOLD_PX = 200;
const SEARCH_DEBOUNCE_MS = 200;
type Tab = "jobs" | "sessions";
type Selected = { type: "job" | "session"; archive: string; id: string } | null;
type ArchiveSearchState = {
  query: string;
  matches: SearchMatch[];
  status: string;
};

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

type Warning = { message: string; tone: "error" | "warning" };

/** Stand-in payload for an archive whose index request failed outright. */
function failedArchivePayload(
  archive: ArchiveStatus,
  message: string,
  previous: IndexResponse | undefined,
): IndexResponse {
  return {
    generated_at: previous?.generated_at ?? new Date().toISOString(),
    jobs: previous?.jobs ?? [],
    sessions: previous?.sessions ?? [],
    archives: [{ ...archive, status: "error", error: message }],
  };
}

/** `reindex --remote` targets the primary by default; extra archives need their URI. */
function reindexCommand(archive: string, archives: ArchiveStatus[]): string {
  const status = archives.find((a) => a.name === archive);
  if (!status || status.primary) return "qmb jobs reindex --remote";
  return `qmb jobs reindex --remote --destination ${status.uri}`;
}

/**
 * The item with `id` in `archives[0]`, else one living in any of `archives`
 * (a local item merged with an archive copy is listed once, under `local`).
 */
function findItem<T extends { archive: string; also_in?: string[] }>(
  items: T[],
  archives: string[],
  id: string,
  idOf: (item: T) => string,
): T | undefined {
  return (
    items.find((item) => item.archive === archives[0] && idOf(item) === id) ??
    items.find(
      (item) => idOf(item) === id && archivesOf(item).some((a) => archives.includes(a)),
    )
  );
}

function isSelected(selected: Selected, type: "job" | "session", archive: string, id: string) {
  return selected?.type === type && selected.archive === archive && selected.id === id;
}

const jobId = (job: JobSummary) => job.qmb_job_id;
const sessionId = (session: SessionSummary) => session.session_id;

export function App() {
  const { theme, toggle: toggleTheme, setAuto } = useTheme();
  const holdToggle = useHoldToggle(toggleTheme, setAuto);

  // Two-phase load: `localIndex` arrives fast (local-scan time only) and
  // renders immediately; each remote archive's half arrives later in the
  // background and is merged in client-side once it lands (see `mergeIndex`).
  const [localIndex, setLocalIndex] = useState<IndexResponse | null>(null);
  const [archiveList, setArchiveList] = useState<ArchiveStatus[]>([]);
  const [remoteIndexes, setRemoteIndexes] = useState<Record<string, IndexResponse>>({});
  const [archivesError, setArchivesError] = useState<string | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [remoteLoading, setRemoteLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  // Dismissed warning messages; a different message (e.g. a new error) shows again.
  const [dismissed, setDismissed] = useState<ReadonlySet<string>>(new Set());
  // Full session detail (agents/tasks/cwds) fetched on-demand for remote
  // sessions, keyed by `itemKey(archive, session_id)`, overriding the derived
  // summary once it resolves. See the effect below and `openSession`.
  const [sessionOverrides, setSessionOverrides] = useState<Record<string, SessionSummary>>({});
  // Sessions whose on-demand remote detail fetch failed — the derived
  // placeholder stays displayed, but the detail view notes it's a fallback.
  const [sessionFetchFailed, setSessionFetchFailed] = useState<Record<string, boolean>>({});

  const [tab, setTab] = useState<Tab>("sessions");
  const [query, setQuery] = useState("");
  const [archiveFilter, setArchiveFilter] = useState(ALL);
  const [agentFilter, setAgentFilter] = useState(ALL);
  const [archiveSearch, setArchiveSearch] = useState<ArchiveSearchState | null>(null);
  const [cursor, setCursor] = useState(0);
  const [selected, setSelected] = useState<Selected>(null);
  const [renderCount, setRenderCount] = useState(ROWS_PER_PAGE);

  const searchRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<HTMLDivElement>(null);

  function load(refresh: boolean) {
    if (refresh) setRefreshing(true);
    setRemoteLoading(true);
    setDismissed(new Set());
    setSessionOverrides({});
    setSessionFetchFailed({});

    const localDone = fetchIndex("local", { refresh })
      .then((data) => {
        setLocalIndex(data);
        setLoadError(null);
      })
      .catch((err: Error) => setLoadError(err.message));

    // Each archive loads (and fails) on its own, so a slow bucket never
    // holds up the others.
    const remoteDone = fetchArchives()
      .then((archives) => {
        setArchiveList(archives);
        setArchivesError(null);
        return Promise.allSettled(
          archives.map((archive) =>
            fetchArchiveIndex(archive.name, { refresh })
              .then((data) => setRemoteIndexes((prev) => ({ ...prev, [archive.name]: data })))
              .catch((err: Error) =>
                setRemoteIndexes((prev) => ({
                  ...prev,
                  [archive.name]: failedArchivePayload(archive, err.message, prev[archive.name]),
                })),
              ),
          ),
        );
      })
      .catch((err: Error) => setArchivesError(err.message))
      .finally(() => setRemoteLoading(false));

    if (refresh) Promise.allSettled([localDone, remoteDone]).finally(() => setRefreshing(false));
  }

  useEffect(() => load(false), []);

  const primaryArchive = archiveList.find((archive) => archive.primary)?.name ?? null;

  const index = useMemo(
    () =>
      mergeIndex(
        localIndex,
        archiveList.flatMap((archive) => remoteIndexes[archive.name] ?? []),
      ),
    [localIndex, archiveList, remoteIndexes],
  );

  // Jobs can reference a `session_id` absent from the manifest-backed
  // `sessions` array (pre-manifest jobs, a failed manifest write, or an
  // un-exported remote manifest). Synthesize a "derived" session for those
  // so every job's session is reachable and Sessions-tab counts don't
  // silently diverge from the Jobs tab.
  const allSessions = useMemo(() => {
    if (!index) return [];
    const derived = deriveMissingSessions(index.jobs, knownSessionKeys(index.sessions));
    const base = derived.length > 0 ? [...index.sessions, ...derived] : index.sessions;
    if (Object.keys(sessionOverrides).length === 0) return base;
    return base.map((s) => sessionOverrides[itemKey(s.archive, s.session_id)] ?? s);
  }, [index, sessionOverrides]);

  const scopedJobs = useMemo(
    () => (index ? filterJobs(index.jobs, archiveFilter, agentFilter) : []),
    [index, archiveFilter, agentFilter],
  );
  const scopedSessions = useMemo(
    () => filterSessions(allSessions, archiveFilter, agentFilter),
    [allSessions, archiveFilter, agentFilter],
  );
  const agentChoices = useMemo(
    () => (index ? agentOptions(index.jobs, allSessions) : []),
    [index, allSessions],
  );

  const searchText = extractSessionToken(query).rest.trim();

  useEffect(() => {
    const { sessionId, rest } = extractSessionToken(query);
    const text = rest.trim();
    if (!text || !localIndex) {
      setArchiveSearch(null);
      return;
    }

    const controller = new AbortController();
    const stages: [SearchPeriod, SearchTarget, string][] = [
      ["recent", "sql", "searching recent SQL…"],
      ["recent", "preview", "searching recent previews…"],
      ["older", "sql", "searching older SQL…"],
      ["older", "preview", "searching older previews…"],
    ];
    setArchiveSearch({ query, matches: [], status: "searching recent SQL…" });

    const timer = window.setTimeout(async () => {
      const found = new Map<string, SearchMatch>();
      try {
        for (const [period, target, status] of stages) {
          setArchiveSearch((current) =>
            current?.query === query ? { ...current, status } : current,
          );
          const result = await fetchArchiveSearch(text, period, target, {
            sessionId,
            signal: controller.signal,
          });
          for (const match of result.matches) {
            found.set(itemKey(match.archive, match.qmb_job_id), match);
          }
          setArchiveSearch((current) =>
            current?.query === query
              ? { query, matches: [...found.values()], status }
              : current,
          );
        }
        setArchiveSearch((current) =>
          current?.query === query ? { ...current, status: "search complete" } : current,
        );
      } catch (error) {
        if (error instanceof DOMException && error.name === "AbortError") return;
        const message = error instanceof Error ? error.message : String(error);
        setArchiveSearch((current) =>
          current?.query === query ? { ...current, status: `search failed: ${message}` } : current,
        );
      }
    }, SEARCH_DEBOUNCE_MS);

    return () => {
      window.clearTimeout(timer);
      controller.abort();
    };
  }, [query, localIndex]);

  const filteredJobs = useMemo(() => {
    if (!index) return [];
    if (!searchText) return searchJobs(scopedJobs, query);
    if (archiveSearch?.query !== query) return [];
    const jobsByKey = new Map<string, JobSummary>();
    for (const job of scopedJobs) {
      for (const archive of archivesOf(job)) jobsByKey.set(itemKey(archive, job.qmb_job_id), job);
    }
    const seen = new Set<JobSummary>();
    return archiveSearch.matches.flatMap((match) => {
      const job = jobsByKey.get(itemKey(match.archive, match.qmb_job_id));
      if (!job || seen.has(job)) return [];
      seen.add(job);
      return [job];
    });
  }, [index, scopedJobs, query, searchText, archiveSearch]);
  const filteredSessions = useMemo(
    () => searchSessions(scopedSessions, query),
    [scopedSessions, query],
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

  // Resync the cursor whenever the query, filters or tab change: if the
  // currently selected item is still in the (possibly re-filtered) list,
  // keep the cursor on it — growing the render window to reveal it if it's
  // beyond the current page — otherwise fall back to the top with a fresh
  // window. This runs after the click handlers below re-render with the new
  // list, so it correctly picks up cases where a click changes both the
  // query and the tab at once (see `openJobFromSession`).
  useEffect(() => {
    if (tab === "jobs" && selected?.type === "job") {
      const i = filteredJobs.findIndex((j) => isSelected(selected, "job", j.archive, j.qmb_job_id));
      if (i >= 0) {
        setCursor(i);
        growRenderCount(Math.max(ROWS_PER_PAGE, i + 1), filteredJobs.length);
        return;
      }
    }
    if (tab === "sessions" && selected?.type === "session") {
      const i = filteredSessions.findIndex((s) =>
        isSelected(selected, "session", s.archive, s.session_id),
      );
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
  }, [query, tab, archiveFilter, agentFilter]);

  function openJob(archive: string, id: string) {
    setSelected({ type: "job", archive, id });
    setTab("jobs");
    const i = filteredJobs.findIndex((j) => j.archive === archive && j.qmb_job_id === id);
    if (i < 0) return;
    growRenderCount(i + 1, filteredJobs.length);
    setCursor(i);
  }

  function openSession(archive: string, id: string) {
    setSelected({ type: "session", archive, id });
    setTab("sessions");
    const i = filteredSessions.findIndex((s) => s.archive === archive && s.session_id === id);
    if (i < 0) return;
    growRenderCount(i + 1, filteredSessions.length);
    setCursor(i);
  }

  /** Follow a session/parent-job link from a job living in `archives`. */
  function openLinkedSession(archives: string[], id: string) {
    const session = findItem(allSessions, archives, id, sessionId);
    openSession(session?.archive ?? archives[0], id);
  }

  function openLinkedJob(archives: string[], id: string) {
    const job = index ? findItem(index.jobs, archives, id, jobId) : undefined;
    openJob(job?.archive ?? archives[0], id);
  }

  /**
   * Clicking a job inside a session's job list: scope the Jobs tab to that
   * session via a `session:<id>` search token (instead of showing every
   * job), then select the clicked job. The token is the only filter state —
   * erasing it from the search bar restores the full jobs list.
   */
  function openJobFromSession(archive: string, id: string, sessionId: string) {
    setSelected({ type: "job", archive, id });
    setTab("jobs");
    setQuery(`session:${sessionId} `);
  }

  // Opening a remote-only session shows the derived summary (already in
  // hand from the merged index) as an instant placeholder, then fetches the
  // full on-demand detail (agents/tasks/cwds) in the background.
  useEffect(() => {
    if (selected?.type !== "session") return;
    const { archive, id } = selected;
    const key = itemKey(archive, id);
    const session = allSessions.find((s) => s.archive === archive && s.session_id === id);
    if (!session || session.origin !== "remote") return;
    if (sessionOverrides[key] || sessionFetchFailed[key]) return;

    let cancelled = false;
    fetchSessionDetail(id, archive)
      .then((detail) => {
        if (cancelled) return;
        setSessionOverrides((prev) => ({ ...prev, [key]: detail }));
      })
      .catch(() => {
        // Keep the derived placeholder on failure — non-fatal, but flag it
        // so the detail view can note the summary is index-derived, not a
        // manifest fetch that hasn't happened yet.
        if (!cancelled) setSessionFetchFailed((prev) => ({ ...prev, [key]: true }));
      });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selected, allSessions]);

  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      const isSearchFocused = document.activeElement === searchRef.current;
      // Leave arrow keys/Enter to a focused filter <select>.
      if (document.activeElement instanceof HTMLSelectElement) return;

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
          openJob(filteredJobs[cursor].archive, filteredJobs[cursor].qmb_job_id);
        } else if (tab === "sessions" && filteredSessions[cursor]) {
          e.preventDefault();
          openSession(filteredSessions[cursor].archive, filteredSessions[cursor].session_id);
        }
      }
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, cursor, filteredJobs, filteredSessions, totalCount]);

  const selectedSession =
    selected?.type === "session"
      ? findItem(allSessions, [selected.archive], selected.id, sessionId)
      : undefined;
  const selectedJob =
    selected?.type === "job" && index
      ? findItem(index.jobs, [selected.archive], selected.id, jobId)
      : undefined;

  const sessionJobs: JobSummary[] = useMemo(() => {
    if (!index || !selectedSession) return [];
    const ids = new Set(selectedSession.jobs);
    return index.jobs.filter(
      (job) => ids.has(job.qmb_job_id) && sharesArchive(job, selectedSession),
    );
  }, [index, selectedSession]);

  const warnings: Warning[] = [];
  if (archivesError) {
    warnings.push({ message: `Could not list remote archives: ${archivesError}`, tone: "error" });
  }
  for (const status of index?.archives ?? []) {
    if (status.status === "error") {
      warnings.push({
        message: `Archive "${status.name}" is unavailable (other archives still load): ${status.error}`,
        tone: "warning",
      });
    } else if (status.index_stale) {
      warnings.push({
        message:
          `Archive "${status.name}" index is missing ${status.index_stale} session(s) — run ` +
          `\`${reindexCommand(status.name, archiveList)}\` to also see their jobs in the Jobs tab.`,
        tone: "warning",
      });
    }
  }
  const visibleWarnings = warnings.filter((warning) => !dismissed.has(warning.message));

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

        <div class="filters">
          <label class="filter">
            <span class="filter__label">Archive</span>
            <select
              class="filter__select"
              value={archiveFilter}
              onChange={(e) => setArchiveFilter((e.target as HTMLSelectElement).value)}
            >
              <option value={ALL}>all</option>
              <option value={LOCAL_ARCHIVE}>local</option>
              {archiveList.map((archive) => (
                <option key={archive.name} value={archive.name}>
                  {archive.name}
                </option>
              ))}
            </select>
          </label>
          <label class="filter">
            <span class="filter__label">Agent</span>
            <select
              class="filter__select"
              value={agentFilter}
              onChange={(e) => setAgentFilter((e.target as HTMLSelectElement).value)}
            >
              <option value={ALL}>all</option>
              {agentChoices.map((agent) => (
                <option key={agent} value={agent}>
                  {agent}
                </option>
              ))}
              <option value={UNKNOWN_AGENT}>unknown</option>
            </select>
          </label>
        </div>

        <div class="tabs">
          <button
            type="button"
            class={`tab ${tab === "sessions" ? "tab--active" : ""}`}
            onClick={() => setTab("sessions")}
          >
            Sessions ({scopedSessions.length})
          </button>
          <button
            type="button"
            class={`tab ${tab === "jobs" ? "tab--active" : ""}`}
            onClick={() => setTab("jobs")}
          >
            Jobs ({scopedJobs.length})
          </button>
        </div>

        {loadError && <div class="pane-error">Failed to load index: {loadError}</div>}

        <div class="list" ref={listRef} onScroll={handleListScroll}>
          {tab === "sessions"
            ? visibleSessions.map((session, i) => {
                const label =
                  session.agents.length > 1 ? session.agents.join(", ") : agentLabel(session);
                return (
                  <button
                    type="button"
                    key={itemKey(session.archive, session.session_id)}
                    class={`row ${i === cursor ? "row--cursor" : ""} ${
                      isSelected(selected, "session", session.archive, session.session_id)
                        ? "row--selected"
                        : ""
                    }`}
                    onClick={() => openSession(session.archive, session.session_id)}
                  >
                    <div class="row__top">
                      <span class="row__id">{session.session_id}</span>
                      <span class="row__badges">
                        <ArchiveBadges item={session} primary={primaryArchive} />
                        {/* Remote sessions summarized from index.json are the normal
                            case now (one GCS download, no manifest scan) — only flag
                            the rarer case of a session with no manifest anywhere. */}
                        {session.derived && session.origin !== "remote" && (
                          <span class="badge badge--derived">no manifest</span>
                        )}
                        {session.unindexed && <span class="badge badge--derived">unindexed</span>}
                      </span>
                    </div>
                    <div class="row__excerpt">
                      {session.unindexed ? "? jobs" : `${session.count} jobs`} ·{" "}
                      {fmtDate(session.first)} → {fmtDate(session.latest)}
                    </div>
                    {label && (
                      <div class="row__meta">
                        <span class="row__agent">{label}</span>
                      </div>
                    )}
                  </button>
                );
              })
            : visibleJobs.map((job, i) => {
                const label = agentLabel(job);
                return (
                  <button
                    type="button"
                    key={itemKey(job.archive, job.qmb_job_id)}
                    class={`row ${i === cursor ? "row--cursor" : ""} ${
                      isSelected(selected, "job", job.archive, job.qmb_job_id)
                        ? "row--selected"
                        : ""
                    }`}
                    onClick={() => openJob(job.archive, job.qmb_job_id)}
                  >
                    <div class="row__top">
                      <span class="row__id">{job.qmb_job_id}</span>
                      <span class="row__badges">
                        <ArchiveBadges item={job} primary={primaryArchive} />
                      </span>
                    </div>
                    <div class="row__excerpt">{job.query_excerpt || "(empty query)"}</div>
                    <div class="row__meta">
                      <span>{fmtDate(job.created_at)}</span>
                      {label && <span class="row__agent">{label}</span>}
                      {job.session_id && <span class="row__session">{job.session_id}</span>}
                      <span>{fmtBytes(job.stats.bytes_processed)}</span>
                    </div>
                  </button>
                );
              })}
          {!index && !loadError && <div class="pane-loading">Loading…</div>}
        </div>

        <div class="list-footer">
          Showing {visibleCount} of {totalCount}
          {tab === "jobs" && archiveSearch?.query === query && (
            <span class="list-footer__remote-status"> · {archiveSearch.status}</span>
          )}
          {remoteLoading && (
            <span class="list-footer__remote-status"> · loading archives…</span>
          )}
        </div>
      </div>

      <div class="right-pane">
        {visibleWarnings.map((warning) => (
          <Banner
            key={warning.message}
            message={warning.message}
            tone={warning.tone}
            onDismiss={() => setDismissed((prev) => new Set([...prev, warning.message]))}
          />
        ))}
        {!selected && <div class="empty-state">Select a job or session to see details.</div>}
        {selected?.type === "job" && (
          <JobDetail
            jobId={selected.id}
            archive={selected.archive}
            tagged={selectedJob ?? null}
            primaryArchive={primaryArchive}
            onSelectSession={(id) =>
              openLinkedSession(selectedJob ? archivesOf(selectedJob) : [selected.archive], id)
            }
            onSelectJob={(id) =>
              openLinkedJob(selectedJob ? archivesOf(selectedJob) : [selected.archive], id)
            }
          />
        )}
        {selected?.type === "session" &&
          index &&
          (selectedSession ? (
            <SessionDetail
              session={selectedSession}
              jobs={sessionJobs}
              fetchFailed={
                !!sessionFetchFailed[itemKey(selectedSession.archive, selectedSession.session_id)]
              }
              primaryArchive={primaryArchive}
              reindexCommand={reindexCommand(selectedSession.archive, archiveList)}
              onSelectJob={(id) => {
                const job = findItem(sessionJobs, archivesOf(selectedSession), id, jobId);
                openJobFromSession(
                  job?.archive ?? selectedSession.archive,
                  id,
                  selectedSession.session_id,
                );
              }}
            />
          ) : (
            <div class="pane-error">Session not found: {selected.id}</div>
          ))}
      </div>
    </div>
  );
}
