# Changelog

All notable changes to qmb are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows a relaxed [Semantic Versioning](https://semver.org/)
while in the 0.x range (minor bumps may include breaking changes; user-visible
behavior changes are called out explicitly).

## [Unreleased]

### Added

- Remote qmb archives for sharing jobs and sessions without re-running
  BigQuery. New `qmb jobs export` and `qmb jobs import` commands publish/load
  the existing local archive artifacts (`metadata.json`, `query.sql`,
  `schema.json`, `preview.jsonl`) to/from GCS under
  `sessions/<session_id>/<qmb_job_id>/`.
- `qmb run --publish` to publish the just-created local archive after a
  successful run, with non-fatal remote archive status surfaced in JSON output
  as `remote_archive`.
- Remote archive configuration via `--destination`, `QMB_REMOTE_ARCHIVE_URI`,
  `~/.qmb/config.toml`, and the built-in default
  `gs://data-platform-moises-temp/qmb/`. `QMB_REMOTE_ARCHIVE_PREVIEW_ROWS`
  controls how many local preview rows are copied remotely.
- The official qmb agent skill now documents remote session sharing and import
  workflows.

## [0.5.1] - 2026-06-04

### Fixed

- Declare `click>=8.0` as an explicit dependency. `qmb` has been using
  `click.Context` / `click.exceptions.*` in `cli.py` since before this
  release; older `typer` versions brought `click` in transitively, but
  `typer>=0.26` no longer does. As a result `uv tool install qmb`
  (which always resolves the latest typer) produced an environment
  that failed on launch with `ModuleNotFoundError: No module named
  'click'`. Existing installs can recover with
  `uv tool upgrade qmb --reinstall`.

## [0.5.0] - 2026-06-04

**Headline change — `qmb browse` is 6–10× faster on large projects.**
On a production project with 117 datasets and ~17,000 tables, the
`browse` command went from ~25 s to ~4 s cold and ~2.5 s with a warm
regions cache. The TUI browser pane benefits identically. Default
output is unchanged.

### Changed

- `qmb browse PATTERN` now builds the catalog by issuing one
  `INFORMATION_SCHEMA.TABLES` query per region in parallel, replacing
  the previous per-dataset `list_tables` fan-out that was bounded by
  the single slowest dataset's pagination. Regions are auto-discovered
  from `list_datasets` and cached at
  `~/.qmb/cache/regions/<project>.json` for 30 days; subsequent runs
  skip discovery entirely. Datasets and tables themselves are never
  cached — every run sees fresh data.
- The TUI browser pane (`qmb browse -t`) uses the same fast path, with
  automatic fallback to per-dataset `list_tables` if the
  INFORMATION_SCHEMA queries fail (e.g., restricted IAM without
  `bigquery.jobs.create`).

### Added

- `qmb browse --refresh-regions` to ignore the cached region list,
  re-run `list_datasets`, and rewrite the cache. Use after a project
  starts using a region it hasn't used before.
- `qmb browse --legacy-list-tables` to opt back into the old
  per-dataset fan-out (required for callers without
  `bigquery.jobs.create`).
- `qmb browse --time` prints per-step wall-clock timings to stderr
  without affecting stdout JSON.
- `qmb browse --workers N` tunes the legacy-mode thread count
  (default `8`); ignored in the default INFORMATION_SCHEMA path.
- `QMB_REGIONS_CACHE_DIR` env var to override the default cache
  location (`~/.qmb/cache/regions`).
- `QMB_TRACE_INFO_SCHEMA=1` and `QMB_TRACE_CATALOG=1` diagnostic
  environment variables for per-region / per-dataset timing traces.

## [0.4.1] - 2026-05-22

### Added

- `qmb jobs sessions --format json` now aggregates `bytes_processed` and a
  sorted `cwds` list (distinct `agent.cwd` values) per session, alongside the
  existing count / first / latest / agents / tasks fields. The text output
  shows a bytes column.
- TUI visual-mode TSV yank (`y` / `yt`) now prepends the selected columns'
  headers so the clipboard text round-trips with names into spreadsheets.

## [0.4.0] - 2026-05-15

### Added

- TUI visual mode: press `v` to anchor a rectangular cell selection, extend it
  with `h`/`j`/`k`/`l` or arrow keys, and see the active range highlighted with
  a `-- VISUAL -- (rows×columns)` page-bar indicator.
- Visual-mode yanks: `y` / `yt` copy the selection as TSV for spreadsheet
  paste, `yc` copies CSV with selected headers, and `yj` copies a JSON array of
  selected row objects.
- `SHORTCUT.md` as a complete per-mode keyboard shortcut reference.

### Changed

- README and in-app TUI help now document visual mode and visual yank formats.

## [0.3.2] - 2026-05-13

### Changed

- Bare `qmb` / `qmb jobs` help output no longer emits a structured JSON error
  after printing usage.
- `qmb jobs list` now defaults to the newest 10 matching jobs, supports
  `--all`, displays `session:<id>` in text output, and exposes
  `effective_session_id` in JSON output for legacy agent-only session archives.

### Added

- `qmb jobs list` filters for `--agent`, `--date`, `--since`, `--until`,
  `--file`, `--model`, `--source`, and `--query`; `--session` is now an alias
  for `--session-id`.

## [0.3.1] - 2026-05-13

### Added

- Agent/session archive metadata: `qmb run` now records a nested `agent`
  object in `metadata.json` (and the JSON formatter's `archive.agent`) with
  agent name, session/conversation/run/turn ids, task label, cwd, git repo
  state, user, host, tags, and arbitrary metadata.
- `QMB_SESSION_ID` fallback for `--session-id`, plus `--agent`,
  `--agent-conversation-id`, `--agent-run-id`, `--agent-turn-id`,
  `--agent-task`, repeatable `--tag`, repeatable `--meta KEY=VALUE`, and
  corresponding `QMB_AGENT_*` environment variables for agent workflows.
- `qmb jobs sessions` to list archived session ids with counts, first/latest
  timestamps, agent names, and task labels (`--format text|json`, `--limit`).
- The TUI archived-jobs picker (`J`) now shows session ids when present and
  includes them in filtering.
- Project skill at `.agents/skills/qmb/SKILL.md` documenting the agent-facing
  qmb workflow and replacing `bq` habits with archived qmb commands.

## [0.3.0] - 2026-05-13

**Headline change — qmb is now CLI-first.** Every command prints structured
JSON to stdout by default and the Textual TUI is strictly opt-in via
`-t` / `--tui`. This makes qmb usable from scripts, pipelines, and AI agents
without screen-scraping; existing TUI-driven workflows are reachable with a
single extra flag. Phase 10 of the refactor plan is complete.

### Added

#### Headless / agent mode
- `--format` flag on `qmb run` with four renderers: `json` (default), `csv`,
  `table` (Rich status lines), `tui` (interactive Textual app). New
  `src/qmb/formatters/` package houses the four renderers behind a single
  `Formatter` protocol; the CLI is the only thing that picks a format.
- `-t` / `--tui` short alias for `--format tui` on every command that has an
  interactive mode (`qmb run`, `qmb history`, `qmb browse`).
- `qmb describe <dataset[.table]>` — new command that prints dataset or
  table metadata as JSON (mirrors `bq show --format prettyjson`). Accepts
  the BQ-native `project:dataset.table` colon shorthand.
- `qmb browse [PATTERN]` — positional fuzzy/glob pattern argument for
  filtering catalog output (e.g. `qmb browse 'analytics_*'`).
- Structured JSON error contract — every failure emits one JSON object on
  **stderr** with `{"error": {"type": "...", "message": "...", "details": {...}}}`.
  `type` values: `user_error`, `engine_error`, `internal_error`,
  `interrupted`. New `src/qmb/errors.py` (`emit_json_error()`, `EXIT_*`
  constants).
- Standard exit codes: `0` success, `1` user error, `2` engine/internal
  error, `3` IO/archive (reserved), `130` interrupted (Ctrl-C).
- `--session-id` and `--parent-job-id` flags on `qmb run` for agent
  workflows. Persisted in the local archive (`metadata.json`) and surfaced
  in the JSON output's new `archive.session_id` / `archive.parent_job_id`
  fields.
- `qmb jobs list --session-id`, `--parent-job-id`, and `--limit` filters.
- Archive failures surfaced explicitly in the JSON output:
  `archive.error` carries the exception message instead of silently
  dropping the archive entry.
- TUI jobs picker: press `J` inside the TUI to browse the local qmb job
  archive (`~/.qmb/jobs/`). Selecting a job swaps the current view to that
  job's preview without re-running the query. The filter matches the
  source label, full or short qmb job ID, the date string, and the SQL text.
- New module `qmb.tui.jobs_picker` (`JobsController`); SQL excerpts are
  read lazily and cached per job ID so the picker stays responsive across
  filter keystrokes.
- 50+ new tests (`test_formatters.py`, `test_cli_errors.py`,
  `test_session_metadata.py`, plus expansions to `test_cli_flow.py` and
  `test_cli.py`). Total: 176 tests passing.

### Changed

- **`qmb "SELECT ..."` no longer opens the TUI.** The default output is now
  structured JSON to stdout. Pass `-t` / `--tui` to open the Textual app.
  Same applies to `qmb history` and `qmb browse`.
- **TUI keybinding rebound**: BigQuery query history moved from `r` to `H`
  for consistency with the new `J` (archived qmb jobs) shortcut. Both
  "history-ish" pickers are now capital letters; `j` (cursor down) is
  unaffected.
- `qmb jobs list` records now include `session_id` and `parent_job_id` in
  the JSON output.
- README rewritten as a comprehensive CLI-first reference with a per-command
  options table sourced from `--help`, JSON output schemas, exit codes, and
  the value-coercion rules used in JSON/CSV output.
- Help screen and README shortcut tables updated.

### Removed

- **`--no-tui` flag removed.** With JSON as the default, the flag's
  meaning collapsed ("don't change anything"). Use `--format table` if you
  want the old Rich status output without the TUI; pass no `-t` for the
  new JSON-only default.

### Migration notes

- Scripts that ran `qmb "SELECT ..." --no-tui --export csv --out f.csv`:
  drop `--no-tui` (it's the default), keep everything else. JSON will also
  be printed to stdout summarizing the run; redirect it (`>/dev/null`) or
  consume it with `jq` if you don't want it on the terminal.
- Scripts that relied on opening the TUI from a CLI invocation: add `-t`.
- Anything parsing qmb's stderr for human-readable errors should now parse
  JSON instead. The error message is in `.error.message`.

## [0.2.0] - 2026-05-12

Major internal architecture refactor across 8 phases. **No user-visible
behavior changes** beyond the new `--version` flag — same commands,
flags, keybindings, and output. Test coverage grew from 40 to 95 tests.

### Added
- `qmb --version` / `qmb -V` — prints the installed version and exits.
- `qmb.__version__` — version string exposed from the package
  (resolved via `importlib.metadata.version`).
- `qmb/application/` orchestration layer (`pipeline.py`, `resolver.py`,
  `protocols.py`, `outcomes.py`) — pure Python, no CLI or TUI imports.
- `qmb/integrations/` package: `editor.py` (open-in-`$EDITOR` helper) and
  `clipboard.py` (`copy()` with explicit `ClipboardUnavailable` errors).
- `SqlResolver` protocol with two implementations:
  - `qmb.sql.resolver.PlainSqlResolver` — ad-hoc SQL and raw `.sql` files.
  - `qmb.dbt.integration.DbtSqlResolver` — dbt models, file-to-model matching,
    `ref()` / `source()` / `var()` resolution.
- New domain value types in `qmb.types`:
  - `TableRef(project, dataset, table)` with `parse()` / `__str__()` /
    `is_empty`.
  - `SchemaField(name, type, mode)` with `from_mapping()` / `to_mapping()`.
  - Sub-config views on `QueryRequest`: `InputSpec`, `DbtOptions`,
    `ExecutionOptions`, `OutputOptions`.
- `ExecutionOutcome` and `ResolutionTrace` for orchestration results.
- TUI decomposed into focused controllers:
  - `tui/browser_pane.py` — `BrowserController`
  - `tui/history_picker.py` — `HistoryController`
  - `tui/export_picker.py` — `ExportController`
  - `tui/search.py` — `CellSearchController` + `ColumnPickerController`
  - `tui/key_router.py` — `PendingKeyRouter` (vim multi-key state)
  - `tui/help_screen.py` — `HelpScreen`
- BigQuery adapter split:
  - `bigquery/catalog.py` — dataset/table metadata
  - `bigquery/catalog_search.py` — fuzzy/glob filtering + `BrowserMatch`
  - `bigquery/catalog_format.py` — details formatting
- Architecture and limitations sections in `README.md`.
- Version-pinned install instructions in `README.md`.
- `REVIEW.md` — current architecture map and review notes.
- `REFACT.md` — phased refactor plan and progress checklist.
- 55 new tests across CLI flows, application layer, resolvers, integrations,
  domain types, and TUI key router.

### Changed
- `cli.py` slimmed down: orchestration logic moved into the application
  layer. `_execute` is now a thin Typer adapter that calls
  `run_query_pipeline()`.
- `tui/app.py` reduced from 1351 to 775 lines; `QueryResultApp` is now a thin
  coordinator that composes widgets and delegates to controllers.
- `bigquery/browser.py` reduced from 482 to 30 lines (re-export facade).
- TUI `self.notify(...)` calls wrapped in small severity helpers
  (`self._info`, `self._warn`, `self._error`).
- README documents the actual `qmb browse` and `qmb history` subcommands
  (previously `--browser-only` / `--browse` were documented as flags).

### Fixed
- `qmb --help` now shows the top-level command list (`run`, `browse`,
  `history`) instead of routing to `run --help`.
- `qmb --version` / `-V` is not rewritten to `qmb run --version` by the
  default-run fallback group.

### Removed
- Unused `InputMode.BROWSER` enum value.
- Unnecessary `from __future__ import annotations` imports where Python 3.11+
  doesn't need them.
- Inline `pyperclip` and `tempfile.NamedTemporaryFile` usage in the TUI
  (moved to `qmb/integrations/`).

### Internal
- Runtime `assert` statements used for control flow replaced with explicit
  `ValueError` / `typer.BadParameter`.
- Application layer no longer imports `qmb.dbt.*` (proof:
  `rg "qmb.dbt" src/qmb/application/` finds only docstrings).

## [0.1.0] - 2026-05-12

First formal release of qmb — a BigQuery CLI with a vim-style Textual TUI,
dbt model support, and export.

### Added

#### CLI
- `qmb "<SQL>"` — run ad-hoc inline queries.
- `qmb --file path.sql` — run queries from `.sql` files (including stdin via
  `--file -`).
- `qmb --model <name>` — run dbt models via `manifest.json`.
- `qmb --manifest <path>` — explicit dbt manifest path; auto-discovers
  `target/manifest.json` from cwd/parents or `DBT_MODEL_PATH` /
  `DBT_PROJECT_DIR` env vars when omitted.
- `qmb --resolve-dbt` — resolve `ref()` / `source()` / `var()` in `.sql`
  files; auto-enabled when a `.sql` file lives inside a dbt project or dbt
  env vars are set.
- File-to-model matching: when a `.sql` file matches a `manifest.json` node,
  qmb uses the compiled SQL or falls back to raw SQL with config blocks
  stripped.
- `--var key=value` — override dbt variables (repeatable).
- `--where "<clause>"` — append a WHERE clause to the resolved SQL at
  runtime (models untouched).
- `--dry-run` — validate query and estimate bytes without executing.
- `--max-bytes-billed` — safety limit for query cost.
- `--project` / `--location` — GCP project and BigQuery location.
- `--page-size` — rows per page in the TUI.
- `qmb history` — browse recent query jobs (last 7 days by default).
- `qmb browse` — open the dataset/table browser without running a query.

#### Export
- `--export csv|json|parquet` with optional `--out` path.
- Streaming exports — Parquet writes in batches, JSON streams row-by-row,
  none materialize the full result set in memory.
- `--no-tui` to skip the TUI after exporting from the CLI.

#### TUI
- Vim-style navigation: `h` `j` `k` `l`, arrow keys, `gg`, `G`, `0`, `$`.
- Pagination: `n` / `p`, `Home` / `End`.
- Cell search (`/`) and column search (`f`) with `n` / `N` to cycle matches.
- Multi-key yank sequences: `yw` (cell), `yc` (row CSV), `yj` (row JSON) —
  clipboard via `pyperclip`.
- Multi-key export sequences: `xc` (CSV), `xj` (JSON), or full picker via `x`.
- Inspect actions in nvim (read-only): `e` (cell, `.json` when applicable),
  `s` (full SQL), `d` (job details).
- History picker (`r`) — search recent query jobs, open selected entry in
  `$EDITOR` for further use.
- Dataset/table browser pane (`b`) — fuzzy/glob search across datasets and
  tables, expand/collapse with `h` / `l`, `Enter` / `d` to open details in
  nvim.
- Row number column with cursor auto-skip (`#`).
- Inline bottom pickers (no modals): column filter, export, history.
- Scrollable help screen (`?`).
- `Ctrl-Q` to quit.

#### Architecture
- Typer-based CLI with default-run routing (`qmb "SELECT 1"` falls through
  to the `run` command).
- Page-based result browsing with explicit row paging.
- Schema-aware display formatting (truncation, JSON pretty-printing for
  dict/list cells, datetime ISO formatting).

[0.4.0]: https://github.com/medeirosthiago/query-me-big/releases/tag/v0.4.0
[0.3.2]: https://github.com/medeirosthiago/query-me-big/releases/tag/v0.3.2
[0.3.1]: https://github.com/medeirosthiago/query-me-big/releases/tag/v0.3.1
[0.3.0]: https://github.com/medeirosthiago/query-me-big/releases/tag/v0.3.0
[0.2.0]: https://github.com/medeirosthiago/query-me-big/releases/tag/v0.2.0
[0.1.0]: https://github.com/medeirosthiago/query-me-big/releases/tag/v0.1.0
