# Refactor Plan

Working checklist for refactoring qmb without changing user-facing behavior.

Ordering principle: low-hanging fruit first, biggest payoff in the middle,
deeper architectural moves last.

Each item should land as its own focused change so it stays easy to review
and easy to revert.

## Ground Rules

- No behavior change unless explicitly noted
- Keep `pytest` green at every step
- Keep `ruff` clean at every step
- Prefer small commits over big rewrites
- Add characterization tests before risky refactors

---

## Phase 1 — Docs and Dead Code Cleanup

Lowest risk, highest clarity-per-effort.

- [x] Fix README vs CLI mismatch for the browser
  - README documents `qmb --browser-only` / `qmb --browse`
  - Actual CLI exposes a `browse` subcommand
  - Decide on one shape (subcommand) and align docs + examples + options table
- [x] Document the `history` feature consistently
  - Add to README CLI section
  - Add to TUI keyboard shortcuts table (`r` to open history)
  - Mention in Quick Examples
- [x] Make top-level `qmb --help` discoverable
  - Currently shows `run` help due to `_DefaultRunGroup`
  - Decide: keep fallback to `run` but make `qmb --help` show all commands
- [x] Remove unused `InputMode.BROWSER`
  - Confirm no usage
  - Remove from `src/qmb/types.py`
- [x] Audit other dead/leftover symbols
  - Quick `rg` pass for unreferenced public names — nothing else removable
- [x] Add a short Architecture section to `README.md`
  - 10–20 lines, mirroring the map in `REVIEW.md`
- [x] Note current behavior limitations in README
  - Cell search is page-local
  - dbt resolution supports only `ref` / `source` / `var`
  - Browser builds a per-dataset table index

---

## Phase 2 — Small Code-Quality Wins

Still low risk, mostly local changes.

- [x] Replace runtime `assert` used for control flow with explicit errors
  - `src/qmb/sql/loader.py` (`request.sql is not None`, `request.file_path is not None`)
  - `src/qmb/cli.py` (`request.manifest_path is not None`, `request.model_name is not None`)
  - Use `ValueError` or `typer.BadParameter` depending on layer
- [x] Remove unnecessary `from __future__ import annotations`
  - Keep it only where it is actually required (e.g. `cli.py` with `TYPE_CHECKING` imports)
  - Drop from `src/qmb/bigquery/history.py` if not needed
- [x] Standardize error notification helpers in the TUI
  - Centralize the `self.notify(..., severity=...)` patterns
  - Added `_info` / `_warn` / `_error` wrappers on `QueryResultApp`
- [x] Tighten a few `Any` usages where the real type is known
  - Skipped: no low-hanging fruit. The remaining `Any` usages are either
    truly polymorphic (`_coerce_var_value` return, `json_default`,
    `QueryRequest.variables`, BigQuery SDK resource attrs in
    `catalog_format`) or part of the schema/row dict shape that Phase 7
    will replace with proper domain types. No type narrowing added here.
- [x] Add a couple of characterization tests before deeper refactors
  - CLI flow: query → resolve → execute → export
  - CLI flow: model → resolve → execute → export
  - CLI flow: file → resolve_dbt auto-detect
  - See `tests/test_cli_flow.py` (10 tests covering ad-hoc / file / dbt /
    model / `--where` / `--dry-run` / `--export` / `--max-bytes-billed`)

---

## Phase 3 — Extract Orchestration From `cli.py`

Goal: turn `cli.py` into a thin CLI adapter, push orchestration into reusable
application functions/services.

- [x] Introduce an `application/` (or `app/`) module
  - Houses orchestration use cases
  - No Typer / Textual imports
- [x] Extract `resolve_request_to_sql(request) -> ResolvedQuery`
  - Encapsulate logic currently in `cli._resolve_sql`
  - dbt auto-detection + manifest discovery stay in `cli.py` (UI concern;
    they print dim status lines). The resolver receives an already-resolved
    `manifest_path` and returns a `ResolutionTrace` the CLI uses for status.
- [x] Extract `apply_where(resolved, where) -> ResolvedQuery`
  - Small helper for the `--where` subquery wrap
- [x] Extract `run_query_pipeline(request) -> ExecutionOutcome`
  - Runs: resolve → optional `--where` → execute → optional export
  - Returns a structured result instead of side-effecting on console
- [x] Keep `cli.py` responsible only for:
  - Typer parsing
  - User input validation
  - Console output / status messages
  - Calling the application layer
  - Launching the TUI
- [x] Move local imports out of CLI functions where they no longer matter
  - Heavy imports (`google.cloud.bigquery`, `textual`) stay deferred

---

## Phase 4 — Split BigQuery Adapter Concerns

`src/qmb/bigquery/browser.py` currently mixes too many concerns.

- [x] Split `browser.py` into focused modules
  - `bigquery/catalog.py` — `list_dataset_ids`, `list_dataset_tables`,
    `build_table_index`, `get_dataset_metadata`, `get_table_metadata`
  - `bigquery/catalog_search.py` — fuzzy/glob filtering + `BrowserMatch`
  - `bigquery/catalog_format.py` — `format_dataset_details`, `format_table_details`,
    helpers like `_format_bytes`, `_format_partitioning`, etc.
- [x] Re-export public names from `bigquery/__init__.py` (or keep import paths) to avoid breaking callers
- [x] Make `_raw_property` / `_value_or_raw` access live only in the format module
  - Keeps SDK-internal coupling in one place

---

## Phase 5 — Extract Editor / Clipboard Integration

These live inside the TUI today but are clearly side concerns.

- [x] Move `_open_in_editor` into a small helper module
  - e.g. `qmb/integrations/editor.py`
  - `open_in_editor(content, *, suffix, prefix, read_only=True)`
- [x] Move clipboard copy + error notification pattern into a helper
  - e.g. `qmb/integrations/clipboard.py`
  - Provides `copy(text) -> bool` with a single failure path
- [x] Update `QueryResultApp` to call these helpers instead of inlining behavior

---

## Phase 6 — Decompose `QueryResultApp`

Biggest readability payoff. Do this only after Phases 1–5.

- [x] Identify clear responsibility groups in `tui/app.py`
  - Result table + paging
  - Cell search
  - Column picker
  - Export picker
  - History picker
  - Browser pane
  - Key sequence handling (pending-key state machine)
- [x] Extract per-feature controllers/components
  - `tui/help_screen.py` — `HelpScreen` + `HELP_TEXT`
  - `tui/key_router.py` — `PendingKeyRouter`
  - `tui/export_picker.py` — `ExportController`
  - `tui/history_picker.py` — `HistoryController`
  - `tui/browser_pane.py` — `BrowserController`
  - `tui/search.py` — `CellSearchController` + `ColumnPickerController`
- [x] Keep `QueryResultApp` as a thin coordinator
  - Compose components
  - Wire events between them via one-line `@on(...)` delegators
  - Hold only top-level app state (result rows / columns / current page)
  - `app.py` slimmed from 1348 → ~775 lines
- [x] Move pending-key logic into a dedicated key router
  - `y`, `x`, `g`, `gg`, etc.
- [x] Move browser async loading flow out of the App class
  - The `@work(thread=True)` wrappers stay on the App (Textual requires
    workers to be App methods), but they delegate post-fetch handling to
    `BrowserController` callbacks (`on_datasets_loaded`,
    `on_datasets_failed`, `on_index_loaded`, `on_index_failed`,
    `on_dataset_tables_loaded`, `on_dataset_tables_failed`).
- [x] Ensure all extracted components have unit tests where reasonable
  - Added `tests/test_tui_key_router.py`
  - Existing `tests/test_tui_app.py` integration tests exercise every
    controller through the App's public surface (browser, history,
    export pickers).

---

## Phase 7 — Tighten Domain Types

Once orchestration and TUI are split, types are easier to evolve.

- [x] Replace `destination_table: str` with a structured reference
  - Added `TableRef(project, dataset, table)` with `parse` / `__str__` /
    `is_empty` round-trip
  - `QueryResultHandle` still stores the string for backwards compatibility
    with existing test fixtures; a `.destination` property returns the
    typed view, and `bigquery/pager.py` uses it
- [x] Replace `schema: list[dict[str, Any]]` with `list[SchemaField]`
  - Added `SchemaField(name, type, mode)` with `from_mapping` / `to_mapping`
  - `QueryResultHandle.schema_fields` exposes the typed view; the dict
    storage stays for backwards compatibility
  - `bigquery/exporters.py` consumes `handle.schema_fields`
- [x] Reconsider row representation
  - Decision: keep `dict[str, Any]` for now; conversion helpers already
    live in `bigquery/pager.py` (`json_default`, `get_raw_value`,
    `_format_display`). No structural change.
- [x] Split `QueryRequest` into focused parts
  - Added `InputSpec` (mode, sql, file_path, model_name)
  - Added `DbtOptions` (resolve_dbt, manifest_path, variables)
  - Added `ExecutionOptions` (project, location, dry_run,
    max_bytes_billed, where)
  - Added `OutputOptions` (export_format, export_path, no_tui, page_size)
  - `QueryRequest` keeps its flat shape (so existing constructions in
    tests and CLI still work) and exposes the sub-configs via
    `.input` / `.dbt` / `.execution` / `.output` view properties
  - Internal helpers (`sql.loader.load_sql`,
    `application.resolver._resolve`, `application.pipeline`) now use
    the sub-configs

---

## Phase 8 — Make dbt Feel Like an Extension

Only after the application layer is explicit.

- [x] Define a `SqlResolver` protocol/interface in core
  - Added `qmb.application.protocols.SqlResolver` with `can_resolve` /
    `resolve` returning `(ResolvedQuery, ResolutionTrace)`
  - `ResolutionTrace` lives alongside the protocol
- [x] Implement `PlainSqlResolver` (current SQL/file behavior without dbt)
  - `qmb.sql.resolver.PlainSqlResolver` handles `InputMode.SQL` and
    `InputMode.FILE` (when `resolve_dbt=False`)
- [x] Implement `DbtSqlResolver` (wraps current dbt logic)
  - `qmb.dbt.integration.DbtSqlResolver` handles `InputMode.MODEL` and
    `InputMode.FILE` with `resolve_dbt=True`
  - Preserves the existing trace fields used by the CLI dim messages
- [x] CLI/application layer chooses resolver based on input + options
  - `cli._execute` constructs `[DbtSqlResolver(), PlainSqlResolver()]`
    and passes them to `run_query_pipeline`
  - The pipeline forwards them to `resolve_request_to_sql`, which picks
    the first resolver whose `can_resolve` returns `True`
- [x] Core stops importing dbt directly
  - `rg "from qmb.dbt|import qmb.dbt" src/qmb/application/` returns nothing
- [x] dbt module only depends on core abstractions, not the other way around
  - `qmb.dbt.integration` imports `qmb.application.protocols` for
    `ResolutionTrace`; nothing in `qmb.application` imports `qmb.dbt`

---

## Phase 9 — Local Job Archive and History Navigation

Goal: make every qmb-run query available later by qmb-owned job ID, with the
resolved SQL and a local result preview saved in an engine-independent archive.
This is the first step toward result navigation, nvim integration, and future
session/tree workflows.

Decision for this phase:

- Build a **flat local job archive** first.
- Do **not** implement fork/tree/session UI yet.
- Use a qmb-owned job ID independent from BigQuery job IDs.
- Store JSONL as the default internal row format.
- Keep Parquet as an explicit export / future archive option, not the default.
- Include future-proof metadata fields such as `session_id` and
  `parent_job_id`, but leave them `null` for now.

Suggested archive shape:

```text
~/.qmb/jobs/<qmb_job_id>/
  metadata.json      # qmb job ID, engine metadata, source metadata, stats
  query.sql          # exact resolved SQL executed
  schema.json        # result schema
  preview.jsonl      # first N rows for fast browsing / nvim preview
  result.jsonl       # optional full result archive, not required initially
```

### Phase 9A — TDD: write failing tests first

Before implementing the archive, add focused tests that describe the intended
behavior. After the tests are written, review/share the test plan, then make
them pass in small implementation steps.

- [x] Add storage/model tests for the local job archive
  - Proposed file: `tests/test_job_store.py`
  - Covers qmb job ID generation with injected clock/randomness
  - Writes `metadata.json`, `query.sql`, `schema.json`, and `preview.jsonl`
  - Reads a stored job record back from disk
  - Lists jobs sorted newest first
  - Resolves full or unambiguous partial qmb job IDs
  - Handles missing/corrupt job directories predictably
- [x] Add JSONL artifact tests
  - Proposed file: `tests/test_job_artifacts.py`
  - Streams row dictionaries to JSONL without building one large JSON array
  - Uses qmb/BigQuery JSON coercion for dates, datetimes, decimals, bytes, etc.
  - Preserves schema column order where applicable
  - Reads preview rows back for CLI/TUI/nvim consumers
- [x] Add pipeline persistence tests
  - Proposed file: `tests/test_job_archive_pipeline.py`
  - Successful non-dry-run executions create a local qmb job archive entry
  - Dry runs do not archive results, or archive only dry-run metadata if we
    explicitly decide to support that
  - Archive metadata records `engine="bigquery"`, BigQuery job ID, project,
    location, bytes processed, row count, source label, and timestamps
  - Archive saves the resolved SQL after dbt/plain resolution and `--where`
    wrapping
  - Export behavior remains separate from history archive behavior
- [x] Add CLI command tests for historical jobs
  - Proposed file: `tests/test_jobs_cli.py`
  - `qmb jobs list` shows local qmb jobs, not remote BigQuery history
  - `qmb jobs list --format json` returns machine-readable records
  - `qmb jobs show <job>` returns job metadata
  - `qmb jobs sql <job>` prints the archived resolved SQL
  - `qmb jobs paths <job> --format json` returns paths for nvim integration
- [x] Add a first archived-result navigation test
  - Proposed file: `tests/test_archived_results.py`
  - Opens or pages from `preview.jsonl` without calling BigQuery
  - Establishes the future `ResultSource` boundary, even if only JSONL preview
    is supported initially

### Phase 9B — Make the tests pass incrementally

- [x] Introduce a small history/archive package
  - Implemented as `qmb.jobs`
  - Types: `JobRecord`, `EngineMetadata`, `SourceMetadata`
  - Store: `JobStore` with `create`, `read`, `list`, `resolve_id`
  - CLI run archives are best-effort so a successful query is not failed by
    local history write issues
- [x] Add JSONL artifact helpers
  - Write/read preview rows
  - Reuse or centralize JSON serialization already used by BigQuery exporters
- [x] Hook archive creation into `run_query_pipeline`
  - Keep it application-layer, not TUI-layer
  - Archive after successful execution
  - Keep explicit user exports independent from archive writes
- [x] Add `qmb jobs` CLI subcommands
  - `qmb jobs list`
  - `qmb jobs show <job>`
  - `qmb jobs sql <job>`
  - `qmb jobs open <job>`
  - `qmb jobs paths <job>`
  - Prefer JSON-capable output to support Phase 10 and nvim integration
- [x] Add minimal archived-result reading
  - Start with `preview.jsonl`
  - Keep full `result.jsonl` / Parquet archive support for later unless needed
- [x] Add first TUI bridge for archived job previews
  - `QueryResultApp` can page from a local result source instead of only
    BigQuery
  - `qmb jobs open <job>` opens `preview.jsonl` in the existing TUI without
    calling BigQuery
  - This is a Phase 9 bridge, not the full Phase 10 renderer split
- [x] Add in-TUI navigation between archived jobs
  - New `tui/jobs_picker.py` (`JobsController`) mirrors the history picker
  - `J` opens the archived-jobs picker; selecting a job swaps the
    current `QueryResultApp` view to that job's preview without
    re-running the query
  - Filter matches against source label, full + short qmb job ID,
    and the date string
  - Rebound BigQuery history from `r` to `H` for consistency (both
    "history-ish" pickers are now capital letters); `J` does not collide
    with `j` (cursor down)
  - Help screen and README shortcut tables updated

### Phase 9C — Later, not now

- [ ] Full result archive policy/config
  - `none | preview | full | auto`
  - row/byte caps and retention
  - optional Parquet archive format
- [ ] Session / tree / fork navigation
  - Build on top of flat jobs using `session_id` and `parent_job_id`
  - Needed for query re-execution and branch-style history replay (see
    `TODO.md`)
- [ ] nvim plugin/workflow
  - Populate quickfix/location list from `qmb jobs list --format json`
  - Open `query.sql` and `preview.jsonl`/result side-by-side
- [ ] Reconsider whether dbt becomes a true plugin/extension boundary
  - Discoverable via entry points or explicit registration
- [ ] Result-set-wide search (instead of page-local)
- [ ] Reconsider browser indexing strategy for very large projects

---

## Phase 10 — CLI-First / Headless Mode

After Phases 1–9 the architecture is decoupled enough that the TUI is no
longer the only sink — it becomes one renderer among many.

Goal: qmb is usable headlessly by humans in scripts, by agents, and by LLMs.
The TUI is an opt-in mode for interactive / editor-style use.

### Behavior target

- `qmb` returns structured **JSON** by default when stdout is not a TTY
- `qmb` opens the Textual TUI by default when stdout *is* a TTY (current UX)
- `--format {json,csv,table,tui}` overrides the default explicitly
- All errors go to stderr in a structured shape (matching the active format)
- Exit codes are predictable for scripting
- No command opens the TUI implicitly when `--format` is non-`tui`

### Policy decisions for this phase

- **Default policy** = TTY-aware: piped → json, interactive terminal → tui.
  Agents and pipelines therefore just call `qmb "SELECT ..."` and get JSON.
  Humans get the TUI as before.
- **`--no-tui` stays as a deprecated alias** for `--format table` for one
  release, so existing scripts keep working.
- **Row source for JSON/CSV** = stream from `bigquery/pager.py` (full result).
  Cap with a new `--limit` only if it proves necessary.
- **Full result archive remains deferred to Phase 9C.** Phase 10 keeps the
  500-row preview archive; agents that need every row pipe stdout to a file.
- **Session/tree fields** (`session_id`, `parent_job_id`) — wire up CLI flags
  in 10E so agents can stitch jobs together, but no UI/navigation yet.

### Phase 10A — Foundation: formatters package + `--format` on `run`

No default flip yet. Everything is opt-in via `--format`, so this commit is
pure addition.

- [x] Add `src/qmb/formatters/` package
  - `base.py` — `Format` enum + `Formatter` protocol
  - `json_fmt.py` — `JsonFormatter` (stdout JSON; rows streamed from pager)
  - `csv_fmt.py` — `CsvFormatter` (stdout CSV; rows streamed from pager)
  - `table_fmt.py` — `TableFormatter` (today's Rich console output)
  - `tui_fmt.py` — `TuiFormatter` (wraps `QueryResultApp.run`)
  - `__init__.py` — `get_formatter(fmt)` factory
- [x] Move all `console.print` from `cli._render_outcome` into `TableFormatter`
- [x] Add `--format` flag to `qmb run`
  - Values: `json`, `csv`, `table`, `tui`
  - Default: keep current (TUI launch + status lines)
  - Explicit `--format tui` overrides `--no-tui`
  - Explicit `--format json|csv|table` implies headless (sets `no_tui`)
- [x] Document the JSON schema for `run` in README
- [x] Tests
  - `tests/test_formatters.py` — 17 unit tests, one per formatter behavior
  - `tests/test_cli_flow.py` — 5 new cases for `--format json`, `--format csv`,
    dry-run JSON, invalid format, and `--format tui` overriding `--no-tui`

### Phase 10B — Headless by default everywhere

Policy decision: JSON is the default for every command; the TUI is
opt-in only via `-t` / `--tui`. No TTY detection, no implicit TUI
launches. `--no-tui` is removed entirely (the default now matches
what that flag used to mean).

- [x] `qmb run`: default `--format` flipped to `json`; add `-t/--tui`;
  remove `--no-tui`
- [x] `qmb history`: default = JSON array of recent jobs on stdout;
  add `-t/--tui` to open the existing picker
- [x] `qmb browse [<pattern>]`: default = JSON catalog listing
  (datasets without a pattern; matches with one); add positional
  pattern arg; add `-t/--tui` to open the browser pane
- [x] New `qmb describe <dataset[.table]>`: dataset or table metadata
  as JSON, mirroring `bq show --format prettyjson`. Uses the SDK's
  `to_api_repr()` for full REST-API fidelity
- [x] README, command table, and examples updated for every command

### Phase 10D — Errors + exit codes

- [ ] When the active format is `json`, errors go to stderr as
  `{"error": {"type": "...", "message": "...", "details": {...}}}`
- [ ] Standard exit codes: `0` success, `1` user error (Typer `BadParameter`),
  `2` BigQuery / GCP error, `3` archive/IO error, `130` Ctrl-C
- [ ] Surface archive failures explicitly in the JSON output
  - `"archive": {"qmb_job_id": null, "error": "..."}` rather than silent skip
- [ ] Tests for stderr shape and exit codes

### Phase 10E — Agent-friendly metadata (pulled from 9C)

- [ ] Add `--session-id` and `--parent-job-id` flags on `run`
- [ ] Persist them in `metadata.json` (already nullable in the model)
- [ ] Include them in the JSON `archive` block on stdout
- [ ] `qmb jobs list --session-id X` filter
- [ ] Defer tree/graph navigation UI to a later phase

### Why this matters

- Scriptable from shells and pipelines
- Usable by agents / LLMs / robots without screen scraping
- TUI stays as a high-quality interactive mode for humans / editor use
- Forces a clean separation between application logic and rendering
- Naturally extends to future renderers (e.g. Markdown, NDJSON, html)
- Combined with the Phase 9 job archive, agents get a permanent,
  queryable record of every BigQuery interaction they make

---

## Suggested Execution Order

A pragmatic, low-risk order to actually do this work:

1. Phase 1 — docs + dead code
2. Phase 2 — small code-quality wins (with new characterization tests)
3. Phase 3 — extract orchestration from `cli.py`
4. Phase 4 — split BigQuery adapter concerns
5. Phase 5 — extract editor/clipboard helpers
6. Phase 6 — decompose `QueryResultApp`
7. Phase 7 — tighten domain types
8. Phase 8 — dbt as resolver behind an interface
9. Phase 9 — anything left, only if/when needed
10. Phase 10 — CLI-first / headless mode (long-term goal)
