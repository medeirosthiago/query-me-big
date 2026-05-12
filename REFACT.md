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

- [ ] Replace `destination_table: str` with a structured reference
  - e.g. `TableRef(project, dataset, table)`
  - Build `bigquery.TableReference` from it where needed
- [ ] Replace `schema: list[dict[str, Any]]` with `list[SchemaField]`
  - Lightweight dataclass with `name`, `type`, `mode`
- [ ] Reconsider row representation
  - Likely keep `dict[str, Any]` but isolate conversion helpers
- [ ] Split `QueryRequest` into focused parts
  - `InputSpec` (mode, sql, file_path, model_name)
  - `DbtOptions` (resolve_dbt, manifest_path, variables)
  - `ExecutionOptions` (project, location, dry_run, max_bytes_billed, where)
  - `OutputOptions` (export_format, export_path, no_tui, page_size)
  - `QueryRequest` becomes a thin composition of these

---

## Phase 8 — Make dbt Feel Like an Extension

Only after the application layer is explicit.

- [ ] Define a `SqlResolver` protocol/interface in core
  - `resolve(input_spec) -> ResolvedQuery`
- [ ] Implement `PlainSqlResolver` (current SQL/file behavior without dbt)
- [ ] Implement `DbtSqlResolver` (wraps current dbt logic)
- [ ] CLI/application layer chooses resolver based on input + options
- [ ] Core stops importing dbt directly
- [ ] dbt module only depends on core abstractions, not the other way around

---

## Phase 9 — Future / Optional

Not required for the immediate cleanup, but worth tracking.

- [ ] Reconsider whether dbt becomes a true plugin/extension boundary
  - Discoverable via entry points or explicit registration
- [ ] Session / result navigation architecture
  - Needed for query re-execution and history replay (see `TODO.md`)
- [ ] Result-set-wide search (instead of page-local)
- [ ] Reconsider browser indexing strategy for very large projects

---

## Phase 10 — CLI-First / Headless Mode (Long-term Goal)

Only after Phases 1–8.

After the previous phases, qmb's architecture is decoupled enough that the
TUI is no longer the default — it becomes one of several renderers.

Goal: qmb is usable headlessly by humans in scripts, by agents, and by LLMs.
The TUI is an opt-in mode for interactive / editor-style use.

### Behavior target

- Default output is structured **JSON** to stdout
- `--format csv` switches to CSV output
- `--format tui` (or `--tui`) opens the existing Textual app
- All errors go to stderr in a structured shape
- Exit codes are predictable for scripting
- No command opens the TUI implicitly

### Features that become headless by default

- [ ] Query execution
  - `qmb "SELECT ..."` prints rows as JSON
  - `--format csv` for CSV
- [ ] Dry runs
  - `qmb --dry-run "SELECT ..."` prints estimated bytes + status as JSON
- [ ] Query history
  - `qmb history` prints recent jobs as JSON
  - Supports filters (e.g. `--since`, `--limit`, `--project`)
- [ ] Browse datasets and tables
  - `qmb browse <pattern>` prints matching datasets/tables as JSON
  - No TUI unless explicitly requested
- [ ] Inspect dataset / table details
  - `qmb describe <dataset>` or `qmb describe <dataset.table>` prints metadata as JSON
- [ ] dbt model resolution
  - `qmb --model orders --no-tui` prints results as JSON by default

### Implementation notes

- Depends on Phase 3 (application/orchestration layer extracted from CLI)
- Depends on Phase 6 (TUI no longer entangled with app logic)
- Depends on Phase 7 (typed domain objects → easy to serialize)
- Introduce `qmb/formatters/` with renderers:
  - `json` (default)
  - `csv`
  - `table` (pretty terminal table, optional)
  - `tui` (opt-in interactive)
- A single `--format` flag selects the renderer
- TUI becomes one renderer among many, not the default
- Each command returns a typed result object; renderers know how to print it
- Document the JSON schema for each command so external tools and agents can rely on it

### Why this matters

- Scriptable from shells and pipelines
- Usable by agents / LLMs / robots without screen scraping
- TUI stays as a high-quality interactive mode for humans / editor use
- Forces a clean separation between application logic and rendering
- Naturally extends to future renderers (e.g. Markdown, NDJSON, table)

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
