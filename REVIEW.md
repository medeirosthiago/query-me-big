# Review

## Current Status

qmb is in a good place functionally.

It already delivers the main goal:
- run BigQuery queries from the CLI
- browse results in a Textual TUI
- navigate with vim-style keys
- export results to CSV / JSON / Parquet
- optionally resolve dbt models / SQL files

This feels like a strong working prototype or early product, not a broken codebase.

## Verification

Current checks run during review:
- `uv run python -m pytest -q` → 40 passed
- `uv run ruff check .` → passed

## Main Review Points

### 1. Overall direction is good
The product shape is coherent and useful:
- CLI entrypoint
- SQL/dbt resolution
- BigQuery execution
- paging/export
- TUI/browser/history

The happy path is easy to explain, which is a strong sign the project has a solid core.

### 2. The package layout is already reasonable
The current split is understandable:
- `src/qmb/cli.py`
- `src/qmb/tui/app.py`
- `src/qmb/bigquery/`
- `src/qmb/dbt/`
- `src/qmb/sql/`
- `src/qmb/types.py`

This is a good small-project structure. The issue is less about folder layout and more about responsibility boundaries inside a few large files.

### 3. `src/qmb/tui/app.py` is the main structural hotspot
This is the biggest maintainability issue.

`QueryResultApp` currently mixes:
- widget composition
- UI state
- key handling
- browser behavior
- search behavior
- export flow
- history flow
- editor integration
- notifications
- async loading
- page rendering

This is the main place where the code starts to feel “vibe-coded” rather than intentionally structured.

### 4. `QueryRequest` carries too many concerns
`src/qmb/types.py` currently uses one request object for:
- input source
- dbt options
- execution options
- output options
- TUI/presentation options

That makes orchestration easy initially, but it also blurs boundaries and increases coupling.

### 5. `src/qmb/cli.py` is doing more than CLI work
`cli.py` is currently both:
- CLI adapter
- validation layer
- application orchestration layer

That means the main flow of the program is tied closely to Typer command functions. This is workable now, but it will make reuse harder if qmb gets another interface later.

### 6. dbt is separated as a module, but not yet as an extension boundary
The current dbt code is physically separated into `src/qmb/dbt/`, which is good.

But dbt is still part of the core execution request shape and core resolution flow.

So today dbt is a well-separated module, not yet a real optional extension/plugin boundary.

### 7. Some important data is still stringly typed
A few important values are represented as plain strings or generic dicts:
- destination table as string
- schema as `list[dict[str, Any]]`
- rows as `dict[str, Any]`

This works, but it reduces readability and makes refactoring harder.

### 8. `src/qmb/bigquery/browser.py` mixes multiple responsibilities
That file currently contains:
- BigQuery metadata fetching
- dataset/table search/index logic
- details formatting for editor display

These are related, but still distinct responsibilities.

### 9. There is some design drift
A few signals:
- `InputMode.BROWSER` exists but is unused
- README documents `--browser-only` / `--browse`, while the code exposes a `browse` command
- top-level CLI help currently shows `run` help because of the fallback group behavior
- history exists in code/TUI but is not consistently surfaced in docs

This is not severe, but it shows the project has evolved faster than its documentation and some of its types.

### 10. `from __future__ import annotations` is not the real issue
Some uses are justified, especially where imports are only needed for typing.
Some uses are optional.

But these imports are not a meaningful architecture problem.

The bigger issues are:
- oversized classes
- mixed responsibilities
- implicit application layer
- request object sprawl
- CLI/application coupling

## General Comments

### What is already good
- clear user goal
- coherent package structure
- practical feature set
- meaningful tests
- working exports
- working browser/history flows
- sensible use of paging and streaming
- dbt integration already isolated better than average for a small tool

### What needs the most attention later
- TUI decomposition
- clearer application/use-case layer
- better boundaries between UI, orchestration, and infrastructure
- stronger typed objects around results / schema / references
- docs alignment with actual CLI behavior

### DDD note
Full DDD does not look necessary here.

A better fit would be lightweight clean architecture / hexagonal thinking:
- core use cases
- adapters for CLI, TUI, BigQuery, dbt, clipboard, editor

## Practical Product Notes

### Current behavior / limitations worth remembering
- cell search is page-local, not result-set-wide
- browser search may become expensive on very large projects because it builds table indexes
- dbt SQL resolution is intentionally partial (`ref`, `source`, `var` only)
- history editing does not re-run queries yet

## Current Architecture Map

### Main flow
1. `src/qmb/cli.py`
   - parses user input
   - builds `QueryRequest`
   - resolves SQL
   - executes query
   - optionally exports
   - optionally launches TUI

2. Resolution layer
   - `src/qmb/sql/loader.py` for plain SQL or file loading
   - `src/qmb/dbt/manifest.py` for manifest discovery/loading
   - `src/qmb/dbt/selector.py` for model lookup
   - `src/qmb/dbt/resolver.py` for `ref` / `source` / `var` resolution

3. BigQuery execution layer
   - `src/qmb/bigquery/client.py` creates the BigQuery client
   - `src/qmb/bigquery/executor.py` runs queries and returns a handle
   - `src/qmb/bigquery/pager.py` pages through results
   - `src/qmb/bigquery/exporters.py` exports all rows
   - `src/qmb/bigquery/history.py` loads recent query jobs
   - `src/qmb/bigquery/browser.py` loads metadata and formats browser details

4. TUI layer
   - `src/qmb/tui/app.py` renders results
   - handles browser, search, export picker, history picker, editor integration, and pagination

5. Shared types
   - `src/qmb/types.py`

### Current dependency shape
- CLI depends on almost everything
- TUI depends directly on BigQuery/browser/export/history/pager modules
- dbt depends on manifest + selector + SQL normalization
- pager/exporter/browser/history depend directly on the BigQuery SDK

### Architectural interpretation
Current structure is best described as:
- thin package structure
- implicit application layer inside CLI and TUI
- infrastructure adapters directly called from UI/orchestration code

This is workable, but it is where future decoupling pressure will come from.

## Refactor Priority List

Below is a no-behavior-change refactor order, with low-hanging fruit first.

### Priority 1 — low hanging fruit
- [ ] Fix README / CLI mismatch (`browse` command vs `--browser-only` / `--browse`)
- [ ] Surface `history` consistently in README/help/examples
- [ ] Remove dead or misleading design leftovers such as unused `InputMode.BROWSER`
- [ ] Replace runtime `assert` statements used for control flow with explicit errors where appropriate
- [ ] Add a short architecture section to `README.md` so the code structure is easier to understand
- [ ] Add more characterization tests around CLI resolution flow before refactoring internals

### Priority 2 — small structural cleanups
- [ ] Extract SQL resolution orchestration from `src/qmb/cli.py` into an application-level module/service
- [ ] Extract execution orchestration from `src/qmb/cli.py` into a small use-case function/service
- [ ] Introduce typed value objects for schema fields / table references instead of raw dicts and strings
- [ ] Separate browser metadata fetching from browser formatting utilities
- [ ] Separate editor integration (`nvim` opening / temp files) from `QueryResultApp`

### Priority 3 — highest payoff refactor
- [ ] Split `QueryResultApp` responsibilities into focused components/controllers
- [ ] Isolate keybinding/pending-key handling from page rendering and picker logic
- [ ] Isolate browser state + behavior from result-table state + behavior
- [ ] Isolate export/history picker flows into dedicated helpers/components
- [ ] Reduce direct infrastructure calls from the TUI to narrower service interfaces

### Priority 4 — medium-term architecture improvements
- [ ] Replace the single broad `QueryRequest` with smaller request/config objects grouped by concern
- [ ] Define explicit application use cases: resolve query, execute query, export results, browse catalog, load history
- [ ] Make dbt feel optional at the application boundary, not only at the package boundary
- [ ] Clarify which modules are core logic and which are adapters to external systems

### Priority 5 — later / optional
- [ ] Revisit whether dbt should become a true feature plugin/extension boundary
- [ ] Consider stronger domain typing only where it genuinely improves clarity
- [ ] Consider session/result navigation architecture when implementing query re-execution and history replay

## Suggested Follow-Up Order

If the goal is “make it easier to read and decouple without changing behavior”, the most pragmatic order is:
1. docs/test alignment
2. cleanup of dead design leftovers
3. extract orchestration from CLI
4. split TUI responsibilities
5. tighten types and boundaries
6. only then reconsider dbt as extension/plugin
