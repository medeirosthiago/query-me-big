# qmb — Query Me Big

A BigQuery CLI with a vim-style TUI, dbt model support, and export.

## Installation

Install the latest from GitHub:

```bash
uv tool install git+https://github.com/medeirosthiago/query-me-big.git
```

```bash
pipx install "git+https://github.com/medeirosthiago/query-me-big.git"
```

If you prefer plain pip:

```bash
pip install "git+https://github.com/medeirosthiago/query-me-big.git"
```

### Install a specific version

Pin to a release tag for reproducible installs:

```bash
uv tool install "git+https://github.com/medeirosthiago/query-me-big.git@v0.2.0"
pipx install "git+https://github.com/medeirosthiago/query-me-big.git@v0.2.0"
pip install   "git+https://github.com/medeirosthiago/query-me-big.git@v0.2.0"
```

See the [releases page](https://github.com/medeirosthiago/query-me-big/releases) for available versions.

Check the installed version at any time:

```bash
qmb --version
```

### Local development

```bash
uv sync
```

Requires Python 3.11+ and Google Cloud credentials configured (`gcloud auth application-default login`).

## Quick Examples

### Ad-hoc query

```bash
# count rows in a table
qmb "SELECT COUNT(*) FROM \`my-project.analytics.events\`"

# sample rows and browse in the TUI
qmb "SELECT * FROM \`my-project.analytics.orders\` WHERE status = 'shipped' LIMIT 500"

# dry-run to check cost before executing
qmb "SELECT * FROM \`my-project.warehouse.big_table\`" --dry-run

# export straight to CSV without opening the TUI
qmb "SELECT user_id, email FROM \`my-project.core.users\`" --export csv --out users.csv --no-tui
```

### dbt model

```bash
# query a dbt model (auto-discovers target/manifest.json)
qmb --model orders

# explicit manifest path
qmb --model orders --manifest /path/to/dbt/target/manifest.json

# override dbt variables
qmb --model orders --var start_date=2024-01-01 --var end_date=2024-12-31

# export a dbt model to parquet
qmb --model customers --export parquet --out customers.parquet --no-tui

# filter a big model with --where (wraps in a subquery at runtime, models untouched)
qmb --model events --where "event_date >= '2024-01-01' AND event_type = 'click'"
```

### Browser only

```bash
# open the dataset/table browser without running a query
qmb browse --project my-project

# with an explicit location
qmb browse --project my-project --location US
```

### Query history

```bash
# browse the last 7 days of query history in the TUI
qmb history

# look back further and cap the number of jobs fetched
qmb history --days 30 --limit 500 --project my-project --location US
```

## Usage

### Ad-hoc SQL

Run an inline query and browse results in the TUI:

```bash
qmb "SELECT * FROM \`project.dataset.table\` LIMIT 1000"
```

### Query from a `.sql` file

```bash
qmb --file queries/my_query.sql
```

If your `.sql` file contains dbt `ref()`, `source()`, or `var()` calls, resolve them with:

```bash
qmb --file queries/my_query.sql --resolve-dbt --manifest target/manifest.json
```

If `--manifest` is omitted, qmb auto-discovers `target/manifest.json` from the current directory and parent directories.

**Auto-detection:** When a `.sql` file lives inside a dbt project (parent `dbt_project.yml`) or `DBT_MODEL_PATH`/`DBT_PROJECT_DIR` env vars are set, `--resolve-dbt` is enabled automatically. If the file matches a manifest node, qmb uses the compiled SQL (after `dbt compile`) or falls back to raw SQL with `ref()`/`source()`/`var()` resolution.

### dbt model

Query a dbt model using its compiled SQL from `manifest.json`:

```bash
qmb --model orders
qmb --model orders --manifest path/to/manifest.json
```

If `--manifest` is omitted, qmb looks for `target/manifest.json` in the current directory and parent directories.

Override dbt variables:

```bash
qmb --model orders --var start_date=2024-01-01 --var end_date=2024-12-31
```

When using `--model` with `--var`, qmb resolves the model SQL directly. If the model relies on other dbt Jinja macros, run `dbt compile --vars ...` first and query the compiled model without `--var`.

### Dry run

Validate a query and see estimated bytes without executing:

```bash
qmb "SELECT * FROM \`project.dataset.table\`" --dry-run
```

### Export from CLI

Export directly without opening the TUI:

```bash
qmb "SELECT 1" --export csv --out results.csv --no-tui
qmb --model orders --export json --out orders.json --no-tui
qmb --file query.sql --export parquet --out data.parquet --no-tui
```

If `--out` is omitted, defaults to `output.<ext>`.

### Browser only

Open qmb directly into the dataset/table explorer without executing a query:

```bash
qmb browse --project my-project
qmb browse --project my-project --location US
```

In browser-only mode, qmb opens straight into the left-side browser pane and uses it as the main view.

### Query history

Browse recent BigQuery jobs (from the Jobs API) in the TUI:

```bash
qmb history
qmb history --days 14 --limit 300
qmb history --project my-project --location US
```

Inside the TUI, press `H` to open the BigQuery history picker at any time. Selecting an entry opens the job's SQL in nvim (read-only).

### Archived qmb jobs

Every successful (non-dry-run) query is archived locally under `~/.qmb/jobs/<qmb_job_id>/` with its resolved SQL, schema, and a preview of the first rows. Inspect or replay them without touching BigQuery:

```bash
qmb jobs list                       # newest first
qmb jobs list --format json         # machine-readable
qmb jobs show <job>                 # metadata
qmb jobs sql <job>                  # print the archived SQL
qmb jobs paths <job> --format json  # absolute paths for editor integrations
qmb jobs open <job>                 # open the preview in the TUI
```

`<job>` accepts a full ID (`qmb_2026-05-13_13-04-32_a1b2c3`) or any unambiguous substring. Inside the TUI, press `J` to open the archived-jobs picker and switch the current view to any job's preview without re-running the query. Each row shows the date, row count, bytes processed, source label, short job ID, and the first part of the resolved SQL. The filter matches against any of these (including SQL text), so you can search for `users` or `model: orders` and narrow the list.

## Commands

| Command | Description |
|---|---|
| `qmb run` | Run a BigQuery query (also the default when no subcommand is given) |
| `qmb browse` | Open the dataset/table browser without running a query |
| `qmb history` | Browse recent BigQuery query history in the TUI |
| `qmb jobs list` | List local qmb job archives |
| `qmb jobs show <job>` | Show metadata for a local qmb job |
| `qmb jobs sql <job>` | Print the archived resolved SQL for a local qmb job |
| `qmb jobs paths <job>` | Print artifact paths for a local qmb job |
| `qmb jobs open <job>` | Open an archived qmb job preview in the TUI |

## CLI Options

Options below apply to `qmb run` (the default command). `qmb browse` accepts `--project` and `--location`. `qmb history` accepts `--days`, `--limit`, `--project`, `--location`, and `--page-size`.

| Option | Short | Description |
|---|---|---|
| `query` | | Positional SQL query argument |
| `--file` | `-f` | Path to a `.sql` file |
| `--model` | `-m` | dbt model name |
| `--manifest` | | Path to `manifest.json` |
| `--resolve-dbt` | | Resolve `ref`/`source`/`var` in SQL files |
| `--var` | `-v` | dbt variable override `key=value` (repeatable) |
| `--project` | | GCP project ID |
| `--location` | | BigQuery location (`US`, `EU`, etc.) |
| `--page-size` | | Rows per page in TUI (default: 200) |
| `--export` | `-e` | Export format: `csv`, `json`, or `parquet` |
| `--out` | `-o` | Export output path |
| `--no-tui` | | Skip TUI, just export or print summary |
| `--dry-run` | | Validate query without executing |
| `--where` | `-w` | WHERE clause appended to the resolved SQL |
| `--max-bytes-billed` | | Maximum bytes billed safety limit |

## Architecture

A short map of the codebase. See [`REVIEW.md`](REVIEW.md) for the full description and [`REFACT.md`](REFACT.md) for the planned cleanup.

- `src/qmb/cli.py` — Typer entrypoint. Parses input, builds a `QueryRequest`, then orchestrates resolve → execute → export/TUI.
- `src/qmb/sql/` — plain SQL and `.sql` file loading + normalization.
- `src/qmb/dbt/` — manifest discovery/loading, model selection, and `ref`/`source`/`var` resolution.
- `src/qmb/bigquery/` — thin adapters over the BigQuery SDK:
  - `client.py` builds the client
  - `executor.py` runs queries
  - `pager.py` pages through results
  - `exporters.py` writes CSV / JSON / Parquet
  - `history.py` lists recent jobs via the Jobs API
  - `browser.py` lists datasets/tables and formats details for the browser pane
- `src/qmb/tui/app.py` — Textual app with vim-style keybindings, inline bottom pickers, browser pane, and nvim integration.
- `src/qmb/types.py` — shared dataclasses and enums (`QueryRequest`, `ResolvedQuery`, `QueryResultHandle`, `PageResult`, `InputMode`, `ExportFormat`).

The rough dependency shape today: CLI depends on almost everything; the TUI talks directly to the BigQuery adapters; dbt sits behind its own module but is still wired into the core request shape.

## TUI Keyboard Shortcuts

### Navigation

| Key | Action |
|---|---|
| `h` `j` `k` `l` / Arrow keys | Move left/down/up/right |
| `gg` | Go to first row |
| `G` | Go to last row |
| `0` | Go to first column |
| `$` | Go to last column |
| `n` | Next page (or next search match) |
| `N` | Previous search match |
| `p` | Previous page |
| `Home` / `End` | First / last page |

### Search

| Key | Action |
|---|---|
| `/` | Search cell values |
| `f` | Search column name (filterable dropdown) |
| `n` / `N` | Next / previous match |
| `Escape` | Clear search |

### Browser

| Key | Action |
|---|---|
| `b` | Toggle dataset browser |
| `/` | Search datasets and tables |
| `Enter` / `d` | Open selected dataset or table details in nvim |
| `h` / `l` or Arrow Left / Right | Collapse / expand selected dataset |
| `j` / `k` or Arrow Down / Up | Move through browser items |
| `gg` / `G` | Go to first / last browser item |
| `Escape` | Exit browser search or close the browser |

### Yank (copy)

| Key | Action |
|---|---|
| `yw` | Copy selected cell value |
| `yc` | Copy selected row as CSV |
| `yj` | Copy selected row as JSON |

### Inspect

| Key | Action |
|---|---|
| `e` | Open cell in nvim (read-only, `.json` if valid JSON) |
| `s` | Open full SQL query in nvim |
| `d` | Open job details in nvim |

### Export

| Key | Action |
|---|---|
| `x` | Open export picker (format → path) |
| `xc` | Quick export to CSV |
| `xj` | Quick export to JSON |

### History

| Key | Action |
|---|---|
| `H` | Browse recent BigQuery query history |
| `J` | Browse archived qmb jobs (local `~/.qmb`) |

### Other

| Key | Action |
|---|---|
| `?` | Show all shortcuts |
| `Ctrl-Q` | Quit |

## Current limitations

A few things are intentionally limited in the current version:

- **Cell search is page-local.** `/` searches only the rows on the current page, not the full result set. Use `n` to jump pages.
- **dbt SQL resolution is partial.** Only `ref()`, `source()`, and `var()` are resolved from `.sql` files. For full Jinja / macro support, run `dbt compile` and query the compiled model.
- **Browser indexing can be slow on very large projects.** The browser builds a per-dataset table index up front, which can take a while when a project has many datasets or tables.

