# qmb — Query Me Big

A headless-first BigQuery CLI. Every command prints structured JSON to stdout
so it can be piped into `jq`, consumed by agents, or scripted. A vim-style
Textual TUI is available as an opt-in renderer via `-t` / `--tui`. dbt model
resolution, local query archives, and CSV/JSON/Parquet export are first-class.

```bash
# headless by default — perfect for agents and pipelines
qmb "SELECT user_id, COUNT(*) FROM analytics.events GROUP BY 1 LIMIT 10" | jq '.rows'

# add -t to drop into the interactive TUI instead
qmb "SELECT * FROM analytics.events LIMIT 1000" -t
```

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


## Quick examples

```bash
# ad-hoc query → JSON
qmb "SELECT COUNT(*) FROM analytics.events"

# dbt model resolved through the manifest → JSON
qmb --model orders | jq '.rows'

# run a .sql file from disk
qmb --file queries/daily_active.sql

# dry-run for cost estimation (no execution, no archive)
qmb "SELECT * FROM warehouse.big_table" --dry-run | jq '.stats.bytes_processed'

# export to a file (JSON still printed on stdout as a summary)
qmb "SELECT * FROM core.users" --export csv --out users.csv

# explore the catalog
qmb browse | jq '.datasets[]'
qmb browse 'analytics_*' | jq '.matches'
qmb describe analytics.orders | jq '.table.schema'

# recent BigQuery history (Jobs API)
qmb history --days 14 | jq '.[].job_id'

# local qmb archive: every successful run is saved under ~/.qmb/jobs/<id>/
qmb jobs list --format json | jq '.[].qmb_job_id'
qmb jobs sql <id>

# tag a related batch of agent runs with a session id
qmb "SELECT 1" --session-id agent-42
qmb jobs list --format json --session-id agent-42

# drop into the TUI for any command that supports it
qmb "SELECT * FROM analytics.orders LIMIT 1000" -t
qmb history -t
qmb browse -t
```

## Commands

| Command | What it does |
|---|---|
| `qmb run [QUERY]` | Run a BigQuery query. Default `qmb` falls through to `run` when no other subcommand matches. |
| `qmb browse [PATTERN]` | List datasets/tables. With a fuzzy or glob pattern, filter to matches. |
| `qmb describe TARGET` | Print dataset or table metadata (BigQuery REST shape). |
| `qmb history` | Print recent BigQuery jobs from the Jobs API. |
| `qmb jobs list` | List local qmb job archives. |
| `qmb jobs show JOB_ID` | Print metadata for a local qmb job. |
| `qmb jobs sql JOB_ID` | Print the archived resolved SQL for a local qmb job. |
| `qmb jobs paths JOB_ID` | Print artifact paths for a local qmb job. |
| `qmb jobs open JOB_ID` | Open an archived qmb job's row preview in the TUI. |
| `qmb --version` / `-V` | Print the installed qmb version. |
| `qmb --help` | Top-level command list. Every subcommand also accepts `--help`. |

Every command supports `--help` for the full flag list.

## Command reference

### `qmb run`

Run a BigQuery query. Default output: a single JSON object on stdout. Pass
`-t` / `--tui` to open the Textual app instead. Exactly one of the positional
`QUERY`, `--file`, or `--model` must be provided.

| Flag | Short | Description |
|---|---|---|
| `QUERY` (positional) | | Inline SQL query. |
| `--file PATH` | `-f` | Read SQL from a `.sql` file. Use `-` to read from stdin. |
| `--model NAME` | `-m` | dbt model name; uses compiled SQL from the manifest. |
| `--manifest PATH` | | Path to `manifest.json` (auto-discovered if omitted). |
| `--resolve-dbt` / `--no-resolve-dbt` | | Resolve `ref()` / `source()` / `var()` in `.sql` files. Auto-enabled inside a dbt project. |
| `--var KEY=VALUE` | `-v` | Override a dbt variable (repeatable). |
| `--project ID` | | GCP project ID. |
| `--location US\|EU\|...` | | BigQuery location. |
| `--page-size N` | | Rows per page in the TUI (default `200`). |
| `--export csv\|json\|parquet` | `-e` | Also export full results to a file. |
| `--out PATH` | `-o` | Export output path (defaults to `output.<ext>`). |
| `--where CLAUSE` | `-w` | Wrap the resolved SQL in a subquery with this `WHERE`. |
| `--dry-run` | | Validate + estimate bytes without executing or archiving. |
| `--max-bytes-billed N` | | BigQuery safety limit (bytes). |
| `--session-id ID` | | Tag this run with an agent/session id. Persisted in the archive. |
| `--parent-job-id ID` | | Reference a prior qmb job this run derives from. |
| `--format json\|csv\|table\|tui` | | Override the output renderer (default `json`). |
| `--tui` | `-t` | Shortcut for `--format tui`. |

### `qmb browse [PATTERN]`

Inspect datasets and tables. Without a pattern, prints the project's full
dataset list. With a fuzzy or glob pattern, prints matching datasets and
tables. Pass `-t` to open the Textual browser pane.

| Flag | Short | Description |
|---|---|---|
| `PATTERN` (positional) | | Fuzzy match (e.g. `orders`) or glob (e.g. `analytics_*`). |
| `--project ID` | | GCP project ID. |
| `--location US\|EU\|...` | | BigQuery location. |
| `--tui` | `-t` | Open the interactive browser pane instead of printing JSON. |

### `qmb describe TARGET`

Print dataset or table metadata as JSON, mirroring `bq show --format prettyjson`.
The shape is the BigQuery REST API representation (schema, partitioning,
clustering, sizes, timestamps, labels, descriptions, ...).

| Flag | Description |
|---|---|
| `TARGET` (positional, required) | `dataset` or `dataset.table`. The BQ-native `project:dataset.table` shorthand is also accepted. |
| `--project ID` | GCP project ID. |
| `--location US\|EU\|...` | BigQuery location. |

### `qmb history`

Print recent BigQuery jobs (Jobs API) as a JSON array. Pass `-t` to open the
interactive picker.

| Flag | Short | Description |
|---|---|---|
| `--days N` | `-d` | Look back N days (default `7`). |
| `--limit N` | `-l` | Cap the number of jobs fetched (default `200`). |
| `--project ID` | | GCP project ID. |
| `--location US\|EU\|...` | | BigQuery location. |
| `--page-size N` | | Rows per page in the TUI (default `200`). |
| `--tui` | `-t` | Open the interactive history picker. |

### `qmb jobs list`

List local qmb job archives. JSON output is suitable for agent workflows.

| Flag | Short | Description |
|---|---|---|
| `--format text\|json` | | Output format (default `text`). |
| `--session-id ID` | | Filter to jobs tagged with this session id. |
| `--parent-job-id ID` | | Filter to jobs that descend from this parent qmb job id. |
| `--limit N` | `-l` | Cap the number of records (newest first). |

### `qmb jobs show JOB_ID`

Print metadata for one archived job.

| Flag | Description |
|---|---|
| `JOB_ID` (positional, required) | Full or unambiguous-prefix qmb job id. |
| `--format text\|json` | Output format (default `text`). |

### `qmb jobs sql JOB_ID`

Print the archived resolved SQL for a local qmb job — exact text that was
sent to BigQuery (post-dbt resolution, post-`--where` wrap).

### `qmb jobs paths JOB_ID`

Print the absolute filesystem paths to each artifact (`metadata.json`,
`query.sql`, `schema.json`, `preview.jsonl`).

| Flag | Description |
|---|---|
| `--format text\|json` | Output format (default `text`). |

### `qmb jobs open JOB_ID`

Open an archived job's row preview in the Textual TUI without re-running the
query. The interactive verb — always TUI-first.

| Flag | Description |
|---|---|
| `--page-size N` | Rows per page in the TUI (default `200`). |

## JSON output schemas

### `qmb run` — success

```json
{
  "dry_run": false,
  "stats": {
    "total_rows": 100,
    "bytes_processed": 12345,
    "execution_seconds": 1.23,
    "job_id": "<bigquery job id>",
    "project": "...",
    "location": "US",
    "source_label": "ad-hoc"
  },
  "schema": [{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}],
  "rows":   [{"id": 1}],
  "archive": {
    "qmb_job_id":    "20260101T120000-abc12345",
    "session_id":    null,
    "parent_job_id": null,
    "error":         null
  },
  "export":  null
}
```

`archive.qmb_job_id` matches the local archive entry under
`~/.qmb/jobs/<id>/` so an agent can immediately recover the resolved SQL,
schema, and the first 500 rows with `qmb jobs show <id>` / `qmb jobs sql <id>`.

### `qmb run` — dry run

```json
{
  "dry_run": true,
  "sql": "<resolved sql>",
  "stats": {"bytes_processed": 12345, "source_label": "ad-hoc"},
  "schema": []
}
```

### `qmb run --format csv`

A CSV with a header row drawn from the result schema, followed by all rows.

### Errors (`stderr`, every command)

Failures emit a single JSON object on **stderr** and exit with a categorized
code (see [Exit codes](#exit-codes)):

```json
{
  "error": {
    "type": "user_error",
    "message": "Invalid format: 'ndjson'. Use one of: json, csv, table, tui.",
    "details": {"class": "BadParameter"}
  }
}
```

`type` values: `user_error`, `engine_error`, `internal_error`, `interrupted`.

## Value coercion in JSON / CSV output

Row values from BigQuery are JSON-coerced as follows:

- Dates, datetimes, and times → ISO 8601 strings
- `NUMERIC` / `BIGNUMERIC` → floats
- `BYTES` → hex strings
- `STRUCT` / `ARRAY` → nested JSON (preserved as-is in JSON output; serialized
  as a JSON string in CSV cells)
- Any unknown type → `str(value)` as a fallback

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success. |
| `1` | User error: bad flag, missing file, unknown format, ambiguous or missing qmb job id. |
| `2` | Engine / internal error: BigQuery API failure, permission denied, unexpected exception. |
| `3` | Local IO / archive error (reserved). |
| `130` | Interrupted (Ctrl-C / SIGINT). |

Combined with the JSON error shape on stderr, this makes qmb safe to script:

```bash
if ! result=$(qmb "SELECT * FROM analytics.orders" 2>err.json); then
  echo "qmb failed: $(jq -r '.error.message' err.json)"
  exit 1
fi
echo "$result" | jq '.rows'
```

## dbt support

When the query input is a `.sql` file or a model name, qmb resolves dbt
references:

- **`qmb --model orders`** — looks up the model in `target/manifest.json` and
  uses its compiled SQL (after `dbt compile`). If the compiled SQL is absent,
  falls back to raw SQL with `ref()`, `source()`, and `var()` resolved.
- **`qmb --file path/to/query.sql --resolve-dbt`** — same resolution applied to
  an arbitrary `.sql` file.
- **Auto-detection** — `--resolve-dbt` is enabled automatically when the file
  lives inside a dbt project (a parent `dbt_project.yml` exists) or the
  `DBT_PROJECT_DIR` / `DBT_MODEL_PATH` env vars are set.
- **`--manifest PATH`** is optional; qmb auto-discovers `target/manifest.json`
  by walking up from the working directory.
- **`--var KEY=VALUE`** overrides individual dbt variables. Repeatable.

For full Jinja / macro support, run `dbt compile` and query the compiled
model — qmb's resolver intentionally only handles `ref` / `source` / `var`.

## Archived qmb jobs

Every successful (non-dry-run) query is archived locally under
`~/.qmb/jobs/<qmb_job_id>/` with:

```text
metadata.json    # qmb job id, BQ job id, project/location, bytes, rows, timing, source
query.sql        # exact resolved SQL sent to BigQuery
schema.json      # result schema
preview.jsonl    # first 500 rows for fast browsing / nvim preview
```

Inspect or replay without touching BigQuery:

```bash
qmb jobs list                            # newest first, text
qmb jobs list --format json              # machine-readable
qmb jobs list --session-id agent-42      # filter by agent session
qmb jobs show <id>                       # full metadata
qmb jobs sql <id>                        # archived resolved SQL
qmb jobs paths <id> --format json        # absolute paths for editor integrations
qmb jobs open <id>                       # browse the preview in the TUI
```

`<id>` accepts a full job id or any unambiguous substring. Inside the TUI,
press `J` to switch the current view to any archived job's preview without
re-running it. The picker filters against the date, row count, bytes
processed, source label, short job id, and the SQL text — so you can search
for `users` or `model: orders` and narrow the list.

## `--format` and renderers

`qmb run` supports four renderers:

| `--format` | What it produces |
|---|---|
| `json` (**default**) | One structured JSON object on stdout. |
| `csv` | CSV header + rows on stdout. |
| `table` | Rich status lines (no TUI, no JSON). |
| `tui` | Launches the Textual TUI (also reachable via `-t`). |

`qmb browse` and `qmb history` print JSON by default and accept `-t` for
their respective TUI panes; other commands have one natural output shape and
do not take `--format`.

## Headless by default; TUI is opt-in

The contract for every command:

- **No `--tui` flag → JSON to stdout, exit cleanly.** Safe for `jq`, agents,
  pipelines, cron.
- **`-t` or `--tui` flag → open the Textual TUI.** Only `qmb run`,
  `qmb history`, `qmb browse`, and `qmb jobs open` accept it.
- **Errors → JSON to stderr + categorized exit code.** Stdout stays clean.

`qmb jobs open` is the only command that's TUI-first by design; the verb
itself means interactive.

## Architecture

Layered, CLI-first. The TUI is one renderer among several. See
[`REVIEW.md`](REVIEW.md) and [`REFACT.md`](REFACT.md) for the full notes.

- `src/qmb/cli.py` — Typer entrypoint. Parses flags, builds a `QueryRequest`,
  picks resolvers, calls the application layer, hands the outcome to a
  formatter. Owns the structured JSON error handler.
- `src/qmb/application/` — pure orchestration (no Typer, no Textual):
  `pipeline.py` (`run_query_pipeline`), `resolver.py`, `protocols.py`
  (`SqlResolver`), `outcomes.py` (`ExecutionOutcome`).
- `src/qmb/formatters/` — the only place that turns an `ExecutionOutcome`
  into stdout: `json_fmt.py`, `csv_fmt.py`, `table_fmt.py`, `tui_fmt.py`.
- `src/qmb/errors.py` — `emit_json_error()` + exit-code constants.
- `src/qmb/sql/` — plain SQL and `.sql` file loading; `PlainSqlResolver`.
- `src/qmb/dbt/` — manifest discovery/loading, model selection, `ref` /
  `source` / `var` resolution; `DbtSqlResolver`.
- `src/qmb/bigquery/` — thin adapters over the BigQuery SDK: `client.py`,
  `executor.py`, `pager.py`, `exporters.py`, `history.py`, `catalog.py`,
  `catalog_search.py`, `catalog_format.py`.
- `src/qmb/jobs/` — local query archive: `store.py`, `models.py`,
  `artifacts.py`, `result_source.py`.
- `src/qmb/tui/` — Textual app with vim-style keybindings, inline bottom
  pickers, browser pane, history/jobs pickers, and nvim integration.
- `src/qmb/integrations/` — editor (nvim) and clipboard helpers.
- `src/qmb/types.py` — shared dataclasses and enums (`QueryRequest`,
  `ResolvedQuery`, `QueryResultHandle`, `PageResult`, `InputMode`,
  `ExportFormat`, `TableRef`, `SchemaField`).

The dependency arrow points one way: `cli` → `formatters` + `application` →
(`sql` ∪ `dbt` ∪ `bigquery` ∪ `jobs`). The TUI is a formatter; nothing in
`application` imports `tui`. dbt is wired in only via the `SqlResolver`
protocol — the core never imports `qmb.dbt`.

## TUI keyboard shortcuts

The TUI is opt-in via `-t` / `--tui` (or `qmb jobs open <id>`). Inside, the
keybindings are vim-style.

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

