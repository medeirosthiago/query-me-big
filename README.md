# qmb — Query Me Big

A BigQuery CLI with a vim-style TUI, dbt model support, and export.

## Installation

```bash
uv sync
```

Requires Python 3.11+ and Google Cloud credentials configured (`gcloud auth application-default login`).

## Usage

### Ad-hoc SQL

Run an inline query and browse results in the TUI:

```bash
qmb run --sql "SELECT * FROM \`project.dataset.table\` LIMIT 1000"
```

### Query from a `.sql` file

```bash
qmb run --file queries/my_query.sql
```

If your `.sql` file contains dbt `ref()`, `source()`, or `var()` calls, resolve them with:

```bash
qmb run --file queries/my_query.sql --resolve-dbt --manifest target/manifest.json
```

### dbt model

Query a dbt model using its compiled SQL from `manifest.json`:

```bash
qmb run --model orders
qmb run --model orders --manifest path/to/manifest.json
```

If `--manifest` is omitted, qmb looks for `target/manifest.json` in the current directory and parent directories.

Override dbt variables:

```bash
qmb run --model orders --var start_date=2024-01-01 --var end_date=2024-12-31
```

### Dry run

Validate a query and see estimated bytes without executing:

```bash
qmb run --sql "SELECT * FROM \`project.dataset.table\`" --dry-run
```

### Export from CLI

Export directly without opening the TUI:

```bash
qmb run --sql "SELECT 1" --export csv --out results.csv --no-tui
qmb run --model orders --export json --out orders.json --no-tui
qmb run --file query.sql --export parquet --out data.parquet --no-tui
```

If `--out` is omitted, defaults to `output.<ext>`.

## CLI Options

| Option | Short | Description |
|---|---|---|
| `--sql` | `-s` | Inline SQL query |
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
| `--max-bytes-billed` | | Maximum bytes billed safety limit |

## TUI Keyboard Shortcuts

### Navigation

| Key | Action |
|---|---|
| `h` `j` `k` `l` / Arrow keys | Move left/down/up/right |
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

### Other

| Key | Action |
|---|---|
| `?` | Show all shortcuts in nvim |
| `q` | Quit |
