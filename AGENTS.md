# Agent Instructions for qmb

## Project Overview

qmb (Query Me Big) is a BigQuery CLI with a vim-style Textual TUI. It supports ad-hoc SQL, `.sql` files (with optional dbt resolution), and dbt model queries.

## Architecture

- `src/qmb/cli.py` — Typer CLI entrypoint, orchestrates resolve → execute → export/TUI
- `src/qmb/tui/app.py` — Textual app with vim-style keybindings, inline bottom pickers, and nvim integration
- `src/qmb/bigquery/` — BigQuery client, executor, pager, and exporters
- `src/qmb/dbt/` — dbt manifest loading and SQL resolution
- `src/qmb/sql/` — SQL file loading
- `src/qmb/types.py` — Shared dataclasses and enums

## Conventions

- Python 3.11+, managed with `uv`
- Linting: `ruff` with isort, line-length 100
- No command palette, no Textual header, no footer — minimal chrome
- All interactive pickers use the same bottom-panel pattern: `Vertical > Input + OptionList`
- Multi-key shortcuts (e.g., `yw`, `xc`, `gg`) use a pending-key state machine with 400ms timeout in `on_key`
- Inspect actions (`e`, `s`, `d`) open nvim read-only via `app.suspend()` and clean up temp files
- The user uses nvim, not vim

## Key Design Decisions

- Row number column (`#`) is display-only — cursor auto-skips it, search/copy ignore it
- `_data_col()` maps DataTable column index to data column index (subtracts 1 for the `#` column)
- Export filenames use `YYYY-MM-DD_HH-MM-SS.ext` format
- No modals — everything is either an inline bottom picker or opens in nvim
- dbt auto-detection: `--resolve-dbt` is auto-enabled when file is inside a dbt project or `DBT_MODEL_PATH`/`DBT_PROJECT_DIR` env vars are set
- File-to-model matching: `resolve_file_to_model()` matches `.sql` files to manifest nodes via `original_file_path`, uses `compiled_code` when available, falls back to `raw_code` with config blocks stripped
