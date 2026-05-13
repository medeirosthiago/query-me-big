# TODO

Outstanding work for qmb. The big refactor plan (REFACT.md) finished with
the 0.3.0 release; what remains are smaller, independent improvements.

## Search
- [ ] Result-set-wide search instead of page-local (`/` currently only
  scans the rows on the current page)
- [ ] Highlight matching cells in the DataTable after search

## Browser
- [ ] Show dataset/table permission lists in browser details (principal,
  role, inherited/direct)
- [ ] Reconsider browser indexing strategy for very large projects
  (today's per-dataset table index can be slow when a project has many
  datasets / tables)

## Visual Mode
- [ ] `v` enters visual mode for selecting ranges of cells/rows
- [ ] Copy selection to clipboard (CSV/JSON)
- [ ] Export selection to file

## Query Re-execution & history navigation
- [ ] When opening SQL in nvim (`s`), allow editing and re-running the
  query via an nvim command
- [ ] Return to qmb with the new result after re-execution
- [ ] Navigate between previous results with `[` and `]`
- [ ] Cache query results per session (by job ID) for back/forward
  navigation
- [ ] Session / tree / fork navigation UI on top of the existing
  `session_id` and `parent_job_id` archive fields (data is persisted
  since 0.3.0, picker/visualization is the missing piece)

## Local archive policy
- [ ] Full result archive policy / config: `none | preview | full | auto`
  (today we archive only the first 500 rows as `preview.jsonl`)
- [ ] Row/byte caps + retention / GC for `~/.qmb/jobs/`
- [ ] Optional Parquet archive format for `result.*`

## Editor Integration
- [ ] nvim plugin for launching qmb from within neovim
- [ ] nvim quickfix/location list workflow populated from
  `qmb jobs list --format json`; open `query.sql` and
  `preview.jsonl` / result side-by-side
- [ ] Integration with other editors (VS Code, Emacs, Helix)

## Architecture
- [ ] Reconsider whether dbt becomes a true plugin/extension boundary
  (discoverable via Python entry points or explicit registration, rather
  than the current hardcoded `DbtSqlResolver` in `cli._execute`)
