# qmb TUI Shortcuts

Complete keyboard reference for the qmb Textual TUI.

The TUI is opt-in via `qmb ... -t` / `qmb ... --tui`, or via TUI-first commands like `qmb jobs open <job-id>`.

## Result table / normal mode

| Shortcut | Action | Notes |
|---|---|---|
| `h` / `←` | Move left | The display-only `#` row-number column is skipped. |
| `j` / `↓` | Move down |  |
| `k` / `↑` | Move up |  |
| `l` / `→` | Move right |  |
| `gg` | Go to first row | Multi-key sequence. |
| `G` | Go to last row |  |
| `0` | Go to first data column | Skips the `#` row-number column. |
| `$` | Go to last column |  |
| `n` | Next page, or next search match | If cell-search matches exist, cycles to the next match. |
| `N` | Previous search match | Only when cell-search matches exist. |
| `p` | Previous page |  |
| `Home` | First page |  |
| `End` | Last page |  |
| `?` | Show in-app help |  |
| `Ctrl-Q` | Quit |  |

## Normal-mode yank / copy

| Shortcut | Action | Notes |
|---|---|---|
| `yw` | Copy current cell value | Uses the raw cell value, not the truncated display value. |
| `yc` | Copy current row as CSV | Includes headers. |
| `yj` | Copy current row as JSON | Pretty JSON object. |

## Visual mode

Visual mode selects a rectangular range of result cells. Press `v` on a cell to anchor the selection, then move the cursor; the selected rectangle is highlighted and the page bar shows `-- VISUAL -- (rows×columns)`.

| Shortcut | Action | Notes |
|---|---|---|
| `v` | Enter visual mode | Anchors at the current result cell. |
| `h` / `←` | Extend selection left | Rectangular selection. |
| `j` / `↓` | Extend selection down | Rectangular selection. |
| `k` / `↑` | Extend selection up | Rectangular selection. |
| `l` / `→` | Extend selection right | Rectangular selection. |
| `y` | Copy selection as TSV | Default; fires after the short multi-key timeout so `yc`, `yj`, and `yt` can be detected. Best for pasting into Sheets / Excel. |
| `yt` | Copy selection as TSV immediately | Same format as bare `y`, without waiting for the timeout. |
| `yc` | Copy selection as CSV | Includes selected column headers. |
| `yj` | Copy selection as JSON | JSON array of selected row objects. |
| `v` | Exit visual mode | Does not copy. |
| `Escape` | Exit visual mode | Does not copy. |

## Search and column picker

| Context | Shortcut | Action | Notes |
|---|---|---|---|
| Normal mode | `/` | Open cell-value search | Searches rows on the current page. |
| Cell search input | `Enter` | Run search | Dismisses input and jumps to the first match. |
| Cell search input | `Escape` | Dismiss search input |  |
| Search results | `n` | Next match | Cycles through matches. |
| Search results | `N` | Previous match | Cycles through matches. |
| Search results | `Escape` | Clear active search matches |  |
| Normal mode | `f` | Open column picker | Filter by column name. |
| Column picker | Type text | Filter columns |  |
| Column picker | `↑` / `↓` | Move highlighted option |  |
| Column picker | `Enter` | Jump to highlighted column |  |
| Column picker | `Escape` | Dismiss picker |  |

## Browser pane

| Context | Shortcut | Action | Notes |
|---|---|---|---|
| Normal mode | `b` | Toggle BigQuery browser | Unavailable for archived-only results. |
| Browser tree | `j` / `↓` | Move down |  |
| Browser tree | `k` / `↑` | Move up |  |
| Browser tree | `h` / `←` | Collapse selected dataset |  |
| Browser tree | `l` / `→` | Expand/select dataset | Loads tables if needed. |
| Browser tree | `gg` | First browser item |  |
| Browser tree | `G` | Last browser item |  |
| Browser tree | `Enter` | Open dataset/table details in nvim |  |
| Browser tree | `d` | Open dataset/table details in nvim | Same as `Enter`. |
| Browser tree | `/` | Open browser search | Dataset/table search. |
| Browser tree | `b` | Close browser |  |
| Browser tree | `Escape` | Close browser |  |
| Browser search | Type text | Filter datasets/tables | Live filtering. |
| Browser search | `Enter` | Close browser search, return to tree | Keeps the filter. |
| Browser search | `Escape` | Close browser search, return to tree | Keeps the filter. |

## Inspect

| Shortcut | Action | Notes |
|---|---|---|
| `e` | Open current cell in nvim | Read-only temp file; uses `.json` suffix when value parses as JSON. |
| `s` | Open resolved SQL query in nvim | Read-only currently. |
| `d` | Open job details in nvim |  |

## Export

| Shortcut | Action | Notes |
|---|---|---|
| `x` | Open export picker | Format → output path. |
| `xc` | Quick export full result to CSV | Timestamped filename. |
| `xj` | Quick export full result to JSON | Timestamped filename. |

## Export picker

| Shortcut | Action | Notes |
|---|---|---|
| `↑` / `↓` | Move highlighted format |  |
| `Enter` | Select format / submit output path | Two-phase picker. |
| `Escape` | Dismiss picker |  |

## History and archived jobs

| Context | Shortcut | Action | Notes |
|---|---|---|---|
| Normal mode | `H` | Browse recent BigQuery query history | Opens inline picker. |
| History picker | Type text | Filter recent queries | Query text, job id, date. |
| History picker | `↑` / `↓` | Move highlighted option |  |
| History picker | `Enter` | Open selected query in nvim | Opens read-write and then reopens picker. |
| History picker | `Escape` | Dismiss picker |  |
| Normal mode | `J` | Browse archived qmb jobs | Opens inline picker for local `~/.qmb/jobs`. |
| Jobs picker | Type text | Filter archived jobs | Label, job id, session id, SQL excerpt, date. |
| Jobs picker | `↑` / `↓` | Move highlighted option |  |
| Jobs picker | `Enter` | Load archived job preview | Swaps current result view. |
| Jobs picker | `Escape` | Dismiss picker |  |
