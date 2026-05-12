"""Help screen for the qmb TUI."""

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import Static

HELP_TEXT = """\
qmb — Keyboard Shortcuts
========================================

Navigation
  h/j/k/l       Move left/down/up/right
  Arrow keys    Move left/down/up/right
  gg            Go to first row
  G             Go to last row
  0             Go to first column
  $             Go to last column
  n             Next page (or next match)
  N             Previous match
  p             Previous page
  Home          First page
  End           Last page

Search
  /             Search cell values
  f             Search column name
  n/N           Next/previous match
  Escape        Clear search

Browser
  b             Toggle dataset browser
  /             Search datasets and tables
  Enter / d     Open dataset or table details in nvim
  h/l           Collapse/expand selected dataset
  gg / G        First/last browser item
  Escape        Close browser (or exit browser search)

Yank (copy)
  yw            Copy selected cell value
  yc            Copy selected row as CSV
  yj            Copy selected row as JSON

Inspect
  e             Open cell in nvim (read-only)
  s             Open full SQL query in nvim
  d             Open job details in nvim

Export
  x             Open export picker
  xc            Quick export to CSV
  xj            Quick export to JSON

History
  r             Browse recent query history

Other
  ?             Show this help
  Ctrl-Q        Quit
"""


class HelpScreen(Screen):
    """Simple scrollable help screen."""

    BINDINGS = [
        Binding("escape", "app.pop_screen", "Back", show=False),
    ]
    DEFAULT_CSS = """
    HelpScreen { padding: 1 2; }
    HelpScreen Static { width: 1fr; }
    """

    def compose(self) -> ComposeResult:
        yield Static(HELP_TEXT)
