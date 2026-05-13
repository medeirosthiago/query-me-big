"""Output renderers for qmb commands.

A :class:`Formatter` consumes a typed result payload (such as
:class:`~qmb.application.outcomes.ExecutionOutcome`) and writes it to
stdout / stderr in a specific shape (JSON, CSV, table, TUI, ...).

The CLI is the composition root: it parses ``--format``, picks the
matching formatter from :func:`get_formatter`, and hands the formatter
the typed payload. Application logic never imports a formatter.
"""

from qmb.formatters.base import Format, Formatter
from qmb.formatters.csv_fmt import CsvFormatter
from qmb.formatters.json_fmt import JsonFormatter
from qmb.formatters.table_fmt import TableFormatter
from qmb.formatters.tui_fmt import TuiFormatter

__all__ = [
    "CsvFormatter",
    "Format",
    "Formatter",
    "JsonFormatter",
    "TableFormatter",
    "TuiFormatter",
    "get_formatter",
]


def get_formatter(fmt: Format) -> Formatter:
    """Return the :class:`Formatter` matching a :class:`Format` value."""
    if fmt is Format.JSON:
        return JsonFormatter()
    if fmt is Format.CSV:
        return CsvFormatter()
    if fmt is Format.TABLE:
        return TableFormatter()
    if fmt is Format.TUI:
        return TuiFormatter()
    raise ValueError(f"Unknown format: {fmt!r}")
