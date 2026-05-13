"""CSV formatter for headless consumption.

Writes a CSV with a header row drawn from the result schema, followed
by all rows streamed from BigQuery. Values are coerced via
:func:`qmb.bigquery.pager.json_default` so dates, decimals, bytes,
and nested arrays/structs serialize predictably (dates → ISO 8601,
decimals → float, bytes → hex, dict/list → JSON string).
"""

from __future__ import annotations

import csv
import json
import sys
from typing import TYPE_CHECKING, Any, TextIO

from qmb.bigquery.pager import iter_all_rows, json_default

if TYPE_CHECKING:
    from qmb.application.outcomes import ExecutionOutcome
    from qmb.types import QueryRequest


class CsvFormatter:
    """Writes the result rows as CSV to stdout."""

    def __init__(self, stream: TextIO | None = None) -> None:
        self.stream = stream or sys.stdout

    def render_run(self, outcome: ExecutionOutcome, request: QueryRequest) -> None:
        if outcome.dry_run:
            # Dry runs have no rows; emit a single status row so the
            # output is still machine-parseable and never empty.
            writer = csv.writer(self.stream)
            writer.writerow(["dry_run", "bytes_processed", "source_label"])
            writer.writerow(
                [
                    "true",
                    outcome.handle.bytes_processed,
                    outcome.resolved.source_label,
                ]
            )
            self.stream.flush()
            return

        handle = outcome.handle
        columns = [field.name for field in handle.schema_fields]

        writer = csv.writer(self.stream)
        writer.writerow(columns)

        if handle.total_rows == 0 or outcome.client is None:
            self.stream.flush()
            return

        for row in iter_all_rows(outcome.client, handle):
            writer.writerow([_csv_cell(row.get(col)) for col in columns])

        self.stream.flush()


def _csv_cell(value: Any) -> Any:
    """Coerce a single cell value to a CSV-safe string."""
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=json_default)
    coerced = json_default(value) if not isinstance(value, (str, int, float, bool)) else value
    return coerced
