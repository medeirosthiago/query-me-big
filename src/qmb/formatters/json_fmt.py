"""Structured JSON formatter for headless / agent consumption.

Schema written to stdout for ``qmb run`` (non dry-run)::

    {
      "stats": {
        "total_rows": int,
        "bytes_processed": int,
        "execution_seconds": float,
        "job_id": str,
        "project": str,
        "location": str,
        "source_label": str
      },
      "schema": [{"name": str, "type": str, "mode": str}, ...],
      "rows":   [{<column>: <value>, ...}, ...],
      "archive": {"qmb_job_id": str | null},
      "export":  {"path": str, "rows": int} | null,
      "dry_run": false
    }

For dry runs::

    {
      "dry_run": true,
      "sql": str,
      "stats": {"bytes_processed": int, "source_label": str},
      "schema": []
    }

Values are JSON-coerced via :func:`qmb.bigquery.pager.json_default`
(dates → ISO 8601 strings, decimals → floats, bytes → hex, etc.).
"""

from __future__ import annotations

import json
import sys
from typing import TYPE_CHECKING, Any, TextIO

from qmb.bigquery.pager import iter_all_rows, json_default

if TYPE_CHECKING:
    from qmb.application.outcomes import ExecutionOutcome
    from qmb.types import QueryRequest


class JsonFormatter:
    """Writes a single JSON object describing the run to stdout."""

    def __init__(self, stream: TextIO | None = None) -> None:
        self.stream = stream or sys.stdout

    def render_run(self, outcome: ExecutionOutcome, request: QueryRequest) -> None:
        payload = self._build_payload(outcome, request)
        json.dump(payload, self.stream, default=json_default)
        self.stream.write("\n")
        self.stream.flush()

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------

    def _build_payload(
        self, outcome: ExecutionOutcome, request: QueryRequest
    ) -> dict[str, Any]:
        resolved = outcome.resolved
        handle = outcome.handle

        schema = [field.to_mapping() for field in handle.schema_fields]

        if outcome.dry_run:
            return {
                "dry_run": True,
                "sql": resolved.sql,
                "stats": {
                    "bytes_processed": handle.bytes_processed,
                    "source_label": resolved.source_label,
                },
                "schema": schema,
            }

        rows = self._collect_rows(outcome)
        return {
            "dry_run": False,
            "stats": {
                "total_rows": handle.total_rows,
                "bytes_processed": handle.bytes_processed,
                "execution_seconds": handle.execution_seconds,
                "job_id": handle.job_id,
                "project": handle.project,
                "location": handle.location,
                "source_label": resolved.source_label,
            },
            "schema": schema,
            "rows": rows,
            "archive": {
                "qmb_job_id": (
                    outcome.archived_job.qmb_job_id if outcome.archived_job else None
                ),
                "session_id": (
                    outcome.archived_job.session_id if outcome.archived_job else None
                ),
                "parent_job_id": (
                    outcome.archived_job.parent_job_id if outcome.archived_job else None
                ),
                "error": outcome.archive_error,
            },
            "export": (
                {
                    "path": str(outcome.exported_path),
                    "rows": outcome.exported_rows,
                }
                if outcome.exported_path is not None
                else None
            ),
        }

    def _collect_rows(self, outcome: ExecutionOutcome) -> list[dict[str, Any]]:
        # ``iter_all_rows`` streams in chunks of 5000; the materialized
        # list is bounded by the actual BigQuery result size. Agents that
        # need to cap output should use ``LIMIT`` in the SQL or a future
        # ``--limit`` flag (Phase 10E).
        if outcome.handle.total_rows == 0 or outcome.client is None:
            return []
        return list(iter_all_rows(outcome.client, outcome.handle))
