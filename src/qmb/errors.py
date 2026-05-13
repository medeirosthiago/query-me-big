"""Structured error emission for the headless CLI.

Whenever a command fails, qmb writes a single JSON object to stderr and
exits with a categorized code. Agents can parse the error type without
screen-scraping Rich-formatted output.

Schema (one JSON object per failure, terminated with a newline)::

    {
      "error": {
        "type": "<category>",
        "message": "<human-readable description>",
        "details": {"class": "<exception class name>", ...}
      }
    }

Exit codes:

* ``0``   success
* ``1``   user error (bad flag, missing file, ambiguous job id, etc.)
* ``2``   engine/internal error (BigQuery API failure, unexpected exception)
* ``3``   archive / local IO error (reserved; see Phase 10C surface-archive)
* ``130`` interrupted by user (SIGINT / Ctrl-C)
"""

from __future__ import annotations

import json
import sys
from typing import Any, NoReturn, TextIO

__all__ = [
    "EXIT_ENGINE_ERROR",
    "EXIT_INTERRUPTED",
    "EXIT_IO_ERROR",
    "EXIT_OK",
    "EXIT_USER_ERROR",
    "emit_json_error",
]


EXIT_OK = 0
EXIT_USER_ERROR = 1
EXIT_ENGINE_ERROR = 2
EXIT_IO_ERROR = 3
EXIT_INTERRUPTED = 130


def emit_json_error(
    *,
    type_: str,
    message: str,
    exit_code: int,
    details: dict[str, Any] | None = None,
    stream: TextIO | None = None,
) -> NoReturn:
    """Write a structured JSON error to ``stream`` (stderr) and exit.

    Always writes exactly one JSON object followed by ``\\n``; never
    prints anything else. Use :class:`SystemExit` semantics for code
    flow (i.e. nothing after this call runs).
    """
    out = stream if stream is not None else sys.stderr
    payload: dict[str, Any] = {"error": {"type": type_, "message": message}}
    if details:
        payload["error"]["details"] = details
    out.write(json.dumps(payload) + "\n")
    out.flush()
    sys.exit(exit_code)
