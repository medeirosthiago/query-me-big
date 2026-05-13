"""Artifact helpers for the local qmb job archive."""

import json
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

from qmb.bigquery.pager import json_default


def write_jsonl_rows(
    output_path: Path,
    rows: Iterable[dict[str, Any]],
    *,
    fieldnames: list[str] | None = None,
) -> int:
    """Write rows as JSON Lines and return the number of rows written."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output_path.open("w", encoding="utf-8") as f:
        for row in rows:
            if fieldnames is not None:
                row = {name: row.get(name) for name in fieldnames}
            f.write(json.dumps(row, default=json_default))
            f.write("\n")
            count += 1
    return count


def read_jsonl_rows(path: Path) -> Iterator[dict[str, Any]]:
    """Read rows from a JSON Lines artifact."""
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)
