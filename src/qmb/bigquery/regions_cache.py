"""On-disk cache of BigQuery region locations per project.

Regions in a BigQuery project are extremely stable (typically set
once and never touched), so caching them eliminates the ~1.7 s
``list_datasets`` discovery step on warm ``qmb browse --via-info-schema``
invocations.

Cache layout::

    ~/.qmb/cache/regions/<project>.json

Override the root with ``QMB_REGIONS_CACHE_DIR``. Delete the file (or
the directory) to force a fresh discovery on the next run.
"""

from __future__ import annotations

import json
import os
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

# 30 days. Regions almost never change; if you really want a fresh read,
# delete ~/.qmb/cache/regions/<project>.json.
DEFAULT_MAX_AGE = timedelta(days=30)

# Same character class allowed in GCP project IDs. Anything outside this
# would be a path-traversal concern, so we refuse to cache it.
_SAFE_PROJECT_RE = re.compile(r"^[a-zA-Z0-9_\-:.]+$")


def default_cache_dir() -> Path:
    """Return the directory used to store cached region lists."""
    if env_dir := os.environ.get("QMB_REGIONS_CACHE_DIR"):
        return Path(env_dir)
    return Path.home() / ".qmb" / "cache" / "regions"


def load_regions(
    project: str,
    *,
    max_age: timedelta = DEFAULT_MAX_AGE,
    cache_dir: Path | None = None,
) -> list[str] | None:
    """Return the cached region list for ``project`` if fresh, else ``None``.

    A missing file, corrupted JSON, or an entry older than ``max_age``
    all return ``None`` — the caller should then fall back to live
    discovery.
    """
    cache_path = _cache_path(project, cache_dir)
    if cache_path is None or not cache_path.is_file():
        return None
    try:
        payload = json.loads(cache_path.read_text())
        fetched_at = datetime.fromisoformat(payload["fetched_at"])
        locations = payload["locations"]
        if not isinstance(locations, list) or not all(isinstance(x, str) for x in locations):
            return None
    except (json.JSONDecodeError, KeyError, TypeError, ValueError, OSError):
        return None

    if datetime.now(UTC) - fetched_at > max_age:
        return None
    return sorted(set(locations))


def save_regions(
    project: str,
    locations: list[str],
    *,
    cache_dir: Path | None = None,
) -> None:
    """Persist a region list for ``project``. Failures are silent.

    Cache writes must never break the user's query — if the directory
    can't be created, we just skip the cache for this run.
    """
    cache_path = _cache_path(project, cache_dir)
    if cache_path is None:
        return
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "project": project,
            "fetched_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "locations": sorted(set(locations)),
        }
        # Atomic write: write to a sibling tmp file, then rename.
        tmp_path = cache_path.with_suffix(cache_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, indent=2))
        tmp_path.replace(cache_path)
    except OSError:
        return


def _cache_path(project: str, cache_dir: Path | None) -> Path | None:
    if not _SAFE_PROJECT_RE.match(project):
        return None
    base = cache_dir if cache_dir is not None else default_cache_dir()
    return base / f"{project}.json"
