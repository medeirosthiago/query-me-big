"""qmb configuration helpers."""

from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Any

DEFAULT_REMOTE_ARCHIVE_URI = "gs://data-platform-moises-temp/qmb/"
DEFAULT_REMOTE_ARCHIVE_PREVIEW_ROWS = 500


def default_config_path() -> Path:
    """Return the default qmb user config path."""
    return Path.home() / ".qmb" / "config.toml"


def load_config(path: Path | None = None) -> dict[str, Any]:
    """Load qmb config from TOML, returning an empty mapping when absent."""
    config_path = path or default_config_path()
    if not config_path.exists():
        return {}
    with config_path.open("rb") as f:
        data = tomllib.load(f)
    return data if isinstance(data, dict) else {}


def remote_archive_uri(destination: str | None = None) -> str:
    """Resolve the remote archive destination URI.

    Precedence is CLI destination, ``QMB_REMOTE_ARCHIVE_URI``,
    ``~/.qmb/config.toml``, then qmb's built-in shared GCS location.
    """
    if destination:
        return destination
    if env_uri := os.environ.get("QMB_REMOTE_ARCHIVE_URI"):
        return env_uri
    config = load_config()
    remote_config = config.get("remote_archive")
    if isinstance(remote_config, dict):
        uri = remote_config.get("uri")
        if isinstance(uri, str) and uri.strip():
            return uri
    return DEFAULT_REMOTE_ARCHIVE_URI


def remote_archive_preview_rows() -> int:
    """Resolve how many locally archived preview rows should be published."""
    if env_value := os.environ.get("QMB_REMOTE_ARCHIVE_PREVIEW_ROWS"):
        return _positive_int(env_value, DEFAULT_REMOTE_ARCHIVE_PREVIEW_ROWS)
    config = load_config()
    remote_config = config.get("remote_archive")
    if isinstance(remote_config, dict):
        return _positive_int(
            remote_config.get("preview_rows"),
            DEFAULT_REMOTE_ARCHIVE_PREVIEW_ROWS,
        )
    return DEFAULT_REMOTE_ARCHIVE_PREVIEW_ROWS


def _positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default
