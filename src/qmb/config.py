"""qmb configuration helpers."""

from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Any

DEFAULT_REMOTE_ARCHIVE_PREVIEW_ROWS = 500
DEFAULT_WEB_HOST = "127.0.0.1"
DEFAULT_WEB_PORT = 8850


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


def remote_archive_uri(destination: str | None = None) -> str | None:
    """Resolve the remote archive destination URI.

    Precedence is CLI destination, ``QMB_REMOTE_ARCHIVE_URI``,
    then ``~/.qmb/config.toml``. Remote archives are disabled when unset.
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
    return None


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


def web_host(host: str | None = None) -> str:
    """Resolve the ``qmb web`` bind host.

    Precedence is CLI ``host``, ``QMB_WEB_HOST``, then ``[web].host`` in
    ``~/.qmb/config.toml``, falling back to ``127.0.0.1``.
    """
    if host:
        return host
    if env_host := os.environ.get("QMB_WEB_HOST"):
        return env_host
    config = load_config()
    web_config = config.get("web")
    if isinstance(web_config, dict):
        value = web_config.get("host")
        if isinstance(value, str) and value.strip():
            return value
    return DEFAULT_WEB_HOST


def web_port(port: int | None = None) -> int:
    """Resolve the ``qmb web`` bind port.

    Precedence is CLI ``port``, ``QMB_WEB_PORT``, then ``[web].port`` in
    ``~/.qmb/config.toml``, falling back to ``8850``.
    """
    if port is not None:
        return port
    if env_port := os.environ.get("QMB_WEB_PORT"):
        return _positive_int(env_port, DEFAULT_WEB_PORT)
    config = load_config()
    web_config = config.get("web")
    if isinstance(web_config, dict):
        return _positive_int(web_config.get("port"), DEFAULT_WEB_PORT)
    return DEFAULT_WEB_PORT


def _positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default
