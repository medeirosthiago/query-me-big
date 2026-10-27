"""qmb configuration helpers."""

from __future__ import annotations

import os
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

DEFAULT_REMOTE_ARCHIVE_PREVIEW_ROWS = 500
DEFAULT_REMOTE_ARCHIVE_NAME = "remote"
DEFAULT_WEB_HOST = "127.0.0.1"
DEFAULT_WEB_PORT = 8850

ARCHIVE_NAME_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")
RESERVED_ARCHIVE_NAMES = frozenset({"local"})


class ConfigError(ValueError):
    """Raised when ``~/.qmb/config.toml`` (or its env overrides) is invalid."""


@dataclass(frozen=True)
class RemoteArchiveConfig:
    """One named remote archive root.

    The primary archive (``[remote_archive]``) is read/write; every
    ``[[remote_archives]]`` entry is read-only.
    """

    name: str
    uri: str
    primary: bool = False


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


def remote_archives(destination: str | None = None) -> list[RemoteArchiveConfig]:
    """Return every configured remote archive, primary first.

    The primary archive's URI follows :func:`remote_archive_uri` precedence
    (CLI destination, ``QMB_REMOTE_ARCHIVE_URI``, ``[remote_archive].uri``)
    and its name comes from ``[remote_archive].name`` (default ``remote``).
    Each ``[[remote_archives]]`` entry follows, in file order. Raises
    :class:`ConfigError` naming the offending entry on invalid names/URIs or
    duplicates.
    """
    config = load_config()
    archives: list[RemoteArchiveConfig] = []

    primary_uri = remote_archive_uri(destination)
    if primary_uri is not None:
        remote_config = config.get("remote_archive")
        name = DEFAULT_REMOTE_ARCHIVE_NAME
        if isinstance(remote_config, dict) and remote_config.get("name") is not None:
            name = remote_config["name"]
        label = "[remote_archive]"
        archives.append(
            RemoteArchiveConfig(
                name=_validated_archive_name(name, label),
                uri=_validated_archive_uri(primary_uri, label),
                primary=True,
            )
        )

    extras = config.get("remote_archives", [])
    if not isinstance(extras, list):
        raise ConfigError("[[remote_archives]] must be an array of tables.")
    for position, entry in enumerate(extras, start=1):
        label = f"[[remote_archives]] entry {position}"
        if not isinstance(entry, dict):
            raise ConfigError(f"{label} must be a table with name and uri.")
        if isinstance(entry.get("name"), str):
            label = f"{label} ({entry['name']!r})"
        archives.append(
            RemoteArchiveConfig(
                name=_validated_archive_name(entry.get("name"), label),
                uri=_validated_archive_uri(entry.get("uri"), label),
            )
        )

    seen_names: dict[str, RemoteArchiveConfig] = {}
    seen_uris: dict[str, RemoteArchiveConfig] = {}
    for archive in archives:
        if archive.name in seen_names:
            raise ConfigError(f"Duplicate remote archive name {archive.name!r}.")
        uri_key = archive.uri.rstrip("/")
        if uri_key in seen_uris:
            raise ConfigError(
                f"Remote archives {seen_uris[uri_key].name!r} and {archive.name!r} "
                f"share the same uri {archive.uri!r}."
            )
        seen_names[archive.name] = archive
        seen_uris[uri_key] = archive
    return archives


def remote_archive_named(
    name: str, destination: str | None = None
) -> RemoteArchiveConfig:
    """Return the configured remote archive called ``name``."""
    archives = remote_archives(destination)
    for archive in archives:
        if archive.name == name:
            return archive
    known = ", ".join(archive.name for archive in archives) or "none configured"
    raise ConfigError(f"Unknown remote archive {name!r} (known: {known}).")


def _validated_archive_name(value: Any, label: str) -> str:
    if not isinstance(value, str) or not ARCHIVE_NAME_PATTERN.match(value):
        raise ConfigError(
            f"{label}: name must match {ARCHIVE_NAME_PATTERN.pattern} (got {value!r})."
        )
    if value in RESERVED_ARCHIVE_NAMES:
        raise ConfigError(f"{label}: name {value!r} is reserved.")
    return value


def _validated_archive_uri(value: Any, label: str) -> str:
    parsed = urlparse(value) if isinstance(value, str) else None
    if parsed is None or parsed.scheme != "gs" or not parsed.netloc:
        raise ConfigError(f"{label}: uri must look like gs://bucket/prefix/ (got {value!r}).")
    return value


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
