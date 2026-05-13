"""Agent/session metadata helpers for archived qmb jobs."""

from __future__ import annotations

import getpass
import json
import os
import socket
import subprocess
from pathlib import Path
from typing import Any

from qmb.types import AgentContext


def env_value(name: str) -> str | None:
    """Return a non-empty environment variable value, or ``None``."""
    value = os.environ.get(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def effective_session_id(session_id: str | None) -> str | None:
    """Return the CLI session id or the ``QMB_SESSION_ID`` fallback."""
    return session_id or env_value("QMB_SESSION_ID")


def build_agent_context(
    *,
    session_id: str | None,
    name: str | None = None,
    conversation_id: str | None = None,
    run_id: str | None = None,
    turn_id: str | None = None,
    task: str | None = None,
    tags: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
    cwd: Path | None = None,
) -> AgentContext:
    """Build a best-effort agent context for local archives.

    Explicit CLI values win over environment variables. Git/user/host metadata
    is discovered opportunistically and never raises if unavailable.
    """
    cwd = (cwd or Path.cwd()).resolve()
    git = discover_git_context(cwd)
    env_metadata = _metadata_from_env()
    merged_metadata = {**env_metadata, **(metadata or {})}
    merged_tags = [*_tags_from_env(), *(tags or [])]

    return AgentContext(
        name=name or env_value("QMB_AGENT_NAME"),
        session_id=session_id,
        conversation_id=conversation_id or env_value("QMB_AGENT_CONVERSATION_ID"),
        run_id=run_id or env_value("QMB_AGENT_RUN_ID"),
        turn_id=turn_id or env_value("QMB_AGENT_TURN_ID"),
        task=task or env_value("QMB_AGENT_TASK"),
        cwd=str(cwd),
        repo_root=git.get("repo_root"),
        git_branch=git.get("git_branch"),
        git_sha=git.get("git_sha"),
        git_dirty=git.get("git_dirty"),
        user=_safe_getuser(),
        host=_safe_hostname(),
        tags=_dedupe_preserve_order(merged_tags),
        metadata=merged_metadata,
    )


def discover_git_context(cwd: Path) -> dict[str, Any]:
    """Return git repository metadata for ``cwd`` when available."""
    repo_root = _git(cwd, "rev-parse", "--show-toplevel")
    if repo_root is None:
        return {"repo_root": None, "git_branch": None, "git_sha": None, "git_dirty": None}

    root = Path(repo_root)
    branch = _git(root, "branch", "--show-current")
    if not branch:
        branch = _git(root, "rev-parse", "--abbrev-ref", "HEAD")
    sha = _git(root, "rev-parse", "HEAD")
    status = _git(root, "status", "--porcelain")

    return {
        "repo_root": str(root),
        "git_branch": branch,
        "git_sha": sha,
        "git_dirty": bool(status) if status is not None else None,
    }


def _git(cwd: Path, *args: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", str(cwd), *args],
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    value = result.stdout.strip()
    return value or None


def _metadata_from_env() -> dict[str, Any]:
    raw = env_value("QMB_AGENT_META_JSON")
    if raw is None:
        return {}
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        return {"_invalid_QMB_AGENT_META_JSON": raw}
    if not isinstance(decoded, dict):
        return {"_invalid_QMB_AGENT_META_JSON": raw}
    return decoded


def _tags_from_env() -> list[str]:
    raw = env_value("QMB_AGENT_TAGS")
    if raw is None:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _safe_getuser() -> str | None:
    try:
        return getpass.getuser()
    except Exception:
        return None


def _safe_hostname() -> str | None:
    try:
        return socket.gethostname()
    except Exception:
        return None
