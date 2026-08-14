"""Shared pytest fixtures for the qmb test suite."""

from __future__ import annotations

import pytest

from qmb import config


@pytest.fixture(autouse=True)
def _isolate_qmb_config(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate every test from the developer's real ``~/.qmb/config.toml`` and env.

    Without this, tests that hit job-not-found paths (which resolve the remote
    archive URI) would fall through to real ambient config/env, potentially
    triggering real network calls to a remote archive.
    """
    monkeypatch.delenv("QMB_REMOTE_ARCHIVE_URI", raising=False)
    monkeypatch.delenv("QMB_REMOTE_ARCHIVE_PREVIEW_ROWS", raising=False)
    monkeypatch.delenv("QMB_WEB_HOST", raising=False)
    monkeypatch.delenv("QMB_WEB_PORT", raising=False)
    monkeypatch.setattr(
        config, "default_config_path", lambda: tmp_path / "_no_such_qmb_config.toml"
    )
