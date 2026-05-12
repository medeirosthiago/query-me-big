"""Tests for ``qmb.integrations.editor``."""

from __future__ import annotations

from pathlib import Path

import pytest

from qmb.integrations.editor import build_editor_command, temp_file_for_editor


def test_build_editor_command_nvim_read_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EDITOR", "nvim")
    cmd = build_editor_command("/tmp/foo.txt", read_only=True)
    assert cmd == ["nvim", "-R", "/tmp/foo.txt"]


def test_build_editor_command_vim_read_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EDITOR", "vim")
    cmd = build_editor_command("/tmp/foo.txt", read_only=True)
    assert cmd == ["vim", "-R", "/tmp/foo.txt"]


def test_build_editor_command_vi_read_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EDITOR", "vi")
    cmd = build_editor_command("/tmp/foo.txt", read_only=True)
    assert cmd == ["vi", "-R", "/tmp/foo.txt"]


def test_build_editor_command_no_read_only_flag_when_writable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EDITOR", "nvim")
    cmd = build_editor_command("/tmp/foo.txt", read_only=False)
    assert cmd == ["nvim", "/tmp/foo.txt"]


def test_build_editor_command_non_vim_editor_skips_R_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EDITOR", "nano")
    cmd = build_editor_command("/tmp/foo.txt", read_only=True)
    assert cmd == ["nano", "/tmp/foo.txt"]


def test_build_editor_command_default_is_nvim(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EDITOR", raising=False)
    cmd = build_editor_command("/tmp/foo.txt", read_only=True)
    assert cmd == ["nvim", "-R", "/tmp/foo.txt"]


def test_build_editor_command_honors_editor_with_args(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EDITOR", "nvim --clean")
    cmd = build_editor_command("/tmp/foo.txt", read_only=True)
    assert cmd == ["nvim", "--clean", "-R", "/tmp/foo.txt"]


def test_temp_file_for_editor_writes_content_and_cleans_up() -> None:
    captured: dict[str, str] = {}
    with temp_file_for_editor("hello world", suffix=".txt", prefix="qmb_test_") as path:
        p = Path(path)
        assert p.exists()
        assert p.name.startswith("qmb_test_")
        assert p.suffix == ".txt"
        assert p.read_text() == "hello world"
        captured["path"] = path
    assert not Path(captured["path"]).exists()


def test_temp_file_for_editor_cleans_up_on_exception() -> None:
    captured: dict[str, str] = {}
    with (
        pytest.raises(RuntimeError, match="boom"),
        temp_file_for_editor("content", suffix=".sql", prefix="qmb_test_") as path,
    ):
        captured["path"] = path
        assert Path(path).exists()
        raise RuntimeError("boom")
    assert not Path(captured["path"]).exists()
