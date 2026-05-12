"""Tests for ``qmb.integrations.clipboard``."""

from __future__ import annotations

import pyperclip
import pytest

from qmb.integrations import clipboard
from qmb.integrations.clipboard import ClipboardUnavailable


def test_copy_delegates_to_pyperclip(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[str] = []

    def fake_copy(text: str) -> None:
        captured.append(text)

    monkeypatch.setattr(pyperclip, "copy", fake_copy)
    clipboard.copy("hello")
    assert captured == ["hello"]


def test_copy_raises_clipboard_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(text: str) -> None:
        raise pyperclip.PyperclipException("no clipboard backend")

    monkeypatch.setattr(pyperclip, "copy", boom)

    with pytest.raises(ClipboardUnavailable, match="no clipboard backend"):
        clipboard.copy("hello")


def test_clipboard_unavailable_is_runtime_error() -> None:
    assert issubclass(ClipboardUnavailable, RuntimeError)
