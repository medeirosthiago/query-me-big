"""Clipboard copy with a single failure path.

Wraps :mod:`pyperclip` so callers see a single ``ClipboardUnavailable``
exception type instead of having to import ``pyperclip`` themselves.
"""

from __future__ import annotations


class ClipboardUnavailable(RuntimeError):
    """Raised when the system clipboard cannot be accessed."""


def copy(text: str) -> None:
    """Copy *text* to the system clipboard.

    Raises:
        ClipboardUnavailable: if the system clipboard is not accessible.
    """
    import pyperclip

    try:
        pyperclip.copy(text)
    except pyperclip.PyperclipException as exc:
        raise ClipboardUnavailable(str(exc)) from exc
