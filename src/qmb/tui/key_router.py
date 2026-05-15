"""Vim-style multi-key sequence state machine.

Tracks pending first-keys (``y``, ``x``, ``g`` and visual-mode yank) so a
second key (``yw``, ``yc``, ``yj``, ``xc``, ``xj``, ``gg``) can complete a
sequence.
"""


class PendingKeyRouter:
    """Tracks vim-style multi-key sequences."""

    def __init__(self, *, timeout: float = 0.4) -> None:
        self.timeout = timeout
        self._pending: str | None = None

    @property
    def pending(self) -> str | None:
        return self._pending

    def start(self, key: str) -> None:
        self._pending = key

    def clear(self) -> None:
        self._pending = None

    def is_pending(self, key: str) -> bool:
        return self._pending == key
