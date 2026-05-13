"""Format enum and Formatter protocol."""

from __future__ import annotations

import enum
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from qmb.application.outcomes import ExecutionOutcome
    from qmb.types import QueryRequest


class Format(enum.Enum):
    """User-facing output format selection."""

    JSON = "json"
    CSV = "csv"
    TABLE = "table"
    TUI = "tui"

    @classmethod
    def parse(cls, value: str) -> Format:
        """Parse a string from the CLI, raising :class:`ValueError` on miss."""
        try:
            return cls(value.lower())
        except ValueError as e:
            valid = ", ".join(f.value for f in cls)
            raise ValueError(f"Invalid format: {value!r}. Use one of: {valid}.") from e


class Formatter(Protocol):
    """Renders a query execution result for one output channel.

    A formatter is given the already-executed :class:`ExecutionOutcome`
    and the originating :class:`QueryRequest` (for context such as
    page size or no-tui) and is responsible for writing the result to
    stdout / launching the TUI / etc.
    """

    def render_run(self, outcome: ExecutionOutcome, request: QueryRequest) -> None:
        """Render the result of a ``qmb run`` invocation."""
        ...
