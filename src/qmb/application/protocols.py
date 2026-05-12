"""Protocols defining how the application layer talks to the outside world.

The application layer (orchestration) speaks to SQL resolvers through the
:class:`SqlResolver` protocol. Concrete implementations live outside this
package — e.g. ``qmb.sql.resolver.PlainSqlResolver`` for raw SQL/files and
``qmb.dbt.integration.DbtSqlResolver`` for dbt-aware resolution.

Keeping these protocols in the application layer means ``qmb.application``
does not depend on any concrete resolver implementation; the dependency
direction points inward, from adapters to abstractions.
"""

from dataclasses import dataclass
from typing import Protocol

from qmb.types import QueryRequest, ResolvedQuery

__all__ = ["ResolutionTrace", "SqlResolver"]


@dataclass(frozen=True)
class ResolutionTrace:
    """Optional metadata about how a request was resolved.

    Used by the CLI to render the existing dim status lines (e.g.
    ``Matched manifest node: ...``). Empty by default for resolvers that
    have no extra metadata to report (such as plain SQL).
    """

    matched_node_id: str | None = None
    matched_via_raw_code: bool = False


class SqlResolver(Protocol):
    """A strategy that turns a :class:`QueryRequest` into executable SQL."""

    def can_resolve(self, request: QueryRequest) -> bool:
        """Return True if this resolver knows how to handle the request."""
        ...

    def resolve(
        self, request: QueryRequest
    ) -> tuple[ResolvedQuery, ResolutionTrace]:
        """Resolve the request into executable SQL plus a trace.

        The trace lets the CLI render status messages without the
        application layer having to depend on Rich/Typer.
        """
        ...
