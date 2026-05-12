"""SQL resolution facade for qmb.

This module exposes :func:`resolve_request_to_sql`, a thin facade that
dispatches a :class:`QueryRequest` to the first registered
:class:`~qmb.application.protocols.SqlResolver` that can handle it.

Concrete resolvers (plain SQL, dbt) are wired in at the CLI boundary and
passed down to the application layer; this module never imports any
concrete implementation. That keeps the dependency direction pointing
*into* the application layer.
"""

from collections.abc import Sequence

from qmb.application.protocols import ResolutionTrace, SqlResolver
from qmb.types import QueryRequest, ResolvedQuery

__all__ = ["ResolutionTrace", "apply_where", "resolve_request_to_sql"]


def resolve_request_to_sql(
    request: QueryRequest,
    resolvers: Sequence[SqlResolver],
) -> tuple[ResolvedQuery, ResolutionTrace]:
    """Resolve a :class:`QueryRequest` into a :class:`ResolvedQuery`.

    Iterates over the provided ``resolvers`` and picks the first one
    whose :meth:`SqlResolver.can_resolve` returns ``True``. Returns the
    resolved SQL plus a trace the CLI can use to print user-facing
    status messages. This function does no I/O to the console.
    """
    for resolver in resolvers:
        if resolver.can_resolve(request):
            return resolver.resolve(request)
    raise ValueError(
        f"No registered SqlResolver can handle input mode: {request.input.mode}"
    )


def apply_where(resolved: ResolvedQuery, where: str | None) -> ResolvedQuery:
    """Wrap a resolved query in a ``WHERE`` filter.

    A no-op when ``where`` is ``None`` or empty. Otherwise wraps the
    existing SQL in ``SELECT * FROM (<sql>) __qmb WHERE <where>`` and
    preserves the original source label.
    """
    if not where:
        return resolved
    return ResolvedQuery(
        sql=f"SELECT * FROM ({resolved.sql}) __qmb WHERE {where}",
        source_label=resolved.source_label,
    )
