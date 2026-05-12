"""Plain SQL resolver (no dbt).

Implements :class:`qmb.application.protocols.SqlResolver` for ad-hoc SQL
strings and raw ``.sql`` files where dbt resolution is *not* requested.
"""

from qmb.application.protocols import ResolutionTrace
from qmb.sql.loader import load_sql
from qmb.types import InputMode, QueryRequest, ResolvedQuery

__all__ = ["PlainSqlResolver"]


class PlainSqlResolver:
    """Resolves ad-hoc SQL strings and raw ``.sql`` files (no dbt)."""

    def can_resolve(self, request: QueryRequest) -> bool:
        spec = request.input
        if spec.mode == InputMode.SQL:
            return True
        return spec.mode == InputMode.FILE and not request.dbt.resolve_dbt

    def resolve(
        self, request: QueryRequest
    ) -> tuple[ResolvedQuery, ResolutionTrace]:
        return load_sql(request.input), ResolutionTrace()
