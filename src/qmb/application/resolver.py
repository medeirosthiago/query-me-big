"""SQL resolution for qmb.

This module turns a :class:`QueryRequest` into a :class:`ResolvedQuery`
along with a small :class:`ResolutionTrace` describing how the request
was resolved. The trace lets the CLI render the same dim status messages
that used to be printed inline from ``cli._resolve_sql`` without having
this layer depend on Rich/Typer.
"""

from dataclasses import dataclass

# Import the modules (not their members) so test monkeypatches against
# the canonical module paths (e.g. ``qmb.dbt.resolver.resolve_file_sql``)
# are picked up at call time.
from qmb.dbt import manifest as _dbt_manifest
from qmb.dbt import resolver as _dbt_resolver
from qmb.sql import loader as _sql_loader
from qmb.types import InputMode, QueryRequest, ResolvedQuery

__all__ = ["ResolutionTrace", "apply_where", "resolve_request_to_sql"]


@dataclass(frozen=True)
class ResolutionTrace:
    """Optional metadata about how a request was resolved.

    Used by the CLI to render the existing dim status lines (e.g.
    ``Matched manifest node: ...``). Empty by default.
    """

    matched_node_id: str | None = None
    matched_via_raw_code: bool = False


def resolve_request_to_sql(
    request: QueryRequest,
) -> tuple[ResolvedQuery, ResolutionTrace]:
    """Resolve a :class:`QueryRequest` into a :class:`ResolvedQuery`.

    Returns the resolved SQL plus a trace the CLI can use to print
    user-facing status messages. This function does no I/O to the
    console — it is pure orchestration over the SQL/dbt modules.
    """
    if request.mode == InputMode.SQL:
        return _sql_loader.load_sql(request), ResolutionTrace()

    if request.mode == InputMode.FILE:
        resolved = _sql_loader.load_sql(request)

        if not request.resolve_dbt:
            return resolved, ResolutionTrace()

        if request.manifest_path is None:
            raise ValueError(
                "InputMode.FILE with resolve_dbt=True requires "
                "request.manifest_path to be set"
            )

        index = _dbt_manifest.load_manifest(request.manifest_path)

        # Try to match the file to a compiled manifest node first.
        if request.file_path:
            node = _dbt_resolver.resolve_file_to_model(str(request.file_path), index)
            if node:
                if node.compiled_code:
                    return (
                        ResolvedQuery(
                            sql=_sql_loader.normalize_sql(node.compiled_code),
                            source_label=f"model: {node.name} ({node.unique_id})",
                        ),
                        ResolutionTrace(matched_node_id=node.unique_id),
                    )
                if node.raw_code:
                    return (
                        _dbt_resolver.resolve_file_sql(
                            _dbt_resolver.strip_config_blocks(node.raw_code),
                            index,
                            request.variables,
                            source_label=f"model: {node.name} ({node.unique_id})",
                        ),
                        ResolutionTrace(
                            matched_node_id=node.unique_id,
                            matched_via_raw_code=True,
                        ),
                    )

        return (
            _dbt_resolver.resolve_file_sql(
                resolved.sql,
                index,
                request.variables,
                source_label=resolved.source_label,
            ),
            ResolutionTrace(),
        )

    if request.mode == InputMode.MODEL:
        if request.manifest_path is None:
            raise ValueError(
                "InputMode.MODEL requires request.manifest_path to be set"
            )
        if request.model_name is None:
            raise ValueError(
                "InputMode.MODEL requires request.model_name to be set"
            )

        index = _dbt_manifest.load_manifest(request.manifest_path)
        return (
            _dbt_resolver.resolve_model_query(
                request.model_name, index, request.variables
            ),
            ResolutionTrace(),
        )

    raise ValueError(f"Unknown mode: {request.mode}")


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
