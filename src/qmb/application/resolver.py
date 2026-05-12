"""SQL resolution for qmb.

This module turns a :class:`QueryRequest` into a :class:`ResolvedQuery`
along with a small :class:`ResolutionTrace` describing how the request
was resolved. The trace lets the CLI render the same dim status messages
that used to be printed inline from ``cli._resolve_sql`` without having
this layer depend on Rich/Typer.
"""

# Import the modules (not their members) so test monkeypatches against
# the canonical module paths (e.g. ``qmb.dbt.resolver.resolve_file_sql``)
# are picked up at call time.
from qmb.application.protocols import ResolutionTrace
from qmb.dbt import manifest as _dbt_manifest
from qmb.dbt import resolver as _dbt_resolver
from qmb.sql import loader as _sql_loader
from qmb.types import DbtOptions, InputMode, InputSpec, QueryRequest, ResolvedQuery

__all__ = ["ResolutionTrace", "apply_where", "resolve_request_to_sql"]


def resolve_request_to_sql(
    request: QueryRequest,
) -> tuple[ResolvedQuery, ResolutionTrace]:
    """Resolve a :class:`QueryRequest` into a :class:`ResolvedQuery`.

    Returns the resolved SQL plus a trace the CLI can use to print
    user-facing status messages. This function does no I/O to the
    console — it is pure orchestration over the SQL/dbt modules.
    """
    return _resolve(request.input, request.dbt)


def _resolve(
    spec: InputSpec, dbt: DbtOptions
) -> tuple[ResolvedQuery, ResolutionTrace]:
    if spec.mode == InputMode.SQL:
        return _sql_loader.load_sql(spec), ResolutionTrace()

    if spec.mode == InputMode.FILE:
        resolved = _sql_loader.load_sql(spec)

        if not dbt.resolve_dbt:
            return resolved, ResolutionTrace()

        if dbt.manifest_path is None:
            raise ValueError(
                "InputMode.FILE with resolve_dbt=True requires "
                "manifest_path to be set"
            )

        index = _dbt_manifest.load_manifest(dbt.manifest_path)

        # Try to match the file to a compiled manifest node first.
        if spec.file_path:
            node = _dbt_resolver.resolve_file_to_model(str(spec.file_path), index)
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
                            dbt.variables,
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
                dbt.variables,
                source_label=resolved.source_label,
            ),
            ResolutionTrace(),
        )

    if spec.mode == InputMode.MODEL:
        if dbt.manifest_path is None:
            raise ValueError(
                "InputMode.MODEL requires manifest_path to be set"
            )
        if spec.model_name is None:
            raise ValueError(
                "InputMode.MODEL requires model_name to be set"
            )

        index = _dbt_manifest.load_manifest(dbt.manifest_path)
        return (
            _dbt_resolver.resolve_model_query(
                spec.model_name, index, dbt.variables
            ),
            ResolutionTrace(),
        )

    raise ValueError(f"Unknown mode: {spec.mode}")


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
