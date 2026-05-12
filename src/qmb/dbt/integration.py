"""dbt integration with the qmb application layer.

This module provides :class:`DbtSqlResolver`, the concrete implementation
of :class:`qmb.application.protocols.SqlResolver` for dbt-aware
resolution (``--model`` queries and ``.sql`` files with
``ref`` / ``source`` / ``var`` patterns).

The application layer never imports this module directly — it is wired in
by the CLI (or any future composition root).
"""

from qmb.application.protocols import ResolutionTrace

# Import the modules (not their members) so test monkeypatches against
# the canonical module paths (e.g. ``qmb.dbt.resolver.resolve_file_sql``)
# are picked up at call time.
from qmb.dbt import manifest as _dbt_manifest
from qmb.dbt import resolver as _dbt_resolver
from qmb.sql import loader as _sql_loader
from qmb.types import InputMode, QueryRequest, ResolvedQuery

__all__ = ["DbtSqlResolver"]


class DbtSqlResolver:
    """Resolves dbt models and ``.sql`` files with ref/source/var to SQL."""

    def can_resolve(self, request: QueryRequest) -> bool:
        spec = request.input
        if spec.mode == InputMode.MODEL:
            return True
        return spec.mode == InputMode.FILE and request.dbt.resolve_dbt

    def resolve(
        self, request: QueryRequest
    ) -> tuple[ResolvedQuery, ResolutionTrace]:
        spec = request.input
        dbt = request.dbt

        if spec.mode == InputMode.FILE:
            resolved = _sql_loader.load_sql(spec)

            if dbt.manifest_path is None:
                raise ValueError(
                    "InputMode.FILE with resolve_dbt=True requires "
                    "manifest_path to be set"
                )

            index = _dbt_manifest.load_manifest(dbt.manifest_path)

            # Try to match the file to a compiled manifest node first.
            if spec.file_path:
                node = _dbt_resolver.resolve_file_to_model(
                    str(spec.file_path), index
                )
                if node:
                    if node.compiled_code:
                        return (
                            ResolvedQuery(
                                sql=_sql_loader.normalize_sql(node.compiled_code),
                                source_label=(
                                    f"model: {node.name} ({node.unique_id})"
                                ),
                            ),
                            ResolutionTrace(matched_node_id=node.unique_id),
                        )
                    if node.raw_code:
                        return (
                            _dbt_resolver.resolve_file_sql(
                                _dbt_resolver.strip_config_blocks(node.raw_code),
                                index,
                                dbt.variables,
                                source_label=(
                                    f"model: {node.name} ({node.unique_id})"
                                ),
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

        raise ValueError(
            f"DbtSqlResolver cannot resolve input mode: {spec.mode}"
        )
