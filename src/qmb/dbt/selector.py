"""Resolve a user-provided model selector to a manifest node."""

from qmb.dbt.manifest import ManifestIndex, ManifestNode


def resolve_model(selector: str, index: ManifestIndex) -> ManifestNode:
    """Resolve a model selector to a ManifestNode.

    Supports:
      - exact unique_id: "model.my_project.orders"
      - qualified name: "my_project.orders"
      - bare name: "orders"
    """
    sel = selector.strip()

    # 1. Exact unique_id match
    if sel in index.nodes_by_id:
        return index.nodes_by_id[sel]

    # 2. Bare name match
    sel_lower = sel.casefold()
    candidates: list[ManifestNode] = []

    for node in index.nodes_by_id.values():
        qualified = f"{node.package_name}.{node.name}".casefold()
        if node.name.casefold() == sel_lower or qualified == sel_lower:
            candidates.append(node)

    if not candidates:
        available = sorted({n.name for n in index.nodes_by_id.values()})
        hint = ", ".join(available[:10])
        raise ValueError(f"Model '{selector}' not found. Available: {hint}...")

    if len(candidates) == 1:
        return candidates[0]

    # Prefer models over seeds/snapshots
    priority = {"model": 0, "seed": 1, "snapshot": 2}
    candidates.sort(key=lambda n: priority.get(n.resource_type, 99))

    if candidates[0].resource_type != candidates[1].resource_type:
        return candidates[0]

    names = [c.unique_id for c in candidates]
    raise ValueError(
        f"Ambiguous selector '{selector}'. Matches: {', '.join(names)}. "
        "Use a more specific selector like 'package_name.model_name'."
    )
