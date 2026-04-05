"""Load and index a dbt manifest.json."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

PREFERRED_PROJECT_DIR_NAMES = ("dbt", "analytics", "transform", "transforms")
SKIP_DISCOVERY_DIRS = {".git", ".venv", "dbt_packages", "logs", "node_modules", "__pycache__"}


@dataclass
class ManifestNode:
    unique_id: str
    name: str
    resource_type: str
    package_name: str
    database: str | None
    schema_name: str | None
    alias: str | None
    compiled_code: str | None
    raw_code: str | None
    original_file_path: str | None
    depends_on_nodes: list[str] = field(default_factory=list)


@dataclass
class ManifestSource:
    unique_id: str
    source_name: str
    name: str  # table name
    database: str | None
    schema_name: str | None
    identifier: str | None


@dataclass
class ManifestIndex:
    nodes_by_name: dict[str, ManifestNode] = field(default_factory=dict)
    nodes_by_id: dict[str, ManifestNode] = field(default_factory=dict)
    sources_by_key: dict[tuple[str, str], ManifestSource] = field(default_factory=dict)
    sources_by_id: dict[str, ManifestSource] = field(default_factory=dict)
    project_vars: dict[str, Any] = field(default_factory=dict)
    project_name: str = ""


def discover_manifest_path(explicit_path: Path | None = None) -> Path:
    """Find manifest.json using priority: explicit > env > cwd search."""
    if explicit_path:
        p = Path(explicit_path)
        if p.is_file():
            return p
        if p.is_dir():
            candidate = p / "target" / "manifest.json"
            if candidate.exists():
                return candidate
            raise FileNotFoundError(f"No manifest.json found at {p}")
        raise FileNotFoundError(f"Path does not exist: {p}")

    env_manifest = os.environ.get("QMB_MANIFEST_PATH") or os.environ.get(
        "MODEL_NAVIGATOR_MANIFEST_PATH"
    )
    if env_manifest:
        p = Path(env_manifest)
        if p.exists():
            return p
        raise FileNotFoundError(f"Manifest from env var not found: {p}")

    env_project = os.environ.get("DBT_PROJECT_DIR")
    if env_project:
        candidate = Path(env_project) / "target" / "manifest.json"
        if candidate.exists():
            return candidate

    # Search from cwd
    cwd = Path.cwd()
    candidate = cwd / "target" / "manifest.json"
    if candidate.exists():
        return candidate

    for name in PREFERRED_PROJECT_DIR_NAMES:
        candidate = cwd / name / "target" / "manifest.json"
        if candidate.exists():
            return candidate

    raise FileNotFoundError(
        "Could not discover manifest.json. Use --manifest or set QMB_MANIFEST_PATH."
    )


def load_manifest(path: Path) -> ManifestIndex:
    """Load and index manifest.json."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    index = ManifestIndex()

    # Project metadata
    metadata = raw.get("metadata", {})
    index.project_name = metadata.get("project_name", "")

    # Project vars from dbt_project.yml are embedded in manifest
    index.project_vars = _extract_vars(raw)

    # Nodes (models, seeds, snapshots)
    for unique_id, node in raw.get("nodes", {}).items():
        resource_type = node.get("resource_type", "")
        if resource_type not in ("model", "seed", "snapshot"):
            continue
        mn = ManifestNode(
            unique_id=unique_id,
            name=node.get("name", ""),
            resource_type=resource_type,
            package_name=node.get("package_name", ""),
            database=node.get("database"),
            schema_name=node.get("schema"),
            alias=node.get("alias"),
            compiled_code=node.get("compiled_code") or node.get("compiled_sql"),
            raw_code=node.get("raw_code") or node.get("raw_sql"),
            original_file_path=node.get("original_file_path"),
            depends_on_nodes=node.get("depends_on", {}).get("nodes", []),
        )
        index.nodes_by_id[unique_id] = mn
        index.nodes_by_name[mn.name] = mn

    # Sources
    for unique_id, source in raw.get("sources", {}).items():
        ms = ManifestSource(
            unique_id=unique_id,
            source_name=source.get("source_name", ""),
            name=source.get("name", ""),
            database=source.get("database"),
            schema_name=source.get("schema"),
            identifier=source.get("identifier"),
        )
        index.sources_by_id[unique_id] = ms
        index.sources_by_key[(ms.source_name, ms.name)] = ms

    return index


def _extract_vars(raw_manifest: dict) -> dict[str, Any]:
    """Extract project-level vars from the manifest."""
    vars_section: dict[str, Any] = {}

    # Some manifests store vars at the top level
    if "vars" in raw_manifest:
        top_vars = raw_manifest["vars"]
        if isinstance(top_vars, dict):
            vars_section.update(top_vars)

    # Also check env vars with QMB_VAR_ prefix
    for key, value in os.environ.items():
        if key.startswith("QMB_VAR_"):
            var_name = key[8:].lower()
            vars_section[var_name] = value

    return vars_section
