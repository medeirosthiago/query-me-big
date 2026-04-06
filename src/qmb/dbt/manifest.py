"""Load and index a dbt manifest.json."""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

PREFERRED_PROJECT_DIR_NAMES = ("dbt", "analytics", "transform", "transforms")


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
    nodes_by_id: dict[str, ManifestNode] = field(default_factory=dict)
    sources_by_key: dict[tuple[str, str], ManifestSource] = field(default_factory=dict)
    project_vars: dict[str, Any] = field(default_factory=dict)
    project_name: str = ""


def is_dbt_project_file(file_path: Path) -> bool:
    """Check if a .sql file lives inside a dbt project directory."""
    resolved = file_path.resolve()
    return any((parent / "dbt_project.yml").exists() for parent in resolved.parents)


def has_dbt_env() -> bool:
    """Check if dbt env vars are set (DBT_MODEL_PATH or DBT_PROJECT_DIR)."""
    return bool(os.environ.get("DBT_MODEL_PATH") or os.environ.get("DBT_PROJECT_DIR"))


def discover_manifest_path() -> Path:
    """Find manifest.json using priority: env > cwd search."""
    env_manifest = os.environ.get("DBT_MODEL_PATH")
    if env_manifest:
        p = Path(env_manifest)
        if p.is_file():
            return p
        candidate = p / "target" / "manifest.json"
        if candidate.exists():
            return candidate
        raise FileNotFoundError(f"Manifest from env var not found: {p}")

    env_project = os.environ.get("DBT_PROJECT_DIR")
    if env_project:
        candidate = Path(env_project) / "target" / "manifest.json"
        if candidate.exists():
            return candidate

    # Search from cwd upward so repo-root and nested project layouts both work.
    cwd = Path.cwd()
    for base in (cwd, *cwd.parents):
        candidate = base / "target" / "manifest.json"
        if candidate.exists():
            return candidate

        for name in PREFERRED_PROJECT_DIR_NAMES:
            candidate = base / name / "target" / "manifest.json"
            if candidate.exists():
                return candidate

    raise FileNotFoundError(
        "Could not discover manifest.json. Use --manifest or set DBT_MODEL_PATH."
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
