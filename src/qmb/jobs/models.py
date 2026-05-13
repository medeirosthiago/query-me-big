"""Domain models for the local qmb job archive."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from qmb.types import SchemaField

ARCHIVE_VERSION = 1


@dataclass(frozen=True)
class SourceMetadata:
    """Metadata about where a query came from before execution."""

    label: str
    input_mode: str
    file_path: str | None = None
    model_name: str | None = None
    manifest_path: str | None = None
    resolver: str | None = None
    matched_node_id: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "input_mode": self.input_mode,
            "file_path": self.file_path,
            "model_name": self.model_name,
            "manifest_path": self.manifest_path,
            "resolver": self.resolver,
            "matched_node_id": self.matched_node_id,
        }

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> "SourceMetadata":
        return cls(
            label=data["label"],
            input_mode=data["input_mode"],
            file_path=data.get("file_path"),
            model_name=data.get("model_name"),
            manifest_path=data.get("manifest_path"),
            resolver=data.get("resolver"),
            matched_node_id=data.get("matched_node_id"),
        )


@dataclass(frozen=True)
class EngineMetadata:
    """Metadata about the query engine that executed a job."""

    name: str
    job_id: str | None = None
    project: str | None = None
    location: str | None = None

    def to_mapping(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "job_id": self.job_id,
            "project": self.project,
            "location": self.location,
        }

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> "EngineMetadata":
        return cls(
            name=data["name"],
            job_id=data.get("job_id"),
            project=data.get("project"),
            location=data.get("location"),
        )


@dataclass(frozen=True)
class JobRecord:
    """A qmb-owned historical job record and its local artifact paths."""

    qmb_job_id: str
    created_at: datetime
    source: SourceMetadata
    engine: EngineMetadata
    total_rows: int
    bytes_processed: int
    execution_seconds: float
    directory: Path
    metadata_path: Path
    query_path: Path
    schema_path: Path
    preview_path: Path
    result_path: Path | None = None
    session_id: str | None = None
    parent_job_id: str | None = None
    schema: list[SchemaField] | None = None

    def to_metadata(self) -> dict[str, Any]:
        """Return the stable JSON metadata shape written to metadata.json."""
        return {
            "version": ARCHIVE_VERSION,
            "qmb_job_id": self.qmb_job_id,
            "created_at": self.created_at.isoformat(),
            "session_id": self.session_id,
            "parent_job_id": self.parent_job_id,
            "source": self.source.to_mapping(),
            "engine": self.engine.to_mapping(),
            "stats": {
                "total_rows": self.total_rows,
                "bytes_processed": self.bytes_processed,
                "execution_seconds": self.execution_seconds,
            },
            "artifacts": {
                "metadata": self.metadata_path.name,
                "query": self.query_path.name,
                "schema": self.schema_path.name,
                "preview": self.preview_path.name,
                "result": self.result_path.name if self.result_path is not None else None,
            },
        }

    def artifact_paths(self) -> dict[str, str | None]:
        """Return absolute artifact paths for editor integrations."""
        return {
            "metadata": str(self.metadata_path),
            "query": str(self.query_path),
            "schema": str(self.schema_path),
            "preview": str(self.preview_path),
            "result": str(self.result_path) if self.result_path is not None else None,
        }
