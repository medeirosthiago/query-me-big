from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from qmb.jobs.models import EngineMetadata, SourceMetadata
from qmb.jobs.remote import GcsRemoteArchive, safe_path_segment
from qmb.jobs.store import JobStore
from qmb.types import SchemaField


class FakeBlob:
    def __init__(self, objects: dict[str, bytes], name: str) -> None:
        self.objects = objects
        self.name = name

    def upload_from_filename(self, filename: str, *, content_type: str | None = None) -> None:
        self.objects[self.name] = Path(filename).read_bytes()

    def upload_from_string(self, data: str, *, content_type: str | None = None) -> None:
        self.objects[self.name] = data.encode()

    def download_to_filename(self, filename: str) -> None:
        Path(filename).write_bytes(self.objects[self.name])


class FakeBucket:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self.objects, name)


class FakeClient:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def bucket(self, bucket_name: str) -> FakeBucket:
        return FakeBucket(self.objects)

    def list_blobs(self, bucket_name: str, *, prefix: str) -> list[FakeBlob]:
        return [
            FakeBlob(self.objects, name)
            for name in sorted(self.objects)
            if name.startswith(prefix)
        ]


def _seed_job(root: Path, *, session_id: str | None = "agent/session") -> Any:
    store = JobStore(root=root, nonce=lambda: "abc123")
    return store.create(
        resolved_sql="SELECT 1 AS id",
        schema=[SchemaField("id", "INTEGER")],
        preview_rows=[{"id": 1}, {"id": 2}],
        source=SourceMetadata(label="ad-hoc", input_mode="sql"),
        engine=EngineMetadata(name="bigquery", job_id="bq-job", project="proj", location="US"),
        total_rows=2,
        session_id=session_id,
    )


def test_safe_path_segment_sanitizes_session_ids() -> None:
    assert safe_path_segment("agent/session with spaces") == "agent_session_with_spaces"
    assert safe_path_segment(None) == "unknown"


def test_gcs_export_writes_qmb_artifacts_under_session_prefix(tmp_path: Path) -> None:
    record = _seed_job(tmp_path / "source")
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb/", client=client)

    result = remote.export_job(record, preview_rows=1)

    prefix = f"qmb/sessions/agent_session/{record.qmb_job_id}"
    assert result.status == "exported"
    assert result.uri == f"gs://bucket/{prefix}/"
    assert sorted(client.objects) == [
        f"{prefix}/metadata.json",
        f"{prefix}/preview.jsonl",
        f"{prefix}/query.sql",
        f"{prefix}/schema.json",
    ]
    assert client.objects[f"{prefix}/preview.jsonl"].decode() == '{"id": 1}\n'


def test_gcs_import_job_restores_local_archive(tmp_path: Path) -> None:
    source_record = _seed_job(tmp_path / "source")
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb", client=client)
    remote.export_job(source_record)

    destination_store = JobStore(root=tmp_path / "destination")
    result = remote.import_job(source_record.qmb_job_id, destination_store)

    imported = destination_store.read(source_record.qmb_job_id)
    assert result.status == "imported"
    assert imported.qmb_job_id == source_record.qmb_job_id
    assert imported.query_path.read_text(encoding="utf-8") == "SELECT 1 AS id"
    assert json.loads(imported.schema_path.read_text(encoding="utf-8")) == [
        {"name": "id", "type": "INTEGER", "mode": "NULLABLE"}
    ]


def test_gcs_import_session_skips_existing_jobs_by_default(tmp_path: Path) -> None:
    source_record = _seed_job(tmp_path / "source", session_id="shared")
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb", client=client)
    remote.export_job(source_record)

    destination_store = JobStore(root=tmp_path / "destination")
    remote.import_session("shared", destination_store)
    results = remote.import_session("shared", destination_store)

    assert [result.status for result in results] == ["skipped"]
