from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from qmb.jobs.models import EngineMetadata, SourceMetadata
from qmb.jobs.remote import GcsRemoteArchive, safe_path_segment
from qmb.jobs.store import JobStore
from qmb.types import AgentContext, SchemaField


class FakeBlob:
    def __init__(self, objects: dict[str, bytes], name: str) -> None:
        self.objects = objects
        self.name = name

    def upload_from_filename(self, filename: str, *, content_type: str | None = None) -> None:
        self.objects[self.name] = Path(filename).read_bytes()

    def upload_from_string(self, data: str | bytes, *, content_type: str | None = None) -> None:
        self.objects[self.name] = data.encode() if isinstance(data, str) else data

    def download_to_filename(self, filename: str) -> None:
        Path(filename).write_bytes(self.objects[self.name])

    def download_as_text(self) -> str:
        return self.objects[self.name].decode("utf-8")

    def download_as_bytes(self) -> bytes:
        return self.objects[self.name]

    def exists(self) -> bool:
        return self.name in self.objects


class FakeBucket:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self.objects, name)


class FakeClient:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.list_blobs_calls: list[str] = []

    def bucket(self, bucket_name: str) -> FakeBucket:
        return FakeBucket(self.objects)

    def list_blobs(self, bucket_name: str, *, prefix: str) -> list[FakeBlob]:
        self.list_blobs_calls.append(prefix)
        return [
            FakeBlob(self.objects, name)
            for name in sorted(self.objects)
            if name.startswith(prefix)
        ]


def _seed_job(
    root: Path,
    *,
    session_id: str | None = "agent/session",
    agent_context: AgentContext | None = None,
    bytes_processed: int = 1024,
) -> Any:
    store = JobStore(root=root, nonce=lambda: "abc123")
    return store.create(
        resolved_sql="SELECT 1 AS id",
        schema=[SchemaField("id", "INTEGER")],
        preview_rows=[{"id": 1}, {"id": 2}],
        source=SourceMetadata(label="ad-hoc", input_mode="sql"),
        engine=EngineMetadata(name="bigquery", job_id="bq-job", project="proj", location="US"),
        total_rows=2,
        bytes_processed=bytes_processed,
        session_id=session_id,
        agent_context=agent_context,
    )


def test_safe_path_segment_sanitizes_session_ids() -> None:
    assert safe_path_segment("agent/session with spaces") == "agent_session_with_spaces"
    assert safe_path_segment(None) == "unknown"


def test_gcs_export_writes_qmb_artifacts_under_flat_prefix(tmp_path: Path) -> None:
    record = _seed_job(tmp_path / "source")
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb/", client=client)

    result = remote.export_job(record, preview_rows=1)

    prefix = f"qmb/{record.qmb_job_id}"
    assert result.status == "exported"
    assert result.uri == f"gs://bucket/{prefix}/"
    assert sorted(client.objects) == [
        f"{prefix}/metadata.json",
        f"{prefix}/preview.jsonl",
        f"{prefix}/query.sql",
        f"{prefix}/schema.json",
        f"qmb/sessions/agent_session.json",
    ]
    assert client.objects[f"{prefix}/preview.jsonl"].decode() == '{"id": 1}\n'


def test_gcs_export_updates_session_manifest(tmp_path: Path) -> None:
    record = _seed_job(tmp_path / "source", session_id="agent-42", bytes_processed=500)
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb/", client=client)

    remote.export_job(record)

    manifest = json.loads(client.objects["qmb/sessions/agent-42.json"].decode())
    assert manifest["session_id"] == "agent-42"
    assert manifest["jobs"] == [record.qmb_job_id]
    assert manifest["count"] == 1
    assert manifest["bytes_processed"] == 500


def test_gcs_export_does_not_write_manifest_for_session_less_job(tmp_path: Path) -> None:
    record = _seed_job(tmp_path / "source", session_id=None)
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb/", client=client)

    remote.export_job(record)

    assert not any(name.startswith("qmb/sessions/") for name in client.objects)


def test_gcs_import_job_is_o1_no_full_scan(tmp_path: Path) -> None:
    """import_job by full id must NOT call list_blobs — direct path check only."""
    source_record = _seed_job(tmp_path / "source", session_id="agent-42")
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb", client=client)
    remote.export_job(source_record)
    client.list_blobs_calls.clear()  # reset after export

    destination_store = JobStore(root=tmp_path / "destination")
    remote.import_job(source_record.qmb_job_id, destination_store)

    assert client.list_blobs_calls == [], (
        "import_job by full id must not list_blobs; got "
        f"{client.list_blobs_calls}"
    )


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


def test_gcs_import_job_updates_local_session_manifest(tmp_path: Path) -> None:
    source_record = _seed_job(tmp_path / "source", session_id="agent-42")
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb", client=client)
    remote.export_job(source_record)

    destination_store = JobStore(root=tmp_path / "destination")
    remote.import_job(source_record.qmb_job_id, destination_store)

    manifest_path = destination_store.manifest_path_for("agent-42")
    assert manifest_path.is_file()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["jobs"] == [source_record.qmb_job_id]


def test_gcs_import_session_reads_manifest(tmp_path: Path) -> None:
    source_record = _seed_job(tmp_path / "source", session_id="shared")
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb", client=client)
    remote.export_job(source_record)

    destination_store = JobStore(root=tmp_path / "destination")
    results = remote.import_session("shared", destination_store)

    assert len(results) == 1
    assert results[0].status == "imported"
    assert results[0].qmb_job_id == source_record.qmb_job_id


def test_gcs_import_session_skips_existing_jobs_by_default(tmp_path: Path) -> None:
    source_record = _seed_job(tmp_path / "source", session_id="shared")
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb", client=client)
    remote.export_job(source_record)

    destination_store = JobStore(root=tmp_path / "destination")
    remote.import_session("shared", destination_store)
    results = remote.import_session("shared", destination_store)

    assert [result.status for result in results] == ["skipped"]


def test_gcs_import_session_falls_back_to_full_scan_when_manifest_missing(
    tmp_path: Path,
) -> None:
    source_record = _seed_job(tmp_path / "source", session_id="shared")
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb", client=client)
    remote.export_job(source_record)
    # Simulate a lost manifest (e.g. older archive that predated manifests).
    del client.objects["qmb/sessions/shared.json"]

    destination_store = JobStore(root=tmp_path / "destination")
    results = remote.import_session("shared", destination_store)

    assert len(results) == 1
    assert results[0].qmb_job_id == source_record.qmb_job_id


def test_gcs_import_job_partial_id_falls_back_to_list(tmp_path: Path) -> None:
    source_record = _seed_job(tmp_path / "source", session_id="agent-42")
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb", client=client)
    remote.export_job(source_record)
    client.list_blobs_calls.clear()

    destination_store = JobStore(root=tmp_path / "destination")
    # Use a partial id (the nonce suffix) to force the listing fallback.
    partial = source_record.qmb_job_id[-6:]
    remote.import_job(partial, destination_store)

    # Partial-id resolution must use list_blobs.
    assert len(client.list_blobs_calls) >= 1
