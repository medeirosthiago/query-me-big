from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from google.api_core.exceptions import PreconditionFailed

from qmb.jobs.models import EngineMetadata, SourceMetadata
from qmb.jobs.remote import GcsRemoteArchive, safe_path_segment
from qmb.jobs.store import JobStore
from qmb.types import AgentContext, SchemaField


class FakeBlob:
    def __init__(self, objects: dict[str, bytes], generations: dict[str, int], name: str) -> None:
        self.objects = objects
        self.generations = generations
        self.name = name
        self.generation = generations.get(name, 0)

    def upload_from_filename(
        self,
        filename: str,
        *,
        content_type: str | None = None,
        if_generation_match: int | None = None,
    ) -> None:
        self._check_precondition(if_generation_match)
        self.objects[self.name] = Path(filename).read_bytes()
        self._bump_generation()

    def upload_from_string(
        self,
        data: str | bytes,
        *,
        content_type: str | None = None,
        if_generation_match: int | None = None,
    ) -> None:
        self._check_precondition(if_generation_match)
        self.objects[self.name] = data.encode() if isinstance(data, str) else data
        self._bump_generation()

    def download_to_filename(self, filename: str) -> None:
        Path(filename).write_bytes(self.objects[self.name])

    def download_as_text(self) -> str:
        self.generation = self.generations.get(self.name, 0)
        return self.objects[self.name].decode("utf-8")

    def download_as_bytes(self) -> bytes:
        self.generation = self.generations.get(self.name, 0)
        return self.objects[self.name]

    def exists(self) -> bool:
        return self.name in self.objects

    def _check_precondition(self, if_generation_match: int | None) -> None:
        if if_generation_match is None:
            return
        current = self.generations.get(self.name, 0)
        if current != if_generation_match:
            raise PreconditionFailed("generation mismatch")

    def _bump_generation(self) -> None:
        new_generation = self.generations.get(self.name, 0) + 1
        self.generations[self.name] = new_generation
        self.generation = new_generation


class FakeBucket:
    def __init__(self, objects: dict[str, bytes], generations: dict[str, int]) -> None:
        self.objects = objects
        self.generations = generations

    def blob(self, name: str) -> FakeBlob:
        return FakeBlob(self.objects, self.generations, name)


class FakeClient:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.generations: dict[str, int] = {}
        self.list_blobs_calls: list[str] = []

    def bucket(self, bucket_name: str) -> FakeBucket:
        return FakeBucket(self.objects, self.generations)

    def list_blobs(self, bucket_name: str, *, prefix: str) -> list[FakeBlob]:
        self.list_blobs_calls.append(prefix)
        return [
            FakeBlob(self.objects, self.generations, name)
            for name in sorted(self.objects)
            if name.startswith(prefix)
        ]


def _seed_job(
    root: Path,
    *,
    session_id: str | None = "agent/session",
    agent_context: AgentContext | None = None,
    bytes_processed: int = 1024,
    resolved_sql: str = "SELECT 1 AS id",
    nonce: str = "abc123",
) -> Any:
    store = JobStore(root=root, nonce=lambda: nonce)
    return store.create(
        resolved_sql=resolved_sql,
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
        "qmb/index.json",
        f"{prefix}/metadata.json",
        f"{prefix}/preview.jsonl",
        f"{prefix}/query.sql",
        f"{prefix}/schema.json",
        "qmb/sessions/agent_session.json",
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


# -- Remote index (index.json) ---------------------------------------------


def test_gcs_export_upserts_remote_index_entry(tmp_path: Path) -> None:
    record = _seed_job(tmp_path / "source", session_id="agent-42", bytes_processed=2048)
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb", client=client)

    result = remote.export_job(record)

    assert result.warning is None
    index = json.loads(client.objects["qmb/index.json"].decode())
    assert index["version"] == 1
    assert index["updated_at"] is not None
    entry = index["jobs"][record.qmb_job_id]
    assert entry == {
        "qmb_job_id": record.qmb_job_id,
        "session_id": "agent-42",
        "created_at": record.created_at.isoformat(),
        "engine": "bigquery",
        "source_label": "ad-hoc",
        "total_rows": 2,
        "bytes_processed": 2048,
        "query_excerpt": "SELECT 1 AS id",
    }


def test_gcs_export_index_entry_collapses_and_truncates_query_excerpt(tmp_path: Path) -> None:
    long_query = "SELECT\n\n  1  AS   id" + " -- padding" * 500
    record = _seed_job(tmp_path / "source", resolved_sql=long_query)
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb", client=client)

    remote.export_job(record)

    index = json.loads(client.objects["qmb/index.json"].decode())
    excerpt = index["jobs"][record.qmb_job_id]["query_excerpt"]
    assert "\n" not in excerpt
    assert "  " not in excerpt
    assert len(excerpt) < len(long_query)
    assert excerpt == " ".join(long_query[:4000].split())


def test_gcs_export_upsert_merges_multiple_jobs_into_one_index(tmp_path: Path) -> None:
    first = _seed_job(tmp_path / "source", session_id="agent-1", nonce="aaa111")
    second = _seed_job(tmp_path / "source", session_id="agent-2", nonce="bbb222")
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb", client=client)

    remote.export_job(first)
    remote.export_job(second)

    index = json.loads(client.objects["qmb/index.json"].decode())
    assert sorted(index["jobs"]) == sorted([first.qmb_job_id, second.qmb_job_id])


def test_gcs_export_retries_index_write_on_generation_conflict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    record = _seed_job(tmp_path / "source", session_id=None)
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb", client=client)

    real_upload_from_string = FakeBlob.upload_from_string
    conflicts_remaining = 2

    def flaky_upload_from_string(self: FakeBlob, *args: Any, **kwargs: Any) -> None:
        nonlocal conflicts_remaining
        if self.name == "qmb/index.json" and conflicts_remaining > 0:
            conflicts_remaining -= 1
            # Simulate a concurrent writer winning the race between our read
            # and our write: it lands real content at a bumped generation.
            self.objects[self.name] = json.dumps(
                {"version": 1, "updated_at": "2026-01-01T00:00:00+00:00", "jobs": {}}
            ).encode()
            self.generations[self.name] = self.generations.get(self.name, 0) + 1
            raise PreconditionFailed("generation mismatch")
        return real_upload_from_string(self, *args, **kwargs)

    monkeypatch.setattr(FakeBlob, "upload_from_string", flaky_upload_from_string)

    result = remote.export_job(record)

    assert conflicts_remaining == 0
    assert result.warning is None
    index = json.loads(client.objects["qmb/index.json"].decode())
    assert record.qmb_job_id in index["jobs"]


def test_gcs_export_succeeds_when_index_write_exhausts_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    record = _seed_job(tmp_path / "source", session_id=None)
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb", client=client)

    def always_conflict(self: FakeBlob, *args: Any, **kwargs: Any) -> None:
        if self.name == "qmb/index.json":
            raise PreconditionFailed("generation mismatch")
        raise AssertionError("only index.json writes should be exercised here")

    monkeypatch.setattr(FakeBlob, "upload_from_string", always_conflict)

    result = remote.export_job(record)

    assert result.status == "exported"
    assert result.warning is not None
    assert "index" in result.warning.lower()
    assert "qmb/index.json" not in client.objects


def test_gcs_list_jobs_returns_index_entries(tmp_path: Path) -> None:
    record = _seed_job(tmp_path / "source")
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb", client=client)
    remote.export_job(record)

    jobs = remote.list_jobs()

    assert [job["qmb_job_id"] for job in jobs] == [record.qmb_job_id]


def test_gcs_list_jobs_empty_when_index_missing(tmp_path: Path) -> None:
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb", client=client)

    assert remote.list_jobs() == []


def test_gcs_list_sessions_reads_session_manifests(tmp_path: Path) -> None:
    first = _seed_job(tmp_path / "source", session_id="agent-1", nonce="aaa111")
    second = _seed_job(tmp_path / "source", session_id="agent-2", nonce="bbb222")
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb", client=client)
    remote.export_job(first)
    remote.export_job(second)

    sessions = remote.list_sessions()

    assert sorted(manifest.session_id for manifest in sessions) == ["agent-1", "agent-2"]


def test_gcs_list_sessions_empty_when_no_sessions(tmp_path: Path) -> None:
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb", client=client)

    assert remote.list_sessions() == []


def test_gcs_build_index_backfills_from_full_scan(tmp_path: Path) -> None:
    """build_index() must reconstruct index entries even without index.json."""
    first = _seed_job(tmp_path / "source", session_id="agent-1", nonce="aaa111")
    second = _seed_job(
        tmp_path / "source", session_id="agent-2", nonce="bbb222", resolved_sql="SELECT 2"
    )
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb", client=client)
    remote.export_job(first)
    remote.export_job(second)
    # Simulate a pre-index archive (or a corrupted index) that needs backfill.
    del client.objects["qmb/index.json"]

    index = remote.build_index()

    assert index["version"] == 1
    assert sorted(index["jobs"]) == sorted([first.qmb_job_id, second.qmb_job_id])
    assert index["jobs"][first.qmb_job_id]["query_excerpt"] == "SELECT 1 AS id"
    assert index["jobs"][second.qmb_job_id]["query_excerpt"] == "SELECT 2"


def test_gcs_build_index_falls_back_to_agent_session_id(tmp_path: Path) -> None:
    """Legacy metadata with session_id only under `agent` must still be indexed."""
    record = _seed_job(
        tmp_path / "source",
        session_id=None,
        agent_context=AgentContext(session_id="agent-legacy"),
    )
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb", client=client)
    remote.export_job(record)
    del client.objects["qmb/index.json"]

    index = remote.build_index()

    assert index["jobs"][record.qmb_job_id]["session_id"] == "agent-legacy"


def test_gcs_write_index_persists_built_index(tmp_path: Path) -> None:
    record = _seed_job(tmp_path / "source")
    client = FakeClient()
    remote = GcsRemoteArchive("gs://bucket/qmb", client=client)
    remote.export_job(record)
    del client.objects["qmb/index.json"]

    index = remote.build_index()
    remote.write_index(index)

    persisted = json.loads(client.objects["qmb/index.json"].decode())
    assert persisted == index
    assert remote.list_jobs() == list(index["jobs"].values())
