"""CLI tests for local historical qmb jobs."""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

import qmb.cli as cli
from qmb.types import AgentContext, SchemaField


def _jobs_modules() -> tuple[Any, Any]:
    try:
        models = importlib.import_module("qmb.jobs.models")
        store = importlib.import_module("qmb.jobs.store")
    except ModuleNotFoundError:  # pragma: no cover - expected until implemented
        pytest.fail("Expected qmb.jobs.models and qmb.jobs.store for Phase 9 archives")
    return models, store


def _iter_callable(values: list[Any]) -> Callable[[], Any]:
    iterator: Iterator[Any] = iter(values)
    return lambda: next(iterator)


def _seed_jobs(root: Path) -> list[Any]:
    models, store_module = _jobs_modules()
    store = store_module.JobStore(
        root=root,
        now=_iter_callable(
            [
                datetime(2026, 5, 12, 10, 0, tzinfo=UTC),
                datetime(2026, 5, 12, 11, 0, tzinfo=UTC),
            ]
        ),
        nonce=_iter_callable(["abc111", "def222"]),
    )
    first = store.create(
        resolved_sql="SELECT 'first' AS label",
        schema=[SchemaField("label", "STRING")],
        preview_rows=[{"label": "first"}],
        source=models.SourceMetadata(label="ad-hoc", input_mode="sql"),
        engine=models.EngineMetadata(
            name="bigquery",
            job_id="bq-first",
            project="proj",
            location="US",
        ),
        total_rows=1,
        bytes_processed=100,
        execution_seconds=0.5,
    )
    second = store.create(
        resolved_sql="SELECT 'second' AS label",
        schema=[SchemaField("label", "STRING")],
        preview_rows=[{"label": "second"}],
        source=models.SourceMetadata(label="file: second.sql", input_mode="file"),
        engine=models.EngineMetadata(
            name="bigquery",
            job_id="bq-second",
            project="proj",
            location="US",
        ),
        total_rows=1,
        bytes_processed=200,
        execution_seconds=0.75,
    )
    return [first, second]


def test_jobs_list_shows_local_qmb_jobs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    jobs_root = tmp_path / "jobs"
    first, second = _seed_jobs(jobs_root)
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))

    result = CliRunner().invoke(cli.app, ["jobs", "list"])

    assert result.exit_code == 0, result.output
    assert second.qmb_job_id in result.output
    assert first.qmb_job_id in result.output
    assert "file: second.sql" in result.output
    assert "ad-hoc" in result.output


def test_jobs_list_json_returns_machine_readable_records(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    jobs_root = tmp_path / "jobs"
    first, second = _seed_jobs(jobs_root)
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))

    result = CliRunner().invoke(cli.app, ["jobs", "list", "--format", "json"])

    assert result.exit_code == 0, result.output
    records = json.loads(result.output)
    assert [record["qmb_job_id"] for record in records] == [
        second.qmb_job_id,
        first.qmb_job_id,
    ]
    assert records[0]["source"]["label"] == "file: second.sql"
    assert records[0]["effective_session_id"] is None
    assert records[0]["engine"] == {
        "name": "bigquery",
        "job_id": "bq-second",
        "project": "proj",
        "location": "US",
    }


def _seed_many_jobs(root: Path, count: int) -> list[Any]:
    models, store_module = _jobs_modules()
    store = store_module.JobStore(
        root=root,
        now=_iter_callable(
            [datetime(2026, 5, 12, 10, minute, tzinfo=UTC) for minute in range(count)]
        ),
        nonce=_iter_callable([f"{i:06d}" for i in range(count)]),
    )
    records = []
    for i in range(count):
        records.append(
            store.create(
                resolved_sql=f"SELECT {i}",
                schema=[SchemaField("x", "INTEGER")],
                preview_rows=[{"x": i}],
                source=models.SourceMetadata(label=f"job-{i}", input_mode="sql"),
                engine=models.EngineMetadata(name="bigquery"),
                total_rows=1,
            )
        )
    return records


def test_jobs_list_defaults_to_fifty_with_all_and_limit_overrides(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    jobs_root = tmp_path / "jobs"
    records = _seed_many_jobs(jobs_root, 52)
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))

    default_result = CliRunner().invoke(cli.app, ["jobs", "list", "--format", "json"])
    all_result = CliRunner().invoke(cli.app, ["jobs", "list", "--format", "json", "--all"])
    limit_result = CliRunner().invoke(
        cli.app, ["jobs", "list", "--format", "json", "--limit", "3"]
    )

    assert default_result.exit_code == 0, default_result.output
    default_payload = json.loads(default_result.output)
    assert len(default_payload) == 50
    assert default_payload[0]["qmb_job_id"] == records[-1].qmb_job_id
    assert default_payload[-1]["qmb_job_id"] == records[2].qmb_job_id

    assert all_result.exit_code == 0, all_result.output
    all_payload = json.loads(all_result.output)
    assert len(all_payload) == 52
    assert all_payload[-1]["qmb_job_id"] == records[0].qmb_job_id

    assert limit_result.exit_code == 0, limit_result.output
    assert len(json.loads(limit_result.output)) == 3


def test_jobs_list_text_and_json_include_effective_session(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    models, store_module = _jobs_modules()
    jobs_root = tmp_path / "jobs"
    store = store_module.JobStore(
        root=jobs_root,
        now=lambda: datetime(2026, 5, 12, 10, 0, tzinfo=UTC),
        nonce=lambda: "abc123",
    )
    store.create(
        resolved_sql="SELECT 1",
        schema=[SchemaField("x", "INTEGER")],
        preview_rows=[{"x": 1}],
        source=models.SourceMetadata(label="ad-hoc", input_mode="sql"),
        engine=models.EngineMetadata(name="bigquery"),
        total_rows=1,
        agent_context=AgentContext(name="pi", session_id="legacy-session"),
    )
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))

    text_result = CliRunner().invoke(cli.app, ["jobs", "list"])
    json_result = CliRunner().invoke(cli.app, ["jobs", "list", "--format", "json"])

    assert text_result.exit_code == 0, text_result.output
    assert "session:legacy-session" in text_result.output

    assert json_result.exit_code == 0, json_result.output
    payload = json.loads(json_result.output)
    assert payload[0]["session_id"] is None
    assert payload[0]["effective_session_id"] == "legacy-session"


def test_jobs_list_filters_by_session_agent_date_file_model_source_and_query(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    models, store_module = _jobs_modules()
    jobs_root = tmp_path / "jobs"
    store = store_module.JobStore(
        root=jobs_root,
        now=_iter_callable(
            [
                datetime(2026, 5, 12, 10, 0, tzinfo=UTC),
                datetime(2026, 5, 13, 11, 0, tzinfo=UTC),
                datetime(2026, 5, 13, 12, 0, tzinfo=UTC),
            ]
        ),
        nonce=_iter_callable(["aaaaaa", "bbbbbb", "cccccc"]),
    )
    common = {
        "schema": [SchemaField("x", "INTEGER")],
        "preview_rows": [{"x": 1}],
        "engine": models.EngineMetadata(name="bigquery"),
        "total_rows": 1,
    }
    orders = store.create(
        resolved_sql="SELECT * FROM orders",
        source=models.SourceMetadata(
            label="file: models/orders.sql",
            input_mode="file",
            file_path="/repo/models/orders.sql",
        ),
        session_id="session-a",
        agent_context=AgentContext(name="pi", session_id="session-a"),
        **common,
    )
    customers = store.create(
        resolved_sql="SELECT * FROM customers",
        source=models.SourceMetadata(
            label="file: models/customers.sql",
            input_mode="file",
            file_path="/repo/models/customers.sql",
        ),
        agent_context=AgentContext(name="codex"),
        **common,
    )
    revenue = store.create(
        resolved_sql="SELECT revenue FROM mart",
        source=models.SourceMetadata(
            label="model: revenue",
            input_mode="model",
            model_name="revenue",
            matched_node_id="model.pkg.revenue",
        ),
        **common,
    )
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))

    def ids_for(*args: str) -> list[str]:
        result = CliRunner().invoke(cli.app, ["jobs", "list", "--format", "json", *args])
        assert result.exit_code == 0, result.output + result.stderr
        return [record["qmb_job_id"] for record in json.loads(result.output)]

    assert ids_for("--session-id", "session-a") == [orders.qmb_job_id]
    assert ids_for("--agent", "code") == [customers.qmb_job_id]
    assert ids_for("--date", "2026-05-13") == [revenue.qmb_job_id, customers.qmb_job_id]
    assert ids_for("--since", "2026-05-13") == [revenue.qmb_job_id, customers.qmb_job_id]
    assert ids_for("--until", "2026-05-12") == [orders.qmb_job_id]
    assert ids_for("--file", "orders.sql") == [orders.qmb_job_id]
    assert ids_for("--model", "revenue") == [revenue.qmb_job_id]
    assert ids_for("--source", "customers") == [customers.qmb_job_id]
    assert ids_for("--query", "revenue") == [revenue.qmb_job_id]


def test_jobs_show_json_returns_metadata_for_partial_job_id(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    jobs_root = tmp_path / "jobs"
    first, _second = _seed_jobs(jobs_root)
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))

    result = CliRunner().invoke(
        cli.app,
        ["jobs", "show", "abc111", "--format", "json"],
    )

    assert result.exit_code == 0, result.output
    metadata = json.loads(result.output)
    assert metadata["qmb_job_id"] == first.qmb_job_id
    assert metadata["engine"]["job_id"] == "bq-first"
    assert metadata["stats"]["bytes_processed"] == 100


def test_jobs_sql_prints_archived_resolved_sql(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    jobs_root = tmp_path / "jobs"
    _first, second = _seed_jobs(jobs_root)
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))

    result = CliRunner().invoke(cli.app, ["jobs", "sql", "def222"])

    assert result.exit_code == 0, result.output
    assert result.output == "SELECT 'second' AS label\n"


def test_jobs_paths_json_returns_artifact_paths_for_nvim(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    jobs_root = tmp_path / "jobs"
    first, _second = _seed_jobs(jobs_root)
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))

    result = CliRunner().invoke(
        cli.app,
        ["jobs", "paths", "abc111", "--format", "json"],
    )

    assert result.exit_code == 0, result.output
    paths = json.loads(result.output)
    assert paths == {
        "metadata": str(first.metadata_path),
        "query": str(first.query_path),
        "schema": str(first.schema_path),
        "preview": str(first.preview_path),
        "result": None,
    }


def test_jobs_sql_auto_imports_missing_job_from_remote(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import shutil

    source_root = tmp_path / "remote-source"
    jobs_root = tmp_path / "jobs"
    _first, remote_record = _seed_jobs(source_root)
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))
    destinations: list[str] = []

    class FakeRemote:
        def import_job(self, job_id: str, store: Any, *, overwrite: bool = False) -> Any:
            assert job_id == "def222"
            assert overwrite is False
            store.root.mkdir(parents=True, exist_ok=True)
            shutil.copytree(remote_record.directory, store.root / remote_record.qmb_job_id)
            return _FakeRemoteResult(remote_record.qmb_job_id, "imported")

    def fake_remote(destination: str) -> FakeRemote:
        destinations.append(destination)
        return FakeRemote()

    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", fake_remote)

    result = CliRunner().invoke(
        cli.app,
        ["jobs", "sql", "def222", "--destination", "gs://bucket/qmb/"],
    )

    assert result.exit_code == 0, result.output + result.stderr
    assert result.output == (
        "qmb: importing remote job def222 from gs://bucket/qmb/\n"
        "SELECT 'second' AS label\n"
    )
    assert destinations == ["gs://bucket/qmb/"]
    assert (jobs_root / remote_record.qmb_job_id / "metadata.json").exists()


def test_jobs_show_does_not_try_remote_for_local_job(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    jobs_root = tmp_path / "jobs"
    first, _second = _seed_jobs(jobs_root)
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))
    monkeypatch.setattr(
        "qmb.jobs.remote.get_remote_archive",
        lambda destination: pytest.fail("remote should not be used for local jobs"),
    )

    result = CliRunner().invoke(cli.app, ["jobs", "show", first.qmb_job_id])

    assert result.exit_code == 0, result.output + result.stderr
    assert first.qmb_job_id in result.output


def test_jobs_list_auto_imports_missing_session_from_remote(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import shutil

    models, store_module = _jobs_modules()
    source_root = tmp_path / "remote-source"
    jobs_root = tmp_path / "jobs"
    source_store = store_module.JobStore(
        root=source_root,
        now=lambda: datetime(2026, 5, 12, 10, 0, tzinfo=UTC),
        nonce=lambda: "abc123",
    )
    remote_record = source_store.create(
        resolved_sql="SELECT 1",
        schema=[SchemaField("x", "INTEGER")],
        preview_rows=[{"x": 1}],
        source=models.SourceMetadata(label="remote", input_mode="sql"),
        engine=models.EngineMetadata(name="bigquery"),
        total_rows=1,
        session_id="shared",
    )
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))
    imported: list[str] = []

    class FakeRemote:
        def import_session(
            self,
            session_id: str,
            store: Any,
            *,
            overwrite: bool = False,
        ) -> list[Any]:
            imported.append(session_id)
            store.root.mkdir(parents=True, exist_ok=True)
            shutil.copytree(remote_record.directory, store.root / remote_record.qmb_job_id)
            return [_FakeRemoteResult(remote_record.qmb_job_id, "imported")]

    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: FakeRemote())

    result = CliRunner().invoke(
        cli.app,
        [
            "jobs",
            "list",
            "--session-id",
            "shared",
            "--format",
            "json",
            "--destination",
            "gs://bucket/qmb/",
        ],
    )

    assert result.exit_code == 0, result.output + result.stderr
    notice, json_payload = result.output.split("\n", 1)
    assert notice == "qmb: importing remote session shared from gs://bucket/qmb/"
    payload = json.loads(json_payload)
    assert [record["qmb_job_id"] for record in payload] == [remote_record.qmb_job_id]
    assert imported == ["shared"]


def test_jobs_list_missing_session_stays_local_without_remote_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    jobs_root = tmp_path / "jobs"
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))
    monkeypatch.delenv("QMB_REMOTE_ARCHIVE_URI", raising=False)
    monkeypatch.setattr("qmb.config.load_config", lambda: {})
    monkeypatch.setattr(
        "qmb.jobs.remote.get_remote_archive",
        lambda destination: pytest.fail("remote should not be used without config"),
    )

    result = CliRunner().invoke(
        cli.app,
        ["jobs", "list", "--session-id", "missing"],
    )

    assert result.exit_code == 0, result.output + result.stderr
    assert result.output == "No local qmb jobs found.\n"
    assert result.stderr == ""


def test_jobs_sql_missing_job_explains_remote_not_configured(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("QMB_JOBS_DIR", str(tmp_path / "jobs"))
    monkeypatch.delenv("QMB_REMOTE_ARCHIVE_URI", raising=False)
    monkeypatch.setattr("qmb.config.load_config", lambda: {})

    result = CliRunner().invoke(cli.app, ["jobs", "sql", "missing"])

    assert result.exit_code != 0
    assert "Remote lookup is not configured" in result.stderr


def test_jobs_open_launches_tui_for_archived_preview(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    jobs_root = tmp_path / "jobs"
    first, _second = _seed_jobs(jobs_root)
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))
    captured: dict[str, Any] = {}

    def fake_init(self: Any, **kwargs: Any) -> None:
        captured.update(kwargs)

    def fake_run(self: Any) -> None:
        captured["ran"] = True

    monkeypatch.setattr("qmb.tui.app.QueryResultApp.__init__", fake_init)
    monkeypatch.setattr("qmb.tui.app.QueryResultApp.run", fake_run)

    result = CliRunner().invoke(
        cli.app,
        ["jobs", "open", "abc111", "--page-size", "50"],
    )

    assert result.exit_code == 0, result.output
    assert captured["ran"] is True
    assert captured["bq_client"] is None
    assert captured["source_label"] == f"archive: {first.qmb_job_id}"
    assert captured["resolved_sql"] == "SELECT 'first' AS label"
    assert captured["page_size"] == 50
    assert captured["handle"].job_id == first.qmb_job_id
    assert captured["handle"].schema == [
        {"name": "label", "type": "STRING", "mode": "NULLABLE"}
    ]
    assert captured["result_source"].__class__.__name__ == "JsonlPreviewResultSource"


def test_jobs_export_publishes_one_job_with_destination(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    jobs_root = tmp_path / "jobs"
    first, _second = _seed_jobs(jobs_root)
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))
    calls: list[tuple[str, str]] = []

    class FakeRemote:
        def export_job(self, record: Any, *, preview_rows: int | None = None) -> Any:
            calls.append((record.qmb_job_id, str(preview_rows)))
            return _FakeRemoteResult(record.qmb_job_id, "exported")

    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: FakeRemote())

    result = CliRunner().invoke(
        cli.app,
        [
            "jobs",
            "export",
            first.qmb_job_id,
            "--destination",
            "gs://bucket/qmb/",
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output + result.stderr
    payload = json.loads(result.output)
    assert payload["destination"] == "gs://bucket/qmb/"
    assert payload["jobs"][0]["qmb_job_id"] == first.qmb_job_id
    assert calls == [(first.qmb_job_id, "500")]


def test_jobs_export_prints_index_warning_to_stderr_in_text_mode(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    jobs_root = tmp_path / "jobs"
    first, _second = _seed_jobs(jobs_root)
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))

    class FakeRemote:
        def export_job(self, record: Any, *, preview_rows: int | None = None) -> Any:
            return _FakeRemoteResult(
                record.qmb_job_id,
                "exported",
                warning="Remote index update failed (run `qmb jobs reindex --remote`): boom",
            )

    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: FakeRemote())

    result = CliRunner().invoke(
        cli.app,
        ["jobs", "export", first.qmb_job_id, "--destination", "gs://bucket/qmb/"],
    )

    assert result.exit_code == 0, result.output + result.stderr
    assert f"exported {first.qmb_job_id}" in result.output
    assert "warning: Remote index update failed" in result.stderr


def test_jobs_export_publishes_all_jobs_for_session(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    models, store_module = _jobs_modules()
    jobs_root = tmp_path / "jobs"
    store = store_module.JobStore(
        root=jobs_root,
        now=_iter_callable(
            [
                datetime(2026, 5, 12, 10, 0, tzinfo=UTC),
                datetime(2026, 5, 12, 11, 0, tzinfo=UTC),
            ]
        ),
        nonce=_iter_callable(["abc111", "def222"]),
    )
    first = store.create(
        resolved_sql="SELECT 1",
        schema=[SchemaField("x", "INTEGER")],
        preview_rows=[{"x": 1}],
        source=models.SourceMetadata(label="first", input_mode="sql"),
        engine=models.EngineMetadata(name="bigquery"),
        total_rows=1,
        session_id="shared",
    )
    second = store.create(
        resolved_sql="SELECT 2",
        schema=[SchemaField("x", "INTEGER")],
        preview_rows=[{"x": 2}],
        source=models.SourceMetadata(label="second", input_mode="sql"),
        engine=models.EngineMetadata(name="bigquery"),
        total_rows=1,
        session_id="shared",
    )
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))
    exported: list[str] = []

    class FakeRemote:
        def export_job(self, record: Any, *, preview_rows: int | None = None) -> Any:
            exported.append(record.qmb_job_id)
            return _FakeRemoteResult(record.qmb_job_id, "exported")

    monkeypatch.setenv("QMB_REMOTE_ARCHIVE_URI", "gs://env-bucket/qmb/")
    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: FakeRemote())

    result = CliRunner().invoke(
        cli.app,
        ["jobs", "export", "--session-id", "shared", "--format", "json"],
    )

    assert result.exit_code == 0, result.output + result.stderr
    assert json.loads(result.output)["destination"] == "gs://env-bucket/qmb/"
    assert exported == [first.qmb_job_id, second.qmb_job_id]


def test_jobs_import_session_uses_remote_archive(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("QMB_JOBS_DIR", str(tmp_path / "jobs"))
    imported: list[tuple[str, bool]] = []

    class FakeRemote:
        def import_session(
            self,
            session_id: str,
            store: Any,
            *,
            overwrite: bool = False,
        ) -> list[Any]:
            imported.append((session_id, overwrite))
            return [_FakeRemoteResult("qmb_job", "imported")]

    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", lambda destination: FakeRemote())

    result = CliRunner().invoke(
        cli.app,
        [
            "jobs",
            "import",
            "--session-id",
            "shared",
            "--destination",
            "gs://bucket/qmb/",
            "--overwrite",
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0, result.output + result.stderr
    payload = json.loads(result.output)
    assert payload["jobs"] == [
        {
            "qmb_job_id": "qmb_job",
            "status": "imported",
            "uri": "gs://bucket/qmb/qmb_job/",
            "error": None,
            "warning": None,
        }
    ]
    assert imported == [("shared", True)]


class _FakeRemoteResult:
    def __init__(self, qmb_job_id: str, status: str, warning: str | None = None) -> None:
        self.qmb_job_id = qmb_job_id
        self.status = status
        self.warning = warning

    def to_mapping(self) -> dict[str, Any]:
        return {
            "qmb_job_id": self.qmb_job_id,
            "status": self.status,
            "uri": f"gs://bucket/qmb/{self.qmb_job_id}/",
            "error": None,
            "warning": self.warning,
        }


def test_jobs_reindex_rebuilds_manifests_from_existing_jobs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """`qmb jobs reindex` rebuilds session manifests from a full scan."""
    import shutil as _shutil

    from qmb.jobs.store import JobStore

    jobs_root = tmp_path / "jobs"
    monkeypatch.setenv("QMB_JOBS_DIR", str(jobs_root))
    store = JobStore(
        root=jobs_root,
        now=_iter_callable(
            [
                datetime(2026, 5, 12, 10, 0, tzinfo=UTC),
                datetime(2026, 5, 12, 11, 0, tzinfo=UTC),
            ]
        ),
        nonce=_iter_callable(["abc111", "def222"]),
    )
    models, _ = _jobs_modules()
    store.create(
        resolved_sql="SELECT 1",
        schema=[SchemaField("x", "INTEGER")],
        preview_rows=[{"x": 1}],
        source=models.SourceMetadata(label="ad-hoc", input_mode="sql"),
        engine=models.EngineMetadata(name="bigquery"),
        total_rows=1,
        session_id="alpha",
        agent_context=AgentContext(name="pi", session_id="alpha"),
    )
    store.create(
        resolved_sql="SELECT 2",
        schema=[SchemaField("x", "INTEGER")],
        preview_rows=[{"x": 2}],
        source=models.SourceMetadata(label="ad-hoc", input_mode="sql"),
        engine=models.EngineMetadata(name="bigquery"),
        total_rows=1,
        session_id="beta",
    )
    # Wipe manifests to simulate a pre-manifest archive.
    _shutil.rmtree(store.sessions_dir())

    result = CliRunner().invoke(cli.app, ["jobs", "reindex", "--format", "json"])

    assert result.exit_code == 0, result.output + result.stderr
    payload = json.loads(result.output)
    assert payload == {"sessions_rebuilt": 2}
    assert store.manifest_path_for("alpha").is_file()
    assert store.manifest_path_for("beta").is_file()


def test_jobs_reindex_reports_nothing_when_empty(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("QMB_JOBS_DIR", str(tmp_path / "jobs"))

    result = CliRunner().invoke(cli.app, ["jobs", "reindex"])

    assert result.exit_code == 0, result.output + result.stderr
    assert "No qmb sessions" in result.output


def test_jobs_reindex_remote_rebuilds_and_writes_index(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`qmb jobs reindex --remote` scans the remote archive and writes index.json."""
    built_index = {
        "version": 1,
        "updated_at": "2026-05-12T11:00:00+00:00",
        "jobs": {
            "qmb_job_1": {"qmb_job_id": "qmb_job_1"},
            "qmb_job_2": {"qmb_job_id": "qmb_job_2"},
        },
    }
    written: list[dict[str, Any]] = []

    class FakeRemote:
        def build_index(self) -> dict[str, Any]:
            return built_index

        def write_index(self, data: dict[str, Any]) -> None:
            written.append(data)

    destinations: list[str] = []

    def fake_remote(destination: str) -> FakeRemote:
        destinations.append(destination)
        return FakeRemote()

    monkeypatch.setattr("qmb.jobs.remote.get_remote_archive", fake_remote)

    result = CliRunner().invoke(
        cli.app,
        ["jobs", "reindex", "--remote", "--destination", "gs://bucket/qmb/", "--format", "json"],
    )

    assert result.exit_code == 0, result.output + result.stderr
    assert json.loads(result.output) == {"jobs_indexed": 2}
    assert destinations == ["gs://bucket/qmb/"]
    assert written == [built_index]


def test_jobs_reindex_remote_requires_destination(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("QMB_REMOTE_ARCHIVE_URI", raising=False)
    monkeypatch.setattr("qmb.config.load_config", lambda: {})
    monkeypatch.setattr(
        "qmb.jobs.remote.get_remote_archive",
        lambda destination: pytest.fail("should not build a remote archive without a destination"),
    )

    result = CliRunner().invoke(cli.app, ["jobs", "reindex", "--remote"])

    assert result.exit_code != 0
    assert "not configured" in (result.output + result.stderr)
