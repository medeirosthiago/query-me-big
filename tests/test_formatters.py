"""Tests for the qmb output formatters (Phase 10A)."""

import csv
import io
import json
from datetime import date, datetime
from decimal import Decimal

from tests.test_bigquery_flow import FakeBigQueryClient, FakeSchemaField

from qmb.application.outcomes import ExecutionOutcome
from qmb.application.resolver import ResolutionTrace
from qmb.formatters import (
    CsvFormatter,
    Format,
    JsonFormatter,
    TableFormatter,
    get_formatter,
)
from qmb.formatters.tui_fmt import TuiFormatter
from qmb.jobs.models import EngineMetadata, JobRecord, SourceMetadata
from qmb.types import QueryRequest, QueryResultHandle, ResolvedQuery

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _schema() -> list[FakeSchemaField]:
    return [
        FakeSchemaField("id", "INTEGER"),
        FakeSchemaField("name", "STRING"),
        FakeSchemaField("amount", "NUMERIC"),
        FakeSchemaField("created", "DATE"),
    ]


def _rows() -> list[dict]:
    return [
        {"id": 1, "name": "alice", "amount": Decimal("1.50"), "created": date(2026, 1, 1)},
        {"id": 2, "name": "bob",   "amount": Decimal("2.25"), "created": date(2026, 1, 2)},
    ]


def _handle(total_rows: int) -> QueryResultHandle:
    return QueryResultHandle(
        job_id="bq-job-xyz",
        project="proj",
        location="US",
        destination_table="proj.ds.tbl",
        schema=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "amount", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "created", "type": "DATE", "mode": "NULLABLE"},
        ],
        total_rows=total_rows,
        bytes_processed=4096,
        execution_seconds=1.25,
    )


def _request(**overrides) -> QueryRequest:
    base = {
        "mode": __import__("qmb.types", fromlist=["InputMode"]).InputMode.SQL,
        "sql": "select 1",
        "no_tui": True,
    }
    base.update(overrides)
    return QueryRequest(**base)


def _outcome(
    *,
    client,
    total_rows: int,
    dry_run: bool = False,
    archived_job: JobRecord | None = None,
    trace: ResolutionTrace | None = None,
    exported_path=None,
    exported_rows=None,
) -> ExecutionOutcome:
    return ExecutionOutcome(
        resolved=ResolvedQuery(sql="select * from t", source_label="ad-hoc"),
        handle=_handle(total_rows),
        client=client,
        trace=trace or ResolutionTrace(),
        dry_run=dry_run,
        archived_job=archived_job,
        exported_path=exported_path,
        exported_rows=exported_rows,
    )


# ---------------------------------------------------------------------------
# Format / factory
# ---------------------------------------------------------------------------


def test_format_parse_accepts_known_values() -> None:
    assert Format.parse("json") is Format.JSON
    assert Format.parse("CSV") is Format.CSV
    assert Format.parse("Table") is Format.TABLE
    assert Format.parse("tui") is Format.TUI


def test_format_parse_rejects_unknown_values() -> None:
    try:
        Format.parse("ndjson")
    except ValueError as e:
        assert "Invalid format" in str(e)
        assert "json" in str(e)
    else:
        raise AssertionError("expected ValueError")


def test_get_formatter_returns_matching_instance() -> None:
    assert isinstance(get_formatter(Format.JSON), JsonFormatter)
    assert isinstance(get_formatter(Format.CSV), CsvFormatter)
    assert isinstance(get_formatter(Format.TABLE), TableFormatter)
    assert isinstance(get_formatter(Format.TUI), TuiFormatter)


# ---------------------------------------------------------------------------
# JsonFormatter
# ---------------------------------------------------------------------------


def test_json_formatter_emits_full_payload_with_rows() -> None:
    client = FakeBigQueryClient(_rows(), _schema())
    outcome = _outcome(client=client, total_rows=2)

    buf = io.StringIO()
    JsonFormatter(stream=buf).render_run(outcome, _request())

    payload = json.loads(buf.getvalue())
    assert payload["dry_run"] is False
    assert payload["stats"] == {
        "total_rows": 2,
        "bytes_processed": 4096,
        "execution_seconds": 1.25,
        "job_id": "bq-job-xyz",
        "project": "proj",
        "location": "US",
        "source_label": "ad-hoc",
    }
    assert payload["schema"] == [
        {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "amount", "type": "NUMERIC", "mode": "NULLABLE"},
        {"name": "created", "type": "DATE", "mode": "NULLABLE"},
    ]
    # Decimals → float, dates → ISO 8601 string via json_default
    assert payload["rows"] == [
        {"id": 1, "name": "alice", "amount": 1.5, "created": "2026-01-01"},
        {"id": 2, "name": "bob", "amount": 2.25, "created": "2026-01-02"},
    ]
    assert payload["archive"] == {"qmb_job_id": None}
    assert payload["export"] is None


def test_json_formatter_includes_archive_and_export_when_present(tmp_path) -> None:
    client = FakeBigQueryClient(_rows(), _schema())
    record = JobRecord(
        qmb_job_id="20260101T120000-abc12345",
        created_at=datetime(2026, 1, 1, 12, 0, 0),
        source=SourceMetadata(label="ad-hoc", input_mode="sql"),
        engine=EngineMetadata(name="bigquery"),
        total_rows=2,
        bytes_processed=4096,
        execution_seconds=1.25,
        directory=tmp_path,
        metadata_path=tmp_path / "metadata.json",
        query_path=tmp_path / "query.sql",
        schema_path=tmp_path / "schema.json",
        preview_path=tmp_path / "preview.jsonl",
    )
    outcome = _outcome(
        client=client,
        total_rows=2,
        archived_job=record,
        exported_path=tmp_path / "out.csv",
        exported_rows=2,
    )

    buf = io.StringIO()
    JsonFormatter(stream=buf).render_run(outcome, _request())

    payload = json.loads(buf.getvalue())
    assert payload["archive"] == {"qmb_job_id": "20260101T120000-abc12345"}
    assert payload["export"] == {"path": str(tmp_path / "out.csv"), "rows": 2}


def test_json_formatter_dry_run_shape() -> None:
    client = FakeBigQueryClient([], _schema())
    outcome = _outcome(client=client, total_rows=0, dry_run=True)

    buf = io.StringIO()
    JsonFormatter(stream=buf).render_run(outcome, _request(dry_run=True))

    payload = json.loads(buf.getvalue())
    assert payload["dry_run"] is True
    assert payload["sql"] == "select * from t"
    assert payload["stats"] == {"bytes_processed": 4096, "source_label": "ad-hoc"}
    assert "rows" not in payload
    assert payload["schema"] == [
        {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "amount", "type": "NUMERIC", "mode": "NULLABLE"},
        {"name": "created", "type": "DATE", "mode": "NULLABLE"},
    ]


def test_json_formatter_handles_zero_rows_without_paging() -> None:
    client = FakeBigQueryClient([], _schema())
    outcome = _outcome(client=client, total_rows=0)

    buf = io.StringIO()
    JsonFormatter(stream=buf).render_run(outcome, _request())

    payload = json.loads(buf.getvalue())
    assert payload["rows"] == []
    assert client.list_rows_calls == []


def test_json_formatter_writes_trailing_newline() -> None:
    client = FakeBigQueryClient(_rows(), _schema())
    outcome = _outcome(client=client, total_rows=2)

    buf = io.StringIO()
    JsonFormatter(stream=buf).render_run(outcome, _request())

    assert buf.getvalue().endswith("\n")


# ---------------------------------------------------------------------------
# CsvFormatter
# ---------------------------------------------------------------------------


def test_csv_formatter_writes_header_and_rows() -> None:
    client = FakeBigQueryClient(_rows(), _schema())
    outcome = _outcome(client=client, total_rows=2)

    buf = io.StringIO()
    CsvFormatter(stream=buf).render_run(outcome, _request())

    parsed = list(csv.reader(io.StringIO(buf.getvalue())))
    assert parsed[0] == ["id", "name", "amount", "created"]
    assert parsed[1] == ["1", "alice", "1.5", "2026-01-01"]
    assert parsed[2] == ["2", "bob", "2.25", "2026-01-02"]


def test_csv_formatter_dry_run_emits_status_row() -> None:
    client = FakeBigQueryClient([], _schema())
    outcome = _outcome(client=client, total_rows=0, dry_run=True)

    buf = io.StringIO()
    CsvFormatter(stream=buf).render_run(outcome, _request(dry_run=True))

    parsed = list(csv.reader(io.StringIO(buf.getvalue())))
    assert parsed[0] == ["dry_run", "bytes_processed", "source_label"]
    assert parsed[1] == ["true", "4096", "ad-hoc"]


def test_csv_formatter_zero_rows_writes_header_only() -> None:
    client = FakeBigQueryClient([], _schema())
    outcome = _outcome(client=client, total_rows=0)

    buf = io.StringIO()
    CsvFormatter(stream=buf).render_run(outcome, _request())

    parsed = list(csv.reader(io.StringIO(buf.getvalue())))
    assert parsed == [["id", "name", "amount", "created"]]


def test_csv_formatter_serializes_nested_values_as_json() -> None:
    rows = [{"id": 1, "name": "x", "amount": Decimal("0"), "created": date(2026, 1, 1)}]
    rows[0]["name"] = {"nested": [1, 2]}  # nested struct in a STRING column
    client = FakeBigQueryClient(rows, _schema())
    outcome = _outcome(client=client, total_rows=1)

    buf = io.StringIO()
    CsvFormatter(stream=buf).render_run(outcome, _request())

    parsed = list(csv.reader(io.StringIO(buf.getvalue())))
    assert parsed[1][1] == '{"nested": [1, 2]}'


# ---------------------------------------------------------------------------
# TableFormatter
# ---------------------------------------------------------------------------


def test_table_formatter_prints_success_lines() -> None:
    from rich.console import Console

    client = FakeBigQueryClient(_rows(), _schema())
    outcome = _outcome(client=client, total_rows=2)

    buf = io.StringIO()
    formatter = TableFormatter(console=Console(file=buf, force_terminal=False, width=200))
    formatter.render_run(outcome, _request())

    out = buf.getvalue()
    assert "Source: ad-hoc" in out
    assert "Executing query" in out
    assert "2 rows" in out
    assert "Job: bq-job-xyz" in out


def test_table_formatter_dry_run_shows_panel_and_estimate() -> None:
    from rich.console import Console

    client = FakeBigQueryClient([], _schema())
    outcome = _outcome(client=client, total_rows=0, dry_run=True)

    buf = io.StringIO()
    formatter = TableFormatter(console=Console(file=buf, force_terminal=False, width=200))
    formatter.render_run(outcome, _request(dry_run=True))

    out = buf.getvalue()
    assert "Resolved SQL (dry run)" in out
    assert "Estimated" in out


def test_table_formatter_shows_matched_dbt_node() -> None:
    from rich.console import Console

    client = FakeBigQueryClient(_rows(), _schema())
    trace = ResolutionTrace(matched_node_id="model.proj.orders")
    outcome = _outcome(client=client, total_rows=2, trace=trace)

    buf = io.StringIO()
    formatter = TableFormatter(console=Console(file=buf, force_terminal=False, width=200))
    formatter.render_run(outcome, _request())

    assert "Matched manifest node: model.proj.orders" in buf.getvalue()


# ---------------------------------------------------------------------------
# TuiFormatter
# ---------------------------------------------------------------------------


def test_tui_formatter_skips_dry_runs() -> None:
    client = FakeBigQueryClient([], _schema())
    outcome = _outcome(client=client, total_rows=0, dry_run=True)
    # If TuiFormatter tried to launch on a dry-run, it would import textual
    # and call .run(). The assertion is that this returns cleanly.
    TuiFormatter().render_run(outcome, _request(dry_run=True))


def test_tui_formatter_skips_empty_results() -> None:
    client = FakeBigQueryClient([], _schema())
    outcome = _outcome(client=client, total_rows=0)
    TuiFormatter().render_run(outcome, _request())
