import json

from qmb.bigquery import exporters
from qmb.types import ExportFormat, QueryResultHandle


def _handle() -> QueryResultHandle:
    return QueryResultHandle(
        job_id="job",
        project="proj",
        location="US",
        destination_table="proj.ds.tbl",
        schema=[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "enabled", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "payload", "type": "JSON", "mode": "NULLABLE"},
        ],
        total_rows=2,
    )


def test_export_results_streams_json_rows(monkeypatch, tmp_path) -> None:
    rows = [
        {"id": 1, "enabled": True, "payload": {"items": [1, 2]}},
        {"id": 2, "enabled": False, "payload": {"items": [3]}},
    ]

    def fake_iter_all_rows(client, handle, chunk_size=5000):
        yield from rows

    monkeypatch.setattr(exporters, "iter_all_rows", fake_iter_all_rows)

    out = tmp_path / "rows.json"
    count = exporters.export_results(None, _handle(), ExportFormat.JSON, out)

    assert count == 2
    assert json.loads(out.read_text(encoding="utf-8")) == rows
