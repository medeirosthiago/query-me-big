"""Execute queries against BigQuery."""

from collections.abc import Mapping

from google.cloud import bigquery

from qmb.bigquery.labels import validate_labels
from qmb.types import QueryResultHandle, ResolvedQuery


def execute_query(
    client: bigquery.Client,
    resolved: ResolvedQuery,
    dry_run: bool = False,
    max_bytes_billed: int | None = None,
    labels: Mapping[str, str] | None = None,
) -> QueryResultHandle:
    """Execute a query and return a handle to the results.

    ``labels`` are attached to the BigQuery job (dry runs included) after
    validation against BigQuery's label rules.
    """
    job_config = bigquery.QueryJobConfig()
    if dry_run:
        job_config.dry_run = True
    if max_bytes_billed is not None:
        job_config.maximum_bytes_billed = max_bytes_billed
    if labels:
        job_config.labels = validate_labels(labels)

    job = client.query(resolved.sql, job_config=job_config)

    if dry_run:
        return QueryResultHandle(
            job_id=job.job_id,
            project=job.project,
            location=job.location,
            destination_table="",
            schema=[],
            total_rows=0,
            bytes_processed=job.total_bytes_processed or 0,
        )

    # Wait for completion
    result = job.result()
    total_rows = result.total_rows or 0

    # Compute execution duration
    execution_seconds = 0.0
    if job.started and job.ended:
        execution_seconds = (job.ended - job.started).total_seconds()

    dest = job.destination
    dest_str = f"{dest.project}.{dest.dataset_id}.{dest.table_id}" if dest else ""

    schema = [
        {"name": field.name, "type": field.field_type, "mode": field.mode}
        for field in result.schema
    ]

    return QueryResultHandle(
        job_id=job.job_id,
        project=job.project,
        location=job.location,
        destination_table=dest_str,
        schema=schema,
        total_rows=total_rows,
        bytes_processed=job.total_bytes_processed or 0,
        execution_seconds=execution_seconds,
    )
