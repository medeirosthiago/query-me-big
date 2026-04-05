"""BigQuery client setup."""

from __future__ import annotations

from google.cloud import bigquery


def get_client(project: str | None = None, location: str | None = None) -> bigquery.Client:
    """Create a BigQuery client."""
    kwargs: dict = {}
    if project:
        kwargs["project"] = project
    if location:
        kwargs["location"] = location
    return bigquery.Client(**kwargs)
