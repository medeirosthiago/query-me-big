"""BigQuery client setup."""

from typing import Any

from google.cloud import bigquery


def get_client(project: str | None = None, location: str | None = None) -> bigquery.Client:
    """Create a BigQuery client."""
    kwargs: dict[str, Any] = {}
    if project:
        kwargs["project"] = project
    if location:
        kwargs["location"] = location
    return bigquery.Client(**kwargs)
