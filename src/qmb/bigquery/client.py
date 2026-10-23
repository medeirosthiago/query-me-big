"""BigQuery client setup."""

from typing import Any

from google.cloud import bigquery


def get_client(
    project: str | None = None,
    location: str | None = None,
    credentials: Any | None = None,
) -> bigquery.Client:
    """Create a BigQuery client.

    ``credentials`` (a ``google.auth.credentials.Credentials``) overrides
    Application Default Credentials, e.g. to run as an end user.
    """
    kwargs: dict[str, Any] = {}
    if project:
        kwargs["project"] = project
    if location:
        kwargs["location"] = location
    if credentials is not None:
        kwargs["credentials"] = credentials
    return bigquery.Client(**kwargs)
