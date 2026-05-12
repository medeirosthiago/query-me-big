"""Backwards-compatible facade for the catalog browser modules.

New code should import directly from the focused modules:

- :mod:`qmb.bigquery.catalog` — metadata listing/fetching
- :mod:`qmb.bigquery.catalog_search` — fuzzy/glob filtering and ``BrowserMatch``
- :mod:`qmb.bigquery.catalog_format` — details formatting for editor display
"""

from qmb.bigquery.catalog import (
    build_table_index,
    get_dataset_metadata,
    get_table_metadata,
    list_dataset_ids,
    list_dataset_tables,
)
from qmb.bigquery.catalog_format import format_dataset_details, format_table_details
from qmb.bigquery.catalog_search import BrowserMatch, filter_browser_matches

__all__ = [
    "BrowserMatch",
    "build_table_index",
    "filter_browser_matches",
    "format_dataset_details",
    "format_table_details",
    "get_dataset_metadata",
    "get_table_metadata",
    "list_dataset_ids",
    "list_dataset_tables",
]
