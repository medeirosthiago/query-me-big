from types import SimpleNamespace

from qmb.bigquery.browser import BrowserMatch, build_table_index, filter_browser_matches


class DummyBigQueryClient:
    project = "proj"

    def list_tables(self, dataset_ref: str):
        tables = {
            "proj.dataset1": ["table1", "table2"],
            "proj.dataset2": ["table9"],
        }
        return [SimpleNamespace(table_id=table_id) for table_id in tables[dataset_ref]]


def test_filter_browser_matches_expands_dataset_matches() -> None:
    matches = filter_browser_matches(
        ["dataset1", "dataset2", "dataset3"],
        {
            "dataset1": ("table1", "table2"),
            "dataset2": ("table9",),
            "dataset3": ("table1", "table2", "table3"),
        },
        "dataset3",
    )

    assert matches == [
        BrowserMatch(
            dataset_id="dataset3",
            tables=("dataset3.table1", "dataset3.table2", "dataset3.table3"),
        )
    ]


def test_filter_browser_matches_returns_matching_tables_only() -> None:
    matches = filter_browser_matches(
        ["dataset1", "dataset2", "dataset3"],
        {
            "dataset1": ("table1", "table2"),
            "dataset2": ("table9",),
            "dataset3": ("table1", "table2", "table3"),
        },
        "table1",
    )

    assert matches == [
        BrowserMatch(dataset_id="dataset1", tables=("dataset1.table1",)),
        BrowserMatch(dataset_id="dataset3", tables=("dataset3.table1",)),
    ]


def test_filter_browser_matches_supports_glob_patterns() -> None:
    matches = filter_browser_matches(
        ["dataset1", "dataset2", "dataset3"],
        {
            "dataset1": ("table1", "table2"),
            "dataset2": ("table9",),
            "dataset3": ("table1", "table2", "table3"),
        },
        "dataset?.table2",
    )

    assert matches == [
        BrowserMatch(dataset_id="dataset1", tables=("dataset1.table2",)),
        BrowserMatch(dataset_id="dataset3", tables=("dataset3.table2",)),
    ]


def test_build_table_index_uses_project_qualified_dataset_refs() -> None:
    client = DummyBigQueryClient()

    assert build_table_index(client, ["dataset1", "dataset2"]) == {
        "dataset1": ("table1", "table2"),
        "dataset2": ("table9",),
    }
