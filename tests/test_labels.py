"""BigQuery job label validation."""

from __future__ import annotations

import pytest

from qmb.bigquery.labels import validate_labels


def test_validate_labels_accepts_valid_labels() -> None:
    labels = {"gq_session": "abc-123", "gq_tool": "run_query", "a" * 63: "b" * 63, "empty": ""}

    assert validate_labels(labels) == labels


def test_validate_labels_accepts_64_labels() -> None:
    labels = {f"k{i}": "v" for i in range(64)}

    assert validate_labels(labels) == labels


@pytest.mark.parametrize(
    ("labels", "fragment"),
    [
        ({"Upper": "v"}, "'Upper'"),
        ({"1key": "v"}, "'1key'"),
        ({"": "v"}, "''"),
        ({"a" * 64: "v"}, "a" * 64),
        ({"key": "v" * 64}, "v" * 64),
        ({"key": "Value"}, "'Value'"),
        ({"key": "with space"}, "'with space'"),
    ],
)
def test_validate_labels_rejects_invalid_key_or_value(
    labels: dict[str, str], fragment: str
) -> None:
    with pytest.raises(ValueError, match="label") as excinfo:
        validate_labels(labels)

    assert fragment in str(excinfo.value)


def test_validate_labels_rejects_more_than_64_labels() -> None:
    with pytest.raises(ValueError, match="65"):
        validate_labels({f"k{i}": "v" for i in range(65)})
