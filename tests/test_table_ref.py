"""Tests for :class:`qmb.types.TableRef`."""

import pytest

from qmb.types import TableRef


def test_parse_full_reference() -> None:
    ref = TableRef.parse("proj.ds.tbl")
    assert ref.project == "proj"
    assert ref.dataset == "ds"
    assert ref.table == "tbl"
    assert not ref.is_empty


def test_parse_empty_string_returns_empty_ref() -> None:
    ref = TableRef.parse("")
    assert ref.is_empty
    assert ref.project == ""
    assert ref.dataset == ""
    assert ref.table == ""


def test_parse_invalid_raises() -> None:
    with pytest.raises(ValueError):
        TableRef.parse("not.enough")
    with pytest.raises(ValueError):
        TableRef.parse("too.many.parts.here")


def test_str_round_trip() -> None:
    s = "proj.ds.tbl"
    assert str(TableRef.parse(s)) == s


def test_str_empty_ref_is_empty_string() -> None:
    assert str(TableRef("", "", "")) == ""


def test_is_empty_true_only_when_all_parts_empty() -> None:
    assert TableRef("", "", "").is_empty
    assert not TableRef("p", "", "").is_empty
    assert not TableRef("", "d", "").is_empty
    assert not TableRef("", "", "t").is_empty
