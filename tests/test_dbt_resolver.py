import pytest

from qmb.dbt.manifest import ManifestIndex, ManifestNode
from qmb.dbt.resolver import _parse_default, _to_sql_literal, resolve_model_query


def test_string_literals_are_sql_escaped() -> None:
    assert _to_sql_literal("O'Reilly") == "'O''Reilly'"
    assert _parse_default("'1'") == "'1'"
    assert _parse_default('"O\'Reilly"') == "'O''Reilly'"


def test_model_var_overrides_use_raw_sql_resolution() -> None:
    orders = ManifestNode(
        unique_id="model.pkg.orders",
        name="orders",
        resource_type="model",
        package_name="pkg",
        database="proj",
        schema_name="analytics",
        alias="orders",
        compiled_code="select * from `proj`.`analytics`.`orders`",
        raw_code="select * from {{ ref('orders') }}",
        original_file_path="models/orders.sql",
    )
    report = ManifestNode(
        unique_id="model.pkg.report",
        name="report",
        resource_type="model",
        package_name="pkg",
        database="proj",
        schema_name="analytics",
        alias="report",
        compiled_code="select 10 as limit_value",
        raw_code=(
            "{{ config(materialized='view') }} select {{ var('limit', 10) }} "
            "as limit_value from {{ ref('orders') }}"
        ),
        original_file_path="models/report.sql",
    )
    index = ManifestIndex(nodes_by_id={orders.unique_id: orders, report.unique_id: report})

    resolved = resolve_model_query("report", index, {"limit": 25})

    assert resolved.sql == "select 25 as limit_value from `proj`.`analytics`.`orders`"


def test_model_var_overrides_fail_on_unsupported_jinja() -> None:
    report = ManifestNode(
        unique_id="model.pkg.report",
        name="report",
        resource_type="model",
        package_name="pkg",
        database="proj",
        schema_name="analytics",
        alias="report",
        compiled_code="select 1",
        raw_code="select {{ custom_macro() }} as value",
        original_file_path="models/report.sql",
    )
    index = ManifestIndex(nodes_by_id={report.unique_id: report})

    with pytest.raises(ValueError, match="Unsupported Jinja"):
        resolve_model_query("report", index, {"limit": 1})
