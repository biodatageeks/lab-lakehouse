"""Session 1: stage upload from a notebook, typed table from CSV, managed Iceberg metadata."""

import pytest
from producer import N_PRODUCTS


@pytest.fixture(scope="module")
def out(nb):
    return nb("01_storage")


def test_file_uploaded_to_stage_from_notebook(out):
    assert out["stage_files"] == ["landing/products/products.csv"]


def test_table_from_csv_with_infer_schema(out, record):
    record("s1.inferred_schema", out["inferred_schema"])
    assert out["dim_products_rows"] == N_PRODUCTS
    assert out["dim_products_columns"] == ["PRODUCT_ID", "NAME", "CATEGORY", "PRICE"]


def test_managed_iceberg_on_snowflake_storage(out, record):
    record("s1.iceberg_table", out["iceberg_table"])
    assert out["iceberg_table"]["external_volume_name"] == "SNOWFLAKE_MANAGED"
    assert out["iceberg_metadata_location"].endswith(".metadata.json")


def test_iceberg_metadata_via_horizon_rest_from_notebook(out, record):
    md = out["horizon_metadata"]
    record("s1.horizon_metadata", md)
    assert isinstance(md, dict), md
    # CTAS + UPDATE + DELETE: usually 3 snapshots, but Snowflake may commit the two changes as one
    assert len(md["snapshots"]) >= 2
    assert md["snapshots"][0]["operation"] == "append" and md["snapshots"][-1]["added_records"] == "49"


def test_iceberg_data_files_not_reachable_from_notebook(out, record):
    """Documents a limit: open clients get metadata, not data, inside a trial notebook."""
    record("s1.horizon_data_read", out["horizon_data_read"])
    assert str(out["horizon_data_read"]).startswith("ERR")
