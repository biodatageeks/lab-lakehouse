"""Session 6: Snowflake / Polars / DuckDB on one table; Iceberg metadata + time travel."""

import pytest
from conftest import MALFORMED_AT, N_TOTAL
from producer import N_PRODUCTS, answer_key

K = answer_key(N_TOTAL, malformed_at=MALFORMED_AT)


@pytest.fixture(scope="module")
def out(nb):
    return nb("06_open_formats")


def test_three_engines_agree(out, record):
    record("s6.timings", {k: out[k] for k in ("snowflake_s", "to_arrow_s", "polars_s", "duckdb_s")})
    assert out["arrow_rows"] == out["polars_rows"] == K["silver_orders_rows"]
    assert out["engines_agree"] is True


def test_iceberg_metadata_and_time_travel(out, record):
    record("s6.horizon", out["horizon"])
    record("s6.products_iceberg_rows", out.get("products_iceberg_rows"))
    assert out["iceberg_table_type"] == "MANAGED"
    assert out["iceberg_rows_polars"] == K["distinct_customers"]
    assert out["iceberg_rows_first_snapshot"] == K["distinct_customers"]
    assert out["products_iceberg_rows"] == {"now": N_PRODUCTS - 1, "first_snapshot": N_PRODUCTS}


def test_open_clients_limits_inside_notebook(out, record):
    record("s6.pyiceberg_data_read", out["pyiceberg_data_read"])
    record("s6.duckdb_install_iceberg", out["duckdb_install_iceberg"])
    record("s6.egress", out["egress"])
    assert str(out["pyiceberg_data_read"]).startswith("ERR")
    assert str(out["duckdb_install_iceberg"]).startswith("ERR")
    assert all(str(v).startswith("ERR") for v in out["egress"].values())
