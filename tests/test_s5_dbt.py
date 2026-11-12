"""Session 5: dbt Projects on Snowflake (build, merge, Iceberg model, tests, docs)."""

import pytest
from conftest import MALFORMED_AT, N_TOTAL
from producer import answer_key

K = answer_key(N_TOTAL, malformed_at=MALFORMED_AT)


@pytest.fixture(scope="module")
def out(nb):
    return nb("05_dbt")


def test_build_twice_and_docs(out, record):
    record("s5.build_1", out["build_1"])
    record("s5.build_2", out["build_2"])
    for k in ("build_1", "build_2", "docs_generate"):
        assert out[k]["ok"] is True, out[k]
    assert "ERROR=0" in out["build_1"]["summary"]


def test_marts_counts(out):
    assert out["fct_orders_rows"] == K["silver_orders_rows"]
    assert out["dim_customers_rows"] == K["distinct_customers"]


def test_incremental_merge(out, record):
    record("s5.merge_statements", out["merge_statements"])
    assert out["merge_statements"] >= 1


def test_dim_customers_is_managed_iceberg(out, record):
    record("s5.dim_customers_iceberg", out["dim_customers_iceberg"])
    record("s5.dim_customers_snapshots", out["dim_customers_snapshots"])
    assert out["dim_customers_iceberg"]["external_volume_name"] == "SNOWFLAKE_MANAGED"
    assert out["dim_customers_snapshots"] >= 2
