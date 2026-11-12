"""Session 3: dynamic tables + DMF expectations."""

import pytest
from conftest import MALFORMED_AT, N_TOTAL
from producer import answer_key

K = answer_key(N_TOTAL, malformed_at=MALFORMED_AT)


@pytest.fixture(scope="module")
def out(nb):
    return nb("03_pipeline")


def test_silver_and_gold_counts(out, record):
    record("s3.dynamic_tables", out["dynamic_tables"])
    assert out["silver_orders_rows"] == K["silver_orders_rows"]
    assert out["gold_hours"] == K["gold_revenue_hours"]
    assert abs(out["gold_revenue"] - K["silver_revenue_total"]) < 0.01


def test_expectations_measure_bad_rows(out, record):
    record("s3.expectations", out["expectations"])
    assert out["expectations"]["VALID_AMOUNT"]["value"] == K["invalid_amount_rows"]
    assert out["expectations"]["VALID_CUSTOMER"]["value"] == K["null_customer_rows"]


def test_refresh_history_is_the_event_log(out, record):
    record("s3.refresh_history", out["refresh_history"])
    assert out["refresh_history"] and all(r["STATE"] == "SUCCEEDED" for r in out["refresh_history"])


def test_fail_fast_workaround(out, record):
    record("s3.strict_refresh", out["strict_refresh"])
    assert str(out["strict_refresh"]).startswith("ERR")
