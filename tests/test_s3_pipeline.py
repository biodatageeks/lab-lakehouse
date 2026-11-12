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


LIVE_MINUTES = 4


@pytest.fixture(scope="module")
def live(out, nb):
    """Second run in live mode: 4 new files, one a minute; nobody calls REFRESH."""
    return nb("03_pipeline", live_minutes=LIVE_MINUTES, interval=60)["live"]


def test_live_mode_processes_files_as_they_land(live, record):
    tl = live["timeline"]
    record("s3.live.files", live["files"])
    record("s3.live.settled_after_last_file_s", live["settled_after_last_file_s"])
    record("s3.live.timeline", [(s["t_s"], s["live_bronze_rows"], s["silver_rows"], s["gold_revenue"]) for s in tl])
    assert len({s["live_bronze_rows"] for s in tl}) >= 3, "bronze should grow file by file"
    assert len({s["silver_rows"] for s in tl}) >= 3, "silver should grow while files arrive"
    assert any(s["silver_rows"] == live["expected_silver_rows"] and s["gold_revenue"] == live["expected_gold_revenue"]
               for s in tl), "every live file should reach gold"


def test_live_mode_uses_scheduled_refreshes_only(live, record):
    record("s3.live.scheduled_refreshes", live["scheduled_refreshes"])
    inserted = [r for r in live["scheduled_refreshes"] if (r["INSERTED"] or 0) > 0]
    assert live["manual_refreshes"] == 0
    assert {r["NAME"] for r in inserted} >= {"ORDERS", "REVENUE_HOURLY"}
    assert len(inserted) >= 4


def test_live_mode_cleans_up(live):
    after = live["after_cleanup"]
    assert after["live_bronze_rows"] == 0
    assert after["silver_rows"] == K["silver_orders_rows"]
    assert abs(after["gold_revenue"] - K["silver_revenue_total"]) < 0.01
