"""Session 4: query history, BEFORE/AT, clone+swap restore, undrop, retention."""

import pytest
from conftest import MALFORMED_AT, N_TOTAL
from producer import answer_key


@pytest.fixture(scope="module")
def out(nb):
    return nb("04_time_travel")


def test_find_bad_update_and_query_before_it(out, record):
    record("s4.bad_query", out["bad_query"])
    assert out["zeroed_now"] > 0
    assert out["zeroed_before_statement"] == 0
    assert out["zeroed_at_timestamp"] == 0


def test_restore_with_clone_and_swap(out):
    assert out["zeroed_after_restore"] == 0
    assert abs(out["revenue_after_restore"] - answer_key(N_TOTAL, malformed_at=MALFORMED_AT)["silver_revenue_total"]) < 0.01


def test_undrop(out):
    assert str(out["after_drop"]).startswith("ERR")
    assert out["after_undrop"] == out["rows"]


def test_retention_and_housekeeping(out, record):
    record("s4.retention_settings", out["retention_settings"])
    record("s4.vacuum", out["vacuum"])
    record("s4.storage_metrics", out["storage_metrics"])
    assert all(v == "ok" for v in out["retention_settings"].values())
    assert "TO_DATE" in out["clustering"]["cluster_by_keys"].upper()
    assert str(out["vacuum"]).startswith("ERR")
