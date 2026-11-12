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



def test_before_and_after_versions_of_the_bad_update(out, record):
    """BEFORE (STATEMENT) = without the update, AT (STATEMENT) = with it; the diff = changed rows."""
    k = answer_key(N_TOTAL, malformed_at=MALFORMED_AT)
    before, after = out["versions"]["before"], out["versions"]["after"]
    record("s4.versions", out["versions"])
    record("s4.changed_sample", out["changed_sample"])
    assert before == {"orders": out["rows"], "zero_amount": 0, "revenue": k["silver_revenue_total"]}
    assert after["orders"] == out["rows"] and after["zero_amount"] == out["zeroed_now"]
    assert after["revenue"] < before["revenue"]
    assert out["changed_rows"] == out["zeroed_now"]
    assert all(r["status"] == "placed" and r["amount_after"] == 0 and r["amount_before"] > 0
               for r in out["changed_sample"])


def test_version_syntax_on_iceberg(out, record):
    """Iceberg: AT (VERSION => snapshot id); native tables have no version numbers."""
    k = answer_key(N_TOTAL, malformed_at=MALFORMED_AT)
    record("s4.iceberg_snapshots", out["iceberg_snapshots"])
    record("s4.iceberg_versions", out["iceberg_versions"])
    record("s4.native_version_syntax", out["native_version_syntax"])
    assert len(out["iceberg_snapshots"]) >= 2
    v = out["iceberg_versions"]
    assert v["before"] == {"orders": out["rows"], "zero_amount": 0, "revenue": k["silver_revenue_total"]}
    assert v["after"] == out["versions"]["after"]  # same bad update, same result as the native table
    assert out["iceberg_before_statement_zero"] == 0  # statement-based time travel works on Iceberg too
    assert str(out["native_version_syntax"]).startswith("ERR")

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
