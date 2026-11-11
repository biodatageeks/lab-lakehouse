"""Session 2: incremental ingestion with Auto Loader on serverless."""

import pytest

from lab.common import CATALOG
from lab.remote import NotebookFailed, run_notebook
from producer import produce
from tests.conftest import MALFORMED_AT, N_FIRST, N_TOTAL

NB = "notebooks/02_autoloader.py"


def _run(stream: str, trigger: str = "availableNow") -> dict:
    return run_notebook(NB, {"catalog": CATALOG, "stream": stream, "trigger": trigger})


def test_first_batch_available_now(record):
    produce.main(["--start", "1", "--count", str(N_FIRST), "--interval", "0"])
    out = _run("orders")
    record("s2.first_run", {k: out[k] for k in ("rows_before", "rows_after")})
    assert out["rows_after"] == produce.answer_key(N_FIRST)["bronze_orders_rows"]
    assert "_rescued_data" in out["columns"]


def test_incremental_run_schema_evolution_and_rescued_data(record):
    produce.main(["--start", str(N_FIRST + 1), "--count", str(N_TOTAL - N_FIRST), "--interval", "0",
                  "--malformed-at", str(MALFORMED_AT)])
    # 1st run: hits coupon_code (file 30), records the new schema and stops with an error.
    with pytest.raises(NotebookFailed) as ei:
        _run("orders")
    err = str(ei.value)
    assert "UNKNOWN_FIELD_EXCEPTION" in err or "UnknownFieldException" in err, err[:500]
    # 2nd run (what a student does: re-run the cell) picks up the new column.
    out = _run("orders")
    key = produce.answer_key(N_TOTAL, malformed_at=MALFORMED_AT)
    record("s2.incremental_run", {k: out[k] for k in ("rows_before", "rows_after", "rescued_rows")}
           | {"first_run_error": err.splitlines()[0][:200]})
    assert out["rows_after"] == key["bronze_orders_rows"]
    assert "coupon_code" in out["columns"], "new column from file 30 not picked up"
    assert out["rescued_rows"] >= 1, "malformed amount should land in _rescued_data"


def test_rerun_processes_nothing_new():
    out = _run("orders")
    assert out["rows_before"] == out["rows_after"]


def test_clicks_stream():
    out = _run("clicks")
    assert out["rows_after"] == produce.answer_key(N_TOTAL)["bronze_clicks_rows"]


def test_processing_time_trigger_on_serverless(record):
    out = _run("orders", "processingTime")
    record("s2.processing_time_trigger", {"supported": out["processing_time_supported"],
                                         "error": (out["errors"] or [None])[0]})
    if out["processing_time_supported"]:
        pytest.fail("processingTime unexpectedly accepted on serverless: update the 'every minute' plan")
