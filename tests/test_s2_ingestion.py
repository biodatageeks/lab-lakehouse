"""Session 2: COPY (incremental, schema evolution, rejects), Snowpipe, file-arrival task."""

import pytest
from conftest import MALFORMED_AT, N_FIRST, N_TOTAL
from producer import answer_key

NO_FILES = "Copy executed with 0 files processed."


@pytest.fixture(scope="module")
def first(nb):
    return nb("02_ingestion", start=1, count=N_FIRST)


@pytest.fixture(scope="module")
def second(first, nb):
    return nb("02_ingestion", start=N_FIRST + 1, count=N_TOTAL - N_FIRST, malformed_at=MALFORMED_AT, with_task=1)


def test_first_batch(first, record):
    k = answer_key(N_FIRST)
    record("s2.first.copy", first["copy"])
    assert first["copy"]["files"] == N_FIRST
    assert first["orders_raw_rows"] == k["bronze_orders_rows"]
    assert "COUPON_CODE" not in first["orders_raw_columns"]


def test_rerun_loads_nothing_new(first):
    assert first["copy_rerun"] == {"status": NO_FILES}


def test_snowpipe_clicks(first, second, record):
    record("s2.pipe_wait_s", [first["pipe_wait_s"], second["pipe_wait_s"]])
    assert first["clicks_raw_rows"] == answer_key(N_FIRST)["bronze_clicks_rows"]
    assert second["clicks_raw_rows"] == answer_key(N_TOTAL)["bronze_clicks_rows"]


def test_triggered_task_loads_new_files(second, record):
    record("s2.task_runs", second["task_runs"])
    record("s2.task_wait_s", second["task_wait_s"])
    assert any(r["STATE"] == "SUCCEEDED" for r in second["task_runs"])
    assert second["files_seen_by_stream"] >= N_TOTAL - N_FIRST
    assert second["copy"] == {"status": NO_FILES}, "the task should already have loaded every file"


def test_schema_evolution_adds_coupon_code(second, record):
    record("s2.schema_evolution_record", second["schema_evolution_record"])
    assert "COUPON_CODE" in second["orders_raw_columns"]
    assert second["schema_evolution_record"]


def test_malformed_row_rejected_and_reported(second, record):
    k = answer_key(N_TOTAL, malformed_at=MALFORMED_AT)
    record("s2.rejected_errors", second["rejected_errors"])
    assert second["rejected_rows"] == k["rejected_rows"] == 1
    assert any("not-a-number" in e for e in second["rejected_errors"])
    assert second["orders_raw_rows"] == k["bronze_orders_rows"]
