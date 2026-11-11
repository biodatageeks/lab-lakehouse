"""Session 3: Lakeflow Declarative Pipelines (+ homework FAIL UPDATE, one-pipeline limit)."""

import pytest
from databricks.sdk.service import pipelines as p

from grading.grade import dropped_by_expectation
from lab import pipelines as lp
from lab.common import CATALOG, sql, ws
from producer import produce
from tests.conftest import MALFORMED_AT, N_TOTAL

MAIN = "lab-orders-pipeline"
PROBE = "lab-probe-fail-update"
KEY = produce.answer_key(N_TOTAL, malformed_at=MALFORMED_AT)


def _running_span(pid: str, uid: str):
    """(INITIALIZING, terminal) timestamps of an update, from update_progress events."""
    times = {}
    for ev in ws().pipelines.list_pipeline_events(pid, max_results=100):
        if ev.event_type == "update_progress" and ev.origin and ev.origin.update_id == uid:
            state = (ev.message or "").rstrip(".").rsplit(" ", 1)[-1].upper()  # "Update x is RUNNING."
            times[state] = min(times.get(state, ev.timestamp), ev.timestamp)
    start = times.get("INITIALIZING") or times.get("SETTING_UP_TABLES") or times.get("RUNNING")
    end = times.get("COMPLETED") or times.get("FAILED") or times.get("CANCELED")
    return (start, end) if start and end else None


@pytest.fixture(scope="module")
def runs(record):
    main_id = lp.ensure(MAIN, "pipelines/03_pipeline/orders.sql", event_log_table="pipeline_event_log")
    probe_id = lp.ensure(PROBE, "pipelines/probe_fail_update.sql")
    main_update = lp.start(main_id, full_refresh=True)
    # One-active-pipeline probe: start the second pipeline while the first is running.
    concurrent = {}
    try:
        probe_update = lp.start(probe_id)
        concurrent = {"second_pipeline_started": True}
    except Exception as e:  # noqa: BLE001
        probe_update = None
        concurrent = {"second_pipeline_started": False, "error": str(e).splitlines()[0][:300]}
    main_info = lp.wait(main_id, main_update)
    if probe_update is None:
        probe_update = lp.start(probe_id)
    probe_info = lp.wait(probe_id, probe_update)
    # Did the two updates actually RUN at the same time (or did one wait in a queue)?
    spans = {"main": _running_span(main_id, main_update), "probe": _running_span(probe_id, probe_update)}
    concurrent["running_spans_utc"] = spans
    if all(spans.values()):
        (a0, a1), (b0, b1) = spans["main"], spans["probe"]
        concurrent["overlapped_while_running"] = a0 < b1 and b0 < a1
    record("s3.concurrent_pipelines", concurrent)
    yield {"main": (main_id, main_update, main_info), "probe": (probe_id, probe_update, probe_info)}
    ws().pipelines.delete(probe_id)  # keep only the course pipeline


def test_pipeline_completes(runs, record):
    pid, uid, info = runs["main"]
    record("s3.main_update_state", str(info.state.value))
    assert info.state == p.UpdateInfoState.COMPLETED, lp.errors(pid, uid)


def test_silver_and_gold_counts(runs):
    assert sql(f"SELECT count(*) FROM {CATALOG}.silver.orders")[0][0] == KEY["silver_orders_rows"]
    rows = sql(f"SELECT count(*), round(sum(revenue), 2) FROM {CATALOG}.gold.revenue_hourly")[0]
    assert rows[0] == KEY["gold_revenue_hours"]
    assert abs(rows[1] - KEY["silver_revenue_total"]) < 0.01


def test_event_log_shows_dropped_rows(runs, record):
    dropped = dropped_by_expectation()
    record("s3.dropped_by_expectation", dropped)
    assert dropped == KEY["expectation_dropped"]


def test_fail_update_expectation_stops_update(runs, record):
    pid, uid, info = runs["probe"]
    errs = lp.errors(pid, uid)
    record("s3.fail_update", {"state": info.state.value, "error": (errs or [None])[0]})
    assert info.state == p.UpdateInfoState.FAILED
    assert any("positive_amount" in e or "expectation" in e.lower() for e in errs), errs


def test_concurrent_pipeline_limit_observed(runs, record):
    """Informational: Free Edition says one active pipeline; we record what happened."""
    pid, uid, _ = runs["probe"]
    events = [e.message for e in ws().pipelines.list_pipeline_events(pid, max_results=50) if e.message]
    waiting = [m for m in events if "limit" in m.lower() or "quota" in m.lower() or "concurrent" in m.lower()]
    record("s3.probe_pipeline_limit_messages", waiting[:3])
