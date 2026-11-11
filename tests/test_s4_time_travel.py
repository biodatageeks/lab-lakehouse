"""Session 4: time travel, RESTORE, OPTIMIZE, VACUUM (SQL warehouse)."""

import time

import pytest

from lab.common import CATALOG, sql

T = f"{CATALOG}.silver.products_incident"


@pytest.fixture(scope="module")
def incident():
    sql(f"CREATE OR REPLACE TABLE {T} AS SELECT * FROM {CATALOG}.silver.dim_products")
    good_sum = sql(f"SELECT round(sum(price), 2) FROM {T}")[0][0]
    time.sleep(2)
    ts_before_bad = sql("SELECT current_timestamp()")[0][0]
    time.sleep(2)
    sql(f"UPDATE {T} SET price = 0")  # the "incident"
    return {"good_sum": good_sum, "ts_before_bad": ts_before_bad}


def _history():
    return [r.asDict() for r in sql(f"DESCRIBE HISTORY {T}")]


def test_describe_history_finds_bad_update(incident, record):
    hist = _history()
    ops = [(h["version"], h["operation"]) for h in hist]
    record("s4.history_ops", ops)
    assert any(op == "UPDATE" for _, op in ops)


def test_version_as_of_and_timestamp_as_of(incident):
    bad_version = next(h["version"] for h in _history() if h["operation"] == "UPDATE")
    assert sql(f"SELECT round(sum(price), 2) FROM {T} VERSION AS OF {bad_version - 1}")[0][0] == incident["good_sum"]
    assert sql(f"SELECT round(sum(price), 2) FROM {T} VERSION AS OF {bad_version}")[0][0] == 0
    ts = incident["ts_before_bad"].strftime("%Y-%m-%d %H:%M:%S.%f")
    assert sql(f"SELECT round(sum(price), 2) FROM {T} TIMESTAMP AS OF '{ts}'")[0][0] == incident["good_sum"]


def test_restore(incident):
    bad_version = next(h["version"] for h in _history() if h["operation"] == "UPDATE")
    sql(f"RESTORE TABLE {T} TO VERSION AS OF {bad_version - 1}")
    assert sql(f"SELECT round(sum(price), 2) FROM {T}")[0][0] == incident["good_sum"]
    assert _history()[0]["operation"] == "RESTORE"


def test_optimize(incident, record):
    rows = sql(f"OPTIMIZE {T}")
    record("s4.optimize_metrics", str(rows[0][1])[:200] if rows else None)
    assert _history()[0]["operation"] in ("OPTIMIZE", "RESTORE")  # no-op OPTIMIZE adds no commit


def test_vacuum_and_retention(incident, record):
    sql(f"VACUUM {T}")  # default 7-day retention: allowed
    try:
        sql(f"VACUUM {T} RETAIN 0 HOURS")
        zero = "accepted"
    except Exception as e:  # noqa: BLE001
        zero = str(e).splitlines()[0][:250]
    record("s4.vacuum_retain_0_hours", zero)
    try:
        sql(f"SET spark.databricks.delta.retentionDurationCheck.enabled = false")
        conf = "accepted"
    except Exception as e:  # noqa: BLE001
        conf = str(e).splitlines()[0][:250]
    record("s4.disable_retention_check_on_warehouse", conf)
