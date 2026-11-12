# %% [markdown]
# # Session 3 — a declarative pipeline: dynamic tables + data quality expectations
# * **Dynamic tables** are declared as a query; Snowflake keeps them fresh (incrementally when
#   it can). `TARGET_LAG = 'DOWNSTREAM'` = refresh only when something downstream needs it.
# * **Data metric functions** (DMFs) with **expectations** measure quality on bronze.
# * The refresh history plays the role of a pipeline event log.
# * **Live mode** (`--live-minutes 5`, or `args(live_minutes=5)`): watch the pipeline process
#   new files as they land: producer → stage → triggered task → bronze → silver → gold.

# %%
import os, sys, time, uuid
sys.path.insert(0, os.getcwd())
from lab_nb import *
import producer

a = args(live_minutes=0, interval=60, files=35, malformed_at=15)
rep = Report("03_pipeline", a.run_id)
started = scalar("SELECT CURRENT_TIMESTAMP()::string")

# %% [markdown]
# ## Expectations on bronze (system DMF + a custom one)

# %%
sql("""CREATE OR REPLACE DATA METRIC FUNCTION SHOP.BRONZE.INVALID_AMOUNT_COUNT(t TABLE(amount NUMBER))
       RETURNS NUMBER AS 'SELECT COUNT_IF(amount IS NULL OR amount <= 0) FROM t'""")
sql("ALTER TABLE SHOP.BRONZE.ORDERS_RAW SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES'")
for dmf, col, name in [("SNOWFLAKE.CORE.NULL_COUNT", "customer_id", "valid_customer"),
                       ("SHOP.BRONZE.INVALID_AMOUNT_COUNT", "amount", "valid_amount")]:
    attempt(lambda: sql(f"ALTER TABLE SHOP.BRONZE.ORDERS_RAW DROP DATA METRIC FUNCTION {dmf} ON ({col})"))
    sql(f"ALTER TABLE SHOP.BRONZE.ORDERS_RAW ADD DATA METRIC FUNCTION {dmf} ON ({col}) EXPECTATION {name} (VALUE = 0)")
quality = rows("SELECT * FROM TABLE(SYSTEM$EVALUATE_DATA_QUALITY_EXPECTATIONS(REF_ENTITY_NAME => 'SHOP.BRONZE.ORDERS_RAW'))")
rep["expectations"] = {q["EXPECTATION_NAME"]: {"metric": q["METRIC_NAME"], "value": q["VALUE"],
                                               "violated": q.get("EXPECTATION_VIOLATED")} for q in quality}
show("SELECT * FROM TABLE(SYSTEM$EVALUATE_DATA_QUALITY_EXPECTATIONS(REF_ENTITY_NAME => 'SHOP.BRONZE.ORDERS_RAW'))")

# %% [markdown]
# ## Silver and gold as dynamic tables
# Silver drops the rows the expectations flag and de-duplicates the re-sent orders.

# %%
t0 = time.time()
sql("""CREATE OR REPLACE DYNAMIC TABLE SHOP.SILVER.ORDERS
       TARGET_LAG = 'DOWNSTREAM' WAREHOUSE = LAB_WH
       COMMENT = 'One row per order_id; valid amount and customer'
       AS SELECT order_id, customer_id, product_id, quantity, amount, status, order_ts, coupon_code, source_file
          FROM SHOP.BRONZE.ORDERS_RAW
          WHERE amount > 0 AND customer_id IS NOT NULL
          QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY source_file DESC) = 1""")
sql("""CREATE OR REPLACE DYNAMIC TABLE SHOP.GOLD.REVENUE_HOURLY
       TARGET_LAG = '1 day' WAREHOUSE = LAB_WH
       COMMENT = 'Revenue per hour'
       AS SELECT DATE_TRUNC('HOUR', order_ts) AS order_hour, COUNT(*) AS orders, ROUND(SUM(amount), 2) AS revenue
          FROM SHOP.SILVER.ORDERS GROUP BY ALL""")
rep["create_s"] = round(time.time() - t0, 1)
rep["silver_orders_rows"] = scalar("SELECT count(*) FROM SHOP.SILVER.ORDERS")
rep["gold_hours"] = scalar("SELECT count(*) FROM SHOP.GOLD.REVENUE_HOURLY")
rep["gold_revenue"] = float(scalar("SELECT round(sum(revenue), 2) FROM SHOP.GOLD.REVENUE_HOURLY"))
rep["dynamic_tables"] = {r["name"]: {"refresh_mode": r["refresh_mode"], "target_lag": r["target_lag"],
                                     "reason": r.get("refresh_mode_reason")}
                         for r in rows("SHOW DYNAMIC TABLES IN DATABASE SHOP")}

# %% [markdown]
# ## Refresh = "run the pipeline"; the refresh history is the event log

# %%
sql("ALTER DYNAMIC TABLE SHOP.GOLD.REVENUE_HOURLY REFRESH")
rep["refresh_history"] = rows("""SELECT name, state, refresh_action, refresh_trigger,
        statistics:numInsertedRows::int AS inserted
    FROM TABLE(SHOP.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(NAME_PREFIX => 'SHOP.'))
    WHERE refresh_start_time >= ?::timestamp_ltz
    ORDER BY refresh_start_time""", [started])

# %% [markdown]
# ## Fail instead of drop?
# Dynamic tables have no `ON VIOLATION FAIL UPDATE`. A workaround: make the query itself fail
# on bad rows; the refresh fails and the table keeps its last good data.

# %%
sql("""CREATE OR REPLACE DYNAMIC TABLE SHOP.SILVER.ORDERS_STRICT
       TARGET_LAG = 'DOWNSTREAM' WAREHOUSE = LAB_WH INITIALIZE = ON_SCHEDULE
       AS SELECT order_id, IFF(amount > 0, amount, 1 / 0) AS amount FROM SHOP.BRONZE.ORDERS_RAW""")
rep["strict_refresh"] = attempt(lambda: sql("ALTER DYNAMIC TABLE SHOP.SILVER.ORDERS_STRICT REFRESH") and "refreshed")
sql("DROP DYNAMIC TABLE SHOP.SILVER.ORDERS_STRICT")

# %% [markdown]
# ## Live mode: watch data flow through the pipeline
# Gold gets `TARGET_LAG = '1 minute'` (the minimum); silver is `DOWNSTREAM`, so Snowflake
# refreshes both on its own. Session 2's task loads each file as it lands. Nobody calls REFRESH.
# Expect about 1–2 minutes from a file landing to gold. A 1-minute lag keeps the warehouse busy
# (~1 credit/hour on X-Small), so the last cell puts everything back.

# %%
def snapshot(t0: float, tag: str) -> dict:
    r = rows(f"""SELECT
        (SELECT count(*) FROM SHOP.BRONZE.ORDERS_RAW WHERE source_file LIKE '%{tag}.json') AS live_bronze_rows,
        (SELECT count(*) FROM SHOP.BRONZE.ORDERS_RAW) AS bronze_rows,
        (SELECT count(*) FROM SHOP.SILVER.ORDERS) AS silver_rows,
        (SELECT round(sum(revenue), 2) FROM SHOP.GOLD.REVENUE_HOURLY) AS gold_revenue""")[0]
    last = rows("""SELECT name, refresh_trigger, refresh_action, refresh_end_time::string AS at
        FROM TABLE(SHOP.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(NAME_PREFIX => 'SHOP.'))
        WHERE state = 'SUCCEEDED' ORDER BY refresh_end_time DESC LIMIT 1""")
    snap = {"t_s": round(time.time() - t0), **{k.lower(): (float(v) if k == "GOLD_REVENUE" else v) for k, v in r.items()},
            "last_refresh": "{NAME} {REFRESH_TRIGGER} {REFRESH_ACTION}".format(**last[0]) if last else None}
    print(snap)
    return snap

if a.live_minutes:
    tag = "live" + uuid.uuid4().hex[:6]
    first = a.files + 1
    n = max(1, int(a.live_minutes * 60 // a.interval))
    expect = producer.answer_key(a.files + n, malformed_at=a.malformed_at or None)
    live = {"tag": tag, "files": [first, first + n - 1], "expected_silver_rows": expect["silver_orders_rows"],
            "expected_gold_revenue": expect["silver_revenue_total"], "timeline": []}
    sql("ALTER DYNAMIC TABLE SHOP.GOLD.REVENUE_HOURLY SET TARGET_LAG = '1 minute'")
    sql("ALTER TASK SHOP.BRONZE.LOAD_ORDERS_ON_ARRIVAL RESUME")
    live_start = scalar("SELECT CURRENT_TIMESTAMP()::string")
    t0 = time.time()
    try:
        for i in range(first, first + n):
            producer.produce(session, i, 1, tag=tag, malformed_at=a.malformed_at or None)
            sql("ALTER STAGE SHOP.BRONZE.LANDING REFRESH")   # internal stage: register the file
            tick = time.time()
            while time.time() - tick < a.interval:
                live["timeline"].append(snapshot(t0, tag))
                time.sleep(15)
        live["last_file_at_s"] = round(time.time() - t0)
        while time.time() - t0 < live["last_file_at_s"] + 240:   # let the last file reach gold
            s = snapshot(t0, tag)
            live["timeline"].append(s)
            if s["silver_rows"] == expect["silver_orders_rows"] and s["gold_revenue"] == expect["silver_revenue_total"]:
                break
            time.sleep(15)
        live["settled_after_last_file_s"] = round(time.time() - t0) - live["last_file_at_s"]
        live["scheduled_refreshes"] = rows(f"""SELECT name, refresh_action, statistics:numInsertedRows::int AS inserted,
                refresh_end_time::string AS at
            FROM TABLE(SHOP.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(NAME_PREFIX => 'SHOP.'))
            WHERE refresh_trigger = 'SCHEDULED' AND state = 'SUCCEEDED'
              AND refresh_start_time >= '{live_start}'::timestamp_ltz ORDER BY refresh_end_time""")
        live["manual_refreshes"] = scalar(f"""SELECT count(*)
            FROM TABLE(SHOP.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(NAME_PREFIX => 'SHOP.'))
            WHERE refresh_trigger = 'MANUAL' AND refresh_start_time >= '{live_start}'::timestamp_ltz""")
    finally:
        # put the course back to its 35-file state (grading and later sessions rely on it)
        sql("ALTER TASK SHOP.BRONZE.LOAD_ORDERS_ON_ARRIVAL SUSPEND")
        sql("ALTER DYNAMIC TABLE SHOP.GOLD.REVENUE_HOURLY SET TARGET_LAG = '1 day'")
        live["deleted_live_rows"] = sql(f"DELETE FROM SHOP.BRONZE.ORDERS_RAW WHERE source_file LIKE '%{tag}.json'")[0][0]
        sql(f"REMOVE {LANDING} PATTERN = '.*_{tag}[.]json'")
        sql("ALTER STAGE SHOP.BRONZE.LANDING REFRESH")
        sql("ALTER DYNAMIC TABLE SHOP.GOLD.REVENUE_HOURLY REFRESH")
        live["after_cleanup"] = snapshot(t0, tag)
    rep["live"] = live
rep.save()
