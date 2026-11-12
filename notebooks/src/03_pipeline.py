# %% [markdown]
# # Session 3 — a declarative pipeline: dynamic tables + data quality expectations
# * **Dynamic tables** are declared as a query; Snowflake keeps them fresh (incrementally when
#   it can). `TARGET_LAG = 'DOWNSTREAM'` = refresh only when something downstream needs it.
# * **Data metric functions** (DMFs) with **expectations** measure quality on bronze.
# * The refresh history plays the role of a pipeline event log.

# %%
import os, sys, time
sys.path.insert(0, os.getcwd())
from lab_nb import *

a = args()
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
rep.save()
