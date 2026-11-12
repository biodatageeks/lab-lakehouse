# %% [markdown]
# # Session 2 — incremental file ingestion
# Snowflake equivalents of Auto Loader:
# * `COPY INTO` remembers which files it loaded (64 days): re-running loads only new files
# * `ENABLE_SCHEMA_EVOLUTION` + `MATCH_BY_COLUMN_NAME` add new columns as they appear
# * `ON_ERROR = CONTINUE` skips bad rows; `COPY_HISTORY` tells you what was rejected
# * **Snowpipe** (`CREATE PIPE`) for the clicks stream, and a **triggered task** on a stage
#   stream that loads orders as soon as files land
#
# Arguments (defaults = first lab run): `--start 1 --count 10 --malformed-at 0 --with-task 0`

# %%
# Install the course packages (duckdb, polars, pyiceberg) from the Snowflake PyPI repository.
# Interactive: pick that repository as the package source when you connect; re-run after a restart.
!pip install -q -r requirements.txt

# %%
import os, sys, time
sys.path.insert(0, os.getcwd())
from lab_nb import *
import producer

a = args(start=1, count=10, malformed_at=0, schema_change_at=30, with_task=0, interval=0)
rep = Report("02_ingestion", a.run_id)
last = a.start + a.count - 1
key = producer.answer_key(last, schema_change_at=a.schema_change_at, malformed_at=a.malformed_at or None)

# %% [markdown]
# ## Bronze tables (typed; new columns allowed)

# %%
sql("CREATE FILE FORMAT IF NOT EXISTS SHOP.BRONZE.JSONL TYPE = JSON")
sql("""CREATE TABLE IF NOT EXISTS SHOP.BRONZE.ORDERS_RAW (
         order_id NUMBER, customer_id NUMBER, product_id NUMBER, quantity NUMBER, amount NUMBER(12, 2),
         status STRING, order_ts TIMESTAMP_NTZ, source_file STRING)
       ENABLE_SCHEMA_EVOLUTION = TRUE""")
sql("""CREATE TABLE IF NOT EXISTS SHOP.BRONZE.CLICKS_RAW (
         event_id NUMBER, session_id STRING, customer_id NUMBER, product_id NUMBER,
         event_type STRING, event_ts TIMESTAMP_NTZ)""")
COPY_ORDERS = f"""COPY INTO SHOP.BRONZE.ORDERS_RAW FROM {LANDING}/orders/
    FILE_FORMAT = (FORMAT_NAME = 'SHOP.BRONZE.JSONL')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    INCLUDE_METADATA = (source_file = METADATA$FILENAME)
    ON_ERROR = CONTINUE"""

def summarize(result) -> dict:
    files = [r.as_dict() for r in result]
    if files and "status" in files[0] and "file" not in files[0]:
        return {"status": files[0]["status"]}
    return {"files": len(files), "rows_loaded": sum(f["rows_loaded"] for f in files),
            "partially_loaded": {f["file"]: f["first_error"] for f in files if f["status"] != "LOADED"}}

# %% [markdown]
# ## (second run) a triggered task that loads files as they arrive
# A stream on the stage's directory table sees new files; the task wakes up only when it has data.

# %%
if a.with_task:
    sql("CREATE OR REPLACE STREAM SHOP.BRONZE.LANDING_FILES ON STAGE SHOP.BRONZE.LANDING")
    sql("CREATE TABLE IF NOT EXISTS SHOP.BRONZE.FILE_ARRIVALS (relative_path STRING, seen_at TIMESTAMP_LTZ)")
    sql(f"""CREATE OR REPLACE TASK SHOP.BRONZE.LOAD_ORDERS_ON_ARRIVAL
            TARGET_COMPLETION_INTERVAL = '1 MINUTE'
            WHEN SYSTEM$STREAM_HAS_DATA('SHOP.BRONZE.LANDING_FILES')
            AS BEGIN
              INSERT INTO SHOP.BRONZE.FILE_ARRIVALS
                SELECT relative_path, CURRENT_TIMESTAMP() FROM SHOP.BRONZE.LANDING_FILES WHERE METADATA$ACTION = 'INSERT';
              {COPY_ORDERS};
            END""")
    sql("ALTER TASK SHOP.BRONZE.LOAD_ORDERS_ON_ARRIVAL RESUME")

# %% [markdown]
# ## Produce files into the stage (class mode: `--interval 60`)

# %%
producer.produce(session, a.start, a.count, a.interval, schema_change_at=a.schema_change_at,
                 malformed_at=a.malformed_at or None)
rep["produced"] = [a.start, last]

# %%
if a.with_task:
    sql("ALTER STAGE SHOP.BRONZE.LANDING REFRESH")  # register new files in the directory table
    t0 = time.time()
    while time.time() - t0 < 360:
        if scalar("SELECT count(*) FROM SHOP.BRONZE.ORDERS_RAW") >= key["bronze_orders_rows"]:
            break
        time.sleep(10)
    rep["task_wait_s"] = round(time.time() - t0)
    rep["task_runs"] = rows("""SELECT state, error_message, scheduled_time::string AS scheduled
        FROM TABLE(SHOP.INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'LOAD_ORDERS_ON_ARRIVAL'))
        WHERE state != 'SCHEDULED' ORDER BY scheduled_time""")
    rep["files_seen_by_stream"] = scalar("SELECT count(*) FROM SHOP.BRONZE.FILE_ARRIVALS WHERE relative_path LIKE 'orders/%'")
    sql("ALTER TASK SHOP.BRONZE.LOAD_ORDERS_ON_ARRIVAL SUSPEND")

# %% [markdown]
# ## COPY: loads only files it has not loaded before

# %%
rep["copy"] = summarize(sql(COPY_ORDERS))
rep["copy_rerun"] = summarize(sql(COPY_ORDERS))
rep["orders_raw_rows"] = scalar("SELECT count(*) FROM SHOP.BRONZE.ORDERS_RAW")
cols = rows("DESCRIBE TABLE SHOP.BRONZE.ORDERS_RAW")
rep["orders_raw_columns"] = [r["name"] for r in cols]
rep["schema_evolution_record"] = next((r["schema evolution record"] for r in cols if r["name"] == "COUPON_CODE"), None)
hist = rows("""SELECT file_name, status, row_count, error_count, first_error_message
    FROM TABLE(SHOP.INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME => 'SHOP.BRONZE.ORDERS_RAW',
                                                    START_TIME => DATEADD(day, -14, CURRENT_TIMESTAMP())))""")
rep["copy_history_files"] = len(hist)
rep["rejected_rows"] = sum(h["ERROR_COUNT"] for h in hist)
rep["rejected_errors"] = sorted({h["FIRST_ERROR_MESSAGE"] for h in hist if h["ERROR_COUNT"]})
rep["expected"] = key

# %% [markdown]
# ## Snowpipe for clicks
# A pipe is a named, serverless COPY. On an internal stage, `ALTER PIPE ... REFRESH` queues the
# new files (on cloud storage, event notifications would do this automatically).

# %%
sql(f"""CREATE PIPE IF NOT EXISTS SHOP.BRONZE.CLICKS_PIPE AS
        COPY INTO SHOP.BRONZE.CLICKS_RAW FROM {LANDING}/clicks/
        FILE_FORMAT = (FORMAT_NAME = 'SHOP.BRONZE.JSONL') MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE""")
rep["pipe_refresh_files"] = len(sql("ALTER PIPE SHOP.BRONZE.CLICKS_PIPE REFRESH"))
t0 = time.time()
while time.time() - t0 < 240 and scalar("SELECT count(*) FROM SHOP.BRONZE.CLICKS_RAW") < key["bronze_clicks_rows"]:
    time.sleep(10)
rep["pipe_wait_s"] = round(time.time() - t0)
rep["clicks_raw_rows"] = scalar("SELECT count(*) FROM SHOP.BRONZE.CLICKS_RAW")
rep["pipe_status"] = json.loads(scalar("SELECT SYSTEM$PIPE_STATUS('SHOP.BRONZE.CLICKS_PIPE')"))
rep.save()
