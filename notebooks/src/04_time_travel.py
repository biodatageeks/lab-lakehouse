# %% [markdown]
# # Session 4 — time travel, restore, undrop, storage housekeeping
# * find a bad change in the **query history**, query the table **before** it
# * restore with a zero-copy **clone** + `SWAP`; bring back a dropped table with `UNDROP`
# * retention (Enterprise: up to 90 days), clustering instead of `OPTIMIZE`, no `VACUUM` needed

# %%
import os, sys, time
sys.path.insert(0, os.getcwd())
from lab_nb import *

a = args()
rep = Report("04_time_travel", a.run_id)
T = "SHOP.GOLD.ORDERS_SNAPSHOT"

# %%
sql(f"CREATE OR REPLACE TABLE {T} AS SELECT * FROM SHOP.SILVER.ORDERS")
rep["rows"] = scalar(f"SELECT count(*) FROM {T}")
time.sleep(2)
before_ts = scalar("SELECT CURRENT_TIMESTAMP()::string")
time.sleep(2)

# %% [markdown]
# ## Oops: a bad update

# %%
sql(f"UPDATE {T} SET amount = 0 WHERE status = 'placed'")
rep["zeroed_now"] = scalar(f"SELECT count_if(amount = 0) FROM {T}")

# %% [markdown]
# ## Find it in the query history and look at the table before it

# %%
bad = rows(f"""SELECT query_id, query_text, start_time::string AS start_time
    FROM TABLE(SHOP.INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION())
    WHERE query_type = 'UPDATE' AND query_text ILIKE '%{T}%' ORDER BY start_time DESC LIMIT 1""")[0]
rep["bad_query"] = {"query_id": bad["QUERY_ID"], "text": bad["QUERY_TEXT"]}
qid = bad["QUERY_ID"]
rep["zeroed_before_statement"] = scalar(f"SELECT count_if(amount = 0) FROM {T} BEFORE (STATEMENT => '{qid}')")
rep["zeroed_at_timestamp"] = scalar(
    f"SELECT count_if(amount = 0) FROM {T} AT (TIMESTAMP => '{before_ts}'::timestamp_ltz)")
rep["zeroed_at_offset_minus_1h"] = attempt(lambda: scalar(f"SELECT count_if(amount = 0) FROM {T} AT (OFFSET => -3600)"))

# %% [markdown]
# ## Restore: clone the old version (zero-copy), then swap it in

# %%
sql(f"CREATE OR REPLACE TABLE {T}_RESTORE CLONE {T} BEFORE (STATEMENT => '{qid}')")
sql(f"ALTER TABLE {T} SWAP WITH {T}_RESTORE")
sql(f"DROP TABLE {T}_RESTORE")
rep["zeroed_after_restore"] = scalar(f"SELECT count_if(amount = 0) FROM {T}")
rep["revenue_after_restore"] = float(scalar(f"SELECT round(sum(amount), 2) FROM {T}"))

# %% [markdown]
# ## Undrop

# %%
sql(f"DROP TABLE {T}")
rep["after_drop"] = attempt(lambda: scalar(f"SELECT count(*) FROM {T}"))
sql(f"UNDROP TABLE {T}")
rep["after_undrop"] = scalar(f"SELECT count(*) FROM {T}")

# %% [markdown]
# ## Housekeeping: retention, clustering, storage
# There is no `OPTIMIZE`/`VACUUM`: Snowflake compacts micro-partitions itself and purges old
# versions after the retention period (+7 days fail-safe). You *can* choose a clustering key.

# %%
retention = {}
for days in (90, 0, 7):
    retention[days] = attempt(lambda d=days: sql(f"ALTER TABLE {T} SET DATA_RETENTION_TIME_IN_DAYS = {d}") and "ok")
rep["retention_settings"] = retention
rep["retention_now"] = rows(f"SHOW TABLES LIKE 'ORDERS_SNAPSHOT' IN SCHEMA SHOP.GOLD")[0]["retention_time"]
sql(f"ALTER TABLE {T} CLUSTER BY (TO_DATE(order_ts))")
sql(f"ALTER TABLE {T} SUSPEND RECLUSTER")  # automatic clustering costs credits; off for the lab
rep["clustering"] = json.loads(scalar(f"SELECT SYSTEM$CLUSTERING_INFORMATION('{T}')"))
rep["storage_metrics"] = rows(f"""SELECT active_bytes, time_travel_bytes, failsafe_bytes
    FROM SHOP.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
    WHERE table_schema = 'GOLD' AND table_name = 'ORDERS_SNAPSHOT' AND table_dropped IS NULL""")
rep["vacuum"] = attempt(lambda: sql(f"VACUUM {T}"))
rep.save()
