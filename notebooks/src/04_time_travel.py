# %% [markdown]
# # Session 4 — time travel, restore, undrop, storage housekeeping
# * find a bad change in the **query history**, query the table **before** it
# * restore with a zero-copy **clone** + `SWAP`; bring back a dropped table with `UNDROP`
# * retention (Enterprise: up to 90 days), clustering instead of `OPTIMIZE`, no `VACUUM` needed

# %%
# Install the course packages (duckdb, polars, pyiceberg) from the Snowflake PyPI repository.
# Interactive: pick that repository as the package source when you connect; re-run after a restart.
!pip install -q -r requirements.txt

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
# ## The same rows, before and after the bad update
# One query id gives you both versions of the table:
# * `BEFORE (STATEMENT => $qid)`: the table just *before* that statement ran
# * `AT (STATEMENT => $qid)`: the table *including* that statement's change
#
# Joining the two versions on the key shows exactly which rows the update changed. The cell
# below runs this plain SQL; you can also paste it into a Snowsight SQL worksheet:
#
# ```sql
# SET qid = '<query id of the bad UPDATE>';   -- from the query history above
#
# SELECT b.order_id, b.status, b.amount AS amount_before, a.amount AS amount_after
# FROM SHOP.GOLD.ORDERS_SNAPSHOT BEFORE (STATEMENT => $qid) b
# JOIN SHOP.GOLD.ORDERS_SNAPSHOT AT (STATEMENT => $qid) a ON a.order_id = b.order_id
# WHERE a.amount IS DISTINCT FROM b.amount
# ORDER BY b.order_id;
#
# SELECT 'before' AS version, count(*) AS orders, count_if(amount = 0) AS zero_amount,
#        round(sum(amount), 2) AS revenue
# FROM SHOP.GOLD.ORDERS_SNAPSHOT BEFORE (STATEMENT => $qid)
# UNION ALL
# SELECT 'after', count(*), count_if(amount = 0), round(sum(amount), 2)
# FROM SHOP.GOLD.ORDERS_SNAPSHOT AT (STATEMENT => $qid);
# ```

# %%
sql(f"SET qid = '{qid}'")   # the bad UPDATE's query id, as a session variable

DIFF_SQL = """
SELECT b.order_id, b.status, b.amount AS amount_before, a.amount AS amount_after
FROM SHOP.GOLD.ORDERS_SNAPSHOT BEFORE (STATEMENT => $qid) b
JOIN SHOP.GOLD.ORDERS_SNAPSHOT AT (STATEMENT => $qid) a ON a.order_id = b.order_id
WHERE a.amount IS DISTINCT FROM b.amount
"""
SUMMARY_SQL = """
SELECT 'before' AS version, count(*) AS orders, count_if(amount = 0) AS zero_amount,
       round(sum(amount), 2) AS revenue
FROM SHOP.GOLD.ORDERS_SNAPSHOT BEFORE (STATEMENT => $qid)
UNION ALL
SELECT 'after', count(*), count_if(amount = 0), round(sum(amount), 2)
FROM SHOP.GOLD.ORDERS_SNAPSHOT AT (STATEMENT => $qid)
"""
show(DIFF_SQL + "ORDER BY b.order_id LIMIT 10")
show(SUMMARY_SQL)
rep["versions"] = {r["VERSION"]: {"orders": r["ORDERS"], "zero_amount": r["ZERO_AMOUNT"], "revenue": float(r["REVENUE"])}
                   for r in rows(SUMMARY_SQL)}
rep["changed_rows"] = scalar(f"SELECT count(*) FROM ({DIFF_SQL})")
rep["changed_sample"] = [{k.lower(): (float(v) if k.startswith("AMOUNT") else v) for k, v in r.items()}
                         for r in rows(DIFF_SQL + "ORDER BY b.order_id LIMIT 3")]

# %% [markdown]
# ## Version syntax: native tables vs Iceberg tables
# Coming from Databricks you may look for `VERSION AS OF n`. In Snowflake, **native tables have
# no version numbers**: you address a past state by statement, timestamp or offset. **Iceberg
# tables do**: every commit is an Iceberg snapshot, and `AT (VERSION => <snapshot id>)` reads it.
#
# | Read the table… | Databricks (Delta) | Snowflake native table | Snowflake Iceberg table |
# | --- | --- | --- | --- |
# | at a version | `VERSION AS OF 3` | not available | `AT (VERSION => <snapshot id>)` |
# | at a time | `TIMESTAMP AS OF '…'` | `AT (TIMESTAMP => '…'::timestamp_ltz)` | same |
# | some seconds ago | — | `AT (OFFSET => -60)` | same |
# | just before / including a statement | — | `BEFORE / AT (STATEMENT => '<query id>')` | same |
# | list the versions | `DESCRIBE HISTORY` | query history | Iceberg snapshots (Horizon catalog) |
#
# Below: an Iceberg copy of the good data gets the same bad update, then we read it by version.
#
# ```sql
# SET v_before = <first snapshot id>;   -- snapshot ids from the Iceberg catalog
# SET v_after  = <last snapshot id>;
# SELECT 'before' AS version, count_if(amount = 0) AS zero_amount, round(sum(amount), 2) AS revenue
# FROM SHOP.GOLD.ORDERS_SNAPSHOT_ICEBERG AT (VERSION => $v_before)
# UNION ALL
# SELECT 'after', count_if(amount = 0), round(sum(amount), 2)
# FROM SHOP.GOLD.ORDERS_SNAPSHOT_ICEBERG AT (VERSION => $v_after);
# ```

# %%
# An Iceberg copy of the good data (Iceberg stores timestamps in microseconds), then the same bad update
sql("""CREATE OR REPLACE ICEBERG TABLE SHOP.GOLD.ORDERS_SNAPSHOT_ICEBERG
       CATALOG = 'SNOWFLAKE' EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'
       AS SELECT * REPLACE (order_ts::TIMESTAMP_NTZ(6) AS order_ts)
          FROM SHOP.GOLD.ORDERS_SNAPSHOT BEFORE (STATEMENT => $qid)""")
sql("UPDATE SHOP.GOLD.ORDERS_SNAPSHOT_ICEBERG SET amount = 0 WHERE status = 'placed'")
ice_qid = scalar("SELECT LAST_QUERY_ID()")

# The versions = Iceberg snapshots, listed through the Horizon catalog (metadata works in the notebook)
snaps = horizon_catalog().load_table("GOLD.ORDERS_SNAPSHOT_ICEBERG").snapshots()
for s_ in snaps:
    print(s_.snapshot_id, s_.summary.operation.value if s_.summary else None)
sql(f"SET v_before = {snaps[0].snapshot_id}")
sql(f"SET v_after = {snaps[-1].snapshot_id}")

VERSIONS_SQL = """
SELECT 'before' AS version, count(*) AS orders, count_if(amount = 0) AS zero_amount, round(sum(amount), 2) AS revenue
FROM SHOP.GOLD.ORDERS_SNAPSHOT_ICEBERG AT (VERSION => $v_before)
UNION ALL
SELECT 'after', count(*), count_if(amount = 0), round(sum(amount), 2)
FROM SHOP.GOLD.ORDERS_SNAPSHOT_ICEBERG AT (VERSION => $v_after)
"""
show(VERSIONS_SQL)
rep["iceberg_snapshots"] = [(s_.snapshot_id, s_.summary.operation.value if s_.summary else None) for s_ in snaps]
rep["iceberg_versions"] = {r["VERSION"]: {"orders": r["ORDERS"], "zero_amount": r["ZERO_AMOUNT"], "revenue": float(r["REVENUE"])}
                           for r in rows(VERSIONS_SQL)}
rep["iceberg_before_statement_zero"] = attempt(lambda: scalar(
    f"SELECT count_if(amount = 0) FROM SHOP.GOLD.ORDERS_SNAPSHOT_ICEBERG BEFORE (STATEMENT => '{ice_qid}')"))
rep["native_version_syntax"] = attempt(lambda: scalar(f"SELECT count(*) FROM {T} AT (VERSION => 1)"))

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
