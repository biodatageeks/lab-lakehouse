# %% [markdown]
# # Session 6 — open formats, three engines, one table
# * Snowflake SQL, **Polars** and **DuckDB** on the same governed table: Snowpark hands the
#   result over as **Arrow** (`to_arrow()`), zero-copy into Polars / DuckDB
# * the dbt-built **Iceberg** table: metadata through the Horizon Iceberg REST catalog,
#   time travel with SQL
#
# What does *not* work from a notebook on a trial account (verified, see the report):
# open-catalog clients cannot fetch Iceberg **data files** (no route to the storage bucket),
# DuckDB cannot `INSTALL` extensions (no internet), and there is no external access
# integration on trials. Read data through Snowflake, then hand it to the engine of your choice.

# %%
import os, sys, time, urllib.request
sys.path.insert(0, os.getcwd())
from lab_nb import *
import duckdb
import polars as pl

a = args(table="SHOP.GOLD.FCT_ORDERS", iceberg_table="SHOP.GOLD.DIM_CUSTOMERS")
rep = Report("06_open_formats", a.run_id)

# %% [markdown]
# ## Same table, three engines

# %%
t0 = time.perf_counter()
snow = [(r[0], r[1], float(r[2])) for r in sql(
    f"SELECT category, count(*), round(sum(amount), 2) FROM {a.table} GROUP BY ALL")]
rep["snowflake_s"] = round(time.perf_counter() - t0, 3)

t0 = time.perf_counter()
arrow = session.table(a.table).to_arrow()  # governed read -> Arrow
rep["to_arrow_s"] = round(time.perf_counter() - t0, 3)

orders = pl.from_arrow(arrow)
t0 = time.perf_counter()
by_polars = orders.group_by("CATEGORY").agg(pl.len(), pl.col("AMOUNT").cast(pl.Float64).sum().round(2))
rep["polars_s"] = round(time.perf_counter() - t0, 4)

t0 = time.perf_counter()
by_duck = duckdb.sql("SELECT CATEGORY, count(*), round(sum(AMOUNT)::double, 2) FROM arrow GROUP BY ALL").fetchall()
rep["duckdb_s"] = round(time.perf_counter() - t0, 4)

norm = lambda rs: sorted((c, int(n), round(float(v), 2)) for c, n, v in rs)
rep.update(arrow_rows=arrow.num_rows, polars_rows=orders.height,
           engines_agree=norm(snow) == norm(by_duck) == norm(by_polars.rows()))
print(by_polars)

# %% [markdown]
# ## The Iceberg table from dbt: catalog metadata, history, time travel (SQL)

# %%
ice = rows(f"SHOW ICEBERG TABLES LIKE '{a.iceberg_table.split('.')[-1]}' IN SCHEMA SHOP.GOLD")
rep["iceberg_table_type"] = ice[0]["iceberg_table_type"] if ice else None
rep["iceberg_metadata_location"] = json.loads(scalar(
    f"SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('{a.iceberg_table}')")).get("metadataLocation")

def horizon_view():
    t = horizon_catalog().load_table(".".join(a.iceberg_table.split(".")[1:]))
    snaps = t.snapshots()
    return {"format_version": t.metadata.format_version, "snapshots": len(snaps),
            # a moment between the first and second snapshot: "as of the first dbt run"
            "between_first_two_ms": (snaps[0].timestamp_ms + snaps[1].timestamp_ms) // 2 if len(snaps) > 1 else None,
            "current_snapshot": t.metadata.current_snapshot_id}

hv = attempt(horizon_view)
rep["horizon"] = hv
rep["iceberg_rows_polars"] = pl.from_arrow(session.table(a.iceberg_table).to_arrow()).height
if isinstance(hv, dict) and hv["snapshots"] > 1:
    rep["iceberg_rows_first_snapshot"] = scalar(
        f"SELECT count(*) FROM {a.iceberg_table} AT (TIMESTAMP => TO_TIMESTAMP_LTZ({hv['between_first_two_ms']}, 3))")

# Session 1's PRODUCTS_ICEBERG had a DELETE after its first snapshot: time travel shows the difference.
snaps = attempt(lambda: [s.timestamp_ms for s in horizon_catalog().load_table("SILVER.PRODUCTS_ICEBERG").snapshots()])
p = (snaps[0] + snaps[1]) // 2 if isinstance(snaps, list) and len(snaps) > 1 else None
if isinstance(p, int):
    rep["products_iceberg_rows"] = {
        "now": scalar("SELECT count(*) FROM SHOP.SILVER.PRODUCTS_ICEBERG"),
        "first_snapshot": scalar(
            f"SELECT count(*) FROM SHOP.SILVER.PRODUCTS_ICEBERG AT (TIMESTAMP => TO_TIMESTAMP_LTZ({p}, 3))")}

# %% [markdown]
# ## Publish an Iceberg copy of the fact table for engines outside Snowflake (Session 6b)
# Horizon serves Iceberg tables only; `FCT_ORDERS` is a native table, so we publish a copy.
# Iceberg stores timestamps in microseconds: cast `TIMESTAMP_NTZ(9)` down to `(6)`.

# %%
sql(f"""CREATE OR REPLACE ICEBERG TABLE SHOP.GOLD.FCT_ORDERS_ICEBERG
        CATALOG = 'SNOWFLAKE' EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'
        AS SELECT * REPLACE (order_ts::TIMESTAMP_NTZ(6) AS order_ts) FROM {a.table}""")
rep["fct_orders_iceberg_rows"] = scalar("SELECT count(*) FROM SHOP.GOLD.FCT_ORDERS_ICEBERG")

# %% [markdown]
# ## What open clients can and cannot do from here

# %%
rep["pyiceberg_data_read"] = attempt(
    lambda: horizon_catalog().load_table(".".join(a.iceberg_table.split(".")[1:])).scan().to_arrow().num_rows)
rep["duckdb_install_iceberg"] = attempt(lambda: duckdb.sql("INSTALL iceberg") or "installed")
egress = {}
for url in ["https://pypi.org/simple/duckdb/", "https://extensions.duckdb.org", "https://example.com"]:
    egress[url] = attempt(lambda url=url: urllib.request.urlopen(url, timeout=8).status)
rep.update(egress=egress, duckdb_version=duckdb.__version__, polars_version=pl.__version__)
rep.save()
