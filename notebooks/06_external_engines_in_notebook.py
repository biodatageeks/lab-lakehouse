# Databricks notebook source
# MAGIC %md
# MAGIC # Session 6 — open formats, three engines, one table
# MAGIC On Free Edition, tables live on Databricks default storage, which open-catalog clients
# MAGIC (PyIceberg, DuckDB `unity_catalog`, Polars `pl.Catalog`) cannot read — not from a laptop,
# MAGIC and not from inside a notebook either (the files use Databricks-managed encryption).
# MAGIC So Spark reads the governed table, hands it over as **Arrow**, and DuckDB / Polars query it.
# MAGIC No "External data access" toggle is needed for this.
# MAGIC
# MAGIC Delta Sharing (the laptop path, `external/delta_sharing_read.py`) does not work from here:
# MAGIC serverless can list shared tables but cannot download their files — the sharing storage
# MAGIC host resolves to a private address that refuses connections (`probe_sharing_egress`).

# COMMAND ----------

# MAGIC %pip install duckdb polars --quiet

# COMMAND ----------

import json
import time
import urllib.request

import duckdb
import polars as pl

dbutils.widgets.text("catalog", "shop")
dbutils.widgets.text("table", "gold.fct_orders")          # Delta
dbutils.widgets.text("iceberg_table", "gold.dim_customers")  # Delta + UniForm (dbt)
CATALOG = dbutils.widgets.get("catalog")
TABLE = f"{CATALOG}.{dbutils.widgets.get('table')}"
ICEBERG = f"{CATALOG}.{dbutils.widgets.get('iceberg_table')}"
out = {"table": TABLE, "iceberg_table": ICEBERG}

# COMMAND ----------

# MAGIC %md ## Same table, three engines

# COMMAND ----------

t0 = time.perf_counter()
spark_rows = spark.sql(f"SELECT category, count(*) AS n, round(sum(amount), 2) AS revenue FROM {TABLE} GROUP BY ALL").collect()
out["spark_s"] = round(time.perf_counter() - t0, 3)

t0 = time.perf_counter()
arrow = spark.table(TABLE).toArrow()  # governed read, then zero-copy hand-off
out["spark_to_arrow_s"] = round(time.perf_counter() - t0, 3)

orders = pl.from_arrow(arrow)
t0 = time.perf_counter()
by_cat_polars = orders.group_by("category").agg(pl.len().alias("n"), pl.col("amount").sum().round(2).alias("revenue"))
out["polars_s"] = round(time.perf_counter() - t0, 4)

t0 = time.perf_counter()
by_cat_duck = duckdb.sql("SELECT category, count(*) AS n, round(sum(amount), 2) AS revenue FROM arrow GROUP BY ALL").fetchall()
out["duckdb_s"] = round(time.perf_counter() - t0, 4)

out.update(
    arrow_rows=arrow.num_rows,
    polars_rows=orders.height,
    duckdb_rows=duckdb.sql("SELECT count(*) FROM arrow").fetchall()[0][0],
    engines_agree=sorted(map(tuple, spark_rows)) == sorted(by_cat_duck) == sorted(by_cat_polars.rows()),
)

# COMMAND ----------

# MAGIC %md ## The UniForm (Iceberg-readable) table from dbt: format, history, time travel

# COMMAND ----------

detail = spark.sql(f"DESCRIBE EXTENDED {ICEBERG}").collect()
out["iceberg_provider"] = next((r.data_type for r in detail if r.col_name == "Provider"), None)
props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {ICEBERG}").collect()}
out["uniform_formats"] = props.get("delta.universalFormat.enabledFormats")
customers = pl.from_arrow(spark.table(ICEBERG).toArrow())
out["iceberg_rows_polars"] = customers.height
history = [r.asDict() for r in spark.sql(f"DESCRIBE HISTORY {ICEBERG}").collect()]
out["iceberg_versions"] = [(h["version"], h["operation"]) for h in history]
if len(history) > 1:
    v = history[-1]["version"]  # first version
    out["iceberg_rows_first_version"] = spark.sql(f"SELECT count(*) FROM {ICEBERG} VERSION AS OF {v}").first()[0]

# COMMAND ----------

# Outbound internet probe (Free Edition restricts egress to trusted domains).
egress = {}
for url in ["https://pypi.org/simple/duckdb/", "https://github.com", "https://example.com"]:
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            egress[url] = r.status
    except Exception as e:  # noqa: BLE001
        egress[url] = f"ERR {type(e).__name__}: {str(e)[:120]}"
out.update(egress=egress, duckdb_version=duckdb.__version__, polars_version=pl.__version__)

dbutils.notebook.exit(json.dumps(out, default=str))
