# Databricks notebook source
# MAGIC %md
# MAGIC # Session 6 — open formats, three engines, one table
# MAGIC Polars and DuckDB read the course tables over **Delta Sharing** — the same code works on a
# MAGIC laptop (`external/delta_sharing_read.py`) and here. Spark runs the same query natively, as
# MAGIC the reference.
# MAGIC
# MAGIC Why not read the catalog directly? On Free Edition every table lives on Databricks default
# MAGIC storage, and open-catalog clients (DuckDB `unity_catalog`, Polars `pl.Catalog`, PyIceberg)
# MAGIC cannot read it — not from a laptop, and not from a notebook either (Databricks-managed
# MAGIC encryption). The sharing server serves the data instead.
# MAGIC
# MAGIC **Before this notebook:** run `uv run python -m prep.delta_sharing setup` once on your
# MAGIC laptop. It shares your course tables and stores the credential as secret `lab/shop_share`.

# COMMAND ----------

# MAGIC %pip install delta-sharing deltalake duckdb polars --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import time
import urllib.request

import delta_sharing
import duckdb
import polars as pl

dbutils.widgets.text("catalog", "shop")
dbutils.widgets.text("share", "lab_shop_share")
CATALOG = dbutils.widgets.get("catalog")
SHARE = dbutils.widgets.get("share")

# The credential never appears in the notebook: it comes from the secret scope.
PROFILE = "/tmp/shop.share"
with open(PROFILE, "w") as f:
    f.write(dbutils.secrets.get("lab", "shop_share"))


def load(table: str, version: int | None = None) -> pl.DataFrame:
    """Read `schema.table` from the share into Polars (use_delta_format = fast path)."""
    url = f"{PROFILE}#{SHARE}.{table}"
    return pl.from_pandas(delta_sharing.load_as_pandas(url, version=version, use_delta_format=True))


out = {"shared_tables": sorted(f"{t.schema}.{t.name}" for t in delta_sharing.SharingClient(PROFILE).list_all_tables())}

# COMMAND ----------

# MAGIC %md ## Same table, three engines

# COMMAND ----------

t0 = time.perf_counter()
orders = load("gold.fct_orders")
out["sharing_load_s"] = round(time.perf_counter() - t0, 3)

t0 = time.perf_counter()
by_cat_polars = orders.group_by("category").agg(pl.len().alias("n"), pl.col("amount").sum().round(2).alias("revenue"))
out["polars_s"] = round(time.perf_counter() - t0, 4)

t0 = time.perf_counter()
by_cat_duck = duckdb.sql("SELECT category, count(*) AS n, round(sum(amount), 2) AS revenue FROM orders GROUP BY ALL").fetchall()
out["duckdb_s"] = round(time.perf_counter() - t0, 4)

t0 = time.perf_counter()
by_cat_spark = spark.sql(
    f"SELECT category, count(*) AS n, round(sum(amount), 2) AS revenue FROM {CATALOG}.gold.fct_orders GROUP BY ALL"
).collect()
out["spark_s"] = round(time.perf_counter() - t0, 3)

out.update(
    polars_rows=orders.height,
    duckdb_rows=duckdb.sql("SELECT count(*) FROM orders").fetchall()[0][0],
    engines_agree=sorted(map(tuple, by_cat_spark)) == sorted(by_cat_duck) == sorted(by_cat_polars.rows()),
)

# COMMAND ----------

# MAGIC %md ## The UniForm (Iceberg-readable) table from dbt, and time travel over the share

# COMMAND ----------

props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {CATALOG}.gold.dim_customers").collect()}
out["dim_customers_uniform"] = props.get("delta.universalFormat.enabledFormats")
out["dim_customers_rows_polars"] = load("gold.dim_customers").height

history = [r.asDict() for r in spark.sql(f"DESCRIBE HISTORY {CATALOG}.gold.fct_orders").collect()]
first = min(h["version"] for h in history)
out["fct_orders_versions"] = [(h["version"], h["operation"]) for h in history]
out["time_travel_rows_v_first"] = load("gold.fct_orders", version=first).height

# COMMAND ----------

# Outbound internet probe (Free Edition restricts egress to trusted domains).
egress = {}
for url in ["https://pypi.org/simple/duckdb/", "https://github.com", "https://example.com"]:
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            egress[url] = r.status
    except Exception as e:  # noqa: BLE001
        egress[url] = f"ERR {type(e).__name__}: {str(e)[:120]}"
out.update(egress=egress, duckdb_version=duckdb.__version__, polars_version=pl.__version__,
           delta_sharing_version=delta_sharing.__version__)

dbutils.notebook.exit(json.dumps(out, default=str))
