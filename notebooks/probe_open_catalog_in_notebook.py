# Databricks notebook source
# MAGIC %md
# MAGIC # Probe: open-catalog clients (PyIceberg, DuckDB) running *inside* serverless compute
# MAGIC Same clients as the laptop scripts, but from Databricks compute. Does UC vend
# MAGIC credentials for default-storage tables here?

# COMMAND ----------

# MAGIC %pip install --upgrade "pyiceberg[pyarrow]" duckdb polars typing_extensions "numpy>=2" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json

import duckdb
import polars as pl
from databricks.sdk import WorkspaceClient
from pyiceberg.catalog import load_catalog

dbutils.widgets.text("catalog", "shop")
CATALOG = dbutils.widgets.get("catalog")

w = WorkspaceClient()
host = w.config.host.rstrip("/")
token = w.config.authenticate()["Authorization"].split(" ", 1)[1]
res = {"auth": w.config.auth_type}


def attempt(name, fn):
    try:
        res[name] = fn()
    except Exception as e:  # noqa: BLE001
        res[name] = f"ERR {type(e).__name__}: {str(e)[-700:]}"


def pyiceberg_read():
    cat = load_catalog("uc", type="rest", uri=f"{host}/api/2.1/unity-catalog/iceberg-rest", warehouse=CATALOG,
                       token=token, **{"header.X-Iceberg-Access-Delegation": "vended-credentials",
                                       "snapshot-loading-mode": "all"})
    t = cat.load_table("gold.iceberg_time_travel")
    cur = t.current_snapshot()
    return {"rows": pl.scan_iceberg(t).select(pl.len()).collect().item(),
            "rows_parent_snapshot": pl.scan_iceberg(t, snapshot_id=cur.parent_snapshot_id).select(pl.len()).collect().item()}


def duckdb_uc_read():
    con = duckdb.connect()
    for ext in ("httpfs", "delta", "unity_catalog"):
        con.sql(f"INSTALL {ext}; LOAD {ext};")
    con.sql(f"CREATE SECRET uc (TYPE unity_catalog, TOKEN '{token}', ENDPOINT '{host}', AWS_REGION 'us-east-2')")
    con.sql(f"ATTACH '{CATALOG}' AS uc (TYPE unity_catalog, SECRET uc, READ_ONLY)")
    return con.sql("SELECT count(*) FROM uc.gold.fct_orders").fetchall()[0][0]


def spark_iceberg_time_travel():
    hist = spark.sql(f"SELECT * FROM {CATALOG}.gold.iceberg_time_travel").count()
    return {"rows": hist}


attempt("pyiceberg_scan", pyiceberg_read)
attempt("duckdb_unity_catalog", duckdb_uc_read)
dbutils.notebook.exit(json.dumps(res))
