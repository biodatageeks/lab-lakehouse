# Databricks notebook source
# MAGIC %md
# MAGIC # Session 2 — incremental ingestion with Auto Loader
# MAGIC Reads JSON files that the producer drops into `/Volumes/shop/bronze/landing/<stream>/`
# MAGIC and appends them to `shop.bronze.<stream>_raw`. Run it again after new files arrive:
# MAGIC the checkpoint makes sure only new files are processed.

# COMMAND ----------

import json
import time

dbutils.widgets.text("catalog", "shop")
dbutils.widgets.dropdown("stream", "orders", ["orders", "clicks"])
dbutils.widgets.dropdown("trigger", "availableNow", ["availableNow", "processingTime"])
CATALOG = dbutils.widgets.get("catalog")
STREAM = dbutils.widgets.get("stream")
TRIGGER = dbutils.widgets.get("trigger")

LANDING = f"/Volumes/{CATALOG}/bronze/landing"
SOURCE = f"{LANDING}/{STREAM}"
CHECKPOINT = f"{LANDING}/_checkpoints/{STREAM}_raw"
SCHEMA_LOC = f"{LANDING}/_schemas/{STREAM}_raw"
TARGET = f"{CATALOG}.bronze.{STREAM}_raw"

# COMMAND ----------


def reader():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", SCHEMA_LOC)
        .option("cloudFiles.inferColumnTypes", "true")
        # default schemaEvolutionMode = addNewColumns: the stream stops once on a new
        # column, records it in the schema location, and picks it up on restart.
        .load(SOURCE)
    )


def start(trigger: str):
    w = reader().writeStream.option("checkpointLocation", CHECKPOINT).option("mergeSchema", "true")
    w = w.trigger(availableNow=True) if trigger == "availableNow" else w.trigger(processingTime="1 minute")
    return w.toTable(TARGET)


result = {"stream": STREAM, "trigger": TRIGGER, "errors": []}

# COMMAND ----------

if TRIGGER == "processingTime":
    # Probe: serverless compute only supports availableNow / once triggers.
    try:
        q = start("processingTime")
        time.sleep(20)
        result["processing_time_supported"] = True
        q.stop()
    except Exception as e:  # noqa: BLE001
        result["processing_time_supported"] = False
        result["errors"].append(str(e)[:500])
    dbutils.notebook.exit(json.dumps(result))

# COMMAND ----------

before = spark.table(TARGET).count() if spark.catalog.tableExists(TARGET) else 0
# When a file brings a new column, this run FAILS with UNKNOWN_FIELD_EXCEPTION after
# recording the new schema. Simply run the cell again (in a Job: set max_retries >= 1).
# Catching the error does not help: serverless still marks the task as failed.
start("availableNow").awaitTermination()
result["attempts"] = 1

df = spark.table(TARGET)
result.update(
    rows_before=before,
    rows_after=df.count(),
    columns=df.columns,
    rescued_rows=df.where("_rescued_data IS NOT NULL").count() if "_rescued_data" in df.columns else None,
)
dbutils.notebook.exit(json.dumps(result))
