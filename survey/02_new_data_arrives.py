# Databricks notebook source
# MAGIC %md
# MAGIC # 2 · A new year arrives — and the pipeline runs by itself
# MAGIC
# MAGIC The 2025 results are published. We only copy the file into the landing volume. Then:
# MAGIC
# MAGIC 1. the **file arrival trigger** notices the new file (it waits until nothing has changed for a minute)
# MAGIC 2. the job refreshes the pipeline
# MAGIC 3. **Auto Loader** reads only the new file — 2023 and 2024 are not read again
# MAGIC 4. 2025 has 172 columns, many new: **schema evolution** adds them to bronze
# MAGIC 5. silver and gold are recomputed
# MAGIC
# MAGIC Keep *Jobs & Pipelines → survey-on-file-arrival* open while the next cells run.

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient
from helpers import CATALOG, JOB_NAME, land_year, now_ms, wait_for_triggered_run

dbutils.widgets.text("year", "2025")
year = int(dbutils.widgets.get("year"))
w = WorkspaceClient()
job_id = [j for j in w.api_client.do("GET", "/api/2.2/jobs/list", query={"name": JOB_NAME}).get("jobs", [])][0]["job_id"]
BRONZE = f"{CATALOG}.bronze.responses"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Before: columns and table version

# COMMAND ----------

columns_before = set(spark.table(BRONZE).columns)
version_before = spark.sql(f"DESCRIBE HISTORY {BRONZE} LIMIT 1").collect()[0].version
print(len(columns_before), "columns, version", version_before)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The file lands — no "run" button pressed

# COMMAND ----------

since = now_ms()
landed = land_year(year)
print(landed)

# COMMAND ----------

run = wait_for_triggered_run(w, job_id, since)
print(json.dumps(run, indent=2))
assert run["result"] == "SUCCESS", run

# COMMAND ----------

# MAGIC %md
# MAGIC ## After
# MAGIC * a new partition `year=2025`
# MAGIC * new columns (schema evolution) — old years simply have NULL there
# MAGIC * the table history has new versions: each pipeline update that writes is a Delta commit

# COMMAND ----------

display(spark.sql(f"SELECT year, count(*) AS rows, min(_ingested_at) AS ingested_at FROM {BRONZE} GROUP BY year ORDER BY year"))

# COMMAND ----------

columns_after = set(spark.table(BRONZE).columns)
new_columns = sorted(columns_after - columns_before)
print(len(columns_after), "columns,", len(new_columns), "new, e.g.:", new_columns[:10])

# COMMAND ----------

history = spark.sql(f"DESCRIBE HISTORY {BRONZE}")
display(history.select("version", "timestamp", "operation", "operationParameters", "operationMetrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Did Auto Loader really read only the new file?
# MAGIC The pipeline event log records, for every update, how many rows each flow wrote.

# COMMAND ----------

flows = spark.sql(f"""
  SELECT origin.update_id, min(timestamp) AS at, origin.flow_name,
         sum(details:flow_progress:metrics:num_output_rows::bigint) AS rows_written
  FROM {CATALOG}.lab.pipeline_events
  WHERE event_type = 'flow_progress' AND details:flow_progress:metrics:num_output_rows IS NOT NULL
  GROUP BY ALL ORDER BY at""")
display(flows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## <span style='background:yellow'> TASK 2 </span>
# MAGIC * In the job's *Runs* tab: what is the **trigger** of the first run, and of the second?
# MAGIC * Open the pipeline: how many rows did `bronze.responses` take in the last update? Why not 200 000?
# MAGIC * Pick one new 2025 column and check it is NULL for 2023 and 2024.

# COMMAND ----------

def counts(table):
    return {r.year: r.n for r in spark.sql(f"SELECT year, count(*) AS n FROM {table} GROUP BY year").collect()}

bronze_writes = [r.rows_written for r in flows.collect() if r.flow_name.endswith("bronze.responses")]
report = {
    "landed": landed, "run": run,
    "bronze": counts(BRONZE), "silver": counts(f"{CATALOG}.silver.responses"),
    "columns_before": len(columns_before), "columns_after": len(columns_after), "new_columns": len(new_columns),
    "version_before": version_before, "version_after": history.agg({"version": "max"}).collect()[0][0],
    "operations": [r.operation for r in history.orderBy("version").collect()],
    "bronze_rows_per_update": bronze_writes,
}
dbutils.notebook.exit(json.dumps(report, default=str))
