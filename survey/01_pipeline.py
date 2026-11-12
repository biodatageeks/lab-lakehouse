# Databricks notebook source
# MAGIC %md
# MAGIC # 1 · The pipeline, the trigger, and the first two years
# MAGIC
# MAGIC The processing is a **Spark Declarative Pipeline** (SDP, "Lakeflow Declarative Pipelines" in the UI).
# MAGIC You *declare* tables; the pipeline works out the order, runs them, checks data quality and keeps the history.
# MAGIC Open the source files next to this notebook:
# MAGIC
# MAGIC | file | declares |
# MAGIC |---|---|
# MAGIC | `pipeline/bronze.py` | `bronze.responses` — **streaming table**, Auto Loader, `PARTITIONED BY year` |
# MAGIC | `pipeline/silver.py` | `silver.responses`, `silver.languages` — materialized views, cleaning + **expectations** |
# MAGIC | `pipeline/gold.sql` | `gold.*` — materialized views in SQL |
# MAGIC
# MAGIC A **job with a file arrival trigger** watches the landing volume and refreshes the pipeline whenever a
# MAGIC new file appears. The first load (2023 + 2024) we start by hand; from then on the trigger takes over
# MAGIC (notebook 2).

# COMMAND ----------

import json, time
from databricks.sdk import WorkspaceClient
from helpers import (CATALOG, LANDING, ensure_job, ensure_pipeline, land_year, notebook_folder, now_ms,
                     run_job_now, wait_for_triggered_run)

dbutils.widgets.text("years", "2023,2024")
years = [int(y) for y in dbutils.widgets.get("years").split(",") if y.strip()]
w = WorkspaceClient()  # the notebook's own identity, no token needed

# COMMAND ----------

# MAGIC %md
# MAGIC ## The first two years land
# MAGIC Each year goes into its own `year=YYYY/` folder — that folder name becomes the partition column.

# COMMAND ----------

landed = [land_year(y) for y in years]
for f in landed:
    print(f)
display(dbutils.fs.ls(LANDING))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the pipeline and the job (safe to re-run)

# COMMAND ----------

pipeline_id = ensure_pipeline(w, notebook_folder())
job_id = ensure_job(w, pipeline_id)
host = w.config.host.rstrip("/")
print("pipeline:", f"{host}/pipelines/{pipeline_id}")
print("job:     ", f"{host}/jobs/{job_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The initial load
# MAGIC A brand-new trigger first takes a snapshot of the folder (within about a minute) and from then on reacts
# MAGIC only to files that arrive **after** it. The files we just copied are part of that snapshot, so the first
# MAGIC load we start by hand — the same as *Run now* in the UI.
# MAGIC Open *Jobs & Pipelines → survey-on-file-arrival* and watch the run.

# COMMAND ----------

since = now_ms()
run_job_now(w, job_id)
run = wait_for_triggered_run(w, job_id, since)
print(json.dumps(run, indent=2))
assert run["result"] == "SUCCESS", run

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the pipeline built
# MAGIC Open the **pipeline** page: the graph shows bronze → silver → gold, rows written per table and the
# MAGIC expectation results (click `silver.responses` → *Data quality*).

# COMMAND ----------

display(spark.sql(f"""SELECT year, count(*) AS rows, count(DISTINCT _source_file) AS files
                      FROM {CATALOG}.bronze.responses GROUP BY year ORDER BY year"""))

# COMMAND ----------

detail = spark.sql(f"DESCRIBE DETAIL {CATALOG}.bronze.responses").collect()[0]
print("partition columns:", detail.partitionColumns, "| files:", detail.numFiles, "| MB:", round(detail.sizeInBytes / 1e6, 1))
display(spark.sql(f"SHOW PARTITIONS {CATALOG}.bronze.responses"))

# COMMAND ----------

# MAGIC %md
# MAGIC Bronze keeps the text exactly as in the file, `NA` included. Silver turns it into typed columns.

# COMMAND ----------

display(spark.sql(f"""SELECT year, ResponseId, Country, Age, ConvertedCompYearly, LanguageHaveWorkedWith
                      FROM {CATALOG}.bronze.responses LIMIT 5"""))
display(spark.sql(f"""SELECT year, response_id, country, age_band, comp_usd_yearly, languages_worked
                      FROM {CATALOG}.silver.responses LIMIT 5"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality: the expectations, from the pipeline's event log

# COMMAND ----------

expectations = spark.sql(f"""
  SELECT e.dataset, e.name, sum(e.passed_records) AS passed, sum(e.failed_records) AS failed
  FROM (SELECT explode(from_json(details:flow_progress:data_quality:expectations,
                 'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
        FROM {CATALOG}.lab.pipeline_events
        WHERE event_type = 'flow_progress' AND origin.update_id = (
          SELECT origin.update_id FROM {CATALOG}.lab.pipeline_events
          WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1))
  GROUP BY ALL ORDER BY ALL""")
display(expectations)

# COMMAND ----------

def counts(table):
    return {r.year: r.n for r in spark.sql(f"SELECT year, count(*) AS n FROM {table} GROUP BY year").collect()}

report = {
    "pipeline_id": pipeline_id, "job_id": job_id, "landed": landed, "run": run,
    "bronze": counts(f"{CATALOG}.bronze.responses"),
    "silver": counts(f"{CATALOG}.silver.responses"),
    "bronze_columns": len(spark.table(f"{CATALOG}.bronze.responses").columns),
    "partition_columns": list(detail.partitionColumns),
    "expectations": [r.asDict() for r in expectations.collect()],
    "gold": {t: spark.table(f"{CATALOG}.gold.{t}").count() for t in
             ("respondents_by_country", "language_trend", "salary_by_country", "ai_adoption", "remote_work", "databases_pro")},
}
dbutils.notebook.exit(json.dumps(report, default=str))
