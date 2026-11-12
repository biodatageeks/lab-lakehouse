# Databricks notebook source
# MAGIC %md
# MAGIC # 4 · Delta: versions, history and time travel
# MAGIC
# MAGIC Every write to a Delta table is a **commit**: a new **version** with the operation, who did it and
# MAGIC metrics. Old versions stay readable (`VERSION AS OF`, `TIMESTAMP AS OF`) until `VACUUM` removes their files.
# MAGIC
# MAGIC We use our own table, `survey.lab.responses_poland` (Polish respondents from silver). The pipeline owns
# MAGIC its tables — a full refresh rewrites them and materialized views accept no `UPDATE` — so for
# MAGIC experiments we make a plain Delta table and change it step by step:
# MAGIC
# MAGIC | version | operation |
# MAGIC |---|---|
# MAGIC | 0 | `CREATE TABLE … AS SELECT` 2023 |
# MAGIC | 1 | `INSERT` 2024 |
# MAGIC | 2 | a **wrong** `UPDATE` (pay × 100) |
# MAGIC | 3 | `DELETE` students |
# MAGIC | 4 | `MERGE` 2025 |
# MAGIC | 5 | `ALTER TABLE … ADD COLUMN` |
# MAGIC | 6 | `OPTIMIZE` |

# COMMAND ----------

import json
T = "survey.lab.responses_poland"
COLS = "year, response_id, age_band, dev_type, is_employed, comp_usd_yearly, work_mode, ai_use"
src = lambda y: f"SELECT {COLS} FROM survey.silver.responses WHERE country = 'Poland' AND year = {y}"

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {T}")
spark.sql(f"CREATE TABLE {T} AS {src(2023)}")                                   # v0
spark.sql(f"INSERT INTO {T} {src(2024)}")                                       # v1
spark.sql(f"UPDATE {T} SET comp_usd_yearly = comp_usd_yearly * 100 WHERE year = 2024")  # v2 - the mistake
spark.sql(f"DELETE FROM {T} WHERE dev_type = 'Student'")                        # v3
spark.sql(f"""MERGE INTO {T} t USING ({src(2025)}) s
              ON t.year = s.year AND t.response_id = s.response_id
              WHEN NOT MATCHED THEN INSERT *""")                                # v4
spark.sql(f"ALTER TABLE {T} ADD COLUMN reviewed BOOLEAN")                       # v5
spark.sql(f"OPTIMIZE {T}")                                                      # v6

# COMMAND ----------

# MAGIC %md
# MAGIC ## The log of operations
# MAGIC Same information in the UI: **Catalog → survey → lab → responses_poland → History**.

# COMMAND ----------

history = spark.sql(f"DESCRIBE HISTORY {T}")
display(history.select("version", "timestamp", "userName", "operation", "operationParameters", "operationMetrics").orderBy("version"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read old versions

# COMMAND ----------

display(spark.sql(f"""
  SELECT 'v' || v AS version, year, count(*) AS rows, round(avg(comp_usd_yearly)) AS avg_pay FROM (
    SELECT 0 AS v, * FROM {T} VERSION AS OF 0 UNION ALL
    SELECT 1, *      FROM {T} VERSION AS OF 1 UNION ALL
    SELECT 2, *      FROM {T} VERSION AS OF 2 UNION ALL
    SELECT 3, *      FROM {T} VERSION AS OF 3 UNION ALL
    SELECT 4, *      FROM {T} VERSION AS OF 4)
  GROUP BY ALL ORDER BY 1, 2"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Before and after the wrong update
# MAGIC The same rows in version 1 (before) and version 2 (after):

# COMMAND ----------

before_after = spark.sql(f"""
  SELECT b.response_id, b.comp_usd_yearly AS before_update, a.comp_usd_yearly AS after_update
  FROM {T} VERSION AS OF 1 b JOIN {T} VERSION AS OF 2 a USING (year, response_id)
  WHERE b.year = 2024 AND b.comp_usd_yearly IS NOT NULL
  ORDER BY b.response_id LIMIT 10""")
display(before_after)

# COMMAND ----------

# MAGIC %md
# MAGIC `TIMESTAMP AS OF` works too: any point in time, Delta picks the version that was current then.

# COMMAND ----------

ts_v1 = history.filter("version = 1").collect()[0].timestamp
print("version 1 was written at", ts_v1)
display(spark.sql(f"SELECT year, count(*) FROM {T} TIMESTAMP AS OF '{ts_v1}' GROUP BY year"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix the mistake
# MAGIC **Option A — repair only the bad values** from the version before the mistake; everything done since
# MAGIC (delete, merge, new column) stays.

# COMMAND ----------

spark.sql(f"""MERGE INTO {T} t USING (SELECT year, response_id, comp_usd_yearly FROM {T} VERSION AS OF 1 WHERE year = 2024) old
              ON t.year = old.year AND t.response_id = old.response_id
              WHEN MATCHED THEN UPDATE SET t.comp_usd_yearly = old.comp_usd_yearly""")   # v7
display(spark.sql(f"SELECT year, round(avg(comp_usd_yearly)) AS avg_pay FROM {T} GROUP BY year ORDER BY year"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Option B — `RESTORE`** the whole table to an old version. The restore is itself a new version,
# MAGIC so it can be undone the same way.

# COMMAND ----------

spark.sql(f"RESTORE TABLE {T} TO VERSION AS OF 1")   # v8: back to 2023 + 2024, before the mistake
print("after restore:", spark.table(T).count(), "rows,", len(spark.table(T).columns), "columns")
spark.sql(f"RESTORE TABLE {T} TO VERSION AS OF 7")   # v9: back to the repaired table
display(spark.sql(f"DESCRIBE HISTORY {T}").select("version", "operation", "operationParameters").orderBy("version"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## The pipeline's tables have a history too
# MAGIC Each pipeline update that wrote to `bronze.responses` is a version: the initial load and the
# MAGIC arrival of 2025. Can we read bronze as it was before 2025 arrived?

# COMMAND ----------

bronze_history = spark.sql("DESCRIBE HISTORY survey.bronze.responses").select("version", "timestamp", "operation").orderBy("version")
display(bronze_history)
last_before_2025 = [r.version for r in bronze_history.collect() if r.operation == "STREAMING UPDATE"][-2]
try:
    old = spark.sql(f"SELECT year, count(*) AS n FROM survey.bronze.responses VERSION AS OF {last_before_2025} GROUP BY year ORDER BY year")
    display(old)
    bronze_time_travel = {str(r.year): r.n for r in old.collect()}
except Exception as e:  # noqa: BLE001
    bronze_time_travel = {"error": str(e).split("\n")[0][:300]}
    print(bronze_time_travel)

# COMMAND ----------

# MAGIC %md
# MAGIC ## <span style='background:yellow'> TASK 4 </span>
# MAGIC * Which version deleted the most rows? (`operationMetrics.numDeletedRows`)
# MAGIC * Write one query that lists the respondents who exist in version 2 but not in version 3. What were they?
# MAGIC * Open the table in Catalog Explorer: find the `RESTORE` entries in *History*.

# COMMAND ----------

final = spark.sql(f"DESCRIBE HISTORY {T}").orderBy("version").collect()
dbutils.notebook.exit(json.dumps({
    "operations": [r.operation for r in final],
    "rows_now": spark.table(T).count(),
    "avg_pay_2024_v1_v2": [spark.sql(f"SELECT round(avg(comp_usd_yearly)) FROM {T} VERSION AS OF {v} WHERE year = 2024").collect()[0][0] for v in (1, 2)],
    "deleted_v3": final[3].operationMetrics.get("numDeletedRows"),
    "before_after_rows": before_after.count(),
    "bronze_time_travel": bronze_time_travel,
}, default=str))
