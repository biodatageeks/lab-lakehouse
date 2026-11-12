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
# MAGIC | step | operation | version (usually) |
# MAGIC |---|---|---|
# MAGIC | create | `CREATE TABLE … AS SELECT` 2023 | 0 |
# MAGIC | insert | `INSERT` 2024 | 1 |
# MAGIC | bad_update | a **wrong** `UPDATE` (pay × 100) | 2 |
# MAGIC | delete | `DELETE` students | 3 |
# MAGIC | merge | `MERGE` 2025 | 4 |
# MAGIC | add_column | `ALTER TABLE … ADD COLUMN` | 5 |
# MAGIC | optimize | `OPTIMIZE` | 6 |
# MAGIC
# MAGIC "Usually": Databricks may add commits of its own — e.g. an automatic `OPTIMIZE` (auto compaction)
# MAGIC after a write that left many small files. So we note the version after each step instead of assuming it.

# COMMAND ----------

import json
T = "survey.lab.responses_poland"
COLS = "year, response_id, age_band, dev_type, is_employed, comp_usd_yearly, work_mode, ai_use"
src = lambda y: f"SELECT {COLS} FROM survey.silver.responses WHERE country = 'Poland' AND year = {y}"
STEPS = [("create", "CREATE TABLE AS SELECT"), ("insert", "WRITE"), ("bad_update", "UPDATE"), ("delete", "DELETE"),
         ("merge", "MERGE"), ("add_column", "ADD COLUMNS"), ("optimize", "OPTIMIZE"), ("repair", "MERGE"),
         ("restore_old", "RESTORE"), ("restore_repaired", "RESTORE")]


def step(name, statement):
    spark.sql(statement)
    v[name] = spark.sql(f"DESCRIBE HISTORY {T} LIMIT 1").collect()[0].version


def steps_from_history():
    """Step name -> version, read back from the table history (so a restarted notebook can go on)."""
    if not spark.catalog.tableExists(T):
        return {}
    hist, out, i = spark.sql(f"DESCRIBE HISTORY {T}").orderBy("version").collect(), {}, 0
    for name, op in STEPS:
        while i < len(hist) and hist[i].operation != op:
            i += 1  # skip commits Databricks made on its own (e.g. auto-compaction OPTIMIZE)
        if i == len(hist):
            break
        out[name], i = hist[i].version, i + 1
    return out


v = steps_from_history()  # step name -> table version right after that step
print(v or "table not created yet")

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {T}")
v.clear()
step("create", f"CREATE TABLE {T} AS {src(2023)}")
step("insert", f"INSERT INTO {T} {src(2024)}")
step("bad_update", f"UPDATE {T} SET comp_usd_yearly = comp_usd_yearly * 100 WHERE year = 2024")  # the mistake
step("delete", f"DELETE FROM {T} WHERE dev_type = 'Student'")
step("merge", f"""MERGE INTO {T} t USING ({src(2025)}) s
                  ON t.year = s.year AND t.response_id = s.response_id
                  WHEN NOT MATCHED THEN INSERT *""")
step("add_column", f"ALTER TABLE {T} ADD COLUMN reviewed BOOLEAN")
step("optimize", f"OPTIMIZE {T}")
print(v)

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
  SELECT version, year, count(*) AS rows, round(avg(comp_usd_yearly)) AS avg_pay FROM (
    SELECT {v['create']} AS version, * FROM {T} VERSION AS OF {v['create']} UNION ALL
    SELECT {v['insert']}, *            FROM {T} VERSION AS OF {v['insert']} UNION ALL
    SELECT {v['bad_update']}, *        FROM {T} VERSION AS OF {v['bad_update']} UNION ALL
    SELECT {v['delete']}, *            FROM {T} VERSION AS OF {v['delete']} UNION ALL
    SELECT {v['merge']}, *             FROM {T} VERSION AS OF {v['merge']})
  GROUP BY ALL ORDER BY 1, 2"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Before and after the wrong update
# MAGIC The same rows in the version before the update (1) and right after it (2).
# MAGIC In plain SQL: `SELECT … FROM survey.lab.responses_poland VERSION AS OF 1` vs. `VERSION AS OF 2`.

# COMMAND ----------

before_after = spark.sql(f"""
  SELECT b.response_id, b.comp_usd_yearly AS before_update, a.comp_usd_yearly AS after_update
  FROM {T} VERSION AS OF {v['insert']} b JOIN {T} VERSION AS OF {v['bad_update']} a USING (year, response_id)
  WHERE b.year = 2024 AND b.comp_usd_yearly IS NOT NULL
  ORDER BY b.response_id LIMIT 10""")
display(before_after)

# COMMAND ----------

# MAGIC %md
# MAGIC `TIMESTAMP AS OF` works too: any point in time, Delta picks the version that was current then.

# COMMAND ----------

ts_v1 = spark.sql(f"DESCRIBE HISTORY {T}").filter(f"version = {v['insert']}").collect()[0].timestamp
print(f"version {v['insert']} was written at", ts_v1)
display(spark.sql(f"SELECT year, count(*) FROM {T} TIMESTAMP AS OF '{ts_v1}' GROUP BY year"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix the mistake
# MAGIC **Option A — repair only the bad values** from the version before the mistake; everything done since
# MAGIC (delete, merge, new column) stays.

# COMMAND ----------

step("repair", f"""MERGE INTO {T} t
    USING (SELECT year, response_id, comp_usd_yearly FROM {T} VERSION AS OF {v['insert']} WHERE year = 2024) old
    ON t.year = old.year AND t.response_id = old.response_id
    WHEN MATCHED THEN UPDATE SET t.comp_usd_yearly = old.comp_usd_yearly""")
display(spark.sql(f"SELECT year, round(avg(comp_usd_yearly)) AS avg_pay FROM {T} GROUP BY year ORDER BY year"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Option B — `RESTORE`** the whole table to an old version. The restore is itself a new version,
# MAGIC so it can be undone the same way.

# COMMAND ----------

step("restore_old", f"RESTORE TABLE {T} TO VERSION AS OF {v['insert']}")   # 2023 + 2024, before the mistake
print("after restore:", spark.table(T).count(), "rows,", len(spark.table(T).columns), "columns")
step("restore_repaired", f"RESTORE TABLE {T} TO VERSION AS OF {v['repair']}")   # back to the repaired table
print(v)
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
# MAGIC * Write one query that lists the respondents who exist in the version before the `DELETE` but not after it. What were they?
# MAGIC * Open the table in Catalog Explorer: find the `RESTORE` entries in *History*.

# COMMAND ----------

final = spark.sql(f"DESCRIBE HISTORY {T}").orderBy("version").collect()
dbutils.notebook.exit(json.dumps({
    "operations": [r.operation for r in final],
    "steps": {name: final[ver].operation for name, ver in v.items()},
    "versions": v,
    "recovered_from_history": steps_from_history() == v,
    "rows_now": spark.table(T).count(),
    "avg_pay_2024_before_after": [spark.sql(f"SELECT round(avg(comp_usd_yearly)) FROM {T} VERSION AS OF {x} WHERE year = 2024").collect()[0][0]
                                  for x in (v["insert"], v["bad_update"])],
    "deleted_rows": final[v["delete"]].operationMetrics.get("numDeletedRows"),
    "before_after_rows": before_after.count(),
    "bronze_time_travel": bronze_time_travel,
}, default=str))
