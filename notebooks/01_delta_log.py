# Databricks notebook source
# MAGIC %md
# MAGIC # Session 1 — what is inside `_delta_log`?
# MAGIC Managed tables hide their storage, so we write a small *path-based* Delta table into the
# MAGIC `landing` Volume where we can open the transaction log as plain JSON.

# COMMAND ----------

import json

dbutils.widgets.text("catalog", "shop")
CATALOG = dbutils.widgets.get("catalog")
path = f"/Volumes/{CATALOG}/bronze/landing/delta_demo"
dbutils.fs.rm(path, recurse=True)

spark.range(0, 5).withColumnRenamed("id", "n").write.format("delta").save(path)          # commit 0
spark.range(5, 8).withColumnRenamed("id", "n").write.format("delta").mode("append").save(path)  # commit 1

# COMMAND ----------

log = sorted(f.name for f in dbutils.fs.ls(f"{path}/_delta_log"))
commit0 = [json.loads(l) for l in dbutils.fs.head(f"{path}/_delta_log/00000000000000000000.json", 65536).splitlines()]
actions = sorted({k for a in commit0 for k in a})
print(log)
print(actions)
rows = spark.read.format("delta").load(path).count()

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"log_files": log, "commit0_actions": actions, "rows": rows}))
