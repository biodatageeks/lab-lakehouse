# Databricks notebook source
# MAGIC %md
# MAGIC # 0 · Setup and a first look at the data
# MAGIC
# MAGIC We build a small lakehouse from the **Stack Overflow Developer Survey** (2023, 2024, 2025):
# MAGIC
# MAGIC ```
# MAGIC GitHub (CSV) ──download──▶ volume /Volumes/survey/raw/landing/year=YYYY/results.csv
# MAGIC                                   │ file arrival trigger starts the pipeline
# MAGIC                                   ▼
# MAGIC  SDP pipeline:  bronze.responses (Auto Loader, partitioned by year)
# MAGIC                  → silver.responses, silver.languages (cleaned)
# MAGIC                  → gold.* (aggregates)
# MAGIC ```
# MAGIC
# MAGIC Notebooks: **0** setup (this one) · **1** pipeline + trigger, first two years · **2** a new year arrives ·
# MAGIC **3** analysis and charts · **4** Delta versions and time travel.
# MAGIC
# MAGIC Data: *Stack Overflow Developer Survey*, Stack Exchange Inc., [ODbL 1.0](https://opendatacommons.org/licenses/odbl/1-0/).

# COMMAND ----------

import json
from helpers import CATALOG, LANDING, MEDIA, RAW, header_columns, http_head, source_url

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog, schemas and a volume for the raw files
# MAGIC A **volume** is a folder of files governed by Unity Catalog, the place where raw files land.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in ("raw", "bronze", "silver", "gold", "lab"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.landing")
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where the data lives
# MAGIC The archive is in the GitHub repository [StackExchange/Survey](https://github.com/StackExchange/Survey/tree/main/packages/archive).
# MAGIC Big files there are stored with **Git LFS**: the "raw" link returns a small *pointer* file, not the data.

# COMMAND ----------

size, body = http_head(source_url(2024, media=False))
print(f"{RAW}/2024/results.csv  ->  {size} bytes:\n{body.decode()}")

# COMMAND ----------

size, body = http_head(source_url(2024))
print(f"{MEDIA}/2024/results.csv  ->  {size / 1e6:.0f} MB, starts with:\n{body[:300].decode('utf-8', 'replace')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## <span style='background:yellow'> TASK 1 </span>
# MAGIC Compare the three years **before** loading anything: file size and columns.
# MAGIC Which columns are new in 2024? In 2025? What does that mean for a table that holds all years?

# COMMAND ----------

years = [2023, 2024, 2025]
cols = {y: header_columns(y) for y in years}
sizes = {y: round(http_head(source_url(y), 16)[0] / 1e6, 1) for y in years}
for y in years:
    print(y, f"{sizes[y]:>6} MB", f"{len(cols[y]):>4} columns", "first column:", repr(cols[y][0]))
for a, b in [(2023, 2024), (2024, 2025)]:
    new = sorted(set(cols[b]) - set(cols[a]))
    print(f"\n{b} vs {a}: {len(new)} new, {len(set(cols[a]) - set(cols[b]))} gone, e.g. {new[:8]}")

# COMMAND ----------

# MAGIC %md
# MAGIC Notice the first 2025 column: `'﻿ResponseId'`. The file starts with a **byte order mark** (BOM),
# MAGIC an invisible character. Spark's CSV reader removes it; plain Python does not.

# COMMAND ----------

report = {
    "schemas": sorted(r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()),
    "volume": LANDING,
    "pointer_bytes": http_head(source_url(2024, media=False))[0],
    "mb": sizes,
    "columns": {y: len(c) for y, c in cols.items()},
    "first_column_2025": cols[2025][0],
}
dbutils.notebook.exit(json.dumps(report))
