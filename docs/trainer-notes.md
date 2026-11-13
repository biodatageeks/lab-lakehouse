# Trainer notes · Stack Overflow survey lab (Databricks Free Edition)

Verified end-to-end on a Free Edition workspace on 2026-11-13 (`uv run pytest`, clean state).

## Topics covered

1. Unity Catalog: catalog, schemas, **volumes** for raw files
2. Getting data in from the internet inside a notebook (Git LFS pointer vs. real file)
3. **Auto Loader** (`cloudFiles`): incremental file loading, schema inference and **schema evolution**
4. A bronze table **partitioned by year** (`year=YYYY/` folders → partition column)
5. **Spark Declarative Pipelines**: streaming table vs. materialized view, dependencies, Python + SQL in one pipeline
6. **Expectations** (data quality): drop vs. warn, results in the UI and the event log
7. Data cleaning across years: `NA`, multi-select answers, answers whose wording changed
8. Orchestration: a **job with a file arrival trigger** — the pipeline runs when data arrives
9. **Spark DataFrame API**: `createDataFrame` with and without a schema, `printSchema`, `describe`/`summary`,
   `select`, `distinct`, `filter`/`where` (string vs `Column`), `groupBy`/`agg`, `withColumn` (`lit`, `concat_ws`,
   `when`), immutability (`drop`), `split`/`explode`/`posexplode`, array functions, `saveAsTable`, CSV to a volume
10. **Spark SQL**: temporary views, a `%sql` cell, one question in both APIs → the same plan (Catalyst),
    CTE, `HAVING`, window functions, subqueries
11. **Lazy evaluation**: transformations vs actions, column and partition pruning in `explain`, late errors on
    Spark Connect, the same question in eager pandas (`read_csv` parses the whole file)
12. Charts: matplotlib, pandas `.plot`, seaborn (histogram, pie, line, bar, box, violin/boxen, strip, heatmap),
    and why `toPandas()` only on small results
13. **Delta Lake**: commits, versions, `DESCRIBE HISTORY`, `VERSION AS OF`, `TIMESTAMP AS OF`, repair, `RESTORE`

## The data

| year | answers | columns | CSV |
|---|---|---|---|
| 2023 | 89 184 | 84 | 159 MB |
| 2024 | 65 437 | 114 (+43) | 160 MB |
| 2025 | 49 191 | 172 (+118) | 141 MB |

Bronze ends with 249 columns (all years' columns + `year`, `_source_file`, `_ingested_at`, `_rescued_data`).

## What to show in the UI

* **Catalog → survey → raw → landing**: the `year=…` folders and files.
* **Jobs & Pipelines → survey-on-file-arrival → Runs**: first run trigger *One time* (notebook 1),
  second *File arrival* (notebook 2). *Job details → Trigger*: the watched path, "wait after last change".
* **Jobs & Pipelines → survey-pipeline**: the graph bronze → silver → gold; click a table for rows
  written, schema and *Data quality* (expectations passed/failed). *Update details*: what each update did.
* **Catalog → survey → bronze → responses → History**: one `STREAMING UPDATE` per load.
  *Details*: partition column `year`, column mapping.
* **Catalog → survey → lab → responses_poland → History**: the ten versions of notebook 4.

## Gotchas (all handled in the code — good discussion points)

| what | why | where |
|---|---|---|
| `raw.githubusercontent.com` returns a 134-byte file | the archive uses Git LFS: that is a pointer; the data is on `media.githubusercontent.com` | notebook 0 |
| 2025 header starts with `﻿` | byte order mark; Spark's CSV reader strips it, Python's `csv` does not | notebook 0 |
| 2025 column names contain spaces and quotes (`AIToolDon't plan to use AI for this task`) | Delta rejects such names unless **column mapping** is on (`delta.columnMapping.mode = name`) | `bronze.py` |
| answers contain commas and quotes | `escape = '"'` (RFC 4180) and `multiLine = true` | `bronze.py` |
| `NA` instead of empty | same as `NULLVALUE "NA"` in the Spark notebook; bronze keeps it, silver turns it into NULL | `silver.py` |
| 2025 changed answers: "Employed, full-time" → "Employed", "Yes" → "Yes, I use AI tools daily/…", "Hybrid (…)" → two hybrid answers + "Your choice", "Engineer, data" → "Data engineer" | surveys evolve; without mapping, 2025 silently drops out of pay statistics | `silver.py` |
| silver is a materialized view, not a streaming table | bronze changes schema when a year brings new columns; a streaming read of it would stop, a materialized view simply recomputes | `silver.py` |
| a new file arrival trigger needs 30-60 s before it reacts | it first snapshots the folder; files already there (or copied right after creating the job) never fire it. Hence the first load uses *Run now* | notebook 1 |
| the trigger waits ~60 s after the last change, then the run starts | `wait_after_last_change_seconds = 60`, so a file copied in several steps is not loaded half-written. Measured: run starts ~90 s after the file lands | notebook 2 |
| the Databricks SDK preinstalled in notebooks is older than some pipeline settings | `helpers.py` calls the REST API directly | `helpers.py` |
| pipeline tables are not for experiments | a full refresh rewrites them, materialized views accept no DML → notebook 4 uses its own Delta table | notebook 4 |
| version numbers are not always "ours" | Databricks can add its own commits, e.g. an automatic `OPTIMIZE` (auto compaction) right after the repair `MERGE` — seen in a test run. Notebook 4 notes the version after each step instead of hard-coding 0-9 | notebook 4 |
| time travel on the pipeline's streaming table | works: `SELECT … FROM survey.bronze.responses VERSION AS OF <n>` returns 2023 + 2024 only, i.e. bronze before 2025 arrived | notebook 4 |

## Task hints

* **Task 1** (notebook 0): 2024 adds 43 columns, 2025 adds 118 and drops 60 → a table holding all
  years must evolve its schema; old years get NULL in new columns.
* **Task 2** (notebook 2): the last update wrote 49 191 rows to bronze — only the 2025 file; the
  checkpoint remembers which files were loaded.
* **Task 3A** (notebook 3, DataFrame API): a) `df.filter("year = 2025").select("dev_type").distinct().count()`
  (NULL counts as one value — `dropna()` or `isNotNull()` to leave it out), same for `country`;
  b) `filter(F.col("country").isin("Poland", "Czech Republic", "Slovakia") & (F.col("years_code") > 30))`;
  c) `df.groupBy("dev_type").pivot("year").count().orderBy(F.desc("2025"))` → `.write.mode("overwrite").saveAsTable("survey.lab.dev_types")`.
* **Task 3B** (SQL): `GROUP BY country HAVING count(*) >= 500`, `avg` + `median`; compare both APIs with
  `collect() ==` or `exceptAll`. `top3` in the DataFrame API: `withColumn("place", F.rank().over(w)).filter("place <= 3")`.
* **Task 3C** (lazy): only `show`, `toPandas`, `saveAsTable` (and `count`, `collect`) run Spark jobs;
  `printSchema` asks for the schema (a quick round trip on Spark Connect, no job), `explain` only prints the plan.
  The second `collect()` is usually faster (warm caches), but it runs the plan again. Filtering on `Country`
  leaves `PartitionFilters` empty: bronze is partitioned by `year` only, so every partition must be read.
* **Task 3D** (charts): `gold.databases_pro` per year; `ed_level` of 2025; pay by `work_mode`; `org_size`
  categories differ between years — filter on 2025 and pass `order=` to seaborn.
* **Task 4** (notebook 4): `numDeletedRows` is on the `DELETE` version (usually 3); the query is
  `SELECT … FROM t VERSION AS OF 2 EXCEPT SELECT … FROM t VERSION AS OF 3` — the students.

## Timings measured on Free Edition (2026-11-12, notebook 3 on 2026-11-13)

| step | time |
|---|---|
| download one year into the volume | 3-10 s |
| first load, 2023 + 2024 (job run incl. pipeline start-up) | 82-102 s |
| 2025 file lands → job starts (file arrival trigger) | 82-86 s |
| pipeline run for 2025 (new columns, silver + gold recomputed) | 162-173 s |
| notebook 3, Run all (parts A-D, without the tasks) | ~2 min |
| whole test-suite (reset + notebooks 0-4) | ~13 min |

## Starting over / a new account

`99_reset` removes the job, the pipeline and the `survey` catalog. A new Free Edition account needs
only the Git folder and notebook 0.
