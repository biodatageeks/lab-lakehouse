# Trainer notes · Stack Overflow survey lab (Databricks Free Edition)

Verified end-to-end on a Free Edition workspace on 2026-11-12 (`uv run pytest`, clean state).

## Topics covered

1. Unity Catalog: catalog, schemas, **volumes** for raw files
2. Getting data in from the internet inside a notebook (Git LFS pointer vs. real file)
3. **Auto Loader** (`cloudFiles`): incremental file loading, schema inference and **schema evolution**
4. A bronze table **partitioned by year** (`year=YYYY/` folders → partition column)
5. **Spark Declarative Pipelines**: streaming table vs. materialized view, dependencies, Python + SQL in one pipeline
6. **Expectations** (data quality): drop vs. warn, results in the UI and the event log
7. Data cleaning across years: `NA`, multi-select answers, answers whose wording changed
8. Orchestration: a **job with a file arrival trigger** — the pipeline runs when data arrives
9. Spark SQL → pandas → matplotlib, and why `toPandas()` only on small results
10. **Delta Lake**: commits, versions, `DESCRIBE HISTORY`, `VERSION AS OF`, `TIMESTAMP AS OF`, repair, `RESTORE`

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

## Task hints

* **Task 1** (notebook 0): 2024 adds 43 columns, 2025 adds 118 and drops 60 → a table holding all
  years must evolve its schema; old years get NULL in new columns.
* **Task 2** (notebook 2): the last update wrote 49 191 rows to bronze — only the 2025 file; the
  checkpoint remembers which files were loaded.
* **Task 3** (notebook 3): `gold.databases_pro` per year; `ed_level` of 2025; pay by `work_mode`.
* **Task 4** (notebook 4): `numDeletedRows` is on version 3; the "exist in v2 but not v3" query is
  `SELECT … FROM t VERSION AS OF 2 EXCEPT SELECT … FROM t VERSION AS OF 3` (the students).

## Starting over / a new account

`99_reset` removes the job, the pipeline and the `survey` catalog. A new Free Edition account needs
only the Git folder and notebook 0.
