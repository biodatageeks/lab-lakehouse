# Running the lakehouse course on a Snowflake trial — what is possible

Verified 2026-11-12 on a fresh Snowflake trial account (Enterprise edition, AWS eu-north-1,
created 2026-10-24, X-Small warehouse, `ACCOUNTADMIN`), with the assumption that **students
work only in Snowflake notebooks** (Workspaces notebooks on Container Runtime) — no laptops.
Every claim below was executed: either by the headless suite (`uv run pytest`, results at
the end) or by one-off probes during the evaluation.

## Verdict

The whole course runs inside Snowflake on a trial with no credit card. Every session has a
native equivalent, several of them simpler than on Databricks Free Edition (schema evolution
without a failed run, ingestion without retries, dbt with no install, Iceberg on
Snowflake-managed storage). The real constraints are **account-level**, not feature-level:

1. **The trial lasts 30 days** (with ~$400 of free credit). A course longer than four weeks
   needs a second trial per student or a compressed schedule (see *Risks*).
2. **No external access on trials**: notebooks cannot reach the internet. Python packages
   still install, through Snowflake's PyPI mirror (`requirements.txt` +
   `snowflake.snowpark.pypi_shared_repository`).
3. **Open-format clients inside notebooks get Iceberg metadata, not data**: PyIceberg can talk
   to the Horizon Iceberg REST catalog with the notebook's own token, but the Parquet files sit
   in a bucket the container cannot reach. DuckDB/Polars read the tables through Snowflake
   (`to_arrow()`), exactly like the Databricks version reads through Spark.

Side-by-side with Databricks Free Edition: [platform-comparison.md](platform-comparison.md).

## Session by session

| Session | Databricks Free Edition | Snowflake trial (notebooks only) | Status |
| --- | --- | --- | --- |
| 0 Setup | SDK/CLI prep script | `prep/bootstrap.sql` run once in a Workspaces SQL file (idempotent) | ✅ |
| 1 Storage | UC catalog/schemas, volume, CTAS, `_delta_log` | database/schemas, internal **stage**, `INFER_SCHEMA` + `USING TEMPLATE` + `COPY`, managed **Iceberg** table on `SNOWFLAKE_MANAGED` storage, metadata via Horizon REST | ✅ (data files not readable by open clients) |
| 1 Upload | Files API from laptop | `session.file.put_stream` from the notebook | ✅ |
| 2 Ingestion | Auto Loader `availableNow`; fails once on new column | `COPY INTO` load history (re-run = 0 files), `ENABLE_SCHEMA_EVOLUTION` adds `COUPON_CODE` in the same run, `ON_ERROR = CONTINUE` + `COPY_HISTORY` for the malformed row | ✅ simpler |
| 2 Streaming | `processingTime` refused on serverless | **Snowpipe** on the internal stage (`ALTER PIPE … REFRESH`), **stream on stage + triggered serverless task** loads files as they land | ✅ |
| 3 Live pipeline | — | `03_pipeline --live-minutes N`: gold `TARGET_LAG = '1 minute'`, silver `DOWNSTREAM`; new files reach gold with scheduled refreshes only, **53–104 s** from landing to gold (4 files, measured); cleans up after | ✅ |
| 3 Pipeline | Lakeflow declarative pipeline + expectations + event log | **dynamic tables** (`TARGET_LAG = DOWNSTREAM`), **DMF expectations** (`SYSTEM$EVALUATE_DATA_QUALITY_EXPECTATIONS`), `DYNAMIC_TABLE_REFRESH_HISTORY` | ✅ |
| 3 `FAIL UPDATE` | expectation action | no native equivalent; workaround: a query that errors on bad rows makes the refresh fail and keeps the last good data | ⚠️ workaround |
| 4 Time travel | `DESCRIBE HISTORY`, `VERSION AS OF`, `RESTORE`, `OPTIMIZE`, `VACUUM` | `QUERY_HISTORY`, `BEFORE(STATEMENT)`, `AT(TIMESTAMP/OFFSET)`, clone + `SWAP`, `UNDROP`, retention 0–90 days, clustering keys; no `VACUUM`/`OPTIMIZE` (automatic) | ✅ |
| 5 dbt | dbt-databricks from laptop | **dbt Projects on Snowflake** (`CREATE/EXECUTE DBT PROJECT`, dbt 1.9.4 + snowflake 1.9.2): views, incremental **merge**, tests, `docs generate` | ✅ |
| 5 Iceberg model | `table_format='iceberg'` / UniForm | built-in `table_format='iceberg'` fails on Snowflake storage (`BASE_LOCATION` not supported); 15-line `iceberg_table` materialization in `dbt_shop/macros/` | ✅ with macro |
| 6 Open formats | Spark → Arrow → DuckDB/Polars; Delta Sharing from laptop | Snowpark `to_arrow()` → Polars/DuckDB in the notebook; Iceberg snapshots via Horizon REST; Iceberg time travel with `AT(TIMESTAMP)` | ✅ |
| 6 Open clients read data | blocked (default storage) | blocked inside notebooks (no route to the bucket); DuckDB `INSTALL iceberg` blocked (no internet) | ❌ same limit as Databricks |
| 7 Jobs | Jobs API, 5 concurrent tasks | serverless **task** on a 1-minute schedule; a task running `EXECUTE NOTEBOOK PROJECT` (= a notebook job) | ✅ |
| 7 Git | Git folder from public repo | Git API integration + `GIT REPOSITORY` (public repo), `COPY FILES` to a stage | ✅ public only |
| 7 Cost | `system.billing.usage` (1-day lag) | `ACCOUNT_USAGE.METERING_HISTORY`, `ORGANIZATION_USAGE.REMAINING_BALANCE_DAILY`, resource monitor | ✅ |

## Session 6b: open engines on the live Iceberg files, from Google Colab

Inside a trial notebook no engine can reach the Iceberg data files (below). From a hosted
notebook with internet — **Google Colab** (free, no card; Kaggle and Codespaces work the same way)
— they can, through the Horizon Iceberg REST catalog with vended credentials, on
Snowflake-managed storage (no bucket of your own). Verified with `tests/test_s6b_colab.py`, which
runs `notebooks/colab/src/06b_open_engines.py` in a clean `python:3.12` Linux container with
Java 17 as the read-only service user from `prep/colab_access.sql`:

| Engine (outside Snowflake) | How | Result |
| --- | --- | --- |
| PyIceberg 0.12 | REST catalog, `X-Iceberg-Access-Delegation: vended-credentials` | ✅ 700 rows; time travel to the first `DIM_CUSTOMERS` snapshot (188) |
| Polars 2.1 | `pl.scan_iceberg(table, storage_options=<vended S3 keys>)` | ✅ same per-category result as Snowflake |
| DuckDB 2.0 | `iceberg` extension, `ATTACH … (TYPE iceberg, ACCESS_DELEGATION_MODE 'vended_credentials')` | ✅ same result |
| PySpark 3.5.6 + Iceberg 1.9.1 | `spark.jars.packages` iceberg-spark-runtime + iceberg-aws-bundle, REST catalog | ✅ same result, `VERSION AS OF`, `.snapshots` table — needs `spark.sql.iceberg.vectorization.enabled=false` (the vectorized reader fails on Snowflake-written Parquet) |

Notes: Horizon serves **Iceberg tables only**, so Session 6 publishes `GOLD.FCT_ORDERS_ICEBERG`
(timestamps cast to microseconds). Auth is key-pair JWT → OAuth token (`session:role:LAB_READER`);
the private key lives in Colab Secrets, and access is revoked with `ALTER USER LAB_COLAB UNSET
RSA_PUBLIC_KEY`. The role sees only the three Iceberg tables. Data and credentials leave Snowflake
— fine for this synthetic data.

## Not possible / limits found

| Limit | Evidence | Impact / workaround |
| --- | --- | --- |
| External access integrations | `External access is not supported for trial accounts.` | No internet from notebooks/procedures (DNS fails for pypi.org, github.com). Packages: use the PyPI mirror in `requirements.txt`. |
| Open-catalog **data** reads inside notebooks | PyIceberg via Horizon loads metadata, then `AWS Error NETWORK_CONNECTION` on the first manifest; Polars `scan_iceberg`: `Could not resolve` (no DNS for any storage host) | Read via Snowflake → Arrow, PySpark via Snowpark Connect, or unload Parquet to a stage → `GET` → `scan_parquet`. For the live files use Session 6b in Google Colab. |
| DuckDB extensions | `INSTALL iceberg` needs extensions.duckdb.org | Use DuckDB on Arrow tables only. |
| Classic notebooks | `Creation of legacy notebooks has been disabled for this account.` | Use Workspaces notebooks (`CREATE NOTEBOOK PROJECT` / `EXECUTE NOTEBOOK PROJECT`). |
| Notebook project straight from Git | `Invalid domain to copy files: 'Git repository'` | `COPY FILES` from the repo into a stage, then create the project (`SHOP.OPS.LOAD_COURSE`), or use *Workspaces → From Git repository*. |
| Private Git repos | `Operation 'clone' is not authorized` (repo is private) | Make the course repo public, or store a GitHub token as a Snowflake `SECRET` on the API integration. |
| dbt `table_format='iceberg'` on Snowflake storage | `BASE_LOCATION property is not supported for Iceberg tables using Snowflake storage` | `iceberg_table` materialization (`dbt_shop/macros/iceberg_table.sql`). |
| dbt packages (`dbt deps`) | needs internet | Course project uses no packages. |
| `FAIL UPDATE` expectations | not in dynamic tables | Fail-fast query workaround, or a task graph with a check task. |
| Headless notebook start-up | ~100–140 s per `EXECUTE NOTEBOOK PROJECT` (container cold start) | Fine for jobs and grading; interactive sessions stay warm. |
| Concurrent notebooks | `SYSTEM_COMPUTE_POOL_CPU` (max 2 nodes): two concurrent runs took 104/106 s in the suite; in an early probe the second waited ~2× as long | Fine per student (one account each). |
| Resource monitors | cover warehouses only | Serverless tasks, Snowpipe and Container Runtime are not capped; watch `METERING_HISTORY`. |

## Risks for the course

* **Trial length.** 30 days from sign-up. For a 6–7-week course, students move to a second trial
  (another e-mail) when the first expires. `prep/new_account.sql` covers it inside Snowsight:
  `bootstrap.sql` → `CALL SHOP.OPS.LOAD_COURSE(...)` (public Git repo, or files uploaded to the
  `SHOP.OPS.COURSE` stage) → `CALL SHOP.OPS.REBUILD(<session>)`, which replays the session notebooks
  headlessly. The producer is seeded, so the rebuilt tables match the answer key exactly.
  Verified end-to-end by `tests/test_s9_new_account.py` (drops `SHOP`, the warehouse and the
  monitor, then rebuilds sessions 1–7 and grades).
* **Credits.** The whole evaluation (probes + four full headless runs) used about 2.7 credits
  (warehouse 2.50, notebook containers 0.20, serverless tasks/COPY < 0.01); one student's course
  run is a fraction of that. The ~$400 free balance is not a constraint.
* **Region.** Pick any AWS region at sign-up; everything above was verified on AWS eu-north-1.

## Verification (2026-11-12)

| Run | Result |
| --- | --- |
| `LAB_NEW_ACCOUNT=1 uv run pytest` (sessions 0–7 + new-account rebuild) | 39 passed, 2 failed (test scoping, fixed below), 43 min 46 s |
| `uv run pytest tests/test_s1_storage.py tests/test_s3_pipeline.py` after the fixes | 12 passed (incl. offline build checks) |
| New-account rebuild (`test_s9_new_account`) | empty account → `bootstrap.sql` → upload → `LOAD_COURSE('')` → `REBUILD(7)`: 8 notebooks, 18.5 min, grading **15/15** |

Rebuild step times (s): 01_storage 104 · 02_ingestion 117 + 168 · 03_pipeline 106 ·
04_time_travel 105 · 05_dbt 158 · 06_open_formats 105 · 07_ops 246.
Two headless notebooks at once: 104.5 s and 105.5 s (no queueing at that size).

Fixed during verification: refresh history must be scoped to `SHOP` and to the current run
(`DYNAMIC_TABLE_REFRESH_HISTORY` is account-wide); Snowflake may commit an UPDATE + DELETE on an
Iceberg table as one snapshot (2–3 snapshots, not always 3).
