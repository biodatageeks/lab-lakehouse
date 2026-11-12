# Databricks Free Edition vs Snowflake trial — what works for this course

Both evaluated on 2026-11-11/12 against the same six-session course. Databricks: Free Edition
on AWS us-east-2 (branch `free-edition-test-suite`). Snowflake: 30-day trial, Enterprise, AWS
eu-north-1 (branch `snowflake-notebooks`). Neither asks for a credit card.

✅ works · ⚠️ works with a caveat / workaround · ❌ doesn't work · — not tested

| Area | Feature | Databricks Free Edition | Snowflake trial |
| --- | --- | --- | --- |
| **Account** | Free, no card | ✅ | ✅ |
| | Lifetime | ✅ no expiry (daily quotas) | ⚠️ 30 days; `prep/new_account.sql` rebuilds on a new trial (~18.5 min) |
| | Course runs without laptop installs | ⚠️ producer, dbt and grading verified from a laptop | ✅ everything runs in Snowflake notebooks (plus Colab for 6b) |
| | Workspace setup by script | ✅ SDK/CLI from a laptop (catalog via SQL fallback) | ✅ `bootstrap.sql` in Snowsight |
| **S1 Storage** | Catalog, schemas, landing area | ✅ catalog + volume | ✅ database + stage |
| | Upload files | ✅ Files API from a laptop | ✅ `put_stream` from a notebook |
| | Typed table from CSV | ✅ CTAS | ✅ `INFER_SCHEMA` + `COPY` |
| | Look inside the table format | ✅ `_delta_log` JSON in the volume | ⚠️ Iceberg metadata via Horizon REST; no direct file access |
| **S2 Ingestion** | Incremental load, re-run = nothing new | ✅ Auto Loader `availableNow` | ✅ `COPY` load history |
| | Schema evolution | ⚠️ fails once on the new column, re-run | ✅ in the same load |
| | Bad records | ✅ `_rescued_data` | ✅ `ON_ERROR = CONTINUE` + `COPY_HISTORY` |
| | Continuous / on-arrival loading | ❌ `processingTime` refused on serverless | ✅ Snowpipe + stream-triggered task |
| | Live pipeline (files flow to gold without manual runs) | — (triggered pipelines; continuous not tested) | ✅ live mode: dynamic tables, 1-minute lag, ~1–1¾ min file → gold |
| **S3 Pipeline** | Declarative pipeline | ✅ Lakeflow | ✅ dynamic tables |
| | Expectations: drop bad rows | ✅ | ✅ DMF expectations + filter |
| | Expectations: fail the update | ✅ `FAIL UPDATE` | ⚠️ fail-fast query workaround |
| | Event log | ✅ event log table | ✅ refresh history (scope to `SHOP`; it is account-wide) |
| **S4 Time travel** | History, as-of queries | ✅ `DESCRIBE HISTORY`, `VERSION AS OF` | ✅ query history, `BEFORE` / `AT` |
| | Restore / undrop | ✅ `RESTORE` | ✅ clone + `SWAP`, `UNDROP` |
| | Compaction | ✅ `OPTIMIZE` | ⚠️ automatic; clustering keys only |
| | Purge old versions | ⚠️ `VACUUM`; `RETAIN 0 HOURS` refused | ⚠️ automatic (retention + fail-safe); no `VACUUM` |
| **S5 dbt** | dbt build, merge, tests, docs | ✅ dbt-databricks from a laptop | ✅ native dbt Projects on Snowflake |
| | Iceberg model | ✅ UniForm (`use_managed_iceberg: false`) | ⚠️ custom materialization + microsecond timestamps |
| **S6 Open formats, inside the platform's notebooks** | DuckDB / Polars on a governed table | ✅ Spark → Arrow | ✅ Snowpark `to_arrow()` |
| | PySpark | ✅ native Spark | ⚠️ Snowpark Connect: PySpark API, executed by Snowflake |
| | Install packages | ✅ `%pip` (PyPI is a trusted domain) | ✅ Snowflake PyPI mirror via `requirements.txt` |
| | Open clients read the table files | ❌ encryption blocks reads | ❌ no route to storage (metadata only) |
| | Polars on Parquet files | — | ⚠️ after unload to a stage + `GET` (a copy) |
| **S6b Open formats, outside the platform** (laptop or Colab) | PyIceberg / Polars / DuckDB on the live table files | ❌ default storage refuses credentials | ✅ Horizon REST + vended credentials, verified in a Colab stand-in |
| | PySpark + Iceberg runtime on the live files | ❌ | ✅ verified (needs `iceberg.vectorization.enabled=false`) |
| | Iceberg time travel from outside | ❌ | ✅ PyIceberg `snapshot_id`, Spark `VERSION AS OF` |
| | Open sharing to outside engines | ⚠️ Delta Sharing from a laptop (not inside a notebook) | — (Horizon covers the read use case) |
| | Iceberg time travel in SQL | ✅ | ✅ `AT(TIMESTAMP)` |
| **S7 Ops** | Scheduled job | ✅ Jobs (max 5 concurrent tasks) | ✅ serverless task, 1-minute schedule |
| | Notebook as a job | ✅ `jobs.submit` | ✅ task running `EXECUTE NOTEBOOK PROJECT` |
| | Git integration | ✅ Git folder (public repo) | ⚠️ public repo only; copy to a stage first |
| | Cost visibility | ⚠️ `system.billing.usage`, ~1-day lag | ✅ metering + free-balance views; resource monitors cap warehouses only |
| | Internet from notebooks | ⚠️ trusted domains only | ❌ none on trials (PyPI mirror still works) |
| **Verification** | Headless test suite | ✅ 44 passed / 10 opt-in skipped | ✅ 39 + 12 passed after fixes; new-account rebuild 15/15; Colab stand-in 3/3 |

## In short

- **Databricks Free Edition** wins on account lifetime (no expiry), native pipeline semantics
  (`FAIL UPDATE`, `OPTIMIZE`, `VACUUM`), real Spark in the notebook, and open sharing to laptops
  via Delta Sharing.
- **Snowflake trial** wins on a browser-only course, ingestion (schema evolution in one load,
  on-arrival loading), native dbt, and open-format reads **from outside**: PyIceberg, Polars,
  DuckDB and Spark read the live Iceberg files via Horizon (Session 6b in Google Colab).
- **Both** block open-format clients from reading table files **inside** their own notebooks, so
  in-platform DuckDB/Polars get the data as Arrow.

Details and exact error texts: [snowflake-feasibility.md](snowflake-feasibility.md). The
Databricks findings are in the `free-edition-test-suite` branch (PR #1).
