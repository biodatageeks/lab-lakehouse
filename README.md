# lab-lakehouse

Course repo and feature test-suite for **Lakehouse Basics on Databricks Free Edition**
(six sessions: Unity Catalog, Auto Loader, Lakeflow pipelines, time travel, dbt, open formats).

Everything is `uv`-managed and runs from a laptop against one Free Edition workspace.

## Quick start

```bash
uv sync                                   # Python 3.12 venv with all deps
cp .env.example .env                      # set DATABRICKS_CONFIG_PROFILE or HOST + TOKEN
uv run python -m prep.setup_workspace     # one-time, idempotent workspace prep (no external access needed)
uv run python check_setup.py              # prints workspace URL, warehouse ID, catalogs
uv run pytest                             # full feature test (~13 min, resets `shop`)
LAB_OPEN_CATALOG=1 uv run pytest          # + outside-engine probes (enables external data access)
```

`prep.setup_workspace` does with the SDK what the plan otherwise asks you to click:

| Step | How | Free Edition quirk handled |
| --- | --- | --- |
| Catalog `shop` | `catalogs.create`, else SQL `CREATE CATALOG` | API refuses catalogs on Default Storage; SQL works |
| Schemas bronze/silver/gold, Volume `bronze.landing` | `schemas.create`, `volumes.create` | — |
| Warm SQL warehouse | `warehouses.start` | — |
| *Optional* (`--external-access`): External data access ON | `metastores.update(external_access_enabled=True)` | Was still `false` after a manual UI toggle; the API sets it reliably |
| *Optional* (`--external-access`): `EXTERNAL USE SCHEMA` | `grants.update` on the **catalog** | Schema-level grant is rejected on `SCHEMA_DB_STORAGE` |

The course does not need external data access: Session 6 reads tables inside notebooks
(Spark → Arrow → DuckDB/Polars), through the SQL warehouse, or over **Delta Sharing** from the
laptop — all verified with the toggle off.

### Laptop engines via Delta Sharing (no external storage, no credit card)

```bash
uv run python -m prep.delta_sharing setup        # scope INTERNAL_AND_EXTERNAL, share + token recipient,
                                                 # credential file -> ~/.lab/shop.share (keep out of git)
uv run python -m external.delta_sharing_read gold.fct_orders             # -> Polars / DuckDB
uv run python -m external.delta_sharing_read gold.fct_orders --version 0 # time travel
uv run python -m prep.delta_sharing teardown --restore-scope
```

Shareable: managed Delta tables, UniForm tables, materialized views. **Not** shareable: managed
Iceberg tables (`DS_UNSUPPORTED_DELTA_TABLE_FEATURES: typewidening`) — which is why dbt builds
`dim_customers` as UniForm (`use_managed_iceberg: false` in `dbt_project.yml`).

## Layout

```
prep/            setup_workspace.py (prep), reset_lab.py (wipe course state), delta_sharing.py (share for laptops)
producer/        produce.py — seeded JSON generator (files.upload), answer key
notebooks/       01_delta_log, 02_autoloader, 06_external_engines_in_notebook (Session 6), probes
pipelines/       03_pipeline/orders.sql (Lakeflow), probe_fail_update.sql
dbt_shop/        dbt-databricks project (staging, marts, tests; dim_customers = Delta + UniForm Iceberg)
external/        delta_sharing_read.py + sql_connector_arrow.py (laptop paths); duckdb_uc.py, polars_iceberg.py (open-catalog probes)
grading/         grade.py — weekly checks through the SQL connector
lab/             shared helpers (auth, SQL, serverless notebook runner, pipelines, dbt)
tests/           test_s0 … test_s7, one file per session; findings -> reports/findings.json
```

## Useful commands

```bash
uv run python -m producer.produce --count 40 --interval 60      # class mode: 1 file/min
uv run python -m producer.produce --answer-key --count 35       # expected counts
uv run python -m grading.grade --files 35 --malformed-at 15     # grade a workspace
uv run python -m lab.dbt build                                  # dbt with SDK credentials
uv run python -m external.sql_connector_arrow gold.fct_orders   # Session 6 fallback
LAB_NO_RESET=1 uv run pytest tests/test_s6_external.py          # one session, keep state
```

## Known platform limits (verified 2026-11-11)

* Open-catalog clients (DuckDB `unity_catalog`/`iceberg`, Polars `pl.Catalog`, PyIceberg) see Unity
  Catalog **metadata** but cannot read **data** on Free Edition default storage:
  * from a laptop: UC refuses credentials for `TABLE_DB_STORAGE` tables, and the bucket explicitly
    denies the vended Iceberg role;
  * from inside a notebook: credentials are vended, but S3 rejects the read because the files use
    Databricks-managed server-side encryption.

  Session 6 therefore uses Delta Sharing (laptop) or Spark → Arrow (notebook) to feed
  DuckDB/Polars, and SQL `DESCRIBE HISTORY` / `VERSION AS OF` for Iceberg time travel.
* `processingTime` triggers are refused on serverless (`INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`).
* An Auto Loader run that meets a new column fails once (`UNKNOWN_FIELD_EXCEPTION`); re-run it.
  In Jobs, set `max_retries >= 1`.
* Max 5 concurrent job tasks; `VACUUM RETAIN 0 HOURS` and disabling the retention check are refused.
