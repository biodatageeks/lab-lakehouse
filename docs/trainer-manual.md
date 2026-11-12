# Lakehouse lab on Snowflake — trainer manual

Nov 12, 2026 · @Marek Wiewiórka

## Topics this lab covers

Eight notebooks, one per session, all run inside Snowflake Workspaces notebooks on a free 30-day trial. Students need only a browser: no laptop installs, no credit card, no cloud bucket of their own.

0. **Account setup and platform limits** (`00_check_setup`)
   - trial account, roles, warehouses, resource monitors
   - what a trial allows: Enterprise features, no external network access
   - Python packages from Snowflake's PyPI mirror
1. **Storage and tables** (`01_storage`)
   - databases, schemas, internal stages as the landing zone
   - schema-on-read: `INFER_SCHEMA` → typed table
   - open table formats: a managed **Apache Iceberg** table on Snowflake storage, its snapshots and metadata through the Iceberg REST catalog (Horizon)
2. **Incremental ingestion** (`02_ingestion`)
   - `COPY INTO` with load history (idempotent re-runs)
   - schema evolution (a new `COUPON_CODE` column appears mid-stream)
   - bad records: `ON_ERROR = CONTINUE`, `COPY_HISTORY`
   - **Snowpipe** and a **stream-triggered task** (load files as they land)
3. **Declarative pipelines and data quality** (`03_pipeline`)
   - medallion layers bronze → silver → gold as **dynamic tables**
   - deduplication, incremental refresh, refresh history as the event log
   - **data metric functions** with expectations; fail-fast vs drop-bad-rows
4. **Time travel and recovery** (`04_time_travel`)
   - finding a bad change in the query history
   - `BEFORE` / `AT`, zero-copy clone + `SWAP` restore, `UNDROP`
   - retention, fail-safe, clustering; why there is no `OPTIMIZE` / `VACUUM`
5. **Transformations with dbt** (`05_dbt`)
   - **dbt Projects on Snowflake** (no dbt install)
   - staging views, incremental **merge**, tests, docs
   - an Iceberg model (custom materialization)
6. **Open formats and multiple engines** (`06_open_formats`)
   - one governed table read by Snowflake SQL, **Polars** and **DuckDB** through Apache Arrow
   - Iceberg snapshots and time travel; what open clients can and cannot do from inside the platform; **6b, in Google Colab** (`colab/06b_open_engines`): PyIceberg, Polars, DuckDB and PySpark read the live Iceberg files through the Horizon REST catalog
7. **Operations** (`07_ops`)
   - scheduled serverless tasks; a task that runs a notebook (a "job")
   - Git integration; cost and credit monitoring
   - automated grading against a seeded answer key

## Before the course

The one decision to make up front is the schedule against the **30-day trial**: a course longer than four weeks needs every student to move to a second trial, which the rebuild procedure handles (see *Trial expired*).

- [ ] **Schedule.** Count the days from sign-up to the last session. Over 30 days, plan the switch to a second trial for the week it falls in; it takes about 20 minutes per student.
- [ ] **Course repo.** Make the course repository **public**, or students must upload the `notebooks/` and `dbt_shop/` folders by hand. A trial's Git integration can only clone a public repo unless a GitHub token is stored as a Snowflake secret.
- [ ] **Sign-up instructions.** Students sign up for a 30-day trial, choosing **Enterprise** edition and any **AWS** region (verified on AWS eu-north-1). No card is asked for. One e-mail address = one trial.
- [ ] **Credits.** Each trial comes with about **$400** of free credit. The whole evaluation, including four full headless runs of the test suite, used about 2.7 credits, so credits are not a constraint.
- [ ] **Dry run.** Run `prep/bootstrap.sql` and notebooks `00`–`07` once on a trainer trial, or run the headless suite (see *For course staff*).
- [ ] **Answer key.** Hand-out values for 35 producer files, malformed row in file 15, are in the session guide below.

## Student setup (day 0, ~15 minutes)

Setup is two SQL calls and one notebook import, all in Snowsight.

1. Sign up for the trial and log in to Snowsight.
2. **Projects → Workspaces → + SQL file**, paste `prep/bootstrap.sql`, **Run All**. It creates warehouse `LAB_WH` (X-Small, 60 s auto-suspend) with resource monitor `LAB_RM`, database `SHOP` (schemas `BRONZE`, `SILVER`, `GOLD`, `OPS`), stage `SHOP.BRONZE.LANDING`, and the procedures `SHOP.OPS.LOAD_COURSE` and `SHOP.OPS.REBUILD`. Re-running it is safe.
3. Load the course files:
   - public repo: `CALL SHOP.OPS.LOAD_COURSE('https://github.com/<org>/lab-lakehouse.git@<branch>');`
   - or upload: **Data → Databases → SHOP → OPS → Stages → COURSE → + Files**, keeping the `notebooks/` and `dbt_shop/` folders, then `CALL SHOP.OPS.LOAD_COURSE('');`
4. Open the notebooks in a workspace (**Workspaces → From Git repository**, or upload the `notebooks/` folder) with these settings:
   - runtime: **Container Runtime** (CPU), compute pool `SYSTEM_COMPUTE_POOL_CPU`
   - query warehouse: `LAB_WH`
   - packages: `notebooks/requirements.txt` (duckdb, polars, pyiceberg) from the Snowflake PyPI repository
5. Run `00_check_setup`. It should report role `ACCOUNTADMIN`, warehouse `LAB_WH`, `retention_90_days = allowed`, the package versions, and every internet URL as an error (expected on a trial).

Keep the helper files `lab_nb.py`, `producer.py` and `grade.py` in the same folder as the notebooks: every notebook imports them.

## Session-by-session guide

Every notebook can be run top to bottom interactively; the defaults are the class values. The data comes from a seeded producer, so every student must see exactly the numbers below.

| Session | Notebook (arguments) | Expected results | Talking points | Pitfalls |
| --- | --- | --- | --- | --- |
| 1 Storage | `01_storage` | `DIM_PRODUCTS` 50 rows, columns `PRODUCT_ID, NAME, CATEGORY, PRICE`; `PRODUCTS_ICEBERG` 2–3 snapshots (create, then update + delete; Snowflake sometimes commits the two changes as one), 49 rows | a stage is the volume; schema-on-read then typed; Iceberg = Parquet + metadata JSON + snapshots, on Snowflake storage without your own bucket | the Iceberg data-file read from PyIceberg fails by design: metadata only from inside a notebook |
| 2 Ingestion, run 1 | `02_ingestion` (defaults: files 1–10) | `ORDERS_RAW` 212 rows, `CLICKS_RAW` 500; re-run of `COPY` = "0 files processed" | load history makes `COPY` idempotent; Snowpipe = a named serverless `COPY` | Snowpipe on an internal stage needs `ALTER PIPE … REFRESH` (no cloud events) |
| 2 Ingestion, run 2 | `02_ingestion --start 11 --count 25 --malformed-at 15 --with-task 1` | `ORDERS_RAW` 746 rows, `CLICKS_RAW` 1750; new column `COUPON_CODE`; 1 row rejected ("not-a-number"); the task loaded every file, so the explicit `COPY` finds 0 | schema evolution happens in the same load (no failed run, unlike Auto Loader); stream on stage + triggered task = load on arrival | internal stages need `ALTER STAGE … REFRESH` to register files; the task fires within ~30–60 s |
| 3 Pipeline | `03_pipeline` | `SILVER.ORDERS` 700 rows; `GOLD.REVENUE_HOURLY` 6 hours, revenue 115,817.83; expectations: `VALID_AMOUNT` = 7, `VALID_CUSTOMER` = 5 (violated) | dynamic tables are declared, refreshed incrementally; DMFs measure quality, silver filters it; refresh history = event log | no `FAIL UPDATE`: show the fail-fast query (refresh fails with "Division by zero", table keeps its data) |
| 4 Time travel | `04_time_travel` | before the bad update 0 zeroed rows; after clone + `SWAP` revenue back to 115,817.83; `UNDROP` restores 700 rows; retention 90 / 0 / 7 days accepted | query history finds the culprit; clones are zero-copy; retention + 7-day fail-safe replace `VACUUM`; clustering instead of `OPTIMIZE` | `AT(OFFSET => -3600)` errors on a table younger than an hour: expected |
| 5 dbt | `05_dbt` | two `build`s, each `PASS=9 ERROR=0`; `FCT_ORDERS` 700 rows via `MERGE`; `DIM_CUSTOMERS` 188 rows, Iceberg, ≥2 snapshots; `docs generate` ok | the dbt project is a Snowflake object (dbt 1.9.4, adapter 1.9.2); incremental merge; Iceberg model via a 15-line materialization | built-in `table_format='iceberg'` fails on Snowflake storage; Iceberg wants microsecond timestamps (`TIMESTAMP_NTZ(6)`); `dbt deps` is unavailable (no internet) |
| 6 Open formats | `06_open_formats` | Snowflake, Polars and DuckDB agree per category (700 rows); `DIM_CUSTOMERS` 188 rows now and at the first snapshot; `PRODUCTS_ICEBERG` 49 now, 50 at its first snapshot | Arrow hand-off is the bridge between engines; Iceberg time travel with SQL `AT(TIMESTAMP)` | open clients read metadata but not data in a notebook; DuckDB `INSTALL iceberg` fails (no internet): both expected |
| 7 Operations | `07_ops --files 35 --malformed-at 15` | ≥2 heartbeats from the 1-minute task; the notebook task `SUCCEEDED`; Git repo copied to a stage; credits by service listed; grading **15/15** | tasks = schedules; a task running `EXECUTE NOTEBOOK PROJECT` = a job; resource monitors cap warehouses only | a headless notebook takes ~2 min to start; usage views lag by hours |

Interactively, arguments are the defaults in each notebook's `args(...)` line: for run 2 of Session 2, students change it to `args(start=11, count=25, malformed_at=15, with_task=1, ...)`. Class mode: set `interval=60` there to produce one file a minute while the class ingests.

**Session 6b runs in Google Colab** (free, browser only), because Snowflake notebooks on a trial have no egress to storage. Setup per student, once:

1. Open `notebooks/colab/06b_open_engines.ipynb` in Colab and run its first two cells. They print a private key and a public key.
2. In Colab → Secrets, add `SNOWFLAKE_ACCOUNT` (`ORGNAME-ACCOUNTNAME`) and `SNOWFLAKE_PRIVATE_KEY`, and grant the notebook access.
3. In Snowflake, run `prep/colab_access.sql` with the public key pasted in. It creates read-only role `LAB_READER` and service user `LAB_COLAB`.
4. Run the notebook top to bottom (about 2 minutes, including installs).

Expected: the role sees 3 Iceberg tables (`GOLD.FCT_ORDERS_ICEBERG`, `GOLD.DIM_CUSTOMERS`, `SILVER.PRODUCTS_ICEBERG`). `FCT_ORDERS_ICEBERG` has 700 rows. Snowflake SQL, Polars, DuckDB and Spark give the same per-category counts and revenue ("all engines agree: True"). Spark `VERSION AS OF` the first `DIM_CUSTOMERS` snapshot returns 188 rows. Talking points: catalog vs data path, vended credentials, why the same code fails inside a Snowflake notebook. Pitfall: Spark needs `spark.sql.iceberg.vectorization.enabled=false`, already set. After the course, revoke the key with `ALTER USER LAB_COLAB UNSET RSA_PUBLIC_KEY`.

## Trial expired: rebuild on a new account

A student whose trial ended gets back to the exact same state in about 20 minutes, in Snowsight only, by following `prep/new_account.sql`. The data is seeded, so rebuilt tables and grades match the answer key.

1. Sign up for a new trial (new e-mail address) and log in.
2. Run `prep/bootstrap.sql` in a Workspaces SQL file.
3. Load the course: `CALL SHOP.OPS.LOAD_COURSE('<public repo URL>@<branch>');`, or upload the two folders to stage `SHOP.OPS.COURSE` and `CALL SHOP.OPS.LOAD_COURSE('');`. Without the files it stops with "upload the course first".
4. `CALL SHOP.OPS.REBUILD(<last finished session>);` replays the session notebooks headlessly with the class arguments, about 2 minutes each, and stops at the first failure. It returns one row per notebook: session, notebook, arguments, seconds, status.
5. Check `SELECT notebook, ts, payload FROM SHOP.OPS.NB_RESULTS ORDER BY ts DESC;` and continue with the next session's notebook.

| Rebuild up to | Notebooks replayed | Time (measured) |
| --- | --- | --- |
| 2 | `01_storage`, `02_ingestion` × 2 | 6.5 min |
| 4 | + `03_pipeline`, `04_time_travel` | 10 min |
| 5 | + `05_dbt` | 12.5 min |
| 7 | + `06_open_formats`, `07_ops` | 18.5 min |

Work students did beyond the notebooks (homework tables, their own experiments) is not rebuilt; keep it in the repo or redo it.

## Grading

The last cell of `07_ops` runs 15 checks (`notebooks/grade.py`) against the answer key and prints PASS/FAIL per check plus a score; a complete lab scores **15/15**. The checks are plain SQL, so a trainer can also run them in the student's account or read the stored result: `SELECT payload:score, payload:grade FROM SHOP.OPS.NB_RESULTS WHERE notebook = '07_ops' ORDER BY ts DESC LIMIT 1;`

| Session | Check | Expected |
| --- | --- | --- |
| 1 | schemas `BRONZE` / `SILVER` / `GOLD` exist | 3 |
| 1 | stage `BRONZE.LANDING` exists | 1 |
| 1 | `SILVER.DIM_PRODUCTS` rows | 50 |
| 2 | `BRONZE.ORDERS_RAW` rows | 746 |
| 2 | `BRONZE.CLICKS_RAW` rows | 1750 |
| 2 | `ORDERS_RAW` evolved a `COUPON_CODE` column | true |
| 2 | rows rejected at load (`COPY_HISTORY`) | 1 |
| 3 | `SILVER.ORDERS` rows | 700 |
| 3 | `GOLD.REVENUE_HOURLY` hours | 6 |
| 3 | `GOLD.REVENUE_HOURLY` total revenue | 115,817.83 |
| 3 | DMF: invalid amounts in bronze | 7 |
| 3 | DMF: NULL customers in bronze | 5 |
| 5 | `GOLD.FCT_ORDERS` rows | 700 |
| 5 | `GOLD.DIM_CUSTOMERS` rows | 188 |
| 5 | `GOLD.DIM_CUSTOMERS` is a managed Iceberg table | true |

The expected values assume 35 producer files with the malformed row in file 15 (`--files 35 --malformed-at 15`). A different file count gives different numbers; `producer.answer_key(n)` prints them.

## Known limits on a trial account

None of these blocks a session; each has a workaround already in the notebooks. All were reproduced on 2026-11-12.

| Limit | What students see | Workaround in the lab |
| --- | --- | --- |
| 30-day trial | account suspended after 30 days | second trial + `REBUILD` |
| No external access integrations | "External access is not supported for trial accounts"; any internet URL fails with a DNS error | packages from Snowflake's PyPI mirror via `requirements.txt` |
| Open clients can't read Iceberg data in a notebook | PyIceberg loads metadata, then `AWS Error NETWORK_CONNECTION` | read through Snowflake, hand over as Arrow to Polars / DuckDB; for the live files, Session 6b in Google Colab |
| No DuckDB extensions | `INSTALL iceberg` fails | DuckDB on Arrow tables only |
| Classic notebooks disabled | "Creation of legacy notebooks has been disabled" | Workspaces notebooks on Container Runtime |
| Private Git repo | "Operation 'clone' is not authorized" | public repo, or upload the folders to a stage |
| Notebook project from a Git stage | "Invalid domain to copy files: 'Git repository'" | `LOAD_COURSE` copies to a stage first |
| dbt Iceberg on Snowflake storage | "BASE_LOCATION property is not supported" | `iceberg_table` materialization in `dbt_shop/macros/` |
| Iceberg timestamp precision | "Invalid time type scale … TIMESTAMP_NTZ(9)" | cast to `TIMESTAMP_NTZ(6)` |
| No `FAIL UPDATE` in dynamic tables | — | fail-fast query (refresh errors, data kept) |
| Resource monitors cover warehouses only | serverless tasks, Snowpipe, notebook containers not capped | watch `ACCOUNT_USAGE.METERING_HISTORY` |

For comparison with the Databricks Free Edition version: the open-client data limit is the same on both platforms; Snowflake removes the Auto Loader fail-and-rerun on schema change and the need for any laptop tooling.

## Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `ModuleNotFoundError: lab_nb` (or `producer`, `grade`) | helper files not next to the notebook | put `lab_nb.py`, `producer.py`, `grade.py` in the notebook's folder |
| `ModuleNotFoundError: duckdb` / `polars` / `pyiceberg` | packages not installed | add `notebooks/requirements.txt` in the notebook's package settings (PyPI repository) |
| `Object 'SHOP…' does not exist` | bootstrap not run, or sessions run out of order | run `prep/bootstrap.sql`; run notebooks in order (or `REBUILD`) |
| `Object 'SHOP.SILVER.ORDERS' does not exist` in Session 4–6 | Session 3 skipped | run `03_pipeline` |
| Session 2 counts too high or low | wrong arguments for run 2, or run 2 before run 1 | run 1 with defaults, run 2 with `start=11, count=25, malformed_at=15, with_task=1`; the producer overwrites the same files, so re-running is safe |
| Session 2 run 2 waits for minutes | directory table not refreshed | the notebook runs `ALTER STAGE SHOP.BRONZE.LANDING REFRESH`; check `TASK_HISTORY` for the task's error |
| dbt `Compilation Error` | edited model or missing source tables | `05_dbt` prints the dbt log; `SELECT SYSTEM$GET_DBT_LOG('<query id>')` shows the full log |
| Warehouse suspended, queries queue | resource monitor `LAB_RM` reached its 50-credit quota | raise `CREDIT_QUOTA` or check what consumed credits |
| `CALL SHOP.OPS.REBUILD` fails | a notebook failed | the returned `status` names the notebook and the cell; fix and re-run (completed steps are safe to repeat) |

## For course staff: verification and the test-suite

The course is verified end to end on a Snowflake trial (AWS eu-north-1), including a rebuild from an empty account that grades 15/15. Code, notebooks and docs are on branch `snowflake-notebooks`, [PR #2](https://github.com/biodatageeks/lab-lakehouse/pull/2).

| Run (2026-11-12) | Result |
| --- | --- |
| Full suite, sessions 0–7 + new-account rebuild | 39 passed, 2 failed (test scoping, fixed), 43 min 46 s |
| Sessions 1 and 3 after the fixes | 12 passed |
| New-account rebuild, `REBUILD(7)` | 8 notebooks in 18.5 min, grading 15/15 |
| Credits for the whole evaluation | about 2.7 (warehouse 2.50, notebook containers 0.20) of a ~$400 free balance |

The harness runs from a staff laptop with a key-pair connection. It deploys the notebooks as a notebook project and runs each one headlessly with `EXECUTE NOTEBOOK PROJECT`, the same code students run.

- `uv run pytest`: bootstrap, reset `SHOP`, deploy, sessions 0–7 (~30 min)
- `LAB_NEW_ACCOUNT=1 uv run pytest tests/test_s9_new_account.py`: drops `SHOP`, the warehouse and the monitor, then rebuilds like a new trial (~20 min)
- `uv run python -m lab.build`: regenerate `notebooks/*.ipynb` after editing `notebooks/src/*.py`

Session 6b is verified by `LAB_COLAB=1 uv run pytest tests/test_s6b_colab.py` (needs Docker; about 1 minute). It runs the Colab notebook code in a clean Python 3.12 Linux container with Java 17 as `LAB_COLAB`: 3 passed, all five readers agree.

Still open:

- [ ] Make the repo public, or decide on uploads, so `LOAD_COURSE` can pull from Git
- [ ] One interactive pass through the notebooks in the Workspaces UI
- [ ] Merge PR #2

Reference: `docs/snowflake-feasibility.md` in the repo lists every limit with the exact error text.
