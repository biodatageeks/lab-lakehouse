-- One-time setup of a student's Snowflake trial account. Idempotent.
-- Students: open Projects > Workspaces, create a SQL file, paste this, Run All.
--           On a NEW trial (the old one expired) use prep/new_account.sql instead: it runs
--           this file's statements and then rebuilds the lab state.
-- Harness:  executed by tests/conftest.py through the Python connector.
USE ROLE ACCOUNTADMIN;

-- Compute: one X-Small warehouse that suspends after 60 s, capped by a resource monitor.
CREATE WAREHOUSE IF NOT EXISTS LAB_WH
  WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
CREATE RESOURCE MONITOR IF NOT EXISTS LAB_RM
  WITH CREDIT_QUOTA = 50 FREQUENCY = MONTHLY START_TIMESTAMP = IMMEDIATELY
  TRIGGERS ON 80 PERCENT DO NOTIFY ON 100 PERCENT DO SUSPEND;
ALTER WAREHOUSE LAB_WH SET RESOURCE_MONITOR = LAB_RM;
USE WAREHOUSE LAB_WH;

-- Storage: medallion schemas + a landing stage (the "volume").
CREATE DATABASE IF NOT EXISTS SHOP DATA_RETENTION_TIME_IN_DAYS = 7;
CREATE SCHEMA IF NOT EXISTS SHOP.BRONZE;
CREATE SCHEMA IF NOT EXISTS SHOP.SILVER;
CREATE SCHEMA IF NOT EXISTS SHOP.GOLD;
CREATE SCHEMA IF NOT EXISTS SHOP.OPS;
CREATE STAGE IF NOT EXISTS SHOP.BRONZE.LANDING
  DIRECTORY = (ENABLE = TRUE) ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- Course files (notebooks + dbt project) and the results table the notebooks write to.
CREATE STAGE IF NOT EXISTS SHOP.OPS.COURSE ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
CREATE TABLE IF NOT EXISTS SHOP.OPS.NB_RESULTS (
  run_id STRING, notebook STRING, ts TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(), payload VARIANT);

-- LOAD_COURSE(source): put the course files into @SHOP.OPS.COURSE and (re)create the notebook
-- project SHOP.OPS.COURSE_NB.
--   source = ''                         -> files were uploaded to @SHOP.OPS.COURSE (Snowsight:
--                                          Data > Databases > SHOP > OPS > Stages > COURSE > + Files,
--                                          keeping the notebooks/ and dbt_shop/ folders)
--   source = 'https://github.com/<org>/<repo>.git@<branch>'
--                                       -> copied from a PUBLIC Git repository
CREATE OR REPLACE PROCEDURE SHOP.OPS.LOAD_COURSE(SOURCE STRING)
RETURNS VARIANT
LANGUAGE PYTHON RUNTIME_VERSION = '3.12' PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run' EXECUTE AS CALLER
AS $$
def run(session, source):
    q = lambda s: session.sql(s).collect()
    if source:
        url, _, branch = source.partition("@")
        prefix = url.rsplit("/", 1)[0]
        q(f"CREATE API INTEGRATION IF NOT EXISTS LAB_GITHUB API_PROVIDER = GIT_HTTPS_API "
          f"API_ALLOWED_PREFIXES = ('{prefix}') ENABLED = TRUE")
        q(f"ALTER API INTEGRATION LAB_GITHUB SET API_ALLOWED_PREFIXES = ('{prefix}')")
        q(f"CREATE OR REPLACE GIT REPOSITORY SHOP.OPS.COURSE_REPO API_INTEGRATION = LAB_GITHUB ORIGIN = '{url}'")
        branch = branch or "main"
        q("REMOVE @SHOP.OPS.COURSE")
        # a notebook project cannot be created from a Git repository stage directly: copy first
        for d in ("notebooks", "dbt_shop"):
            q(f"COPY FILES INTO @SHOP.OPS.COURSE/{d}/ FROM @SHOP.OPS.COURSE_REPO/branches/{branch}/{d}/")
    files = [r[0] for r in q("LIST @SHOP.OPS.COURSE")]
    if not any(f.endswith("notebooks/02_ingestion.ipynb") for f in files):
        raise ValueError(f"@SHOP.OPS.COURSE has no notebooks/ folder ({len(files)} files): upload the course first")
    q("CREATE OR REPLACE NOTEBOOK PROJECT SHOP.OPS.COURSE_NB FROM '@SHOP.OPS.COURSE/notebooks/'")
    return {"files": len(files), "notebook_project": "SHOP.OPS.COURSE_NB"}
$$;

-- REBUILD(upto): replay the session notebooks headlessly, in order, with the class arguments,
-- so a new account reaches the state "end of session <upto>" (deterministic data: same grades).
-- Runs one container per notebook (~2 min each); stops at the first failure.
CREATE OR REPLACE PROCEDURE SHOP.OPS.REBUILD(UPTO INT)
RETURNS VARIANT
LANGUAGE PYTHON RUNTIME_VERSION = '3.12' PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run' EXECUTE AS CALLER
AS $$
import time, uuid

STEPS = [  # (session, notebook, arguments) -- the same runs the class does
    (1, "01_storage", ""),
    (2, "02_ingestion", "--start 1 --count 10"),
    (2, "02_ingestion", "--start 11 --count 25 --malformed-at 15 --with-task 1"),
    (3, "03_pipeline", ""),
    (4, "04_time_travel", ""),
    (5, "05_dbt", ""),
    (6, "06_open_formats", ""),
    (7, "07_ops", "--files 35 --malformed-at 15 --notebook-task 0"),
]

def run(session, upto):
    out = []
    for s, nb, argv in STEPS:
        if s > upto:
            break
        run_id = "rebuild-" + uuid.uuid4().hex[:8]
        t0 = time.time()
        try:
            session.sql(
                f"EXECUTE NOTEBOOK PROJECT SHOP.OPS.COURSE_NB MAIN_FILE = '{nb}.ipynb' "
                "COMPUTE_POOL = 'SYSTEM_COMPUTE_POOL_CPU' QUERY_WAREHOUSE = 'LAB_WH' RUNTIME = 'V2.9-CPU-PY3.12' "
                f"ARGUMENTS = '--run-id {run_id} {argv}' REQUIREMENTS_FILE = 'requirements.txt' "
                "ARTIFACT_REPOSITORIES = (snowflake.snowpark.pypi_shared_repository)").collect()
            status = "ok"
        except Exception as e:
            status = "FAILED: " + " ".join(str(e).split())[:500]
        out.append({"session": s, "notebook": nb, "args": argv, "run_id": run_id,
                    "seconds": round(time.time() - t0), "status": status})
        if status != "ok":
            break
    return out
$$;
