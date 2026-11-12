# %% [markdown]
# # Session 7 — operations: schedules, notebooks as jobs, Git, cost, grading
# * a serverless **task** on a 1-minute schedule
# * a task that runs a **notebook** headlessly (`EXECUTE NOTEBOOK PROJECT`) — a "job"
# * course files from a public **Git** repository, copied into a stage
# * credits used and the trial's remaining free balance
# * the weekly grading checks

# %%
# Install the course packages (duckdb, polars, pyiceberg) from the Snowflake PyPI repository.
# Interactive: pick that repository as the package source when you connect; re-run after a restart.
!pip install -q -r requirements.txt

# %%
import os, sys, time
sys.path.insert(0, os.getcwd())
from lab_nb import *
import grade

a = args(files=35, malformed_at=15, notebook_task=1, git_repo="https://github.com/delta-io/delta-sharing.git")
rep = Report("07_ops", a.run_id)

# %% [markdown]
# ## A scheduled serverless task

# %%
sql("CREATE OR REPLACE TABLE SHOP.GOLD.HEARTBEATS (at TIMESTAMP_LTZ)")
sql("""CREATE OR REPLACE TASK SHOP.GOLD.HEARTBEAT SCHEDULE = '1 MINUTE'
       AS INSERT INTO SHOP.GOLD.HEARTBEATS SELECT CURRENT_TIMESTAMP()""")
sql("ALTER TASK SHOP.GOLD.HEARTBEAT RESUME")
t0 = time.time()
while time.time() - t0 < 200 and scalar("SELECT count(*) FROM SHOP.GOLD.HEARTBEATS") < 2:
    time.sleep(10)
sql("ALTER TASK SHOP.GOLD.HEARTBEAT SUSPEND")
rep["heartbeats"] = scalar("SELECT count(*) FROM SHOP.GOLD.HEARTBEATS")
rep["heartbeat_wait_s"] = round(time.time() - t0)

# %% [markdown]
# ## A task that runs a notebook (the `heartbeat` notebook) — like a scheduled job

# %%
if a.notebook_task:
    child = f"{a.run_id}-child"
    sql(f"""CREATE OR REPLACE TASK SHOP.GOLD.NOTEBOOK_JOB WAREHOUSE = LAB_WH
            SCHEDULE = 'USING CRON 0 6 * * * UTC'
            AS EXECUTE NOTEBOOK PROJECT SHOP.OPS.COURSE_NB MAIN_FILE = 'heartbeat.ipynb'
               COMPUTE_POOL = 'SYSTEM_COMPUTE_POOL_CPU' QUERY_WAREHOUSE = 'LAB_WH' RUNTIME = 'V2.9-CPU-PY3.12'
               ARGUMENTS = '--run-id {child}' REQUIREMENTS_FILE = 'requirements.txt'
               ARTIFACT_REPOSITORIES = (snowflake.snowpark.pypi_shared_repository)""")
    sql("EXECUTE TASK SHOP.GOLD.NOTEBOOK_JOB")
    t0, state = time.time(), None
    while time.time() - t0 < 600:
        h = rows("""SELECT state, error_message FROM TABLE(SHOP.INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'NOTEBOOK_JOB'))
                    ORDER BY scheduled_time DESC LIMIT 1""")
        state = h[0] if h else None
        if state and state["STATE"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(15)
    rep["notebook_task"] = {"state": state, "wait_s": round(time.time() - t0),
                            "child_reported": scalar("SELECT count(*) FROM SHOP.OPS.NB_RESULTS WHERE run_id = ?", [child])}
    sql("DROP TASK SHOP.GOLD.NOTEBOOK_JOB")

# %% [markdown]
# ## Course files from Git (public repository; a private one needs a token stored as a SECRET)

# %%
prefix = a.git_repo.rsplit("/", 1)[0]
def git():
    sql(f"""CREATE API INTEGRATION IF NOT EXISTS LAB_GITHUB_PUBLIC API_PROVIDER = GIT_HTTPS_API
            API_ALLOWED_PREFIXES = ('{prefix}') ENABLED = TRUE""")
    sql(f"ALTER API INTEGRATION LAB_GITHUB_PUBLIC SET API_ALLOWED_PREFIXES = ('{prefix}')")
    sql(f"CREATE OR REPLACE GIT REPOSITORY SHOP.OPS.PUBLIC_REPO API_INTEGRATION = LAB_GITHUB_PUBLIC ORIGIN = '{a.git_repo}'")
    branches = [r["name"] for r in rows("SHOW GIT BRANCHES IN SHOP.OPS.PUBLIC_REPO")]
    sql("CREATE OR REPLACE STAGE SHOP.OPS.GIT_COPY")
    sql(f"COPY FILES INTO @SHOP.OPS.GIT_COPY FROM @SHOP.OPS.PUBLIC_REPO/branches/{branches[0]}/")
    return {"branches": branches, "files_copied": len(sql("LIST @SHOP.OPS.GIT_COPY"))}
rep["git"] = attempt(git)

# %% [markdown]
# ## What did this cost? (account usage lags by up to a few hours)

# %%
rep["credits_by_service_7d"] = {r["SERVICE_TYPE"]: float(r["CREDITS"]) for r in rows(
    """SELECT service_type, round(sum(credits_used), 4) AS credits FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
       WHERE start_time > DATEADD(day, -7, CURRENT_TIMESTAMP()) GROUP BY 1""")}
rep["free_balance_usd"] = attempt(lambda: float(scalar(
    "SELECT free_usage_balance FROM SNOWFLAKE.ORGANIZATION_USAGE.REMAINING_BALANCE_DAILY ORDER BY date DESC LIMIT 1")))
rm = rows("SHOW RESOURCE MONITORS LIKE 'LAB_RM'")
rep["resource_monitor"] = {k: rm[0][k] for k in ("credit_quota", "used_credits", "remaining_credits")} if rm else None

# %% [markdown]
# ## Grading

# %%
results = grade.checks(session, a.files, a.malformed_at or None)
for r in results:
    print(f"[{'PASS' if r.ok else 'FAIL'}] S{r.session} {r.name}: expected={r.expected} actual={r.actual}")
rep["grade"] = grade.as_dicts(results)
rep["score"] = f"{sum(r.ok for r in results)}/{len(results)}"
rep.save()
