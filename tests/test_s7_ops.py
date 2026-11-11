"""Cross-cutting checks: scheduled jobs, task concurrency, quota visibility, Git folders, grading."""

import json
from datetime import timedelta

import pytest
from databricks.sdk.service import jobs

from grading import grade
from lab.common import sql, ws
from lab.remote import deploy, workspace_root
from tests.conftest import MALFORMED_AT, N_TOTAL


def _task(key: str, path: str, seconds: int) -> jobs.Task:
    return jobs.Task(task_key=key, notebook_task=jobs.NotebookTask(
        notebook_path=path, base_parameters={"seconds": str(seconds)}, source=jobs.Source.WORKSPACE))


def test_scheduled_job_every_few_minutes(record):
    w = ws()
    path = deploy("notebooks/probe_sleep.py")
    job = w.jobs.create(
        name="lab-test-scheduled",
        tasks=[_task("t", path, 1)],
        schedule=jobs.CronSchedule(quartz_cron_expression="0 0/5 * * * ?", timezone_id="UTC",
                                   pause_status=jobs.PauseStatus.PAUSED),
        max_concurrent_runs=1,
    )
    try:
        run = w.jobs.run_now(job.job_id).result(timeout=timedelta(minutes=20))
        record("ops.scheduled_job", {"created_with_cron": True, "run_state": run.state.result_state.value})
        assert run.state.result_state == jobs.RunResultState.SUCCESS
    finally:
        w.jobs.delete(job.job_id)


def test_parallel_task_limit(record):
    """Free Edition caps concurrent tasks (documented as 5). Six 60 s tasks in parallel."""
    w = ws()
    path = deploy("notebooks/probe_sleep.py")
    waiter = w.jobs.submit(run_name="lab-test-concurrency", tasks=[
        jobs.SubmitTask(task_key=f"t{i}", notebook_task=jobs.NotebookTask(
            notebook_path=path, base_parameters={"seconds": "60"}, source=jobs.Source.WORKSPACE))
        for i in range(6)])
    try:
        run = waiter.result(timeout=timedelta(minutes=30))
    except Exception:  # noqa: BLE001
        run = w.jobs.get_run(waiter.run_id)
    spans = []
    for t in run.tasks:
        out = w.jobs.get_run_output(t.run_id)
        if out.notebook_output and out.notebook_output.result:
            d = json.loads(out.notebook_output.result)
            spans.append((d["start"], d["end"]))
    max_overlap = max(sum(1 for s, e in spans if s <= t0 < e) for t0, _ in spans) if spans else 0
    record("ops.parallel_tasks", {"tasks": len(run.tasks), "succeeded": len(spans),
                                  "max_overlapping": max_overlap,
                                  "states": sorted({t.state.result_state.value for t in run.tasks if t.state.result_state})})
    assert len(spans) == 6


def test_quota_usage_visible(record):
    try:
        rows = sql("""
            SELECT usage_date, billing_origin_product, round(sum(usage_quantity), 2) AS dbus
            FROM system.billing.usage WHERE usage_date >= current_date() - INTERVAL 1 DAY
            GROUP BY ALL ORDER BY usage_date, dbus DESC
        """)
        record("ops.billing_usage_last_day", [tuple(map(str, r)) for r in rows])
    except Exception as e:  # noqa: BLE001
        record("ops.billing_usage_last_day", f"not accessible: {str(e).splitlines()[0][:200]}")
        pytest.xfail("system.billing.usage not accessible on this account")


def test_git_folder_from_public_repo(record):
    w = ws()
    path = f"{workspace_root()}-gitfolder-probe"
    repo = w.repos.create(url="https://github.com/octocat/Hello-World", provider="gitHub", path=path)
    try:
        files = [o.path for o in w.workspace.list(path)]
        record("ops.git_folder", {"branch": repo.branch, "files": files[:5]})
        assert files
    finally:
        w.repos.delete(repo.id)


def test_grading_script_full_marks(record):
    res = grade.checks(N_TOTAL, MALFORMED_AT)
    failed = [(r.session, r.name, r.expected, r.actual) for r in res if not r.ok]
    record("ops.grade", {"score": f"{sum(r.ok for r in res)}/{len(res)}", "failed": failed})
    assert not failed
