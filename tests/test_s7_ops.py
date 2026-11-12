"""Session 7: scheduled task, notebook-as-job, Git, cost visibility, grading, concurrency."""

import threading

import pytest
from conftest import MALFORMED_AT, N_TOTAL

from lab import runner


@pytest.fixture(scope="module")
def out(nb):
    return nb("07_ops", files=N_TOTAL, malformed_at=MALFORMED_AT)


def test_scheduled_task_every_minute(out, record):
    record("s7.heartbeat_wait_s", out["heartbeat_wait_s"])
    assert out["heartbeats"] >= 2


def test_task_runs_a_notebook(out, record):
    record("s7.notebook_task", out["notebook_task"])
    assert out["notebook_task"]["state"]["STATE"] == "SUCCEEDED"
    assert out["notebook_task"]["child_reported"] == 1


def test_git_repository_to_stage(out, record):
    record("s7.git", out["git"])
    assert isinstance(out["git"], dict) and out["git"]["files_copied"] > 0


def test_cost_visible(out, record):
    record("s7.credits_by_service_7d", out["credits_by_service_7d"])
    record("s7.free_balance_usd", out["free_balance_usd"])
    record("s7.resource_monitor", out["resource_monitor"])
    assert out["credits_by_service_7d"]
    assert out["resource_monitor"]


def test_grading_full_marks(out, record):
    record("s7.score", out["score"])
    failed = [g for g in out["grade"] if not g["ok"]]
    assert not failed, failed


def test_concurrent_notebook_runs(course, record):
    """Two headless notebooks at once on the default compute pool: both finish, the second queues."""
    res = {}

    def run(i):
        try:
            res[i] = runner.run_notebook("heartbeat")["_elapsed_s"]
        except Exception as e:  # noqa: BLE001
            res[i] = f"ERR {e}"[:200]

    threads = [threading.Thread(target=run, args=(i,)) for i in range(2)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    record("s7.concurrent_notebook_elapsed_s", sorted(res.values(), key=str))
    assert all(isinstance(v, float) for v in res.values()), res
