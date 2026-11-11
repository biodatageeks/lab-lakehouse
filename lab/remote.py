"""Deploy repo files to the workspace and run notebooks on serverless jobs compute."""

from __future__ import annotations

import base64
import json
from datetime import timedelta
from functools import lru_cache
from pathlib import Path

from databricks.sdk.service import jobs, workspace

from lab.common import ROOT, ws


@lru_cache
def workspace_root() -> str:
    me = ws().current_user.me().user_name
    return f"/Users/{me}/lab-lakehouse"


def deploy(rel: str) -> str:
    """Upload ``rel`` (repo-relative). .py notebooks keep their Databricks source header."""
    src = ROOT / rel
    w = ws()
    target = f"{workspace_root()}/{rel}"
    w.workspace.mkdirs(str(Path(target).parent))
    content = base64.b64encode(src.read_bytes()).decode()
    if src.suffix == ".py" and src.read_text().startswith("# Databricks notebook source"):
        target = target.removesuffix(".py")
        w.workspace.import_(target, content=content, format=workspace.ImportFormat.SOURCE,
                            language=workspace.Language.PYTHON, overwrite=True)
    else:
        w.workspace.import_(target, content=content, format=workspace.ImportFormat.AUTO, overwrite=True)
    return target


class NotebookFailed(RuntimeError):
    def __init__(self, msg: str, run: jobs.Run | None = None):
        super().__init__(msg)
        self.run = run


def run_notebook(rel: str, params: dict | None = None, timeout_min: int = 30) -> dict:
    """Run a repo notebook once on serverless; returns the parsed ``dbutils.notebook.exit`` JSON."""
    path = deploy(rel)
    w = ws()
    run = _submit(w, path, params, timeout_min)
    task_run = run.tasks[0]
    out = w.jobs.get_run_output(task_run.run_id)
    # The task state is authoritative: the parent run can read SUCCESS while the task FAILED.
    ts = task_run.state
    state = ts.result_state.value if ts and ts.result_state else "UNKNOWN"
    if state != "SUCCESS":
        raise NotebookFailed(f"{rel}: {state}: {out.error or (ts.state_message if ts else "")}\n{out.error_trace or ''}"[:4000], run)
    result = out.notebook_output.result if out.notebook_output else None
    return json.loads(result) if result else {}


def _submit(w, path: str, params: dict | None, timeout_min: int) -> jobs.Run:
    waiter = w.jobs.submit(
        run_name=f"lab-test {Path(path).name}",
        tasks=[
            jobs.SubmitTask(
                task_key="t",
                notebook_task=jobs.NotebookTask(
                    notebook_path=path,
                    base_parameters={k: str(v) for k, v in (params or {}).items()},
                    source=jobs.Source.WORKSPACE,
                ),
            )
        ],
    )
    try:
        return waiter.result(timeout=timedelta(minutes=timeout_min))
    except Exception:
        # submit_and_wait raises on FAILED runs; fetch the run so the caller sees the output.
        return w.jobs.get_run(waiter.run_id)
