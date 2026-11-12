"""Trainer harness: upload survey/ to the workspace and run its notebooks on serverless jobs compute.

Students never use this; they open the repo as a Databricks Git folder. The tests use it to run the
same notebooks headlessly and read what each one reports with ``dbutils.notebook.exit``.
"""

from __future__ import annotations

import base64
import json
import os
from datetime import timedelta
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")


@lru_cache
def ws():
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient(profile=os.environ.get("DATABRICKS_CONFIG_PROFILE"))


@lru_cache
def workspace_root() -> str:
    return f"/Users/{ws().current_user.me().user_name}/lab-lakehouse-test"


def deploy(folder: str = "survey") -> str:
    """Upload every file under ``folder``; Databricks-source .py files become notebooks."""
    from databricks.sdk.service import workspace

    w = ws()
    for src in sorted((ROOT / folder).rglob("*")):
        if src.is_dir() or "__pycache__" in src.parts:
            continue
        rel = src.relative_to(ROOT).as_posix()
        target = f"{workspace_root()}/{rel}"
        w.workspace.mkdirs(str(Path(target).parent))
        content = base64.b64encode(src.read_bytes()).decode()
        if src.suffix == ".py" and src.read_text().startswith("# Databricks notebook source"):
            w.workspace.import_(target.removesuffix(".py"), content=content, format=workspace.ImportFormat.SOURCE,
                                language=workspace.Language.PYTHON, overwrite=True)
        else:
            w.workspace.import_(target, content=content, format=workspace.ImportFormat.AUTO, overwrite=True)
    return f"{workspace_root()}/{folder}"


class NotebookFailed(RuntimeError):
    pass


def run_notebook(name: str, params: dict | None = None, timeout_min: int = 40, folder: str = "survey") -> dict:
    """Run ``<folder>/<name>`` once on serverless and return its exit JSON."""
    from databricks.sdk.service import jobs

    w = ws()
    path = f"{workspace_root()}/{folder}/{name}"
    waiter = w.jobs.submit(
        run_name=f"lab-test {name}",
        tasks=[jobs.SubmitTask(task_key="t", notebook_task=jobs.NotebookTask(
            notebook_path=path, base_parameters={k: str(v) for k, v in (params or {}).items()},
            source=jobs.Source.WORKSPACE))],
    )
    try:
        run = waiter.result(timeout=timedelta(minutes=timeout_min))
    except Exception:  # noqa: BLE001 - failed runs raise; fetch the run to show its output
        run = w.jobs.get_run(waiter.run_id)
    task = run.tasks[0]
    out = w.jobs.get_run_output(task.run_id)
    state = task.state.result_state.value if task.state and task.state.result_state else "UNKNOWN"
    if state != "SUCCESS":
        raise NotebookFailed(f"{name}: {state}: {out.error}\n{out.error_trace or ''}"[:6000])
    result = out.notebook_output.result if out.notebook_output else None
    return json.loads(result) if result else {}


if __name__ == "__main__":
    import sys

    deploy()
    for nb in sys.argv[1:]:
        print(nb, json.dumps(run_notebook(nb), indent=2, default=str))
