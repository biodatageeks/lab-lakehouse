"""Create / run Lakeflow Declarative Pipelines from repo SQL files."""

from __future__ import annotations

import time

from databricks.sdk.service import pipelines as p

from lab.common import CATALOG, LANDING, ws
from lab.remote import deploy

TERMINAL = {p.UpdateInfoState.COMPLETED, p.UpdateInfoState.FAILED, p.UpdateInfoState.CANCELED}


def find(name: str) -> str | None:
    for pl in ws().pipelines.list_pipelines(filter=f"name LIKE '{name}'"):
        if pl.name == name:
            return pl.pipeline_id
    return None


def ensure(name: str, sql_rel: str, schema: str = "silver", event_log_table: str | None = None) -> str:
    """Create or update a serverless, triggered pipeline whose only source is ``sql_rel``."""
    w = ws()
    path = deploy(sql_rel)
    spec = dict(
        name=name,
        catalog=CATALOG,
        schema=schema,
        serverless=True,
        continuous=False,
        development=True,
        channel="CURRENT",
        configuration={"landing": LANDING, "catalog": CATALOG},
        libraries=[p.PipelineLibrary(file=p.FileLibrary(path=path))],
        event_log=p.EventLogSpec(catalog=CATALOG, schema=schema, name=event_log_table) if event_log_table else None,
    )
    pid = find(name)
    if pid:
        w.pipelines.update(pipeline_id=pid, **spec)
    else:
        pid = w.pipelines.create(**spec).pipeline_id
    return pid


def start(pid: str, full_refresh: bool = False) -> str:
    return ws().pipelines.start_update(pid, full_refresh=full_refresh).update_id


def wait(pid: str, update_id: str, timeout_s: int = 1800) -> p.UpdateInfo:
    w = ws()
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        u = w.pipelines.get_update(pid, update_id).update
        if u.state in TERMINAL:
            return u
        time.sleep(10)
    raise TimeoutError(f"pipeline {pid} update {update_id} did not finish in {timeout_s}s")


def errors(pid: str, update_id: str, limit: int = 20) -> list[str]:
    out = []
    for ev in ws().pipelines.list_pipeline_events(pid, filter="level='ERROR'", max_results=limit):
        if ev.origin and ev.origin.update_id == update_id:
            out.append(ev.message or "")
        if len(out) >= limit:
            break
    return out
