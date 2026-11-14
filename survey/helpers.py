"""Small helpers shared by the survey notebooks (kept out of the notebooks so the lesson stays readable)."""

from __future__ import annotations

import os
import shutil
import time
import urllib.request

CATALOG = "survey"
LANDING = f"/Volumes/{CATALOG}/raw/landing"
PIPELINE_NAME = "survey-pipeline"
JOB_NAME = "survey-on-file-arrival"
PIPELINE_FILES = ("bronze.py", "silver.py", "gold.sql")

# The archive is stored with Git LFS: raw.githubusercontent.com serves a 130-byte pointer,
# media.githubusercontent.com serves the real file.
MEDIA = "https://media.githubusercontent.com/media/StackExchange/Survey/refs/heads/main/packages/archive"
RAW = "https://raw.githubusercontent.com/StackExchange/Survey/main/packages/archive"


def source_url(year: int, media: bool = True) -> str:
    return f"{MEDIA if media else RAW}/{year}/results.csv"


def http_head(url: str, nbytes: int = 4096) -> tuple[int, bytes]:
    """(total size in bytes, first ``nbytes`` bytes) without downloading the whole file."""
    req = urllib.request.Request(url, headers={"Range": f"bytes=0-{nbytes - 1}"})
    with urllib.request.urlopen(req, timeout=60) as r:
        body = r.read()
        rng = r.headers.get("Content-Range")  # "bytes 0-4095/158626799"
    return (int(rng.rsplit("/", 1)[1]) if rng else len(body)), body


def header_columns(year: int) -> list[str]:
    import csv

    _, body = http_head(source_url(year), 65536)
    first = body.decode("utf-8", "replace").splitlines()[0]
    return next(csv.reader([first]))


def land_year(year: int, landing: str = LANDING, tmp: str = "/tmp/survey") -> dict:
    """Download one survey year to ``<landing>/year=YYYY/results.csv`` (skipped if already there).

    The file is downloaded to local disk first and then copied into the volume in one go,
    so Auto Loader and the file arrival trigger never see a half-written file.
    """
    target = f"{landing}/year={year}"
    if os.path.exists(f"{target}/results.csv"):
        return {"year": year, "path": f"{target}/results.csv", "already_landed": True}
    os.makedirs(tmp, exist_ok=True)
    local = f"{tmp}/{year}.csv"
    t0 = time.time()
    with urllib.request.urlopen(source_url(year), timeout=600) as r, open(local, "wb") as f:
        shutil.copyfileobj(r, f, 8 << 20)
    size = os.path.getsize(local)
    os.makedirs(target, exist_ok=True)
    shutil.copyfile(local, f"{target}/results.csv")
    os.remove(local)
    return {"year": year, "path": f"{target}/results.csv", "mb": round(size / 1e6, 1), "seconds": round(time.time() - t0, 1)}


def notebook_folder() -> str:
    """Workspace folder of the running notebook, e.g. /Users/me/lab-lakehouse/survey."""
    return os.getcwd().removeprefix("/Workspace")


def _find(w, path: str, key: str, name: str, params: dict | None = None):
    res = w.api_client.do("GET", path, query=params or {})
    return [x for x in res.get(key, []) if (x.get("name") or x.get("settings", {}).get("name")) == name]


def ensure_pipeline(w, folder: str) -> str:
    """Create (or update) the serverless, triggered SDP pipeline from the files in ``<folder>/pipeline``.

    Plain REST calls: the SDK preinstalled in notebooks can be older than these settings.
    """
    spec = {
        "name": PIPELINE_NAME,
        "catalog": CATALOG,
        "schema": "bronze",
        "serverless": True,
        "continuous": False,
        "development": False,
        "channel": "CURRENT",
        "configuration": {"survey.landing": LANDING},
        "root_path": f"{folder}/pipeline",
        "libraries": [{"file": {"path": f"{folder}/pipeline/{f}"}} for f in PIPELINE_FILES],
        "event_log": {"catalog": CATALOG, "schema": "lab", "name": "pipeline_events"},
    }
    found = _find(w, "/api/2.0/pipelines", "statuses", PIPELINE_NAME, {"filter": f"name LIKE '{PIPELINE_NAME}'"})
    if found:
        pid = found[0]["pipeline_id"]
        w.api_client.do("PUT", f"/api/2.0/pipelines/{pid}", body={"id": pid, **spec})
        return pid
    return w.api_client.do("POST", "/api/2.0/pipelines", body=spec)["pipeline_id"]


def ensure_job(w, pipeline_id: str) -> int:
    """A job that refreshes the pipeline whenever a new file lands anywhere under LANDING."""
    settings = {
        "name": JOB_NAME,
        "max_concurrent_runs": 1,
        "queue": {"enabled": True},
        "trigger": {
            "pause_status": "UNPAUSED",
            "file_arrival": {"url": LANDING + "/", "min_time_between_triggers_seconds": 60,
                             "wait_after_last_change_seconds": 60},
        },
        "tasks": [{"task_key": "refresh_pipeline", "pipeline_task": {"pipeline_id": pipeline_id}}],
    }
    found = _find(w, "/api/2.2/jobs/list", "jobs", JOB_NAME, {"name": JOB_NAME})
    if found:
        w.api_client.do("POST", "/api/2.2/jobs/reset", body={"job_id": found[0]["job_id"], "new_settings": settings})
        return found[0]["job_id"]
    return w.api_client.do("POST", "/api/2.2/jobs/create", body=settings)["job_id"]


def run_job_now(w, job_id: int) -> int:
    return w.api_client.do("POST", "/api/2.2/jobs/run-now", body={"job_id": job_id})["run_id"]


def wait_for_triggered_run(w, job_id: int, since_ms: int, timeout_s: int = 1500, poll_s: int = 15) -> dict:
    """Wait until the first run of ``job_id`` started after ``since_ms`` finishes; returns a short summary."""
    t0 = time.time()
    run = None
    while time.time() - t0 < timeout_s:
        runs = w.api_client.do("GET", "/api/2.2/jobs/runs/list",
                               query={"job_id": job_id, "start_time_from": since_ms}).get("runs", [])
        if runs:
            first = min(runs, key=lambda r: r["start_time"])
            run = w.api_client.do("GET", "/api/2.2/jobs/runs/get", query={"run_id": first["run_id"]})
            lcs = run["state"].get("life_cycle_state")
            if lcs in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                break
            print(f"{round(time.time() - t0)} s: run {run['run_id']} {lcs} (trigger {run.get('trigger')})")
        else:
            print(f"{round(time.time() - t0)} s: waiting for the file arrival trigger ...")
        time.sleep(poll_s)
    if run is None:
        return {"run_id": None, "waited_s": round(time.time() - t0)}
    st = run["state"]
    return {
        "run_id": run["run_id"],
        "url": run.get("run_page_url"),
        "trigger": run.get("trigger"),
        "result": st.get("result_state"),
        "message": st.get("state_message"),
        "started_after_s": round((run["start_time"] - since_ms) / 1000),
        "run_s": round((run["end_time"] - run["start_time"]) / 1000) if run.get("end_time") else None,
        "waited_s": round(time.time() - t0),
    }


def now_ms() -> int:
    return int(time.time() * 1000)
