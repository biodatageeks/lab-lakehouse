"""Deploy the course to the account and run notebooks headlessly (EXECUTE NOTEBOOK PROJECT).

    uv run python -m lab.runner deploy
    uv run python -m lab.runner run 02_ingestion --start 1 --count 10

Each notebook writes its findings to SHOP.OPS.NB_RESULTS under the --run-id it was given;
``run_notebook`` returns them as a dict.
"""

from __future__ import annotations

import json
import sys
import time
import uuid

from lab import build
from lab.sf import COURSE_STAGE, NOTEBOOK_PROJECT, ROOT, WH, connect, sql

COMPUTE_POOL = "SYSTEM_COMPUTE_POOL_CPU"
RUNTIME = "V2.9-CPU-PY3.12"
PYPI = "snowflake.snowpark.pypi_shared_repository"  # Snowflake's PyPI mirror: no external access needed


class NotebookFailed(RuntimeError):
    pass


def _put(local, stage_dir: str) -> None:
    sql(f"PUT 'file://{local}' '{stage_dir}' AUTO_COMPRESS = FALSE OVERWRITE = TRUE")


def upload() -> None:
    """Put notebooks/ and dbt_shop/ into @SHOP.OPS.COURSE (what a student does with + Files)."""
    build.build()
    sql(f"REMOVE {COURSE_STAGE}")
    nb = ROOT / "notebooks"
    for f in sorted([*nb.glob("*.ipynb"), *nb.glob("*.py"), nb / "requirements.txt"]):
        _put(f, f"{COURSE_STAGE}/notebooks/")
    dbt = ROOT / "dbt_shop"
    for f in sorted(dbt.rglob("*")):
        if f.is_file() and not {"target", "logs"} & set(f.relative_to(dbt).parts):
            _put(f, f"{COURSE_STAGE}/dbt_shop/{f.parent.relative_to(dbt).as_posix()}/".replace("/./", "/"))


def deploy() -> dict:
    """Upload the course and create the notebook project with the same procedure students use."""
    upload()
    return json.loads(sql("CALL SHOP.OPS.LOAD_COURSE('')")[0][0])


def execute_sql(main_file: str, arguments: str) -> str:
    return (
        f"EXECUTE NOTEBOOK PROJECT {NOTEBOOK_PROJECT} MAIN_FILE = '{main_file}' "
        f"COMPUTE_POOL = '{COMPUTE_POOL}' QUERY_WAREHOUSE = '{WH}' RUNTIME = '{RUNTIME}' "
        f"ARGUMENTS = '{arguments}' REQUIREMENTS_FILE = 'requirements.txt' ARTIFACT_REPOSITORIES = ({PYPI})"
    )


def run_notebook(name: str, **args) -> dict:
    """Run notebooks/<name>.ipynb in a fresh container; return what it reported."""
    run_id = uuid.uuid4().hex[:12]
    argv = " ".join([f"--run-id {run_id}", *(f"--{k.replace('_', '-')} {v}" for k, v in args.items())])
    t0 = time.perf_counter()
    with connect() as c:  # own connection: notebooks may run in parallel threads
        try:
            c.cursor().execute(execute_sql(f"{name}.ipynb", argv))
        except Exception as e:  # noqa: BLE001
            raise NotebookFailed(" ".join(str(e).split())[:1500]) from e
        cur = c.cursor()
        cur.execute("SELECT payload FROM SHOP.OPS.NB_RESULTS WHERE run_id = %s ORDER BY ts", (run_id,))
        out: dict = {}
        for (payload,) in cur.fetchall():
            out.update(json.loads(payload))
    out["_elapsed_s"] = round(time.perf_counter() - t0, 1)
    out["_run_id"] = run_id
    return out


if __name__ == "__main__":
    if sys.argv[1] == "deploy":
        deploy()
        print("deployed", NOTEBOOK_PROJECT)
    elif sys.argv[1] == "run":
        kv = dict(zip(sys.argv[3::2], sys.argv[4::2]))
        print(json.dumps(run_notebook(sys.argv[2], **{k.lstrip("-").replace("-", "_"): v for k, v in kv.items()}),
                         indent=1, default=str))
