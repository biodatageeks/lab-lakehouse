"""Run dbt for the dbt_shop project with credentials taken from the SDK auth chain."""

from __future__ import annotations

import os
import subprocess
import sys

from lab.common import CATALOG, ROOT, conn

PROJECT = ROOT / "dbt_shop"


def env() -> dict:
    c = conn()
    return {
        **os.environ,
        "DBT_DATABRICKS_HOST": c.hostname,
        "DBT_DATABRICKS_HTTP_PATH": c.http_path,
        "DBT_DATABRICKS_TOKEN": c.token,
        "LAB_CATALOG": CATALOG,
        "DBT_PROFILES_DIR": str(PROJECT),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }


def dbt(*args: str) -> subprocess.CompletedProcess:
    dbt_bin = os.path.join(os.path.dirname(sys.executable), "dbt")
    return subprocess.run(
        [dbt_bin, *args, "--project-dir", str(PROJECT)],
        cwd=PROJECT, env=env(), capture_output=True, text=True, timeout=1800,
    )


if __name__ == "__main__":
    r = dbt(*sys.argv[1:])
    print(r.stdout, r.stderr)
    sys.exit(r.returncode)
