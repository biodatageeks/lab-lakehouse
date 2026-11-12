"""Helpers shared by the course notebooks. Lives next to the notebooks, so ``import lab_nb`` works.

Notebooks run on Container Runtime (Workspaces notebooks). Interactive or headless
(``EXECUTE NOTEBOOK PROJECT ... ARGUMENTS = '--run-id x --count 10'``), the same code runs.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from snowflake.snowpark.context import get_active_session

DB, WH = "SHOP", "LAB_WH"
LANDING = "@SHOP.BRONZE.LANDING"

session = get_active_session()
session.sql(f"USE WAREHOUSE {WH}").collect()
session.sql(f"USE DATABASE {DB}").collect()


def args(**defaults):
    """Parse ``--key value`` notebook arguments; defaults apply when run interactively."""
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--run-id", default="interactive")
    for k, v in defaults.items():
        p.add_argument("--" + k.replace("_", "-"), type=type(v) if v is not None else str, default=v)
    return p.parse_known_args(sys.argv[1:])[0]


def sql(query: str, params=None):
    return session.sql(query, params=params).collect()


def rows(query: str, params=None) -> list[dict]:
    return [r.as_dict() for r in sql(query, params)]


def scalar(query: str, params=None):
    r = sql(query, params)
    return r[0][0] if r else None


def show(query: str, n: int = 20) -> None:
    session.sql(query).show(n)


def err(e: Exception) -> str:
    return f"ERR {type(e).__name__}: " + " ".join(str(e).split())[:300]


def attempt(fn):
    """Run ``fn``; return its result, or the error as a string (for 'is this possible?' probes)."""
    try:
        return fn()
    except Exception as e:  # noqa: BLE001
        return err(e)


class Report(dict):
    """Collects what a notebook found; ``save()`` writes it to SHOP.OPS.NB_RESULTS."""

    def __init__(self, notebook: str, run_id: str):
        super().__init__()
        self.notebook, self.run_id = notebook, run_id

    def save(self) -> None:
        payload = json.dumps(self, default=str)
        sql("INSERT INTO SHOP.OPS.NB_RESULTS (run_id, notebook, payload) SELECT ?, ?, PARSE_JSON(?)",
            [self.run_id, self.notebook, payload])
        print(json.dumps(self, indent=1, default=str))


def horizon_catalog(database: str = DB):
    """PyIceberg client for Snowflake's Horizon Iceberg REST catalog, authenticated with the
    notebook container's own session token (no secrets, no external access integration)."""
    from pyiceberg.catalog import load_catalog

    host = os.environ["SNOWFLAKE_HOST"]
    with open("/snowflake/session/token") as f:
        token = f.read().strip()
    return load_catalog(
        "horizon", type="rest", uri=f"https://{host}/polaris/api/catalog", warehouse=database, token=token,
        **{"header.X-Iceberg-Access-Delegation": "vended-credentials"},
    )
