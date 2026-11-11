"""Common configuration and clients.

Auth resolution (in order): DATABRICKS_HOST/DATABRICKS_TOKEN env vars (students put
them in .env), otherwise a ~/.databrickscfg profile named by DATABRICKS_CONFIG_PROFILE.
"""

from __future__ import annotations

import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

CATALOG = os.environ.get("LAB_CATALOG", "shop")
SCHEMAS = ("bronze", "silver", "gold")
VOLUME = "landing"
LANDING = f"/Volumes/{CATALOG}/bronze/{VOLUME}"


@lru_cache
def ws():
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


@dataclass(frozen=True)
class Conn:
    host: str  # https://...
    hostname: str  # no scheme
    token: str
    warehouse_id: str
    http_path: str


@lru_cache
def conn() -> Conn:
    w = ws()
    host = w.config.host.rstrip("/")
    token = w.config.token
    if not token:
        # OAuth profiles: mint a header token from the SDK auth chain.
        token = w.config.authenticate()["Authorization"].split(" ", 1)[1]
    wh_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if not wh_id:
        whs = list(w.warehouses.list())
        if not whs:
            raise RuntimeError("No SQL warehouse found in workspace")
        wh_id = whs[0].id
    return Conn(
        host=host,
        hostname=host.removeprefix("https://"),
        token=token,
        warehouse_id=wh_id,
        http_path=f"/sql/1.0/warehouses/{wh_id}",
    )


@lru_cache
def sql_connection():
    from databricks import sql

    c = conn()
    return sql.connect(server_hostname=c.hostname, http_path=c.http_path, access_token=c.token)


def sql(query: str, params: dict | None = None):
    """Run SQL on the warehouse and return a list of Row objects (empty for DDL)."""
    with sql_connection().cursor() as cur:
        cur.execute(query, params)
        try:
            return cur.fetchall()
        except Exception:
            return []


def sql_arrow(query: str):
    with sql_connection().cursor() as cur:
        cur.execute(query)
        return cur.fetchall_arrow()


def scalar(query: str):
    rows = sql(query)
    return rows[0][0] if rows else None


@contextmanager
def timer():
    t = {"start": time.perf_counter()}
    yield t
    t["seconds"] = round(time.perf_counter() - t["start"], 3)
