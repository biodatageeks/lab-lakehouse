"""Harness-side Snowflake access (the laptop that *tests* the course; students never need it)."""

from __future__ import annotations

import os
import warnings
from pathlib import Path

import snowflake.connector as sc
from dotenv import load_dotenv

warnings.filterwarnings("ignore", category=FutureWarning, module="snowflake")

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

CONNECTION = os.environ.get("SNOWFLAKE_CONNECTION_NAME", "default")
DB, WH = "SHOP", "LAB_WH"
COURSE_STAGE = "@SHOP.OPS.COURSE"
NOTEBOOK_PROJECT = "SHOP.OPS.COURSE_NB"

_conn: sc.SnowflakeConnection | None = None


def connect(**kw) -> sc.SnowflakeConnection:
    return sc.connect(connection_name=CONNECTION, session_parameters={"QUERY_TAG": "lab-harness"}, **kw)


def conn() -> sc.SnowflakeConnection:
    global _conn
    if _conn is None or _conn.is_closed():
        _conn = connect()
    return _conn


def sql(query: str, params=None) -> list[tuple]:
    cur = conn().cursor()
    cur.execute(query, params)
    return cur.fetchall() if cur.description else []


def dicts(query: str, params=None) -> list[dict]:
    cur = conn().cursor(sc.DictCursor)
    cur.execute(query, params)
    return cur.fetchall() if cur.description else []


def scalar(query: str, params=None):
    rows = sql(query, params)
    return rows[0][0] if rows else None


def run_script(path: Path) -> None:
    for cur in conn().execute_string(path.read_text()):
        cur.fetchall() if cur.description else None
