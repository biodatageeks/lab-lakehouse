"""Session 6 primary path: DuckDB on a laptop reading Unity Catalog.

Two ways in:
  * ``unity_catalog`` extension (Delta tables, credentials from temporary-table-credentials)
  * ``iceberg`` extension against the UC Iceberg REST catalog (Iceberg / UniForm tables)

    uv run python -m external.duckdb_uc gold.fct_orders
"""

from __future__ import annotations

import sys
import time

import duckdb

from external._auth import CATALOG, conn, iceberg_uri, redact, region


def connect() -> duckdb.DuckDBPyConnection:
    c = conn()
    con = duckdb.connect()
    for ext in ("httpfs", "delta", "iceberg", "unity_catalog"):
        con.sql(f"INSTALL {ext}; LOAD {ext};")
    con.sql(
        f"CREATE OR REPLACE SECRET uc (TYPE unity_catalog, TOKEN '{c.token}', "
        f"ENDPOINT '{c.host}', AWS_REGION '{region()}')"
    )
    con.sql(f"ATTACH '{CATALOG}' AS uc (TYPE unity_catalog, SECRET uc, READ_ONLY)")
    con.sql(f"CREATE OR REPLACE SECRET ice (TYPE iceberg, TOKEN '{c.token}')")
    con.sql(
        f"ATTACH '{CATALOG}' AS ice (TYPE iceberg, ENDPOINT '{iceberg_uri()}', SECRET ice, READ_ONLY)"
    )
    return con


def probe(table: str, via: str = "uc") -> dict:
    """``table`` = 'schema.name'. Returns stage-by-stage outcome instead of raising."""
    out: dict = {"engine": f"duckdb/{via}", "table": table, "metadata": False, "data": False, "error": None}
    try:
        con = connect()
        schema, name = table.split(".")
        listed = con.sql(
            "SELECT table_name FROM duckdb_tables() WHERE database_name=? AND schema_name=?", params=[via, schema]
        ).fetchall()
        out["metadata"] = (name,) in listed
        t0 = time.perf_counter()
        out["rows"] = con.sql(f"SELECT count(*) FROM {via}.{schema}.{name}").fetchall()[0][0]
        out["seconds"] = round(time.perf_counter() - t0, 3)
        out["data"] = True
    except Exception as e:  # noqa: BLE001
        out["error"] = redact(str(e))[:600]
    return out


if __name__ == "__main__":
    tbl = sys.argv[1] if len(sys.argv) > 1 else "gold.fct_orders"
    for via in ("uc", "ice"):
        print(probe(tbl, via))
