"""Session 6 fallback: SQL warehouse -> Arrow -> Polars / DuckDB on the laptop.

The governed query runs on the Databricks SQL warehouse; only the result set travels
to the laptop as Arrow, where Polars and DuckDB take over.

    uv run python -m external.sql_connector_arrow gold.fct_orders
"""

from __future__ import annotations

import sys
import time

import duckdb
import polars as pl

from lab.common import CATALOG, sql_arrow


def fetch(table: str) -> tuple[pl.DataFrame, float]:
    t0 = time.perf_counter()
    arrow = sql_arrow(f"SELECT * FROM {CATALOG}.{table}")
    return pl.from_arrow(arrow), round(time.perf_counter() - t0, 3)


def demo(table: str = "gold.fct_orders") -> dict:
    df, fetch_s = fetch(table)
    t0 = time.perf_counter()
    by_cat = (
        df.group_by("category").agg(pl.len().alias("orders"), pl.col("amount").sum().round(2).alias("revenue"))
        .sort("revenue", descending=True)
    ) if "category" in df.columns else df.head()
    polars_s = round(time.perf_counter() - t0, 3)
    t0 = time.perf_counter()
    duck = duckdb.sql("SELECT count(*) AS n, round(sum(amount), 2) AS revenue FROM df").fetchall()[0]
    duck_s = round(time.perf_counter() - t0, 3)
    return {
        "engine": "sql-connector->arrow", "table": table, "rows": df.height, "fetch_s": fetch_s,
        "polars_s": polars_s, "duckdb_s": duck_s, "duckdb_revenue": duck[1], "top_category": by_cat.row(0) if by_cat.height else None,
    }


if __name__ == "__main__":
    print(demo(sys.argv[1] if len(sys.argv) > 1 else "gold.fct_orders"))
