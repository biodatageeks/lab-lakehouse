"""Session 6 laptop path: read course tables over open Delta Sharing into Polars / DuckDB.

Works on Free Edition default storage, with the "External data access" toggle OFF.
Create the share and credential once with ``uv run python -m prep.delta_sharing setup``.

    uv run python -m external.delta_sharing_read                    # list tables + read fct_orders
    uv run python -m external.delta_sharing_read gold.orders_uniform --version 0

Not shareable: managed Iceberg tables (``typeWidening`` table feature). Share a Delta or
UniForm table instead.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import delta_sharing
import duckdb
import polars as pl

from prep.delta_sharing import SHARE

DEFAULT_PROFILE = Path.home() / ".lab" / "shop.share"


def tables(profile: Path = DEFAULT_PROFILE) -> list[str]:
    return [f"{t.schema}.{t.name}" for t in delta_sharing.SharingClient(str(profile)).list_all_tables()]


def read(table: str, profile: Path = DEFAULT_PROFILE, version: int | None = None) -> pl.DataFrame:
    """``table`` = 'schema.name' inside the course share. ``version`` = Delta time travel."""
    url = f"{profile}#{SHARE}.{table}"
    # use_delta_format=True asks the server for Delta log + files (fast, needs `deltalake`);
    # the default parquet-response path re-materializes MVs and can take ~1 min.
    return pl.from_pandas(delta_sharing.load_as_pandas(url, version=version, use_delta_format=True))


def demo(table: str = "gold.fct_orders", profile: Path = DEFAULT_PROFILE, version: int | None = None) -> dict:
    t0 = time.perf_counter()
    df = read(table, profile, version)
    load_s = round(time.perf_counter() - t0, 3)
    duck = duckdb.sql("SELECT count(*), round(sum(amount), 2) FROM df").fetchall()[0] if "amount" in df.columns else None
    return {"engine": "delta-sharing->polars/duckdb", "table": table, "version": version, "rows": df.height,
            "load_s": load_s, "duckdb_count": duck[0] if duck else None, "duckdb_revenue": duck[1] if duck else None}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("table", nargs="?", default="gold.fct_orders")
    ap.add_argument("--profile", type=Path, default=DEFAULT_PROFILE)
    ap.add_argument("--version", type=int, default=None)
    a = ap.parse_args()
    print("shared tables:", tables(a.profile))
    print(demo(a.table, a.profile, a.version))
