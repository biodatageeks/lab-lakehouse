"""Session 6 primary path: Polars and PyIceberg on a laptop.

  * ``pl.Catalog`` (Unity Catalog API) -> ``scan_table``
  * PyIceberg REST catalog -> ``pl.scan_iceberg`` and snapshot time travel

    uv run python -m external.polars_iceberg gold.dim_customers
"""

from __future__ import annotations

import sys
import time

import polars as pl
from pyiceberg.catalog import load_catalog

from external._auth import CATALOG, conn, iceberg_uri, redact


def iceberg_catalog():
    return load_catalog(
        "uc",
        type="rest",
        uri=iceberg_uri(),
        warehouse=CATALOG,
        token=conn().token,
        **{
            "header.X-Iceberg-Access-Delegation": "vended-credentials",
            # UC's /config defaults to "refs" (current snapshot only), which hides history
            # and breaks time travel; ask for every snapshot.
            "snapshot-loading-mode": "all",
        },
    )


def probe_polars_unity(table: str) -> dict:
    out: dict = {"engine": "polars/pl.Catalog", "table": table, "metadata": False, "data": False, "error": None}
    try:
        c = conn()
        cat = pl.Catalog(c.host, bearer_token=c.token)
        schema, name = table.split(".")
        out["metadata"] = name in {t.name for t in cat.list_tables(CATALOG, schema)}
        # Polars swallows a credential-vending refusal and falls back to the local AWS
        # chain, which hides the real reason; ask for the credentials explicitly first.
        info = cat.get_table_info(CATALOG, schema, name)
        cat._get_table_credentials(info.table_id, write=False)
        t0 = time.perf_counter()
        out["rows"] = cat.scan_table(CATALOG, schema, name).select(pl.len()).collect().item()
        out["seconds"] = round(time.perf_counter() - t0, 3)
        out["data"] = True
    except Exception as e:  # noqa: BLE001
        out["error"] = redact(str(e))[:600]
    return out


def probe_pyiceberg(table: str) -> dict:
    out: dict = {"engine": "polars/scan_iceberg+pyiceberg", "table": table, "metadata": False, "data": False,
                 "snapshots": None, "error": None}
    try:
        cat = iceberg_catalog()
        schema, name = table.split(".")
        out["metadata"] = (schema, name) in set(cat.list_tables(schema))
        t = cat.load_table(table)
        snaps = t.metadata.snapshots
        out["snapshots"] = len(snaps)
        cur = t.current_snapshot()
        out["parent_snapshot_id"] = cur.parent_snapshot_id if cur else None
        out["schema"] = [f.name for f in t.schema().fields]
        t0 = time.perf_counter()
        out["rows"] = pl.scan_iceberg(t).select(pl.len()).collect().item()
        out["seconds"] = round(time.perf_counter() - t0, 3)
        out["data"] = True
        parent = t.current_snapshot().parent_snapshot_id if t.current_snapshot() else None
        if parent:  # time travel: the snapshot before the latest commit
            out["rows_parent_snapshot"] = pl.scan_iceberg(t, snapshot_id=parent).select(pl.len()).collect().item()
    except Exception as e:  # noqa: BLE001
        out["error"] = redact(str(e))[:600]
    return out


if __name__ == "__main__":
    tbl = sys.argv[1] if len(sys.argv) > 1 else "gold.dim_customers"
    print(probe_polars_unity(tbl))
    print(probe_pyiceberg(tbl))
