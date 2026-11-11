"""Reset the course state in the `shop` catalog (instructor / test use).

    uv run python -m prep.reset_lab            # asks for confirmation
    uv run python -m prep.reset_lab --yes

Deletes lab pipelines (names starting with ``lab-``), drops every table/view in
shop.bronze/silver/gold and empties the landing Volume. Catalog, schemas, Volume and
grants stay, so prep.setup_workspace does not need to run again.
"""

from __future__ import annotations

import argparse
import sys

from lab.common import CATALOG, LANDING, SCHEMAS, sql, ws


def rm_tree(w, path: str) -> int:
    n = 0
    try:
        entries = list(w.files.list_directory_contents(path))
    except Exception:  # noqa: BLE001 - path missing
        return 0
    for e in entries:
        if e.is_directory:
            n += rm_tree(w, e.path)
            w.files.delete_directory(e.path)
        else:
            w.files.delete(e.path)
            n += 1
    return n


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--yes", action="store_true")
    a = ap.parse_args(argv)
    if not a.yes and input(f"Drop all tables in {CATALOG}.{{bronze,silver,gold}} and empty {LANDING}? [y/N] ") != "y":
        return 1
    w = ws()
    for pl in w.pipelines.list_pipelines(filter="name LIKE 'lab-%'"):
        w.pipelines.delete(pl.pipeline_id)
        print(f"deleted pipeline {pl.name}")
    for s in SCHEMAS:
        for t in w.tables.list(catalog_name=CATALOG, schema_name=s):
            kind = str(t.table_type)
            stmt = "DROP VIEW" if kind.endswith("VIEW") and "MATERIALIZED" not in kind else (
                "DROP MATERIALIZED VIEW" if "MATERIALIZED" in kind else "DROP TABLE")
            try:
                sql(f"{stmt} IF EXISTS {t.full_name}")
            except Exception:  # noqa: BLE001 - pipeline-owned leftovers
                sql(f"DROP TABLE IF EXISTS {t.full_name}")
            print(f"dropped {t.full_name}")
    print(f"deleted {rm_tree(w, LANDING)} files from {LANDING}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
