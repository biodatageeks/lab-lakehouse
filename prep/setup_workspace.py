"""Idempotent workspace preparation (run once per Free Edition account).

    uv run python -m prep.setup_workspace

    uv run python -m prep.setup_workspace --external-access   # also open UC to outside engines

Does with the SDK what the course otherwise asks you to click:
  1. catalog `shop`, schemas bronze/silver/gold, managed Volume bronze.landing
  2. Starts the SQL warehouse so the first lab query isn't a cold start.
  3. Only with --external-access: Metastore -> "External data access" ON and
     EXTERNAL USE SCHEMA on the catalog. The course does NOT need this: Session 6 reads
     tables inside notebooks (Spark -> Arrow -> DuckDB/Polars), and outside engines cannot
     read Free Edition default storage anyway. Kept for re-testing the laptop path.
"""

from __future__ import annotations

import argparse
import sys

from databricks.sdk.errors import InvalidState, NotFound, ResourceAlreadyExists
from databricks.sdk.service import catalog as uc

from lab.common import CATALOG, SCHEMAS, VOLUME, conn, sql, ws


def enable_external_access(w) -> bool:
    summary = w.metastores.summary()
    if summary.external_access_enabled:
        print(f"[ok] external data access already enabled on {summary.name}")
        return True
    w.metastores.update(id=summary.metastore_id, external_access_enabled=True)
    enabled = bool(w.metastores.summary().external_access_enabled)
    print(f"[{'ok' if enabled else 'FAIL'}] external data access enabled on {summary.name}: {enabled}")
    return enabled


def ensure_objects(w) -> None:
    try:
        w.catalogs.get(CATALOG)
        print(f"[ok] catalog {CATALOG} exists")
    except NotFound:
        try:
            w.catalogs.create(name=CATALOG, comment="Lakehouse course catalog")
        except InvalidState:
            # Free Edition uses Default Storage: the REST API refuses a catalog without a
            # MANAGED LOCATION, but SQL CREATE CATALOG on the warehouse picks default storage.
            sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG} COMMENT 'Lakehouse course catalog'")
        print(f"[new] catalog {CATALOG}")
    for s in SCHEMAS:
        try:
            w.schemas.create(name=s, catalog_name=CATALOG)
            print(f"[new] schema {CATALOG}.{s}")
        except ResourceAlreadyExists:
            print(f"[ok] schema {CATALOG}.{s} exists")
        except Exception as e:  # some API versions return BadRequest for duplicates
            if "already exists" not in str(e).lower():
                raise
            print(f"[ok] schema {CATALOG}.{s} exists")
    try:
        w.volumes.read(f"{CATALOG}.bronze.{VOLUME}")
        print(f"[ok] volume {CATALOG}.bronze.{VOLUME} exists")
    except NotFound:
        w.volumes.create(
            catalog_name=CATALOG, schema_name="bronze", name=VOLUME, volume_type=uc.VolumeType.MANAGED
        )
        print(f"[new] volume {CATALOG}.bronze.{VOLUME}")


def grant_external_use(w, principal: str) -> None:
    # Free Edition schemas live on Default Storage (SCHEMA_DB_STORAGE) and reject a
    # schema-level EXTERNAL USE SCHEMA grant ("not applicable to this entity"). A
    # catalog-level grant is accepted and inherited by every schema in it.
    privs = [
        uc.Privilege.USE_CATALOG,
        uc.Privilege.USE_SCHEMA,
        uc.Privilege.SELECT,
        uc.Privilege.EXTERNAL_USE_SCHEMA,
    ]
    w.grants.update(
        securable_type="catalog",
        full_name=CATALOG,
        changes=[uc.PermissionsChange(principal=principal, add=privs)],
    )
    print(f"[ok] granted USE_CATALOG, USE_SCHEMA, SELECT, EXTERNAL_USE_SCHEMA on catalog {CATALOG} to {principal}")


def start_warehouse(w) -> None:
    wh = conn().warehouse_id
    state = w.warehouses.get(wh).state
    if str(state).endswith("RUNNING"):
        print(f"[ok] warehouse {wh} running")
        return
    w.warehouses.start(wh).result()
    print(f"[ok] warehouse {wh} started")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--external-access", action="store_true",
                    help="enable metastore External data access + grant EXTERNAL USE SCHEMA")
    a = ap.parse_args(argv)
    w = ws()
    me = w.current_user.me().user_name
    print(f"workspace {w.config.host} as {me}")
    ensure_objects(w)
    ok = True
    if a.external_access:
        ok = enable_external_access(w)
        grant_external_use(w, me)
    else:
        print("[skip] external data access (not needed; pass --external-access to enable)")
    start_warehouse(w)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
