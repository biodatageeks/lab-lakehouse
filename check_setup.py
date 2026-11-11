"""Student homework check: prints the workspace URL, the warehouse ID and the catalog list.

    uv run python check_setup.py
"""

from __future__ import annotations

import sys

from lab.common import conn, scalar, ws


def main() -> int:
    w = ws()
    c = conn()
    me = w.current_user.me().user_name
    catalogs = sorted(cat.name for cat in w.catalogs.list())
    print(f"workspace : {c.host}")
    print(f"user      : {me}")
    print(f"warehouse : {c.warehouse_id}  ({w.warehouses.get(c.warehouse_id).name})")
    print(f"catalogs  : {', '.join(catalogs)}")
    print(f"SELECT 1  : {scalar('SELECT 1')}")
    print(f"external data access on metastore: {w.metastores.summary().external_access_enabled}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
