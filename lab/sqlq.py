"""Run SQL on the workspace's SQL warehouse from the laptop (trainer use only)."""
from functools import lru_cache
from lab.dbx import ws


@lru_cache
def warehouse_id() -> str:
    return list(ws().warehouses.list())[0].id


def q(sql: str) -> list[list]:
    r = ws().statement_execution.execute_statement(warehouse_id=warehouse_id(), statement=sql, wait_timeout="50s")
    if r.status.error:
        raise RuntimeError(r.status.error.message)
    return r.result.data_array or [] if r.result else []
