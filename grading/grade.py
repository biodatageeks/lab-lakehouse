"""Weekly lab checks: compares a student's workspace against the seeded answer key.

    uv run python -m grading.grade --files 35 [--malformed-at 15] [--session 3]

Every check is a SQL query through the SQL connector, so grading needs only the
student's host + PAT (or a profile).
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass

from lab.common import CATALOG, sql
from producer.produce import answer_key


@dataclass
class Check:
    session: int
    name: str
    expected: object
    actual: object

    @property
    def ok(self) -> bool:
        if isinstance(self.expected, float) and isinstance(self.actual, (int, float)):
            return abs(self.expected - self.actual) < 0.01
        return self.expected == self.actual


def _q(query: str):
    try:
        rows = sql(query)
        return rows[0][0] if rows else None
    except Exception as e:  # noqa: BLE001
        return f"ERROR: {str(e).splitlines()[0][:120]}"


def _tblprop(fqn: str, key: str):
    try:
        return dict(sql(f"SHOW TBLPROPERTIES {fqn}")).get(key)
    except Exception as e:  # noqa: BLE001
        return f"ERROR: {str(e).splitlines()[0][:120]}"


def _exists(fqn: str) -> bool:
    cat, sch, tbl = fqn.split(".")
    return bool(sql(
        f"SELECT 1 FROM {cat}.information_schema.tables WHERE table_schema = :s AND table_name = :t",
        {"s": sch, "t": tbl},
    ))


def dropped_by_expectation() -> dict:
    """Rows dropped per expectation since the latest full refresh of the latest pipeline."""
    log = f"{CATALOG}.silver.pipeline_event_log"
    rows = sql(f"""
        WITH latest AS (
          SELECT origin.pipeline_id AS pid FROM {log} ORDER BY timestamp DESC LIMIT 1
        ),
        since AS (
          SELECT coalesce(max(timestamp), TIMESTAMP'1970-01-01') AS ts FROM {log}
          WHERE event_type = 'create_update' AND origin.pipeline_id = (SELECT pid FROM latest)
            AND details:create_update.full_refresh::boolean
        )
        SELECT e.name, SUM(e.failed_records) AS dropped
        FROM (
          SELECT explode(from_json(details:flow_progress.data_quality.expectations,
                 'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS e
          FROM {log}
          WHERE event_type = 'flow_progress' AND origin.pipeline_id = (SELECT pid FROM latest)
            AND timestamp >= (SELECT ts FROM since)
        )
        GROUP BY e.name
    """)
    return {r[0]: int(r[1]) for r in rows}


def checks(n_files: int, malformed_at: int | None = None, session: int | None = None) -> list[Check]:
    k = answer_key(n_files, malformed_at=malformed_at)
    c = CATALOG
    out: list[Check] = []

    def add(s, name, expected, actual_fn):
        if session is None or session == s:
            out.append(Check(s, name, expected, actual_fn()))

    add(1, "schemas bronze/silver/gold exist", 3, lambda: _q(
        f"SELECT count(*) FROM {c}.information_schema.schemata WHERE schema_name IN ('bronze','silver','gold')"))
    add(1, "volume bronze.landing exists", 1, lambda: _q(
        f"SELECT count(*) FROM {c}.information_schema.volumes WHERE volume_schema='bronze' AND volume_name='landing'"))
    add(1, "silver.dim_products rows", k["dim_products_rows"], lambda: _q(f"SELECT count(*) FROM {c}.silver.dim_products"))
    add(2, "bronze.orders_raw rows", k["bronze_orders_rows"], lambda: _q(f"SELECT count(*) FROM {c}.bronze.orders_raw"))
    add(2, "bronze.clicks_raw rows", k["bronze_clicks_rows"], lambda: _q(f"SELECT count(*) FROM {c}.bronze.clicks_raw"))
    add(2, "bronze.orders_raw has coupon_code (schema evolution)", k["coupon_rows"] > 0, lambda: _q(
        f"SELECT count(*) > 0 FROM {c}.information_schema.columns WHERE table_schema='bronze' "
        "AND table_name='orders_raw' AND column_name='coupon_code'"))
    add(3, "silver.orders rows (deduped, expectations applied)", k["silver_orders_rows"],
        lambda: _q(f"SELECT count(*) FROM {c}.silver.orders"))
    add(3, "gold.revenue_hourly hours", k["gold_revenue_hours"], lambda: _q(f"SELECT count(*) FROM {c}.gold.revenue_hourly"))
    add(3, "gold.revenue_hourly total revenue", k["silver_revenue_total"],
        lambda: _q(f"SELECT round(sum(revenue), 2) FROM {c}.gold.revenue_hourly"))
    add(3, "event log: rows dropped per expectation", k["expectation_dropped"], dropped_by_expectation)
    add(5, "gold.fct_orders rows", k["silver_orders_rows"], lambda: _q(f"SELECT count(*) FROM {c}.gold.fct_orders"))
    add(5, "gold.dim_customers rows", k["distinct_customers"], lambda: _q(f"SELECT count(*) FROM {c}.gold.dim_customers"))
    add(5, "gold.dim_customers has UniForm (Iceberg) enabled", "iceberg", lambda: _tblprop(
        f"{c}.gold.dim_customers", "delta.universalFormat.enabledFormats"))
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--files", type=int, required=True, help="number of producer files uploaded")
    ap.add_argument("--malformed-at", type=int, default=None)
    ap.add_argument("--session", type=int, default=None)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    res = checks(a.files, a.malformed_at, a.session)
    if a.json:
        print(json.dumps([{**asdict(r), "ok": r.ok} for r in res], indent=2, default=str))
    else:
        for r in res:
            print(f"[{'PASS' if r.ok else 'FAIL'}] S{r.session} {r.name}: expected={r.expected} actual={r.actual}")
        print(f"score: {sum(r.ok for r in res)}/{len(res)}")
    return 0 if all(r.ok for r in res) else 1


if __name__ == "__main__":
    sys.exit(main())
