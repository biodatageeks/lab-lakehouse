"""Weekly lab checks against the seeded answer key, run inside a notebook (07_ops)."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from producer import answer_key


@dataclass
class Check:
    session: int
    name: str
    expected: object
    actual: object

    @property
    def ok(self) -> bool:
        if isinstance(self.expected, float) and isinstance(self.actual, (int, float)):
            return abs(self.expected - float(self.actual)) < 0.01
        return self.expected == self.actual


def checks(session, n_files: int, malformed_at: int | None = None) -> list[Check]:
    k = answer_key(n_files, malformed_at=malformed_at)

    def q(query: str):
        try:
            r = session.sql(query).collect()
            v = r[0][0] if r else None
            return float(v) if hasattr(v, "as_integer_ratio") and not isinstance(v, (int, float)) else v
        except Exception as e:  # noqa: BLE001
            return "ERROR: " + " ".join(str(e).split())[:120]

    def quality(metric: str):
        return q(f"""SELECT value FROM TABLE(SYSTEM$EVALUATE_DATA_QUALITY_EXPECTATIONS(
                     REF_ENTITY_NAME => 'SHOP.BRONZE.ORDERS_RAW')) WHERE metric_name = '{metric}'""")

    out = [
        Check(1, "schemas BRONZE/SILVER/GOLD exist", 3, q(
            "SELECT count(*) FROM SHOP.INFORMATION_SCHEMA.SCHEMATA WHERE schema_name IN ('BRONZE','SILVER','GOLD')")),
        Check(1, "stage BRONZE.LANDING exists", 1, q(
            "SELECT count(*) FROM SHOP.INFORMATION_SCHEMA.STAGES WHERE stage_schema='BRONZE' AND stage_name='LANDING'")),
        Check(1, "SILVER.DIM_PRODUCTS rows", k["dim_products_rows"], q("SELECT count(*) FROM SHOP.SILVER.DIM_PRODUCTS")),
        Check(2, "BRONZE.ORDERS_RAW rows", k["bronze_orders_rows"], q("SELECT count(*) FROM SHOP.BRONZE.ORDERS_RAW")),
        Check(2, "BRONZE.CLICKS_RAW rows", k["bronze_clicks_rows"], q("SELECT count(*) FROM SHOP.BRONZE.CLICKS_RAW")),
        Check(2, "ORDERS_RAW evolved a COUPON_CODE column", True, q(
            "SELECT count(*) = 1 FROM SHOP.INFORMATION_SCHEMA.COLUMNS WHERE table_schema='BRONZE' "
            "AND table_name='ORDERS_RAW' AND column_name='COUPON_CODE'")),
        Check(2, "rows rejected at load", k["rejected_rows"], q(
            "SELECT coalesce(sum(error_count), 0) FROM TABLE(SHOP.INFORMATION_SCHEMA.COPY_HISTORY("
            "TABLE_NAME => 'SHOP.BRONZE.ORDERS_RAW', START_TIME => DATEADD(day, -14, CURRENT_TIMESTAMP())))")),
        Check(3, "SILVER.ORDERS rows (deduped, filtered)", k["silver_orders_rows"], q("SELECT count(*) FROM SHOP.SILVER.ORDERS")),
        Check(3, "GOLD.REVENUE_HOURLY hours", k["gold_revenue_hours"], q("SELECT count(*) FROM SHOP.GOLD.REVENUE_HOURLY")),
        Check(3, "GOLD.REVENUE_HOURLY total revenue", k["silver_revenue_total"],
              q("SELECT round(sum(revenue), 2) FROM SHOP.GOLD.REVENUE_HOURLY")),
        Check(3, "DMF: invalid amounts in bronze", k["invalid_amount_rows"], quality("INVALID_AMOUNT_COUNT")),
        Check(3, "DMF: NULL customers in bronze", k["null_customer_rows"], quality("NULL_COUNT")),
        Check(5, "GOLD.FCT_ORDERS rows", k["silver_orders_rows"], q("SELECT count(*) FROM SHOP.GOLD.FCT_ORDERS")),
        Check(5, "GOLD.DIM_CUSTOMERS rows", k["distinct_customers"], q("SELECT count(*) FROM SHOP.GOLD.DIM_CUSTOMERS")),
        Check(5, "GOLD.DIM_CUSTOMERS is a managed Iceberg table", True, q(
            "SELECT count(*) = 1 FROM SHOP.INFORMATION_SCHEMA.TABLES WHERE table_schema='GOLD' "
            "AND table_name='DIM_CUSTOMERS' AND is_iceberg = 'YES'")),
    ]
    return out


def as_dicts(results: list[Check]) -> list[dict]:
    return [{**asdict(r), "ok": r.ok} for r in results]
