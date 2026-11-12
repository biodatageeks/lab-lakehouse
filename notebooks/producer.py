"""Seeded e-commerce event producer, run from a notebook.

Writes one orders and one clicks JSON file per tick into the landing stage with
``session.file.put_stream``. Deterministic: the same seed gives every student the same rows,
so counts match the answer key.

Data quirks the labs rely on:
  * from file 2 on, every orders file repeats the previous file's last order   (dedupe)
  * every 5th file has one order with a negative amount                        (expectation)
  * every 7th file has one order with a NULL customer_id                       (expectation)
  * from ``schema_change_at`` (default 30) orders carry ``coupon_code``        (schema evolution)
  * ``malformed_at=N`` puts amount="not-a-number" into file N                  (rejected at load)
"""

from __future__ import annotations

import io
import json
import random
import time
from datetime import datetime, timedelta, timezone

ORDERS_PER_FILE = 20
CLICKS_PER_FILE = 50
N_PRODUCTS = 50
N_CUSTOMERS = 200
START = datetime(2026, 1, 5, 8, 0, tzinfo=timezone.utc)
CATEGORIES = ["books", "games", "garden", "kitchen", "sports"]
LANDING = "@SHOP.BRONZE.LANDING"


def products(seed: int = 42) -> list[dict]:
    rnd = random.Random(seed)
    return [
        {"product_id": pid, "name": f"product-{pid:03d}", "category": CATEGORIES[pid % len(CATEGORIES)],
         "price": round(rnd.uniform(2, 120), 2)}
        for pid in range(1, N_PRODUCTS + 1)
    ]


def products_csv(seed: int = 42) -> str:
    lines = ["product_id,name,category,price"]
    lines += [f"{p['product_id']},{p['name']},{p['category']},{p['price']}" for p in products(seed)]
    return "\n".join(lines) + "\n"


def _order(rnd: random.Random, order_id: int, ts: datetime, prices: dict, coupon: bool) -> dict:
    pid = rnd.randint(1, N_PRODUCTS)
    qty = rnd.randint(1, 5)
    o = {
        "order_id": order_id,
        "customer_id": rnd.randint(1, N_CUSTOMERS),
        "product_id": pid,
        "quantity": qty,
        "amount": round(prices[pid] * qty, 2),
        "status": rnd.choice(["placed", "paid", "shipped"]),
        "order_ts": ts.isoformat(),
    }
    if coupon:
        o["coupon_code"] = rnd.choice([None, "SAVE10", "WELCOME"])
    return o


def _regular_orders(i: int, seed: int, schema_change_at: int):
    rnd = random.Random(seed * 100_003 + i)
    prices = {p["product_id"]: p["price"] for p in products(seed)}
    coupon = i >= schema_change_at
    base_ts = START + timedelta(minutes=10 * i)
    rows = [_order(rnd, (i - 1) * ORDERS_PER_FILE + k + 1, base_ts + timedelta(seconds=k), prices, coupon)
            for k in range(ORDERS_PER_FILE)]
    return rows, rnd, prices, coupon, base_ts


def orders_file(i: int, seed: int = 42, schema_change_at: int = 30, malformed_at: int | None = None) -> list[dict]:
    """Orders for file number ``i`` (1-based)."""
    rows, rnd, prices, coupon, base_ts = _regular_orders(i, seed, schema_change_at)
    if i >= 2:  # duplicate: previous file's last order, re-sent verbatim
        dup = dict(_regular_orders(i - 1, seed, schema_change_at)[0][-1])
        if coupon and "coupon_code" not in dup:
            dup["coupon_code"] = None
        rows.insert(0, dup)
    if i % 5 == 0:
        bad = _order(rnd, 1_000_000 + i, base_ts, prices, coupon)
        bad["amount"] = -abs(bad["amount"])
        rows.append(bad)
    if i % 7 == 0:
        bad = _order(rnd, 2_000_000 + i, base_ts, prices, coupon)
        bad["customer_id"] = None
        rows.append(bad)
    if malformed_at and i == malformed_at:
        bad = _order(rnd, 3_000_000 + i, base_ts, prices, coupon)
        bad["amount"] = "not-a-number"
        rows.append(bad)
    return rows


def clicks_file(i: int, seed: int = 42) -> list[dict]:
    rnd = random.Random(seed * 200_003 + i)
    base_ts = START + timedelta(minutes=10 * i)
    return [
        {"event_id": (i - 1) * CLICKS_PER_FILE + k + 1, "session_id": f"s{rnd.randint(1, 400):04d}",
         "customer_id": rnd.randint(1, N_CUSTOMERS), "product_id": rnd.randint(1, N_PRODUCTS),
         "event_type": rnd.choice(["view", "view", "view", "add_to_cart", "checkout"]),
         "event_ts": (base_ts + timedelta(seconds=k)).isoformat()}
        for k in range(CLICKS_PER_FILE)
    ]


def answer_key(n_files: int, seed: int = 42, schema_change_at: int = 30, malformed_at: int | None = None) -> dict:
    """Expected counts. Snowflake rejects the malformed row at COPY time (ON_ERROR = CONTINUE),
    so it never reaches bronze; the quality checks then see the remaining bad rows."""
    raw = [o for i in range(1, n_files + 1) for o in orders_file(i, seed, schema_change_at, malformed_at)]
    loaded = [o for o in raw if isinstance(o["amount"], (int, float))]
    kept = [o for o in loaded if o["amount"] > 0 and o["customer_id"] is not None]
    unique = {o["order_id"]: o for o in kept}
    hours = {datetime.fromisoformat(o["order_ts"]).replace(minute=0, second=0) for o in unique.values()}
    return {
        "files": n_files,
        "bronze_orders_rows": len(loaded),
        "rejected_rows": len(raw) - len(loaded),
        "bronze_clicks_rows": n_files * CLICKS_PER_FILE,
        "invalid_amount_rows": sum(1 for o in loaded if o["amount"] <= 0),
        "null_customer_rows": sum(1 for o in loaded if o["customer_id"] is None),
        "silver_orders_rows": len(unique),
        "silver_revenue_total": round(sum(o["amount"] for o in unique.values()), 2),
        "gold_revenue_hours": len(hours),
        "distinct_customers": len({o["customer_id"] for o in unique.values()}),
        "dim_products_rows": N_PRODUCTS,
        "coupon_rows": sum(1 for o in loaded if "coupon_code" in o),
    }


def _ndjson(rows: list[dict]) -> bytes:
    return ("\n".join(json.dumps(r) for r in rows) + "\n").encode()


def put(session, path: str, data: bytes) -> None:
    session.file.put_stream(io.BytesIO(data), f"{LANDING}/{path}", auto_compress=False, overwrite=True)


def upload_products(session, seed: int = 42) -> None:
    put(session, "products/products.csv", products_csv(seed).encode())


def produce(session, start: int = 1, count: int = 10, interval: float = 0, seed: int = 42,
            schema_change_at: int = 30, malformed_at: int | None = None) -> list[int]:
    """Upload files ``start .. start+count-1``; ``interval`` seconds apart (class mode: 60)."""
    done = []
    for i in range(start, start + count):
        put(session, f"orders/orders_{i:05d}.json", _ndjson(orders_file(i, seed, schema_change_at, malformed_at)))
        put(session, f"clicks/clicks_{i:05d}.json", _ndjson(clicks_file(i, seed)))
        done.append(i)
        print(f"{datetime.now():%H:%M:%S} file {i} uploaded")
        if interval and i < start + count - 1:
            time.sleep(interval)
    return done
