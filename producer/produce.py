"""Seeded e-commerce event producer.

Writes one orders JSON file and one clicks JSON file per tick into the landing Volume
through the Files API (databricks-sdk ``files.upload``). Deterministic: the same
``--seed`` gives every student the same rows, so counts match the answer key.

    uv run python -m producer.produce --count 40 --interval 60      # class mode
    uv run python -m producer.produce --count 40 --interval 0       # burst (tests)
    uv run python -m producer.produce --products                    # products.csv only
    uv run python -m producer.produce --answer-key --count 40       # print expected counts

Data quirks the labs rely on:
  * from file 2 on, every orders file repeats the previous file's last order (dedupe)
  * every 5th file has one order with a negative amount            (expectation)
  * every 7th file has one order with a NULL customer_id           (expectation)
  * from ``--schema-change-at`` (default 30) orders carry ``coupon_code``  (schema evolution)
  * ``--malformed-at N`` puts amount="not-a-number" into file N    (_rescued_data homework)
"""

from __future__ import annotations

import argparse
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


def products(seed: int = 42) -> list[dict]:
    rnd = random.Random(seed)
    return [
        {
            "product_id": pid,
            "name": f"product-{pid:03d}",
            "category": CATEGORIES[pid % len(CATEGORIES)],
            "price": round(rnd.uniform(2, 120), 2),
        }
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
    rows = [
        _order(rnd, (i - 1) * ORDERS_PER_FILE + k + 1, base_ts + timedelta(seconds=k), prices, coupon)
        for k in range(ORDERS_PER_FILE)
    ]
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
    if malformed_at is not None and i == malformed_at:
        bad = _order(rnd, 3_000_000 + i, base_ts, prices, coupon)
        bad["amount"] = "not-a-number"
        rows.append(bad)
    return rows


def clicks_file(i: int, seed: int = 42) -> list[dict]:
    rnd = random.Random(seed * 200_003 + i)
    base_ts = START + timedelta(minutes=10 * i)
    return [
        {
            "event_id": (i - 1) * CLICKS_PER_FILE + k + 1,
            "session_id": f"s{rnd.randint(1, 400):04d}",
            "customer_id": rnd.randint(1, N_CUSTOMERS),
            "product_id": rnd.randint(1, N_PRODUCTS),
            "event_type": rnd.choice(["view", "view", "view", "add_to_cart", "checkout"]),
            "event_ts": (base_ts + timedelta(seconds=k)).isoformat(),
        }
        for k in range(CLICKS_PER_FILE)
    ]


def answer_key(n_files: int, seed: int = 42, schema_change_at: int = 30, malformed_at: int | None = None) -> dict:
    """Expected counts after bronze / silver / gold, mirroring the pipeline rules."""
    raw, kept, dropped = [], [], {"valid_amount": 0, "valid_customer": 0}
    for i in range(1, n_files + 1):
        for o in orders_file(i, seed, schema_change_at, malformed_at):
            raw.append(o)
            amt = o["amount"] if isinstance(o["amount"], (int, float)) else None
            if amt is None or amt <= 0:
                dropped["valid_amount"] += 1
            if o["customer_id"] is None:
                dropped["valid_customer"] += 1
            if amt is not None and amt > 0 and o["customer_id"] is not None:
                kept.append(o)
    unique = {o["order_id"]: o for o in kept}
    hours = {datetime.fromisoformat(o["order_ts"]).replace(minute=0, second=0) for o in unique.values()}
    return {
        "files": n_files,
        "bronze_orders_rows": len(raw),
        "bronze_clicks_rows": n_files * CLICKS_PER_FILE,
        "expectation_dropped": dropped,
        "silver_orders_rows": len(unique),
        "silver_revenue_total": round(sum(o["amount"] for o in unique.values()), 2),
        "gold_revenue_hours": len(hours),
        "distinct_customers": len({o["customer_id"] for o in unique.values()}),
        "dim_products_rows": N_PRODUCTS,
        "coupon_rows": sum(1 for o in raw if "coupon_code" in o),
    }


def _ndjson(rows: list[dict]) -> bytes:
    return ("\n".join(json.dumps(r) for r in rows) + "\n").encode()


def upload(w, path: str, data: bytes) -> None:
    w.files.upload(path, io.BytesIO(data), overwrite=True)


def main(argv: list[str] | None = None) -> None:
    from lab.common import LANDING, ws

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", type=int, default=1, help="first file number")
    ap.add_argument("--count", type=int, default=40, help="number of files to write")
    ap.add_argument("--interval", type=float, default=60, help="seconds between files")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--schema-change-at", type=int, default=30)
    ap.add_argument("--malformed-at", type=int, default=None)
    ap.add_argument("--products", action="store_true", help="upload products.csv and exit")
    ap.add_argument("--answer-key", action="store_true", help="print expected counts and exit")
    a = ap.parse_args(argv)

    if a.answer_key:
        print(json.dumps(answer_key(a.start + a.count - 1, a.seed, a.schema_change_at, a.malformed_at), indent=2))
        return
    w = ws()
    if a.products:
        upload(w, f"{LANDING}/products/products.csv", products_csv(a.seed).encode())
        print(f"uploaded {LANDING}/products/products.csv")
        return
    for i in range(a.start, a.start + a.count):
        upload(w, f"{LANDING}/orders/orders_{i:05d}.json", _ndjson(orders_file(i, a.seed, a.schema_change_at, a.malformed_at)))
        upload(w, f"{LANDING}/clicks/clicks_{i:05d}.json", _ndjson(clicks_file(i, a.seed)))
        print(f"{datetime.now():%H:%M:%S} file {i} uploaded")
        if a.interval and i < a.start + a.count - 1:
            time.sleep(a.interval)


if __name__ == "__main__":
    main()
