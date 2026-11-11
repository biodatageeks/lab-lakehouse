"""Session 1: Lakehouse 101 and Unity Catalog."""

import json

from lab.common import CATALOG, LANDING, sql, ws
from lab.remote import run_notebook
from producer import produce


def test_files_api_upload_from_laptop():
    produce.main(["--products"])
    meta = ws().files.get_metadata(f"{LANDING}/products/products.csv")
    assert meta.content_length > 0


def test_ctas_products_csv():
    sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.silver.dim_products AS
        SELECT * FROM read_files('{LANDING}/products/', format => 'csv', header => true, inferSchema => true)
    """)
    assert sql(f"SELECT count(*) FROM {CATALOG}.silver.dim_products")[0][0] == produce.N_PRODUCTS
    cols = [r[0] for r in sql(f"DESCRIBE {CATALOG}.silver.dim_products")]
    assert {"product_id", "name", "category", "price"} <= set(cols)


def test_describe_detail(record):
    row = sql(f"DESCRIBE DETAIL {CATALOG}.silver.dim_products")[0].asDict()
    record("s1.describe_detail", {k: row[k] for k in ("format", "numFiles", "sizeInBytes") if k in row})
    assert row["format"] == "delta"
    assert row["numFiles"] >= 1


def test_delta_log_json_in_volume(record):
    out = run_notebook("notebooks/01_delta_log.py", {"catalog": CATALOG})
    assert out["rows"] == 8
    assert "00000000000000000001.json" in out["log_files"]
    assert {"add", "commitInfo", "metaData", "protocol"} <= set(out["commit0_actions"])
    # ...and students can open the same JSON from the laptop through the Files API.
    resp = ws().files.download(f"{LANDING}/delta_demo/_delta_log/00000000000000000000.json")
    actions = [json.loads(line) for line in resp.contents.read().decode().splitlines()]
    record("s1.delta_log", {"files": out["log_files"], "commit0_actions": out["commit0_actions"]})
    assert any("add" in a for a in actions)
