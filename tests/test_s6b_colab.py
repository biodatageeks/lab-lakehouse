"""Session 6b: engines OUTSIDE Snowflake (the Colab notebook) read the Iceberg tables via Horizon.

    LAB_COLAB=1 uv run pytest tests/test_s6b_colab.py     # needs Docker; ~4 min

Stands in for Google Colab with a clean python:3.12 Linux container + Java 17 (what Colab
provides), runs notebooks/colab/src/06b_open_engines.py as the read-only LAB_COLAB user created
by prep/colab_access.sql, and checks that Snowflake SQL, PyIceberg, Polars, DuckDB and Spark agree.
The throwaway key is unset at the end. Run after Session 6 (it publishes GOLD.FCT_ORDERS_ICEBERG).
"""

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest
from conftest import MALFORMED_AT, N_TOTAL
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from producer import answer_key

from lab.sf import ROOT, run_script, scalar, sql

pytestmark = pytest.mark.skipif(os.environ.get("LAB_COLAB") != "1" or not shutil.which("docker"),
                                reason="set LAB_COLAB=1 (needs Docker)")

PIP = ['pyiceberg[pyarrow]>=0.9', "polars", "duckdb", "pyspark==3.5.6", "snowflake-connector-python",
       "pyjwt", "cryptography", "requests"]


@pytest.fixture(scope="module")
def out(record):
    assert scalar("SELECT count(*) FROM SHOP.GOLD.FCT_ORDERS_ICEBERG"), "run Session 6 first"
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                            serialization.NoEncryption()).decode()
    pub = "".join(key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
                  .decode().strip().splitlines()[1:-1])
    run_script((ROOT / "prep" / "colab_access.sql").read_text().replace("<paste the public key here>", pub))
    account = scalar("SELECT current_organization_name() || '-' || current_account_name()")
    with tempfile.TemporaryDirectory() as d:
        (Path(d) / "key.p8").write_text(pem)
        script = (f"apt-get update -qq >/dev/null && apt-get install -y -qq openjdk-17-jre-headless >/dev/null 2>&1 && "
                  f"pip install -q --root-user-action=ignore {' '.join(repr(p) for p in PIP)} >/dev/null 2>&1 && "
                  f'export SNOWFLAKE_PRIVATE_KEY="$(cat /key/key.p8)" && cd /tmp && python /nb/06b_open_engines.py')
        try:
            r = subprocess.run(
                ["docker", "run", "--rm", "-v", f"{ROOT / 'notebooks' / 'colab' / 'src'}:/nb:ro", "-v", f"{d}:/key:ro",
                 "-e", f"SNOWFLAKE_ACCOUNT={account}", "python:3.12-slim-bookworm", "bash", "-c", script],
                capture_output=True, text=True, timeout=1200)
        finally:
            sql("ALTER USER LAB_COLAB UNSET RSA_PUBLIC_KEY")
    line = next((ln for ln in r.stdout.splitlines() if ln.startswith("RESULT_JSON=")), None)
    assert line, (r.stdout + r.stderr)[-3000:]
    res = json.loads(line.removeprefix("RESULT_JSON="))
    record("s6b.versions", res["versions"])
    record("s6b.timings_s", {k: res[k] for k in ("polars_s", "duckdb_s", "spark_s")})
    return res


def test_read_only_role_sees_only_iceberg_tables(out, record):
    record("s6b.tables", out["tables"])
    assert out["role"] == "LAB_READER"
    assert {"GOLD.FCT_ORDERS_ICEBERG", "GOLD.DIM_CUSTOMERS", "SILVER.PRODUCTS_ICEBERG"} <= set(out["tables"])
    assert "GOLD.FCT_ORDERS" not in out["tables"]  # native table: not served by Horizon


def test_all_engines_agree_with_snowflake(out):
    assert out["pyiceberg_fct_rows"] == answer_key(N_TOTAL, malformed_at=MALFORMED_AT)["silver_orders_rows"]
    assert out["all_engines_agree"] is True
    assert out["snowflake"] == out["polars"] == out["duckdb"] == out["spark"]


def test_time_travel_from_outside(out, record):
    record("s6b.dim_snapshots", out["dim_snapshots"])
    k = answer_key(N_TOTAL, malformed_at=MALFORMED_AT)
    assert len(out["dim_snapshots"]) >= 2
    assert out["pyiceberg_dim_rows_first_snapshot"] == out["spark_dim_rows_first_snapshot"] == k["distinct_customers"]
    assert out["spark_snapshots_table"] == len(out["dim_snapshots"])
