# %% [markdown]
# # Session 6b — Polars, DuckDB, PyIceberg and Spark read the Snowflake Iceberg tables directly
# Runs in **Google Colab** (or Kaggle / Codespaces): outside Snowflake, with internet access.
# The engines talk to Snowflake's **Horizon Iceberg REST catalog**, get short-lived **vended
# credentials**, and read the Iceberg metadata and Parquet files themselves. No bucket of
# your own: the files are on Snowflake-managed storage.
#
# One-time setup:
# 1. Finish Session 6 in Snowflake (it publishes `GOLD.FCT_ORDERS_ICEBERG`).
# 2. Run the first two cells here. The second prints a **public key** and a **private key**.
# 3. In Colab: 🔑 *Secrets* → add `SNOWFLAKE_ACCOUNT` (e.g. `ORGNAME-ACCOUNTNAME`, from
#    Snowsight → account menu → *Account identifier*) and `SNOWFLAKE_PRIVATE_KEY` (the whole
#    private key, BEGIN/END lines included). Give this notebook access to both.
# 4. In Snowflake: run `prep/colab_access.sql` with the public key pasted in.
# 5. Run the notebook top to bottom.

# %%
import importlib.util
import subprocess
import sys

IN_COLAB = importlib.util.find_spec("google.colab") is not None
if IN_COLAB:  # ~1 min; Colab has Java already
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "pyiceberg[pyarrow]>=0.9", "polars", "duckdb",
                    "pyspark==3.5.6", "snowflake-connector-python", "pyjwt", "cryptography", "requests"], check=True)

# %%
import base64
import hashlib
import json
import os
import time

import jwt
import requests
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa


def secret(name: str, default: str | None = None) -> str | None:
    if IN_COLAB:
        from google.colab import userdata
        try:
            return userdata.get(name)
        except Exception:  # noqa: BLE001 - secret not defined / not granted
            return default
    return os.environ.get(name, default)


ACCOUNT = (secret("SNOWFLAKE_ACCOUNT") or "").upper()   # ORGNAME-ACCOUNTNAME
USER = (secret("SNOWFLAKE_USER") or "LAB_COLAB").upper()
ROLE = (secret("SNOWFLAKE_ROLE") or "LAB_READER").upper()
DATABASE = "SHOP"
PRIVATE_KEY_PEM = secret("SNOWFLAKE_PRIVATE_KEY")

if not PRIVATE_KEY_PEM:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                            serialization.NoEncryption()).decode()
    pub = key.public_key().public_bytes(serialization.Encoding.PEM,
                                        serialization.PublicFormat.SubjectPublicKeyInfo).decode()
    print("1) Add this as Colab secret SNOWFLAKE_PRIVATE_KEY:\n\n" + pem)
    print("2) Paste this into prep/colab_access.sql (RSA_PUBLIC_KEY):\n\n"
          + "".join(pub.strip().splitlines()[1:-1]))
    raise SystemExit("Add the secrets, run prep/colab_access.sql, then re-run the notebook.")
assert ACCOUNT, "set the SNOWFLAKE_ACCOUNT secret (ORGNAME-ACCOUNTNAME)"

# %% [markdown]
# ## Log in: key-pair JWT → OAuth access token for Horizon

# %%
HOST = f"https://{ACCOUNT.lower()}.snowflakecomputing.com"
CATALOG_URI = f"{HOST}/polaris/api/catalog"
private_key = serialization.load_pem_private_key(PRIVATE_KEY_PEM.encode(), None)


def keypair_jwt() -> str:
    pub = private_key.public_key().public_bytes(serialization.Encoding.DER,
                                                serialization.PublicFormat.SubjectPublicKeyInfo)
    fp = "SHA256:" + base64.b64encode(hashlib.sha256(pub).digest()).decode()
    now = int(time.time())
    return jwt.encode({"iss": f"{ACCOUNT}.{USER}.{fp}", "sub": f"{ACCOUNT}.{USER}", "iat": now, "exp": now + 3600},
                      private_key, "RS256")


r = requests.post(f"{CATALOG_URI}/v1/oauth/tokens", timeout=30, data={
    "grant_type": "client_credentials", "scope": f"session:role:{ROLE}", "client_secret": keypair_jwt()})
r.raise_for_status()
TOKEN = r.json()["access_token"]   # valid ~1 h; re-run this cell to refresh
out = {"user": USER, "role": ROLE}
print("logged in as", USER, "with role", ROLE)

# %% [markdown]
# ## PyIceberg: browse the catalog, load tables, list snapshots

# %%
from pyiceberg.catalog import load_catalog

catalog = load_catalog("horizon", type="rest", uri=CATALOG_URI, warehouse=DATABASE, token=TOKEN,
                       **{"header.X-Iceberg-Access-Delegation": "vended-credentials"})
out["tables"] = sorted(".".join(t) for ns in catalog.list_namespaces() for t in catalog.list_tables(ns))
fct = catalog.load_table("GOLD.FCT_ORDERS_ICEBERG")
dim = catalog.load_table("GOLD.DIM_CUSTOMERS")
REGION = fct.io.properties.get("client.region")
out["pyiceberg_fct_rows"] = fct.scan().to_arrow().num_rows
out["dim_snapshots"] = [(s.snapshot_id, s.summary.operation.value if s.summary else None) for s in dim.snapshots()]
out["pyiceberg_dim_rows_first_snapshot"] = dim.scan(snapshot_id=dim.snapshots()[0].snapshot_id).to_arrow().num_rows
print(json.dumps({k: out[k] for k in ("tables", "pyiceberg_fct_rows")}, indent=1))


def by_category(rows) -> list:
    return sorted((str(c), int(n), round(float(v), 2)) for c, n, v in rows)

# %% [markdown]
# ## Polars: `scan_iceberg` on the same files (pass the vended credentials through)

# %%
import polars as pl

p = fct.io.properties
storage = {"aws_access_key_id": p["s3.access-key-id"], "aws_secret_access_key": p["s3.secret-access-key"],
           "aws_session_token": p["s3.session-token"], "aws_region": REGION}
t0 = time.perf_counter()
polars_rows = (pl.scan_iceberg(fct, storage_options=storage)
               .group_by("CATEGORY").agg(pl.len(), pl.col("AMOUNT").cast(pl.Float64).sum().round(2))
               .collect().rows())
out["polars_s"] = round(time.perf_counter() - t0, 2)
out["polars"] = by_category(polars_rows)

# %% [markdown]
# ## DuckDB: attach the Horizon catalog with the `iceberg` extension

# %%
import duckdb

con = duckdb.connect()
con.sql("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
con.sql(f"CREATE SECRET horizon (TYPE iceberg, TOKEN '{TOKEN}')")
con.sql(f"""ATTACH '{DATABASE}' AS horizon (TYPE iceberg, SECRET horizon, ENDPOINT '{CATALOG_URI}',
            ACCESS_DELEGATION_MODE 'vended_credentials', READ_ONLY)""")
t0 = time.perf_counter()
duck_rows = con.sql("SELECT CATEGORY, count(*), round(sum(AMOUNT)::double, 2) "
                    "FROM horizon.GOLD.FCT_ORDERS_ICEBERG GROUP BY ALL").fetchall()
out["duckdb_s"] = round(time.perf_counter() - t0, 2)
out["duckdb"] = by_category(duck_rows)
out["duckdb_dim_rows"] = con.sql("SELECT count(*) FROM horizon.GOLD.DIM_CUSTOMERS").fetchall()[0][0]

# %% [markdown]
# ## Spark: a local SparkSession with the Iceberg runtime and Horizon as a REST catalog

# %%
from pyspark.sql import SparkSession

ICEBERG = "1.9.1"
spark = (SparkSession.builder.appName("lab-6b").master("local[2]")
         .config("spark.jars.packages", f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG},"
                                        f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG}")
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.horizon", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.horizon.type", "rest")
         .config("spark.sql.catalog.horizon.uri", CATALOG_URI)
         .config("spark.sql.catalog.horizon.warehouse", DATABASE)
         .config("spark.sql.catalog.horizon.token", TOKEN)
         .config("spark.sql.catalog.horizon.header.X-Iceberg-Access-Delegation", "vended-credentials")
         .config("spark.sql.catalog.horizon.client.region", REGION)
         # Snowflake-written Parquet trips Iceberg's vectorized reader; the row reader is fine
         .config("spark.sql.iceberg.vectorization.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
t0 = time.perf_counter()
spark_rows = spark.sql("SELECT CATEGORY, count(*), round(sum(AMOUNT), 2) FROM horizon.GOLD.FCT_ORDERS_ICEBERG "
                       "GROUP BY CATEGORY").collect()
out["spark_s"] = round(time.perf_counter() - t0, 2)
out["spark"] = by_category([tuple(r) for r in spark_rows])
first = dim.snapshots()[0].snapshot_id
out["spark_dim_rows_first_snapshot"] = spark.sql(
    f"SELECT count(*) FROM horizon.GOLD.DIM_CUSTOMERS VERSION AS OF {first}").collect()[0][0]
out["spark_snapshots_table"] = spark.sql(
    "SELECT count(*) FROM horizon.GOLD.DIM_CUSTOMERS.snapshots").collect()[0][0]
spark.sql("SELECT committed_at, snapshot_id, operation FROM horizon.GOLD.DIM_CUSTOMERS.snapshots").show(truncate=False)

# %% [markdown]
# ## Reference: the same query in Snowflake SQL, then compare

# %%
import snowflake.connector

with snowflake.connector.connect(account=ACCOUNT, user=USER, role=ROLE, warehouse="LAB_WH",
                                 authenticator="SNOWFLAKE_JWT", private_key=private_key) as c:
    cur = c.cursor()
    cur.execute("SELECT category, count(*), round(sum(amount), 2) FROM SHOP.GOLD.FCT_ORDERS_ICEBERG GROUP BY ALL")
    out["snowflake"] = by_category(cur.fetchall())

out["all_engines_agree"] = out["snowflake"] == out["polars"] == out["duckdb"] == out["spark"]
out["versions"] = {"polars": pl.__version__, "duckdb": duckdb.__version__, "pyspark": spark.version,
                   "iceberg_spark": ICEBERG}
print("all engines agree:", out["all_engines_agree"])
print("RESULT_JSON=" + json.dumps(out, default=str))
