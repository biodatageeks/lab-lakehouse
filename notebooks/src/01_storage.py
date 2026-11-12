# %% [markdown]
# # Session 1 — databases, stages, tables, and what an Iceberg table really is
# * the landing **stage** plays the role of a volume: files in, tables out
# * a table created from a CSV with `INFER_SCHEMA` (schema-on-read, then typed)
# * a **managed Iceberg table** on Snowflake storage, and its metadata seen through the
#   Iceberg REST catalog (Horizon) — the Snowflake counterpart of browsing `_delta_log`

# %%
# Install the course packages (duckdb, polars, pyiceberg) from the Snowflake PyPI repository.
# Interactive: pick that repository as the package source when you connect; re-run after a restart.
!pip install -q -r requirements.txt

# %%
import os, sys
sys.path.insert(0, os.getcwd())
from lab_nb import *
import producer

a = args()
rep = Report("01_storage", a.run_id)

# %% [markdown]
# ## Upload a file to the stage from the notebook and build a typed table from it

# %%
producer.upload_products(session)
rep["stage_files"] = [r["name"] for r in rows(f"LIST {LANDING}/products/")]
sql("CREATE OR REPLACE FILE FORMAT SHOP.BRONZE.CSV_HEADER TYPE = CSV PARSE_HEADER = TRUE FIELD_OPTIONALLY_ENCLOSED_BY = '\"'")
inferred = rows(f"""SELECT column_name, type FROM TABLE(INFER_SCHEMA(
    LOCATION => '{LANDING}/products/', FILE_FORMAT => 'SHOP.BRONZE.CSV_HEADER'))""")
rep["inferred_schema"] = {r["COLUMN_NAME"]: r["TYPE"] for r in inferred}
sql(f"""CREATE OR REPLACE TABLE SHOP.SILVER.DIM_PRODUCTS USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT('COLUMN_NAME', UPPER(column_name), 'TYPE', type, 'NULLABLE', nullable))
           WITHIN GROUP (ORDER BY order_id)
    FROM TABLE(INFER_SCHEMA(LOCATION => '{LANDING}/products/', FILE_FORMAT => 'SHOP.BRONZE.CSV_HEADER')))""")
sql(f"""COPY INTO SHOP.SILVER.DIM_PRODUCTS FROM {LANDING}/products/
        FILE_FORMAT = (FORMAT_NAME = 'SHOP.BRONZE.CSV_HEADER') MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE""")
rep["dim_products_rows"] = scalar("SELECT count(*) FROM SHOP.SILVER.DIM_PRODUCTS")
rep["dim_products_columns"] = [r["name"] for r in rows("DESCRIBE TABLE SHOP.SILVER.DIM_PRODUCTS")]
detail = rows("SHOW TABLES LIKE 'DIM_PRODUCTS' IN SCHEMA SHOP.SILVER")[0]
rep["table_detail"] = {k: detail[k] for k in ("kind", "rows", "bytes", "retention_time", "cluster_by")}
show("SELECT * FROM SHOP.SILVER.DIM_PRODUCTS LIMIT 5")

# %% [markdown]
# ## A managed Iceberg table — no cloud bucket of your own needed
# `EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'` stores the Parquet + Iceberg metadata on
# Snowflake storage. Every write makes a new Iceberg snapshot.

# %%
sql("""CREATE OR REPLACE ICEBERG TABLE SHOP.SILVER.PRODUCTS_ICEBERG
       CATALOG = 'SNOWFLAKE' EXTERNAL_VOLUME = 'SNOWFLAKE_MANAGED'
       AS SELECT * FROM SHOP.SILVER.DIM_PRODUCTS""")
sql("UPDATE SHOP.SILVER.PRODUCTS_ICEBERG SET price = price * 1.1 WHERE category = 'books'")
sql("DELETE FROM SHOP.SILVER.PRODUCTS_ICEBERG WHERE product_id = 50")
info = json.loads(scalar("SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('SHOP.SILVER.PRODUCTS_ICEBERG')"))
rep["iceberg_metadata_location"] = info.get("metadataLocation")
ice = rows("SHOW ICEBERG TABLES LIKE 'PRODUCTS_ICEBERG' IN SCHEMA SHOP.SILVER")[0]
rep["iceberg_table"] = {k: ice.get(k) for k in ("external_volume_name", "catalog_name", "iceberg_table_type")}

# %% [markdown]
# ## Look inside: the Iceberg REST catalog (Horizon) from the notebook
# The notebook authenticates with its own session token. Metadata (schema, snapshots,
# manifests' locations) is readable; the data files themselves are not reachable from the
# notebook container (no egress to the storage bucket) — read data through Snowflake instead.

# %%
def iceberg_metadata():
    t = horizon_catalog().load_table("SILVER.PRODUCTS_ICEBERG")
    return {
        "format_version": t.metadata.format_version,
        "schema": [f.name for f in t.schema().fields],
        "snapshots": [{"id": s.snapshot_id, "operation": s.summary.operation.value if s.summary else None,
                       "added_records": s.summary.additional_properties.get("added-records") if s.summary else None}
                      for s in t.snapshots()],
        "metadata_location": t.metadata_location,
    }

rep["horizon_metadata"] = attempt(iceberg_metadata)
rep["horizon_data_read"] = attempt(lambda: horizon_catalog().load_table("SILVER.PRODUCTS_ICEBERG").scan().to_arrow().num_rows)
rep.save()
