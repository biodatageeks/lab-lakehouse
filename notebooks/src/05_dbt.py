# %% [markdown]
# # Session 5 — dbt, natively in Snowflake (dbt Projects on Snowflake)
# The dbt project (`dbt_shop/`) is a Snowflake object: `CREATE DBT PROJECT ... FROM <stage>`,
# then `EXECUTE DBT PROJECT ... ARGS = 'build'`. No dbt install, no laptop, no credentials.
# * `stg_orders` view (SILVER) → `fct_orders` incremental **merge** → `dim_customers` as a
#   **managed Iceberg table** (custom materialization in `dbt_shop/macros/iceberg_table.sql`)
# * tests: unique / not_null + a singular test

# %%
import os, re, sys
sys.path.insert(0, os.getcwd())
from lab_nb import *

a = args(dbt_src="@SHOP.OPS.COURSE/dbt_shop")
rep = Report("05_dbt", a.run_id)

# %%
sql(f"CREATE OR REPLACE DBT PROJECT SHOP.OPS.DBT_SHOP FROM '{a.dbt_src}'")
rep["dbt_project"] = rows("SHOW DBT PROJECTS LIKE 'DBT_SHOP' IN SCHEMA SHOP.OPS")[0].get("default_version")

def dbt(command: str) -> dict:
    try:
        r = rows(f"EXECUTE DBT PROJECT SHOP.OPS.DBT_SHOP ARGS = '{command}'")[0]
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": err(e)}
    texts = [v for v in r.values() if isinstance(v, str)]
    stdout = re.sub(r"\x1b\[[0-9;]*m", "", max(texts, key=len) if texts else "")
    m = re.search(r"Done\. PASS=(\d+) WARN=(\d+) ERROR=(\d+) SKIP=(\d+)", stdout)
    print(stdout[-1500:])
    return {"ok": next((v for v in r.values() if isinstance(v, bool)), None), "columns": list(r),
            "summary": m.group(0) if m else None,
            "versions": re.findall(r"Running with dbt=(\S+)|adapter: (snowflake=\S+)", stdout)}

# %% [markdown]
# ## Build twice: the second run merges into `fct_orders` instead of rebuilding it

# %%
rep["build_1"] = dbt("build")
rep["build_2"] = dbt("build")
rep["docs_generate"] = dbt("docs generate")
rep["fct_orders_rows"] = scalar("SELECT count(*) FROM SHOP.GOLD.FCT_ORDERS")
rep["dim_customers_rows"] = scalar("SELECT count(*) FROM SHOP.GOLD.DIM_CUSTOMERS")
rep["merge_statements"] = scalar("""SELECT count(*) FROM TABLE(SHOP.INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 1000))
    WHERE query_type = 'MERGE' AND query_text ILIKE 'merge into SHOP.GOLD.fct_orders%'""")

# %% [markdown]
# ## `dim_customers` is an Iceberg table; each dbt run is a new snapshot

# %%
ice = rows("SHOW ICEBERG TABLES LIKE 'DIM_CUSTOMERS' IN SCHEMA SHOP.GOLD")
rep["dim_customers_iceberg"] = {k: ice[0].get(k) for k in ("external_volume_name", "catalog_name", "iceberg_table_type")} if ice else None
rep["dim_customers_snapshots"] = attempt(lambda: len(horizon_catalog().load_table("GOLD.DIM_CUSTOMERS").snapshots()))
rep.save()
