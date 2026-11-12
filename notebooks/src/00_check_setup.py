# %% [markdown]
# # Session 0 — check your trial account
# Run `prep/bootstrap.sql` once first (Workspaces > SQL file > Run All). This notebook checks
# what the account gives you: edition features, credit balance, Python packages, network.

# %%
import os, sys, urllib.request
sys.path.insert(0, os.getcwd())
from lab_nb import *

a = args()
rep = Report("00_check_setup", a.run_id)

# %%
r = rows("""SELECT current_account() account, current_region() region, current_role() role,
            current_warehouse() warehouse, current_organization_name() || '-' || current_account_name() account_identifier""")[0]
rep.update({k.lower(): v for k, v in r.items()})
rep["databases"] = [x["name"] for x in rows("SHOW DATABASES")]
rep["warehouse_size"] = rows("SHOW WAREHOUSES LIKE 'LAB_WH'")[0]["size"]
show("SHOW SCHEMAS IN DATABASE SHOP")

# %% [markdown]
# Trial accounts are **Enterprise** edition: Time Travel up to 90 days and data metric functions work.

# %%
rep["retention_90_days"] = attempt(lambda: sql("ALTER DATABASE SHOP SET DATA_RETENTION_TIME_IN_DAYS = 90") and "allowed")
sql("ALTER DATABASE SHOP SET DATA_RETENTION_TIME_IN_DAYS = 7")
rep["free_credit_balance_usd"] = attempt(lambda: float(scalar(
    "SELECT free_usage_balance FROM SNOWFLAKE.ORGANIZATION_USAGE.REMAINING_BALANCE_DAILY ORDER BY date DESC LIMIT 1")))
rep["external_access_integration"] = attempt(lambda: sql(
    "CREATE OR REPLACE NETWORK RULE SHOP.OPS.PYPI_RULE MODE = EGRESS TYPE = HOST_PORT VALUE_LIST = ('pypi.org')")
    and sql("CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION LAB_PYPI ALLOWED_NETWORK_RULES = (SHOP.OPS.PYPI_RULE) ENABLED = TRUE")
    and "allowed")

# %% [markdown]
# Packages come from Snowflake's PyPI mirror (`requirements.txt` + the shared PyPI artifact
# repository), so no external access integration is needed. The open internet is not reachable.

# %%
import duckdb, polars, pyiceberg, pyarrow
rep["packages"] = {m.__name__: m.__version__ for m in (duckdb, polars, pyiceberg, pyarrow)}
rep["python"] = sys.version.split()[0]
egress = {}
for url in ["https://pypi.org/simple/duckdb/", "https://github.com", "https://example.com"]:
    egress[url] = attempt(lambda url=url: urllib.request.urlopen(url, timeout=8).status)
rep["egress"] = egress
rep["session_token_file"] = os.path.exists("/snowflake/session/token")
rep.save()
