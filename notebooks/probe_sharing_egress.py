# Databricks notebook source
# Probe: can serverless reach the Delta Sharing data-file host, and which client path works?

# COMMAND ----------

# MAGIC %pip install delta-sharing --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json, socket, urllib.request
import delta_sharing

res = {}
for host in ["us-east-2.storage.cloud.databricks.com", "pypi.org"]:
    try:
        res[f"dns:{host}"] = socket.gethostbyname(host)
    except Exception as e:  # noqa: BLE001
        res[f"dns:{host}"] = f"ERR {e}"
    try:
        with urllib.request.urlopen(f"https://{host}/", timeout=10) as r:
            res[f"https:{host}"] = r.status
    except urllib.error.HTTPError as e:
        res[f"https:{host}"] = f"HTTP {e.code} (reachable)"
    except Exception as e:  # noqa: BLE001
        res[f"https:{host}"] = f"ERR {type(e).__name__}: {str(e)[:150]}"

PROFILE = "/tmp/shop.share"
open(PROFILE, "w").write(dbutils.secrets.get("lab", "shop_share"))
url = f"{PROFILE}#lab_shop_share.gold.fct_orders"
try:
    res["parquet_path_rows"] = len(delta_sharing.load_as_pandas(url))  # python requests/fsspec path
except Exception as e:  # noqa: BLE001
    res["parquet_path_rows"] = f"ERR {type(e).__name__}: {str(e)[:300]}"
dbutils.notebook.exit(json.dumps(res))
