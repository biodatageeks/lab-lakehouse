# Databricks notebook source
# MAGIC %md
# MAGIC # Reset (trainers, or to start over)
# MAGIC Deletes the job, the pipeline (and with it the pipeline's tables) and the whole `survey` catalog,
# MAGIC including the downloaded files. Then run notebook 0 again.

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient
from helpers import CATALOG, JOB_NAME, PIPELINE_NAME

w = WorkspaceClient()
done = []
for j in w.api_client.do("GET", "/api/2.2/jobs/list", query={"name": JOB_NAME}).get("jobs", []):
    w.api_client.do("POST", "/api/2.2/jobs/delete", body={"job_id": j["job_id"]})
    done.append(f"job {j['job_id']}")
for p in w.api_client.do("GET", "/api/2.0/pipelines", query={"filter": f"name LIKE '{PIPELINE_NAME}'"}).get("statuses", []):
    if p["name"] == PIPELINE_NAME:
        w.api_client.do("DELETE", f"/api/2.0/pipelines/{p['pipeline_id']}")
        done.append(f"pipeline {p['pipeline_id']}")
spark.sql(f"DROP CATALOG IF EXISTS {CATALOG} CASCADE")
done.append(f"catalog {CATALOG}")
print(done)
dbutils.notebook.exit(json.dumps(done))
