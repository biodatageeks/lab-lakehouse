# Databricks notebook source
# Tiny task used by the jobs tests (schedule + concurrency probes).
import json
import time

dbutils.widgets.text("seconds", "30")
t0 = time.time()
time.sleep(int(dbutils.widgets.get("seconds")))
dbutils.notebook.exit(json.dumps({"start": t0, "end": time.time()}))
