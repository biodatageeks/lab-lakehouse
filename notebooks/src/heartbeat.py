# %% [markdown]
# Tiny notebook that Session 7 schedules with a task (EXECUTE NOTEBOOK PROJECT inside a task).

# %%
import os, sys
sys.path.insert(0, os.getcwd())
from lab_nb import *

a = args()
rep = Report("heartbeat", a.run_id)
rep["heartbeat_at"] = scalar("SELECT current_timestamp()::string")
rep.save()
