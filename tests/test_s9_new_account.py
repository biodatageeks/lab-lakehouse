"""New trial account: rebuild the whole lab from scratch with prep/ scripts only (opt-in).

    LAB_NEW_ACCOUNT=1 uv run pytest tests/test_s9_new_account.py     # ~20 min, DROPS database SHOP

Simulates a student whose trial expired: no SHOP database, no warehouse, no monitor. Then:
bootstrap.sql -> upload course files -> LOAD_COURSE('') -> REBUILD(7) -> grading = full marks.
"""

import json
import os

import pytest

from lab import runner
from lab.sf import ROOT, run_script, scalar, sql

pytestmark = pytest.mark.skipif(os.environ.get("LAB_NEW_ACCOUNT") != "1",
                                reason="drops database SHOP; set LAB_NEW_ACCOUNT=1")


def test_rebuild_on_empty_account(record):
    for stmt in ("DROP DATABASE IF EXISTS SHOP", "DROP WAREHOUSE IF EXISTS LAB_WH",
                 "DROP RESOURCE MONITOR IF EXISTS LAB_RM", "DROP INTEGRATION IF EXISTS LAB_GITHUB",
                 "DROP INTEGRATION IF EXISTS LAB_GITHUB_PUBLIC"):
        sql(stmt)

    run_script(ROOT / "prep" / "bootstrap.sql")
    with pytest.raises(Exception, match="upload the course first"):
        sql("CALL SHOP.OPS.LOAD_COURSE('')")  # clear message before files are there
    runner.upload()
    loaded = json.loads(scalar("CALL SHOP.OPS.LOAD_COURSE('')"))
    record("s9.load_course", loaded)

    steps = json.loads(scalar("CALL SHOP.OPS.REBUILD(7)"))
    record("s9.rebuild_steps", [(s["notebook"], s["seconds"], s["status"][:120]) for s in steps])
    assert [s["status"] for s in steps] == ["ok"] * 8, steps

    report = json.loads(scalar("SELECT payload FROM SHOP.OPS.NB_RESULTS WHERE run_id = %s", (steps[-1]["run_id"],)))
    record("s9.score", report["score"])
    assert report["score"] == "15/15", [g for g in report["grade"] if not g["ok"]]
