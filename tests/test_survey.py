"""End-to-end test of the survey lab on a Databricks Free Edition workspace.

Runs the notebooks in class order on serverless, from a clean state:
reset -> 0 setup -> 1 pipeline + initial load -> 2 the 2025 file arrives (trigger) -> 3 analysis -> 4 time travel.
~15 minutes. ``LAB_NO_RESET=1`` keeps the current state (then 1 and 2 only re-check).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from lab.dbx import deploy, run_notebook

# Official answer counts of the Stack Overflow Developer Survey.
ROWS = {"2023": 89184, "2024": 65437, "2025": 49191}
COLUMNS = {"2023": 84, "2024": 114, "2025": 172}
REPORTS = Path(__file__).resolve().parents[1] / "reports"
results: dict[str, dict] = {}


def nb(name: str, **params) -> dict:
    if name not in results:
        results[name] = run_notebook(name, params)
        REPORTS.mkdir(exist_ok=True)
        (REPORTS / "survey_results.json").write_text(json.dumps(results, indent=2, default=str))
    return results[name]


@pytest.fixture(scope="module", autouse=True)
def course():
    deploy()
    if not os.environ.get("LAB_NO_RESET"):
        run_notebook("99_reset")


def test_0_setup():
    r = nb("00_setup")
    assert {"raw", "bronze", "silver", "gold", "lab"} <= set(r["schemas"])
    assert r["pointer_bytes"] < 200, "raw.githubusercontent.com should serve only the LFS pointer"
    assert r["columns"] == COLUMNS
    assert r["first_column_2025"].startswith("﻿")


def test_1_initial_load():
    r = nb("01_pipeline")
    assert r["run"]["result"] == "SUCCESS" and r["run"]["trigger"] == "ONE_TIME"
    assert r["bronze"] == {k: ROWS[k] for k in ("2023", "2024")}
    assert r["silver"] == r["bronze"]
    assert r["partition_columns"] == ["year"]
    exp = {e["name"]: e for e in r["expectations"]}
    assert exp["has_response_id"]["failed"] == 0
    assert exp["has_country"]["failed"] > 0 and exp["pay_in_range"]["failed"] > 0
    assert all(n > 0 for n in r["gold"].values()), r["gold"]


def test_2_file_arrival_trigger():
    r = nb("02_new_data_arrives")
    assert r["run"]["trigger"] == "FILE_ARRIVAL", r["run"]
    assert r["run"]["result"] == "SUCCESS", r["run"]
    assert r["bronze"] == ROWS and r["silver"] == ROWS
    assert r["new_columns"] == 118
    assert r["bronze_rows_per_update"][-1] == ROWS["2025"], "Auto Loader must read only the new file"
    assert r["operations"].count("STREAMING UPDATE") >= 2


def test_3_analysis():
    r = nb("03_analysis")
    assert len(r["per_year"]) == 3  # respondents with a country, per year
    assert r["top_country"] == "United States"
    assert set(r["python_share"]) == {"2023", "2024", "2025"}
    assert r["pay_countries"] > 5 and r["ds_languages"] > 5


def test_4_time_travel():
    r = nb("04_time_travel")
    # each step is its own commit; Databricks may add extra ones (auto-compaction OPTIMIZE)
    assert r["steps"] == {
        "create": "CREATE TABLE AS SELECT", "insert": "WRITE", "bad_update": "UPDATE", "delete": "DELETE",
        "merge": "MERGE", "add_column": "ADD COLUMNS", "optimize": "OPTIMIZE", "repair": "MERGE",
        "restore_old": "RESTORE", "restore_repaired": "RESTORE"}, r["operations"]
    versions = list(r["versions"].values())
    assert versions == sorted(versions) and len(set(versions)) == len(versions)
    before, after = r["avg_pay_2024_before_after"]
    assert after == pytest.approx(before * 100, rel=1e-3)
    assert int(r["deleted_rows"]) > 0 and r["before_after_rows"] == 10
    assert r["bronze_time_travel"] == {"2023": ROWS["2023"], "2024": ROWS["2024"]}, "time travel on the pipeline's bronze table"
