"""Shared fixtures for the course feature tests.

Tests run in file order (s0 -> s7) and build on each other exactly like the sessions.
Set LAB_NO_RESET=1 to keep the existing state instead of starting clean.
Set LAB_OPEN_CATALOG=1 to also run the `open_catalog` tests: external data access setup
and outside engines (laptop DuckDB/Polars/PyIceberg). The course does not depend on them.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import pytest

from lab.common import ROOT

N_FIRST = 10  # files before the first Auto Loader run
N_TOTAL = 35  # files in total (schema change at 30)
MALFORMED_AT = 15  # file with amount="not-a-number" (_rescued_data)

for _noisy in ("databricks.sql", "databricks.sdk", "urllib3", "pyiceberg"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

REPORT = ROOT / "reports" / "findings.json"
_findings: dict[str, object] = {}


@pytest.fixture(scope="session")
def record():
    def _record(key: str, value) -> None:
        _findings[key] = value

    return _record


OPEN_CATALOG = os.environ.get("LAB_OPEN_CATALOG") == "1"


def pytest_configure(config):
    config.addinivalue_line("markers", "open_catalog: outside-engine / external data access tests (opt-in)")


def pytest_collection_modifyitems(config, items):
    if OPEN_CATALOG:
        return
    skip = pytest.mark.skip(reason="open-catalog path not used by the course; set LAB_OPEN_CATALOG=1")
    for item in items:
        if "open_catalog" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session", autouse=True)
def clean_state():
    if os.environ.get("LAB_NO_RESET") != "1":
        from prep import reset_lab, setup_workspace

        assert setup_workspace.main(["--external-access"] if OPEN_CATALOG else []) == 0
        reset_lab.main(["--yes"])
    yield


def pytest_sessionfinish(session, exitstatus):
    REPORT.parent.mkdir(exist_ok=True)
    REPORT.write_text(json.dumps(_findings, indent=2, default=str))


def pytest_terminal_summary(terminalreporter):
    if not _findings:
        return
    terminalreporter.section("findings")
    for k, v in _findings.items():
        terminalreporter.write_line(f"{k}: {json.dumps(v, default=str)[:300]}")
    terminalreporter.write_line(f"(full findings in {Path(REPORT).relative_to(ROOT)})")
