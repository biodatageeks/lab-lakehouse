"""Harness: bootstrap + reset the account, deploy the notebook project, run notebooks headlessly.

Env: LAB_NO_RESET=1 keeps course state; LAB_NO_DEPLOY=1 skips re-uploading the notebooks.
"""

from __future__ import annotations

import json
import logging
import os
import sys

import pytest

from lab import runner
from lab.sf import ROOT, run_script

sys.path.insert(0, str(ROOT / "notebooks"))  # producer / grade are shared with the notebooks

N_FIRST, N_TOTAL, MALFORMED_AT = 10, 35, 15
FINDINGS: dict[str, object] = {}

for noisy in ("snowflake.connector", "botocore", "urllib3"):
    logging.getLogger(noisy).setLevel(logging.WARNING)


@pytest.fixture(scope="session")
def record():
    def _record(key: str, value) -> None:
        FINDINGS[key] = value
    return _record


@pytest.fixture(scope="session")
def course():
    run_script(ROOT / "prep" / "bootstrap.sql")
    if os.environ.get("LAB_NO_RESET") != "1":
        run_script(ROOT / "prep" / "reset.sql")
    if os.environ.get("LAB_NO_DEPLOY") != "1":
        runner.deploy()
    return True


@pytest.fixture(scope="session")
def nb(course, record):
    """nb(name, **args) -> report dict; each distinct call runs once per test session."""
    cache: dict = {}

    def _run(name: str, **args):
        key = (name, tuple(sorted(args.items())))
        if key not in cache:
            out = runner.run_notebook(name, **args)
            record(f"notebook.{name}{'' if not args else '(' + ','.join(f'{k}={v}' for k, v in args.items()) + ')'}.elapsed_s",
                   out["_elapsed_s"])
            cache[key] = out
        return cache[key]
    return _run


def pytest_sessionfinish(session, exitstatus):
    if FINDINGS:
        (ROOT / "reports").mkdir(exist_ok=True)
        (ROOT / "reports" / "findings.json").write_text(json.dumps(FINDINGS, indent=1, default=str))


def pytest_terminal_summary(terminalreporter):
    if FINDINGS:
        terminalreporter.section("findings")
        for k, v in FINDINGS.items():
            terminalreporter.write_line(f"{k}: {json.dumps(v, default=str)[:300]}")
