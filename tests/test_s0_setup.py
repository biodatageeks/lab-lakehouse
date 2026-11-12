"""Session 0: account prep (SQL bootstrap) and what a trial account allows."""

import pytest

from lab.sf import ROOT, run_script, scalar


@pytest.fixture(scope="module")
def out(nb):
    return nb("00_check_setup")


def test_bootstrap_is_idempotent(course):
    run_script(ROOT / "prep" / "bootstrap.sql")
    assert scalar("SELECT count(*) FROM SHOP.INFORMATION_SCHEMA.SCHEMATA "
                  "WHERE schema_name IN ('BRONZE','SILVER','GOLD','OPS')") == 4


def test_notebook_runs_headless_with_session(out, record):
    record("s0.account", {k: out[k] for k in ("account_identifier", "region", "role", "warehouse")})
    assert out["role"] == "ACCOUNTADMIN" and out["warehouse"] == "LAB_WH"


def test_enterprise_features_on_trial(out, record):
    record("s0.retention_90_days", out["retention_90_days"])
    record("s0.free_credit_balance_usd", out["free_credit_balance_usd"])
    assert out["retention_90_days"] == "allowed"
    assert isinstance(out["free_credit_balance_usd"], float)


def test_packages_from_snowflake_pypi_mirror(out, record):
    record("s0.packages", out["packages"])
    assert {"duckdb", "polars", "pyiceberg", "pyarrow"} <= set(out["packages"])


def test_no_external_access_on_trial(out, record):
    record("s0.external_access_integration", out["external_access_integration"])
    record("s0.egress", out["egress"])
    assert "trial" in out["external_access_integration"].lower()
    assert all(str(v).startswith("ERR") for v in out["egress"].values())
