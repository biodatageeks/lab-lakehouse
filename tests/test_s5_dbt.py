"""Session 5: dbt-databricks against the SQL warehouse."""

import json

import pytest

from lab.common import CATALOG, sql
from lab.dbt import PROJECT, dbt
from producer import produce
from tests.conftest import MALFORMED_AT, N_TOTAL

KEY = produce.answer_key(N_TOTAL, malformed_at=MALFORMED_AT)


def _ok(r, record=None, key=None):
    if record and key:
        record(key, r.stdout.strip().splitlines()[-1] if r.stdout.strip() else r.stderr[-300:])
    assert r.returncode == 0, (r.stdout[-3000:] + r.stderr[-2000:])


def test_dbt_debug(record):
    _ok(dbt("debug"), record, "s5.dbt_debug")


@pytest.fixture(scope="module")
def build(record):
    r = dbt("build")
    _ok(r, record, "s5.dbt_build")
    return json.loads((PROJECT / "target" / "run_results.json").read_text())


def test_models_and_tests_pass(build, record):
    statuses = {res["unique_id"]: res["status"] for res in build["results"]}
    record("s5.dbt_nodes", statuses)
    assert all(s in ("success", "pass") for s in statuses.values()), statuses
    assert sum(1 for u in statuses if u.startswith("test.")) >= 6  # 5 generic + 1 singular


def test_marts_counts(build):
    assert sql(f"SELECT count(*) FROM {CATALOG}.gold.fct_orders")[0][0] == KEY["silver_orders_rows"]
    assert sql(f"SELECT count(*) FROM {CATALOG}.gold.dim_customers")[0][0] == KEY["distinct_customers"]


def test_dim_customers_is_uniform_iceberg(build, record):
    fmt = sql(f"SELECT data_source_format FROM {CATALOG}.information_schema.tables "
              "WHERE table_schema='gold' AND table_name='dim_customers'")[0][0]
    props = dict(sql(f"SHOW TBLPROPERTIES {CATALOG}.gold.dim_customers"))
    record("s5.dim_customers_format", {"format": fmt, **{k: v for k, v in props.items()
                                                           if "iceberg" in k.lower() or "universal" in k.lower()
                                                           or "typeWidening" in k}})
    assert fmt.upper() == "DELTA"
    assert props.get("delta.universalFormat.enabledFormats") == "iceberg"
    assert "delta.feature.typeWidening" not in props, "typeWidening would block Delta Sharing"


def test_incremental_merge(build, record):
    _ok(dbt("run", "-s", "fct_orders"))
    ops = [r.asDict()["operation"] for r in sql(f"DESCRIBE HISTORY {CATALOG}.gold.fct_orders")]
    record("s5.fct_orders_history", ops[:5])
    # dbt-databricks runs MERGE, then may re-apply liquid clustering (ALTER ... CLUSTER BY).
    assert "MERGE" in ops[:2], ops[:3]
    assert sql(f"SELECT count(*) FROM {CATALOG}.gold.fct_orders")[0][0] == KEY["silver_orders_rows"]


def test_docs_generate(build):
    _ok(dbt("docs", "generate"))
    cat = json.loads((PROJECT / "target" / "catalog.json").read_text())
    assert any(k.endswith("fct_orders") for k in cat["nodes"])
