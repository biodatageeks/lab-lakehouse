"""Session 6: open formats and external engines.

Course path: everything inside Databricks (notebook Spark -> Arrow -> DuckDB/Polars,
SQL time travel on Iceberg) or via the SQL warehouse. Tests marked `open_catalog` probe
the outside-engine path and run only with LAB_OPEN_CATALOG=1.

Laptop engines are tested stage by stage: catalog metadata must work; data reads are
expected to fail on Free Edition default storage and are reported as XFAIL with the
exact reason (an XPASS means Databricks lifted the restriction: switch to the primary path).
"""

import time

import pytest
from databricks.sdk.service import catalog as uc

from external import delta_sharing_read, duckdb_uc, polars_iceberg, sql_connector_arrow
from external._auth import denied_by_default_storage
from lab.common import CATALOG, sql, ws
from lab.remote import run_notebook
from producer import produce
from tests.conftest import MALFORMED_AT, N_TOTAL

KEY = produce.answer_key(N_TOTAL, malformed_at=MALFORMED_AT)
TT = "gold.iceberg_time_travel"
UNIFORM = "gold.orders_uniform"


@pytest.fixture(scope="module", autouse=True)
def open_tables():
    sql(f"CREATE OR REPLACE TABLE {CATALOG}.{TT} (id INT, label STRING) USING ICEBERG")
    sql(f"INSERT INTO {CATALOG}.{TT} VALUES (1, 'first')")
    sql(f"INSERT INTO {CATALOG}.{TT} VALUES (2, 'second'), (3, 'third')")
    sql(f"""
        CREATE OR REPLACE TABLE {CATALOG}.{UNIFORM}
        TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true',
                       'delta.universalFormat.enabledFormats' = 'iceberg')
        AS SELECT * FROM {CATALOG}.silver.orders
    """)


def _check(out: dict, record, key: str, expected_rows: int | None = None):
    record(key, out)
    assert out["metadata"], f"catalog metadata not visible: {out['error']}"
    if not out["data"]:
        if denied_by_default_storage(out["error"]):
            pytest.xfail(f"default storage blocks external reads: {out['error'][:160]}")
        pytest.fail(out["error"])
    if expected_rows is not None:
        assert out["rows"] == expected_rows


def test_uniform_table_has_iceberg_metadata(record):
    props = dict(sql(f"SHOW TBLPROPERTIES {CATALOG}.{UNIFORM}"))
    record("s6.uniform_props", {k: v for k, v in props.items() if "iceberg" in k.lower() or "universal" in k.lower()})
    assert props.get("delta.universalFormat.enabledFormats") == "iceberg"


@pytest.mark.open_catalog
def test_pyiceberg_managed_iceberg_and_time_travel(record):
    out = polars_iceberg.probe_pyiceberg(TT)
    record("s6.pyiceberg_time_travel", out)
    assert out["snapshots"] and out["snapshots"] >= 2, "snapshot history not exposed"
    assert out["parent_snapshot_id"], "no parent snapshot to travel to"
    _check(out, record, "s6.pyiceberg_time_travel", 3)
    assert out["rows_parent_snapshot"] == 1


@pytest.mark.open_catalog
def test_polars_scan_iceberg_dim_customers(record):
    _check(polars_iceberg.probe_pyiceberg("gold.dim_customers"), record, "s6.polars_scan_iceberg", KEY["distinct_customers"])


@pytest.mark.open_catalog
def test_pyiceberg_reads_uniform_table(record):
    _check(polars_iceberg.probe_pyiceberg(UNIFORM), record, "s6.pyiceberg_uniform", KEY["silver_orders_rows"])


@pytest.mark.open_catalog
def test_polars_unity_catalog_fct_orders(record):
    _check(polars_iceberg.probe_polars_unity("gold.fct_orders"), record, "s6.polars_pl_catalog", KEY["silver_orders_rows"])


@pytest.mark.open_catalog
def test_duckdb_unity_catalog_fct_orders(record):
    _check(duckdb_uc.probe("gold.fct_orders", "uc"), record, "s6.duckdb_unity_catalog", KEY["silver_orders_rows"])


@pytest.mark.open_catalog
def test_duckdb_iceberg_rest_dim_customers(record):
    _check(duckdb_uc.probe("gold.dim_customers", "ice"), record, "s6.duckdb_iceberg_rest", KEY["distinct_customers"])


def test_fallback_sql_connector_arrow(record):
    out = sql_connector_arrow.demo("gold.fct_orders")
    record("s6.fallback_sql_connector", out)
    assert out["rows"] == KEY["silver_orders_rows"]
    assert abs(out["duckdb_revenue"] - KEY["silver_revenue_total"]) < 0.01


def test_three_engines_inside_notebook(record):
    """Primary Session 6 path: Spark -> Arrow -> DuckDB / Polars, plus Iceberg time travel."""
    out = run_notebook("notebooks/06_external_engines_in_notebook.py",
                       {"catalog": CATALOG, "table": "gold.fct_orders", "iceberg_table": "gold.dim_customers"})
    record("s6.notebook_three_engines", out)
    assert out["polars_rows"] == out["duckdb_rows"] == KEY["silver_orders_rows"]
    assert out["engines_agree"], "Spark, DuckDB and Polars disagree on revenue by category"
    assert str(out["iceberg_provider"]).lower() == "delta" and out["uniform_formats"] == "iceberg"
    assert out["iceberg_rows_polars"] == KEY["distinct_customers"]


def test_iceberg_time_travel_with_sql(record):
    """Replaces PyIceberg snapshot time travel: DESCRIBE HISTORY + VERSION AS OF on managed Iceberg."""
    hist = [r.asDict() for r in sql(f"DESCRIBE HISTORY {CATALOG}.{TT}")]
    ops = [(h["version"], h["operation"]) for h in hist]
    record("s6.iceberg_history", ops)
    # CREATE OR REPLACE keeps older history: travel to the first write after the latest create.
    created = max(h["version"] for h in hist if h["operation"].startswith("CREATE"))
    first_insert = min(h["version"] for h in hist if h["operation"] == "WRITE" and h["version"] > created)
    assert sql(f"SELECT count(*) FROM {CATALOG}.{TT} VERSION AS OF {first_insert}")[0][0] == 1
    assert sql(f"SELECT count(*) FROM {CATALOG}.{TT}")[0][0] == 3
    ts = next(h["timestamp"] for h in hist if h["version"] == first_insert).strftime("%Y-%m-%d %H:%M:%S.%f")
    assert sql(f"SELECT count(*) FROM {CATALOG}.{TT} TIMESTAMP AS OF '{ts}'")[0][0] == 1


@pytest.mark.open_catalog
def test_open_catalog_clients_inside_notebook(record):
    """Research probe: do PyIceberg / DuckDB unity_catalog work from serverless compute? (No: SSE.)"""
    out = run_notebook("notebooks/probe_open_catalog_in_notebook.py", {"catalog": CATALOG})
    record("s6.open_catalog_in_notebook", out)
    if all(str(v).startswith("ERR") for k, v in out.items() if k != "auth"):
        pytest.xfail("open-catalog clients cannot read default storage from a notebook either")


def test_timing_warehouse_vs_fallback(record):
    q = f"SELECT category, count(*), round(sum(amount), 2) FROM {CATALOG}.gold.fct_orders GROUP BY category"
    t0 = time.perf_counter()
    sql(q)
    wh = round(time.perf_counter() - t0, 3)
    fb = sql_connector_arrow.demo("gold.fct_orders")
    record("s6.timings_s", {"warehouse_groupby": wh, "fallback_fetch_all": fb["fetch_s"],
                            "polars_groupby_local": fb["polars_s"], "duckdb_local": fb["duckdb_s"]})


# --- Delta Sharing: the laptop path that works on Free Edition --------------------------

@pytest.fixture(scope="module")
def share_profile(tmp_path_factory, record):
    """Share the course tables to a token recipient, yield its credential file, then clean up."""
    from prep import delta_sharing as dsp

    w = ws()
    before = w.metastores.summary()
    record("s6.delta_sharing_preconditions", {"external_access_enabled": before.external_access_enabled,
                                              "scope_before": before.delta_sharing_scope.value})
    profile = tmp_path_factory.mktemp("share") / "shop.share"
    assert dsp.main(["setup", "--profile", str(profile), "--token-days", "1"]) in (0, 1)
    yield profile
    dsp.teardown(restore_scope=False)
    if before.delta_sharing_scope == uc.DeltaSharingScopeEnum.INTERNAL:
        w.metastores.update(id=before.metastore_id, delta_sharing_scope=uc.DeltaSharingScopeEnum.INTERNAL)


def test_delta_sharing_lists_course_tables(share_profile, record):
    listed = delta_sharing_read.tables(share_profile)
    record("s6.delta_sharing_tables", listed)
    assert {"gold.fct_orders", "silver.orders", "gold.revenue_hourly", "gold.orders_uniform",
            "gold.dim_customers"} <= set(listed)


@pytest.mark.parametrize("table,key", [("gold.fct_orders", "silver_orders_rows"),
                                       ("gold.orders_uniform", "silver_orders_rows"),
                                       ("gold.dim_customers", "distinct_customers"),
                                       ("silver.orders", "silver_orders_rows"),
                                       ("gold.revenue_hourly", "gold_revenue_hours")])
def test_delta_sharing_read_into_polars_duckdb(share_profile, record, table, key):
    out = delta_sharing_read.demo(table, share_profile)
    record(f"s6.delta_sharing_read.{table}", out)
    assert out["rows"] == KEY[key]
    if out["duckdb_revenue"] is not None:
        assert abs(out["duckdb_revenue"] - KEY["silver_revenue_total"]) < 0.01


def test_delta_sharing_time_travel(share_profile, record):
    first = min(r.asDict()["version"] for r in sql(f"DESCRIBE HISTORY {CATALOG}.gold.fct_orders"))
    out = delta_sharing_read.demo("gold.fct_orders", share_profile, version=first)
    record("s6.delta_sharing_time_travel", out)
    assert out["rows"] == KEY["silver_orders_rows"]


def test_delta_sharing_managed_iceberg_not_shareable(share_profile, record):
    """Managed Iceberg tables carry the typeWidening feature, which Delta Sharing rejects."""
    from prep import delta_sharing as dsp

    dsp.ensure_share([TT])  # gold.iceberg_time_travel is a managed Iceberg table
    with pytest.raises(Exception) as ei:
        delta_sharing_read.read(TT, share_profile)
    record("s6.delta_sharing_managed_iceberg", str(ei.value).splitlines()[0][:200])
    assert "DS_UNSUPPORTED_DELTA_TABLE_FEATURES" in str(ei.value)
