"""Before class: workspace prep done by SDK/CLI instead of clicking."""

import subprocess
import sys

import pytest
from databricks.sdk.service import catalog as uc

from lab.common import CATALOG, ROOT, ws


@pytest.mark.open_catalog
def test_external_data_access_enabled_via_sdk(record):
    s = ws().metastores.summary()
    record("metastore.external_access_enabled", s.external_access_enabled)
    assert s.external_access_enabled is True


@pytest.mark.open_catalog
def test_external_use_schema_effective_on_gold(record):
    w = ws()
    me = w.current_user.me().user_name
    eff = w.grants.get_effective(securable_type="schema", full_name=f"{CATALOG}.gold", principal=me)
    privs = {p.privilege for a in eff.privilege_assignments for p in a.privileges}
    record("grants.effective_on_gold", sorted(str(p.value) for p in privs))
    assert uc.Privilege.EXTERNAL_USE_SCHEMA in privs


@pytest.mark.open_catalog
def test_schema_level_external_use_grant_is_rejected_on_default_storage(record):
    """Documents a Free Edition quirk: grant at catalog level, not schema level."""
    w = ws()
    me = w.current_user.me().user_name
    with pytest.raises(Exception) as ei:
        w.grants.update(
            securable_type="schema", full_name=f"{CATALOG}.gold",
            changes=[uc.PermissionsChange(principal=me, add=[uc.Privilege.EXTERNAL_USE_SCHEMA])],
        )
    record("grants.schema_level_external_use", str(ei.value).splitlines()[0][:200])
    assert "not applicable" in str(ei.value)


def test_check_setup_script():
    r = subprocess.run([sys.executable, "check_setup.py"], cwd=ROOT, capture_output=True, text=True, timeout=300)
    assert r.returncode == 0, r.stderr
    for label in ("workspace", "warehouse", "catalogs"):
        assert label in r.stdout
    assert CATALOG in r.stdout
