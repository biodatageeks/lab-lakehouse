"""Open Delta Sharing of course tables to a token recipient (Session 6, laptop path).

    uv run python -m prep.delta_sharing setup  --profile ~/.lab/shop.share   # writes the credential file
    uv run python -m prep.delta_sharing teardown                              # drops share + recipient

`setup`:
  1. metastore Delta Sharing scope -> INTERNAL_AND_EXTERNAL (recipient tokens valid for --token-days)
  2. share `lab_shop_share` with the course tables (history on, so recipients can time travel)
  3. recipient `lab_shop_laptop` (TOKEN auth) granted SELECT on the share
  4. downloads the one-time activation credential (`.share` profile) to --profile
  5. only with --secret: stores the same profile as secret `lab/shop_share`. Note that Free
     Edition serverless cannot download shared files (the sharing storage host is private and
     refuses connections), so notebooks read tables with Spark instead; this is for probes.

The profile is a bearer credential to the shared tables: keep it out of git.
`teardown` removes the share and recipient. It leaves the metastore scope as it is unless
--restore-scope is given.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from databricks.sdk.errors import NotFound
from databricks.sdk.service import catalog as uc
from databricks.sdk.service import sharing as ds

from lab.common import CATALOG, ws

SECRET_SCOPE = "lab"
SECRET_KEY = "shop_share"
SHARE = "lab_shop_share"
RECIPIENT = "lab_shop_laptop"
TABLES = ["gold.fct_orders", "gold.orders_uniform", "silver.orders", "gold.revenue_hourly", "gold.dim_customers"]


def enable_external_sharing(token_days: int) -> None:
    w = ws()
    s = w.metastores.summary()
    w.metastores.update(
        id=s.metastore_id,
        delta_sharing_scope=uc.DeltaSharingScopeEnum.INTERNAL_AND_EXTERNAL,
        delta_sharing_recipient_token_lifetime_in_seconds=token_days * 86400,
        delta_sharing_organization_name=s.delta_sharing_organization_name or "lab-lakehouse",
    )
    print(f"[ok] delta sharing scope = {w.metastores.summary().delta_sharing_scope.value}")


def ensure_share(tables: list[str]) -> dict:
    w = ws()
    try:
        w.shares.get(SHARE)
    except NotFound:
        w.shares.create(SHARE, comment="Lakehouse course tables for laptop engines")
    existing = {o.name for o in (w.shares.get(SHARE, include_shared_data=True).objects or [])}
    results = {}
    for t in tables:
        fqn = f"{CATALOG}.{t}"
        if fqn in existing:
            results[t] = "already shared"
            continue
        kind = str(w.tables.get(fqn).table_type.value)
        otype = (ds.SharedDataObjectDataObjectType.MATERIALIZED_VIEW if "MATERIALIZED" in kind
                 else ds.SharedDataObjectDataObjectType.STREAMING_TABLE if "STREAMING" in kind
                 else ds.SharedDataObjectDataObjectType.TABLE)
        for history in (True, False):  # some table types refuse history sharing
            obj = ds.SharedDataObject(
                name=fqn,
                data_object_type=otype,
                history_data_sharing_status=ds.SharedDataObjectHistoryDataSharingStatus.ENABLED if history
                else ds.SharedDataObjectHistoryDataSharingStatus.DISABLED,
            )
            try:
                w.shares.update(SHARE, updates=[ds.SharedDataObjectUpdate(action=ds.SharedDataObjectUpdateAction.ADD,
                                                                         data_object=obj)])
                results[t] = "shared with history" if history else "shared (no history)"
                break
            except Exception as e:  # noqa: BLE001
                results[t] = f"ERR {str(e).splitlines()[0][:200]}"
    for t, r in results.items():
        print(f"[{'ok' if not r.startswith('ERR') else 'FAIL'}] {t}: {r}")
    return results


def ensure_recipient(profile: Path) -> str:
    w = ws()
    try:
        w.recipients.delete(RECIPIENT)  # activation links are one-time: recreate for a fresh one
    except NotFound:
        pass
    r = w.recipients.create(RECIPIENT, ds.AuthenticationType.TOKEN, comment="laptop engines for the course")
    w.shares.update_permissions(SHARE, changes=[uc.PermissionsChange(principal=RECIPIENT, add=[uc.Privilege.SELECT])])
    activation = r.tokens[0].activation_url
    token = activation.rstrip("/").rsplit("?", 1)[-1].split("=")[-1] if "?" in activation else activation.rstrip("/").rsplit("/", 1)[-1]
    cred = w.recipient_activation.retrieve_token(token)
    content = json.dumps({
        "shareCredentialsVersion": cred.share_credentials_version or 1,
        "bearerToken": cred.bearer_token,
        "endpoint": cred.endpoint,
        "expirationTime": cred.expiration_time,
    })
    profile.parent.mkdir(parents=True, exist_ok=True)
    profile.write_text(content)
    profile.chmod(0o600)
    print(f"[ok] recipient {RECIPIENT}; credential written to {profile}")
    return content


def store_secret(content: str) -> None:
    w = ws()
    if SECRET_SCOPE not in {s.name for s in w.secrets.list_scopes()}:
        w.secrets.create_scope(SECRET_SCOPE)
    w.secrets.put_secret(SECRET_SCOPE, SECRET_KEY, string_value=content)
    print(f"[ok] credential stored as secret {SECRET_SCOPE}/{SECRET_KEY} (for notebooks)")


def teardown(restore_scope: bool) -> None:
    w = ws()
    try:
        w.secrets.delete_secret(SECRET_SCOPE, SECRET_KEY)
        print(f"[ok] deleted secret {SECRET_SCOPE}/{SECRET_KEY}")
    except Exception:  # noqa: BLE001 - scope or key absent
        pass
    for fn, name in ((w.recipients.delete, RECIPIENT), (w.shares.delete, SHARE)):
        try:
            fn(name)
            print(f"[ok] deleted {name}")
        except NotFound:
            print(f"[ok] {name} not present")
    if restore_scope:
        s = w.metastores.summary()
        w.metastores.update(id=s.metastore_id, delta_sharing_scope=uc.DeltaSharingScopeEnum.INTERNAL)
        print(f"[ok] delta sharing scope = {w.metastores.summary().delta_sharing_scope.value}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["setup", "teardown"])
    ap.add_argument("--profile", type=Path, default=Path.home() / ".lab" / "shop.share")
    ap.add_argument("--token-days", type=int, default=120)
    ap.add_argument("--restore-scope", action="store_true", help="teardown: set sharing scope back to INTERNAL")
    ap.add_argument("--secret", action="store_true", help="setup: also store the credential as secret lab/shop_share")
    a = ap.parse_args(argv)
    if a.action == "setup":
        enable_external_sharing(a.token_days)
        res = ensure_share(TABLES)
        content = ensure_recipient(a.profile.expanduser())
        if a.secret:
            store_secret(content)
        return 0 if all(not v.startswith("ERR") for v in res.values()) else 1
    teardown(a.restore_scope)
    return 0


if __name__ == "__main__":
    sys.exit(main())
