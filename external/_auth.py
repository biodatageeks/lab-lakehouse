from __future__ import annotations

from lab.common import CATALOG, conn, ws

ICEBERG_REST_PATH = "/api/2.1/unity-catalog/iceberg-rest"

# Markers of the Free Edition default-storage restriction.
DEFAULT_STORAGE_DENIED = (
    "TABLE_DB_STORAGE",
    "EXTERNAL_ACCESS_NOT_ALLOWED",
    "ACCESS_DENIED",
    "explicit deny",
    "AccessDenied",
    "Failed to execute scan",
    "HTTP GET error reading 's3://",
)


def denied_by_default_storage(err: str | None) -> bool:
    return bool(err) and any(m in err for m in DEFAULT_STORAGE_DENIED)


def region() -> str:
    return ws().metastores.summary().region or "us-east-2"


def iceberg_uri() -> str:
    return conn().host + ICEBERG_REST_PATH


def redact(s: str) -> str:
    tok = conn().token
    return s.replace(tok, "***") if tok else s


__all__ = ["CATALOG", "conn", "iceberg_uri", "region", "redact", "denied_by_default_storage"]
