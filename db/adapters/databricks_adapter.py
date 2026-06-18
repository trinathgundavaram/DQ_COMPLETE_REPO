"""
db/adapters/databricks_adapter.py
----------------------------------
Adapter for Databricks SQL Warehouses via the official
databricks-sql-connector (Thrift-based SQL connector).

Env vars required (prefix = DQ_<NAME>_):
    DQ_<NAME>_HOST          — Databricks workspace hostname
                              e.g. adb-1234567890123456.7.azuredatabricks.net
    DQ_<NAME>_HTTP_PATH     — SQL Warehouse HTTP path
                              e.g. /sql/1.0/warehouses/abc123def456
    DQ_<NAME>_TOKEN         — Personal access token (PAT) or service-principal
                              OAuth token
    DQ_<NAME>_CATALOG       (optional) — Unity Catalog name (default: hive_metastore)
    DQ_<NAME>_SCHEMA        (optional) — default schema / database

Notes
-----
* The databricks-sql-connector cursor returns rows as named tuples;
  cursor.description is standard DB-API format so the executor's
  `[c[0].lower() for c in cursor.description]` pattern works as-is.
* executemany() is NOT supported by the Databricks connector.
  This is not a problem because bulk_insert() is only called on td_conn
  (the Teradata metadata connection), never on a Databricks db_conn.
"""

import logging
import os

from db.adapters.base import SourceAdapter

logger = logging.getLogger(__name__)

try:
    from databricks import sql as databricks_sql
    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False
    logger.warning(
        "databricks-sql-connector not installed — "
        "Databricks connections unavailable."
    )


class DatabricksAdapter(SourceAdapter):
    """Wraps a databricks-sql-connector connection."""

    def __init__(self, conn):
        self._conn = conn

    # ------------------------------------------------------------------
    # SourceAdapter interface
    # ------------------------------------------------------------------

    def cursor(self):
        return self._conn.cursor()

    def commit(self):
        # Databricks SQL warehouses are read-committed; no explicit commit needed.
        pass

    def close(self):
        self._conn.close()

    def ping(self) -> bool:
        try:
            cur = self._conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
            return True
        except Exception:
            return False

    # prepare() is a no-op — inherited from base

    # ------------------------------------------------------------------
    # Factory helper
    # ------------------------------------------------------------------

    @classmethod
    def build(cls, name: str) -> "DatabricksAdapter":
        """Open a Databricks SQL connection using env vars for `name`."""
        if not _AVAILABLE:
            raise ImportError(
                "databricks-sql-connector is required for Databricks connections. "
                "Install with: pip install databricks-sql-connector"
            )

        prefix    = f"DQ_{name.upper()}"
        host      = _require(f"{prefix}_HOST")
        http_path = _require(f"{prefix}_HTTP_PATH")
        token     = _require(f"{prefix}_TOKEN")
        catalog   = os.getenv(f"{prefix}_CATALOG")
        schema    = os.getenv(f"{prefix}_SCHEMA")

        kwargs = dict(
            server_hostname=host,
            http_path=http_path,
            access_token=token,
        )
        if catalog:
            kwargs["catalog"] = catalog
        if schema:
            kwargs["schema"] = schema

        conn = databricks_sql.connect(**kwargs)
        logger.debug("Databricks SQL connected to %s%s", host, http_path)
        return cls(conn)


# ---------------------------------------------------------------------------

def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"Required env var '{key}' is not set.")
    return val
