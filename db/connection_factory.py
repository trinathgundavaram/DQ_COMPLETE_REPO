"""
db/connection_factory.py
-------------------------
Multi-source connection factory.

Manages a pool of named source adapters.  Each named connection in
DQ_CONNECTION_NAMES gets a DQ_<NAME>_TYPE env var that tells the factory
which adapter to build.

Supported types
---------------
    teradata    → TeradataAdapter   (teradatasql)
    postgresql  → PostgresAdapter   (psycopg2; also covers AWS Aurora PG)
    aurora      → PostgresAdapter   (alias for postgresql)
    databricks  → DatabricksAdapter (databricks-sql-connector)
    sqlserver   → SqlServerAdapter  (pyodbc)
    file        → FileAdapter       (DuckDB + pandas, CSV/Excel/TSV/Parquet)

Env vars
--------
    DQ_CONNECTION_NAMES          comma-separated list of connection names
                                 e.g. "teradata,claims_pg,datalake"

    Per connection  (prefix = DQ_<NAME>_):
        DQ_<NAME>_TYPE           one of the types listed above
        + driver-specific vars   see each adapter module for details

Example
-------
    DQ_CONNECTION_NAMES=teradata,claims_pg,datalake,erp_sql,raw_files

    DQ_TERADATA_TYPE=teradata
    DQ_TERADATA_HOST=...
    DQ_TERADATA_USER=...
    DQ_TERADATA_PASSWORD=...

    DQ_CLAIMS_PG_TYPE=postgresql
    DQ_CLAIMS_PG_HOST=claims-aurora.cluster-xyz.us-east-1.rds.amazonaws.com
    DQ_CLAIMS_PG_DATABASE=claims
    DQ_CLAIMS_PG_USER=...
    DQ_CLAIMS_PG_PASSWORD=...

    DQ_DATALAKE_TYPE=databricks
    DQ_DATALAKE_HOST=adb-1234.azuredatabricks.net
    DQ_DATALAKE_HTTP_PATH=/sql/1.0/warehouses/abc
    DQ_DATALAKE_TOKEN=dapi...

    DQ_ERP_SQL_TYPE=sqlserver
    DQ_ERP_SQL_HOST=erp-db.internal
    DQ_ERP_SQL_DATABASE=ERP
    DQ_ERP_SQL_USER=...
    DQ_ERP_SQL_PASSWORD=...

    DQ_RAW_FILES_TYPE=file
    DQ_RAW_FILES_BASE_PATH=/data/inputs/

Usage
-----
    cf = ConnectionFactory()
    cf.load()                             # open / initialise all connections
    td = cf.get("teradata")               # cached; auto-reconnects if stale
    td_fresh = cf.new_connection("teradata")  # fresh per-thread copy
"""

import logging
import os
from typing import Dict, Optional

from db.adapters.base import SourceAdapter
from db.adapters.teradata_adapter import TeradataAdapter
from db.adapters.postgres_adapter import PostgresAdapter
from db.adapters.databricks_adapter import DatabricksAdapter
from db.adapters.sqlserver_adapter import SqlServerAdapter
from db.adapters.file_adapter import FileAdapter

logger = logging.getLogger(__name__)

# Map DQ_<NAME>_TYPE values → adapter build classmethod
_TYPE_MAP = {
    "teradata":   TeradataAdapter,
    "postgresql": PostgresAdapter,
    "postgres":   PostgresAdapter,   # alias
    "aurora":     PostgresAdapter,   # alias — Aurora PG-compatible
    "databricks": DatabricksAdapter,
    "sqlserver":  SqlServerAdapter,
    "mssql":      SqlServerAdapter,  # alias
    "file":       FileAdapter,
    "csv":        FileAdapter,       # alias
    "excel":      FileAdapter,       # alias
}


class ConnectionFactory:
    """
    Builds and caches source-system adapters with health-check and reconnect.

    A FileAdapter is never "reconnected" — DuckDB is always alive in-process.
    All other adapters support ping-based staleness detection.
    """

    def __init__(self):
        self._conns: Dict[str, SourceAdapter] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self):
        """
        Initialise all connections listed in DQ_CONNECTION_NAMES.
        Failed connections are logged but do not abort the load.
        """
        names_raw = os.getenv("DQ_CONNECTION_NAMES", "teradata")
        names = [n.strip() for n in names_raw.split(",") if n.strip()]

        for name in names:
            try:
                self._conns[name] = self._build(name)
                src_type = self._get_type(name)
                logger.info("Connection '%s' (%s) established.", name, src_type)
            except Exception as exc:
                logger.error(
                    "Failed to initialise connection '%s': %s",
                    name, exc, exc_info=True,
                )

    def get(self, name: str) -> Optional[SourceAdapter]:
        """
        Return the cached adapter for `name`.

        For DB adapters (Teradata, Postgres, etc.) the connection is
        ping-checked and automatically reconnected if stale.
        FileAdapters are always considered alive.

        Returns None if the adapter is not found or cannot be reconnected.
        """
        adapter = self._conns.get(name)
        if adapter is None:
            logger.error("Connection '%s' not found in pool.", name)
            return None

        # FileAdapter is always alive — skip ping overhead
        if isinstance(adapter, FileAdapter):
            return adapter

        if not adapter.ping():
            logger.warning("Connection '%s' is stale — reconnecting.", name)
            try:
                adapter = self._build(name)
                self._conns[name] = adapter
                logger.info("Connection '%s' reconnected.", name)
            except Exception as exc:
                logger.error(
                    "Reconnect failed for '%s': %s", name, exc, exc_info=True
                )
                return None

        return adapter

    def new_connection(self, name: str) -> Optional[SourceAdapter]:
        """
        Open and return a FRESH adapter (not cached).

        Use this to give each ThreadPoolExecutor worker its own independent
        Teradata metadata connection so concurrent writes never share state.

        FileAdapters are shared-safe (thread-local DuckDB connections) and
        should be obtained via get() rather than new_connection().
        Returns None on failure.
        """
        adapter_cls = _TYPE_MAP.get(self._get_type(name))
        if adapter_cls is FileAdapter:
            # File sources share the singleton adapter; return the cached one.
            logger.debug(
                "new_connection('%s'): returning cached FileAdapter (shared-safe).", name
            )
            return self._conns.get(name)

        try:
            adapter = self._build(name)
            logger.debug("Fresh connection created for '%s'.", name)
            return adapter
        except Exception as exc:
            logger.error(
                "Failed to create fresh connection '%s': %s",
                name, exc, exc_info=True,
            )
            return None

    def close_all(self):
        """
        Close every open adapter and clear the pool.

        Call this in a finally block or SIGTERM handler after run_engine()
        finishes to prevent leaked connections to Teradata, Postgres, etc.
        """
        for name, adapter in list(self._conns.items()):
            try:
                adapter.close()
                logger.debug("Connection '%s' closed.", name)
            except Exception as exc:
                logger.warning("Error closing connection '%s': %s", name, exc)
        self._conns.clear()
        logger.info("All connections closed.")

    def get_all(self) -> dict:
        return dict(self._conns)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build(self, name: str) -> SourceAdapter:
        """Dispatch to the correct adapter builder based on DQ_<NAME>_TYPE."""
        src_type = self._get_type(name)
        adapter_cls = _TYPE_MAP.get(src_type)

        if adapter_cls is None:
            supported = ", ".join(sorted(_TYPE_MAP.keys()))
            raise ValueError(
                f"Unknown source type '{src_type}' for connection '{name}'. "
                f"Supported types: {supported}"
            )

        return adapter_cls.build(name)

    @staticmethod
    def _get_type(name: str) -> str:
        """
        Read DQ_<NAME>_TYPE, defaulting to 'teradata' for backward compatibility.
        """
        return os.getenv(f"DQ_{name.upper()}_TYPE", "teradata").lower()
