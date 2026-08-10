"""
db/connection_factory.py
-------------------------
Multi-source connection factory.

Builds and caches a pool of named source adapters from config/connections.yaml
(the connection catalogue -- see config/connections.py for its schema and
validation rules). Each entry there declares a connection's `name` and
`source_type`, plus whatever non-secret settings that adapter needs (host,
port, database, region, ...). Credentials are never read from that file --
every adapter's build() classmethod (db/adapters.py) reads them from
DQ_<NAME>_* environment variables at connect time instead.

Supported types
---------------
    teradata    → TeradataAdapter   (teradatasql)
    postgresql  → PostgresAdapter   (psycopg2; also covers AWS Aurora PG)
    aurora      → PostgresAdapter   (alias for postgresql)
    sqlserver   → SqlServerAdapter  (pyodbc)
    file        → FileAdapter       (DuckDB + pandas, CSV/Excel/TSV/Parquet)
    s3          → S3Adapter         (DuckDB-over-S3, Parquet/CSV)

Example config/connections.yaml
--------------------------------
    connections:
      - name: teradata
        source_type: teradata
        host: your-teradata-host.example.com

      - name: claims_pg
        source_type: postgresql
        host: claims-aurora.cluster-xyz.us-east-1.rds.amazonaws.com
        database: claims

      - name: raw_files
        source_type: file
        base_path: /data/inputs/

Matching secrets (env vars, prefix DQ_<NAME>_ — never in the YAML file):
    DQ_TERADATA_USER=...
    DQ_TERADATA_PASSWORD=...
    DQ_CLAIMS_PG_USER=...
    DQ_CLAIMS_PG_PASSWORD=...

Usage
-----
    cf = ConnectionFactory()
    cf.load()                             # open / initialise all active connections
    td = cf.get("teradata")               # cached; auto-reconnects if stale
    td_fresh = cf.new_connection("teradata")  # fresh per-thread copy
"""

import logging
import threading
from typing import Dict, Optional

from config.connections import load_connections
from db.adapters import (
    SourceAdapter, TeradataAdapter, PostgresAdapter,
    SqlServerAdapter, FileAdapter, S3Adapter,
)

logger = logging.getLogger(__name__)

# Map source_type values (config/connections.yaml) → adapter build classmethod
_TYPE_MAP = {
    # ── Sanctioned for this engine instance (Section 2): teradata, postgresql, s3 ──
    "teradata":   TeradataAdapter,
    "postgresql": PostgresAdapter,
    "postgres":   PostgresAdapter,   # alias
    "aurora":     PostgresAdapter,   # alias — Aurora PG-compatible
    "s3":         S3Adapter,         # DuckDB-over-S3 (Parquet/CSV, read directly)
    # ── Adapter interface stays pluggable; this exists but is untested/
    #    uncatalogued for this instance until a real use case needs it.
    #    Adding a new source = one new adapter file + one new entry here.
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
        self._entries: Dict[str, dict] = {}   # name -> config/connections.yaml entry
        # get()'s stale-reconnect path is called concurrently from
        # ThreadPoolExecutor worker threads during run_engine(). Without a
        # lock, two threads that both observe the same connection as stale
        # each independently rebuild and overwrite self._conns[name], and
        # whichever adapter loses that race is silently orphaned (leaked —
        # never explicitly closed). Per-name locks confine reconnects for
        # different connections to run independently, while a small guard
        # lock protects lazy creation of those per-name locks themselves.
        self._locks: Dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self):
        """
        Load config/connections.yaml (DQ_CONNECTIONS_FILE override) and
        initialise every active connection in it.

        A malformed connections file is a deployment/config error, not a
        transient one -- ConnectionConfigError is NOT caught here and
        propagates to the caller, so the process fails fast at startup
        with a specific, actionable message instead of silently starting
        with zero usable connections. An individual connection failing to
        connect (bad host, wrong credentials, network issue) is different
        -- that's logged and skipped so the other connections still load.
        """
        self._entries = load_connections()

        for name, entry in self._entries.items():
            try:
                self._conns[name] = self._build(name, entry)
                logger.info("Connection '%s' (%s) established.", name, entry["source_type"])
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

        # FileAdapter/S3Adapter are DuckDB-in-process — always alive, skip ping overhead
        if isinstance(adapter, (FileAdapter, S3Adapter)):
            return adapter

        if not adapter.ping():
            with self._lock_for(name):
                # Re-check under the lock: another thread may have already
                # rebuilt this connection while we were waiting. Compare by
                # identity (not a second ping()) to avoid a redundant round
                # trip and to definitively tell "still the stale instance I
                # observed" apart from "already replaced".
                current = self._conns.get(name)
                if current is not adapter:
                    return current

                logger.warning("Connection '%s' is stale — reconnecting.", name)
                try:
                    entry = self._entries.get(name, {})
                    adapter = self._build(name, entry)
                    self._conns[name] = adapter
                    logger.info("Connection '%s' reconnected.", name)
                except Exception as exc:
                    logger.error(
                        "Reconnect failed for '%s': %s", name, exc, exc_info=True
                    )
                    return None

        return adapter

    def _lock_for(self, name: str) -> threading.Lock:
        """Lazily create/return the per-connection-name reconnect lock."""
        with self._locks_guard:
            lock = self._locks.get(name)
            if lock is None:
                lock = threading.Lock()
                self._locks[name] = lock
            return lock

    def new_connection(self, name: str) -> Optional[SourceAdapter]:
        """
        Open and return a FRESH adapter (not cached).

        Use this to give each ThreadPoolExecutor worker its own independent
        Teradata metadata connection so concurrent writes never share state.

        FileAdapters are shared-safe (thread-local DuckDB connections) and
        should be obtained via get() rather than new_connection().
        Returns None on failure (including "no such connection in config").
        """
        entry = self._entries.get(name)
        if entry is None:
            logger.error("new_connection('%s'): no such connection in config/connections.yaml.", name)
            return None

        adapter_cls = _TYPE_MAP.get(entry["source_type"])
        if adapter_cls in (FileAdapter, S3Adapter):
            # File/S3 sources share the singleton adapter (per-thread DuckDB
            # connections inside it are already thread-safe); return the cached one.
            logger.debug(
                "new_connection('%s'): returning cached %s (shared-safe).",
                name, adapter_cls.__name__,
            )
            return self._conns.get(name)

        try:
            adapter = self._build(name, entry)
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

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build(self, name: str, entry: dict) -> SourceAdapter:
        """Dispatch to the correct adapter builder based on the entry's source_type."""
        src_type = entry["source_type"]
        adapter_cls = _TYPE_MAP.get(src_type)

        if adapter_cls is None:
            supported = ", ".join(sorted(_TYPE_MAP.keys()))
            raise ValueError(
                f"Unknown source type '{src_type}' for connection '{name}'. "
                f"Supported types: {supported}"
            )

        return adapter_cls.build(name, entry)
