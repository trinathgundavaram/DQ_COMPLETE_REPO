"""
db/connection_factory.py  (Airflow package -- Teradata + Postgres only)
-------------------------------------------------------------------------
Source connections for this engine instance: exactly ONE connection per
source_type. This copy of connection_factory.py, kept specifically in
the airflow/ folder, supports only 'teradata' and 'postgres' -- the flat
file (CSV/Excel/TSV/Parquet) and S3 adapters from the original repo's
db/connection_factory.py have been removed here on request, since this
Airflow integration only needs Teradata + Postgres connectivity.

A rule (or sampling config) selects its source by type
(`gre_rules.sql_dialect` / `gre_sampling_config.source_type`), and
ConnectionFactory hands back that type's single adapter -- unchanged
from the original design, just with fewer types registered.

FileAdapter/S3Adapter below are intentionally left as disabled stub
classes rather than deleted outright: rules_engine/parallel.py (an
unmodified copy of the original rules_engine/, per this package's "no
changes inside rules_engine/" rule) imports FileAdapter and S3Adapter by
name and does `isinstance(adapter, (FileAdapter, S3Adapter))` checks.
Deleting the classes would break that import and force a change inside
rules_engine/ itself. The stubs satisfy that import/isinstance contract
but can never be built or produce a working connection here -- neither
is registered in _TYPE_MAP, and calling .build() on either raises
NotImplementedError immediately, so a rule pointed at a file/s3 source
fails loudly and immediately rather than silently doing nothing.

Adapter interface (SourceAdapter ABC)
--------------------------------------
    cursor()              -> DB-API-compatible cursor, for running SELECTs
    commit()               -> commit the current transaction (no-op for read-only sources)
    close()                 -> release the underlying connection
    ping() -> bool           -> lightweight liveness check (default: SELECT 1)
    prepare(rule)              -> per-rule setup before rule_syntax runs (no-op --
                                  neither teradata nor postgres needs this)
    qualified_name(rule) -> str -> the FROM-clause identifier for the
                                  auto-generated total-record count
                                  (rules_engine/executor.py::_build_total_query);
                                  "database_name.src_tbl_nm"
    source_type: str            -> 'teradata' | 'postgres'

Env vars
--------
    TERADATA_HOST / TERADATA_USER / TERADATA_PASSWORD / TERADATA_LOGMECH (LDAP)
    POSTGRES_HOST / POSTGRES_PORT (5432) / POSTGRES_DATABASE / POSTGRES_USER /
        POSTGRES_PASSWORD / POSTGRES_SSLMODE (prefer)

Usage
-----
    cf = ConnectionFactory()
    cf.load()                        # build every connection that's configured
    td = cf.get("teradata")          # cached; ping-checked and reconnected if stale
    td_fresh = cf.new_connection("teradata")   # independent copy, for parallel workers
"""

import logging
import os
import threading
from abc import ABC, abstractmethod
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"Required env var '{key}' is not set.")
    return val


# =============================================================================
# Base interface
# =============================================================================

class SourceAdapter(ABC):
    """Minimal DB-API 2.0-compatible wrapper around a source connection."""

    source_type: str = "unknown"

    # Best-effort "where is this actually pointed at" for logging only --
    # host[:port]/database, NEVER credentials. Set by each adapter's
    # build(); lets a caller log exactly which environment a connection
    # resolved to (see rules_engine/runner.py's "run starting" log lines)
    # -- the kind of detail that catches a run silently pointed at the
    # wrong Teradata/Postgres system, which otherwise surfaces as a
    # confusing "column not present" error against a schema that looks
    # identical by name.
    host: str = None

    @abstractmethod
    def cursor(self):
        ...

    @abstractmethod
    def commit(self):
        ...

    @abstractmethod
    def close(self):
        ...

    def ping(self) -> bool:
        try:
            cur = self.cursor()
        except Exception:
            return False
        try:
            cur.execute("SELECT 1")
            return True
        except Exception:
            return False
        finally:
            cur.close()

    def prepare(self, rule: dict) -> None:
        """Per-rule setup hook. No-op for teradata/postgres."""

    def qualified_name(self, rule: dict) -> str:
        """FROM-clause identifier for rule['database_name']/rule['src_tbl_nm']."""
        return f"{rule['database_name']}.{rule['src_tbl_nm']}"


# =============================================================================
# Teradata -- teradatasql
# =============================================================================

try:
    import teradatasql
    _TERADATA_AVAILABLE = True
except ImportError:
    _TERADATA_AVAILABLE = False
    logger.warning("teradatasql not installed -- Teradata connections unavailable.")


class TeradataAdapter(SourceAdapter):
    source_type = "teradata"

    def __init__(self, conn, host: str = None):
        self._conn = conn
        self.host = host

    def cursor(self):
        return self._conn.cursor()

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()

    @classmethod
    def build(cls) -> "TeradataAdapter":
        if not _TERADATA_AVAILABLE:
            raise ImportError("teradatasql is required. Install with: pip install teradatasql")
        host = _require("TERADATA_HOST")
        user = _require("TERADATA_USER")
        logmech = os.getenv("TERADATA_LOGMECH", "LDAP")
        # host/user/logmech only -- never the password. This is exactly the
        # detail needed to catch "app is pointed at the wrong Teradata
        # system" (dev vs. test vs. prod, or a typo'd host) -- the kind of
        # mismatch that otherwise surfaces as a confusing "column not
        # present" error against a schema that looks identical by name.
        logger.info("Connecting to Teradata: host=%s user=%s logmech=%s", host, user, logmech)
        return cls(teradatasql.connect(
            host=host,
            user=user,
            password=_require("TERADATA_PASSWORD"),
            logmech=logmech,
        ), host=host)


# =============================================================================
# PostgreSQL / AWS RDS & Aurora (PostgreSQL-compatible) -- psycopg2
# =============================================================================

try:
    import psycopg2
    _POSTGRES_AVAILABLE = True
except ImportError:
    _POSTGRES_AVAILABLE = False
    logger.warning("psycopg2-binary not installed -- Postgres/RDS connections unavailable.")


class PostgresAdapter(SourceAdapter):
    source_type = "postgres"

    def __init__(self, conn, host: str = None):
        self._conn = conn
        self._conn.autocommit = True   # read-only source queries; avoid open txns
        self.host = host

    def cursor(self):
        return self._conn.cursor()

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()

    @classmethod
    def build(cls) -> "PostgresAdapter":
        if not _POSTGRES_AVAILABLE:
            raise ImportError("psycopg2-binary is required. Install with: pip install psycopg2-binary")
        host = _require("POSTGRES_HOST")
        port = int(os.getenv("POSTGRES_PORT", "5432"))
        dbname = _require("POSTGRES_DATABASE")
        user = _require("POSTGRES_USER")
        # host/port/dbname/user only -- never the password. Same rationale
        # as TeradataAdapter.build() above.
        logger.info("Connecting to Postgres: host=%s port=%d dbname=%s user=%s", host, port, dbname, user)
        return cls(psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=_require("POSTGRES_PASSWORD"),
            sslmode=os.getenv("POSTGRES_SSLMODE", "prefer"),
        ), host=f"{host}:{port}/{dbname}")


# =============================================================================
# Disabled stubs -- file/S3 connectivity removed from this Airflow package.
#
# Kept ONLY so rules_engine/parallel.py's unmodified
# `from db.connection_factory import FileAdapter, S3Adapter` and its
# isinstance() checks keep working without touching rules_engine/ itself.
# Neither type is registered in _TYPE_MAP below, so ConnectionFactory
# never builds one on its own; .build() also raises immediately, so a
# rule that's still configured for a 'file'/'s3' source_type fails loudly
# instead of silently doing nothing.
# =============================================================================

class FileAdapter(SourceAdapter):
    """Disabled in this package -- Teradata + Postgres connectivity only."""

    source_type = "file"

    def __init__(self, *args, **kwargs):
        raise NotImplementedError(
            "File source connectivity is not included in this Airflow package "
            "(Teradata + Postgres only)."
        )

    def cursor(self):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    @classmethod
    def build(cls) -> "FileAdapter":
        raise NotImplementedError(
            "File source connectivity is not included in this Airflow package "
            "(Teradata + Postgres only)."
        )


class S3Adapter(SourceAdapter):
    """Disabled in this package -- Teradata + Postgres connectivity only."""

    source_type = "s3"

    def __init__(self, *args, **kwargs):
        raise NotImplementedError(
            "S3 source connectivity is not included in this Airflow package "
            "(Teradata + Postgres only)."
        )

    def cursor(self):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    @classmethod
    def build(cls) -> "S3Adapter":
        raise NotImplementedError(
            "S3 source connectivity is not included in this Airflow package "
            "(Teradata + Postgres only)."
        )


# =============================================================================
# ConnectionFactory -- one adapter per source_type
# =============================================================================

_TYPE_MAP = {
    "teradata": TeradataAdapter,
    "postgres": PostgresAdapter,
}


class ConnectionFactory:
    """
    Builds and caches at most one adapter per source_type ('teradata',
    'postgres' -- the only two registered in this package).
    """

    def __init__(self):
        self._conns: Dict[str, SourceAdapter] = {}

    def load(self) -> None:
        """
        Build every source_type this environment has credentials for.
        A type that fails to build (not configured, driver missing, bad
        credentials) is logged and skipped -- not every deployment needs
        both.
        """
        for source_type in _TYPE_MAP:
            try:
                self._conns[source_type] = self._build(source_type)
                logger.info("Connection '%s' established.", source_type)
            except Exception as exc:
                logger.warning("Connection '%s' not initialised: %s", source_type, exc)

    def get(self, source_type: str) -> Optional[SourceAdapter]:
        """Cached adapter for `source_type`, ping-reconnected if stale. None if unavailable."""
        adapter = self._conns.get(source_type)
        if adapter is None:
            logger.error("Connection '%s' not found in pool.", source_type)
            return None

        if isinstance(adapter, (FileAdapter, S3Adapter)):
            return adapter   # unreachable in practice -- never built by this package

        if not adapter.ping():
            logger.warning("Connection '%s' is stale -- reconnecting.", source_type)
            try:
                new_adapter = self._build(source_type)
            except Exception as exc:
                logger.error("Reconnect failed for '%s': %s", source_type, exc, exc_info=True)
                return None
            # Build succeeded before we drop the old handle, so a failed
            # reconnect never leaves this source_type with no adapter at
            # all. Close the stale connection only now, after the new one
            # is already in place -- otherwise a dead TCP/session handle
            # (and, for Teradata/Postgres, its server-side session slot)
            # would leak on every stale-connection reconnect.
            try:
                adapter.close()
            except Exception as exc:
                logger.warning("Error closing stale connection '%s': %s", source_type, exc)
            adapter = new_adapter
            self._conns[source_type] = adapter
            logger.info("Connection '%s' reconnected.", source_type)

        return adapter

    def new_connection(self, source_type: str) -> Optional[SourceAdapter]:
        """
        A FRESH, independent adapter for `source_type` (not cached) -- for
        giving each parallel worker its own session.
        """
        adapter_cls = _TYPE_MAP.get(source_type)
        if adapter_cls in (FileAdapter, S3Adapter):
            return self._conns.get(source_type)   # unreachable -- not in _TYPE_MAP

        try:
            return self._build(source_type)
        except Exception as exc:
            logger.error("Failed to create fresh connection '%s': %s", source_type, exc, exc_info=True)
            return None

    def close_all(self) -> None:
        """Close every open adapter and clear the pool."""
        for source_type, adapter in list(self._conns.items()):
            try:
                adapter.close()
            except Exception as exc:
                logger.warning("Error closing connection '%s': %s", source_type, exc)
        self._conns.clear()
        logger.info("All connections closed.")

    def get_all(self) -> dict:
        return dict(self._conns)

    def _build(self, source_type: str) -> SourceAdapter:
        adapter_cls = _TYPE_MAP.get(source_type)
        if adapter_cls is None:
            raise ValueError(
                f"Unknown/unsupported source_type '{source_type}' for this Airflow package. "
                f"Supported: {', '.join(sorted(_TYPE_MAP))}"
            )
        return adapter_cls.build()


def build_and_load_connection_factory() -> "ConnectionFactory":
    """
    ConnectionFactory() + .load() in one call -- the two-line "bring up
    every configured source_type" boilerplate run_rules.py needs before
    it can do anything.
    """
    cf = ConnectionFactory()
    cf.load()
    return cf
