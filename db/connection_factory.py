"""
db/connection_factory.py
--------------------------
Source connections for this engine instance: exactly ONE connection per
source_type -- teradata, postgres (RDS/Aurora-compatible), s3, file. No
named/multi-connection setup: a rule (or sampling config) selects its
source by type (`gre_rules.sql_dialect` / `gre_sampling_config.source_type`),
and ConnectionFactory hands back that type's single adapter.

Adapter interface (SourceAdapter ABC)
--------------------------------------
    cursor()              -> DB-API-compatible cursor, for running SELECTs
    commit()               -> commit the current transaction (no-op for read-only sources)
    close()                 -> release the underlying connection
    ping() -> bool           -> lightweight liveness check (default: SELECT 1)
    prepare(rule)              -> per-rule setup before rule_sql runs (no-op by
                                  default; FileAdapter/S3Adapter register a
                                  DuckDB view from rule['database_name']/
                                  rule['table_name'] -- the metadata table
                                  IS the source of the file/S3 path, nothing
                                  else to configure per rule)
    qualified_name(rule) -> str -> the FROM-clause identifier for the
                                  auto-generated total-record count
                                  (rules_engine/executor.py::_build_total_query);
                                  "database_name.table_name" for a real DB,
                                  the prepared view name for file/S3
    source_type: str            -> 'teradata' | 'postgres' | 's3' | 'file'

Env vars
--------
    TERADATA_HOST / TERADATA_USER / TERADATA_PASSWORD / TERADATA_LOGMECH (LDAP)
    POSTGRES_HOST / POSTGRES_PORT (5432) / POSTGRES_DATABASE / POSTGRES_USER /
        POSTGRES_PASSWORD / POSTGRES_SSLMODE (prefer)
    S3_REGION / S3_ACCESS_KEY_ID / S3_SECRET_ACCESS_KEY / S3_SESSION_TOKEN /
        S3_ENDPOINT -- all optional; falls back to AWS_DEFAULT_REGION /
        AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN, or the
        instance/task IAM role if none are set
    FILE_BASE_PATH -- optional root directory used only when a rule's
        database_name is empty/relative; a rule normally supplies its own
        full directory via database_name

Usage
-----
    cf = ConnectionFactory()
    cf.load()                        # build every connection that's configured
    td = cf.get("teradata")          # cached; ping-checked and reconnected if stale
    td_fresh = cf.new_connection("teradata")   # independent copy, for parallel workers
"""

import logging
import os
import re
import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"Required env var '{key}' is not set.")
    return val


def _escape(value: str) -> str:
    return value.replace("'", "''")


def _view_name(table_name: str) -> str:
    """
    A safe SQL identifier for a file/S3 rule's DuckDB view, derived from
    table_name (e.g. "claims.csv" -> "claims", "pull_date=*/*.parquet" ->
    "pull_date___"). rule_sql for a file/S3 rule references this same name,
    so keep table_name simple (no need to match it exactly -- just be aware
    non-alnum characters become underscores).
    """
    stem = Path(table_name).stem or table_name
    cleaned = re.sub(r"[^0-9a-zA-Z_]", "_", stem) or "t"
    return f"t_{cleaned}" if cleaned[0].isdigit() else cleaned


# =============================================================================
# Base interface
# =============================================================================

class SourceAdapter(ABC):
    """Minimal DB-API 2.0-compatible wrapper around a source connection."""

    source_type: str = "unknown"

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
            cur.execute("SELECT 1")
            cur.close()
            return True
        except Exception:
            return False

    def prepare(self, rule: dict) -> None:
        """Per-rule setup hook. No-op for real databases; file/S3 override this."""

    def qualified_name(self, rule: dict) -> str:
        """FROM-clause identifier for rule['database_name']/rule['table_name']."""
        return f"{rule['database_name']}.{rule['table_name']}"


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

    def __init__(self, conn):
        self._conn = conn

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
        return cls(teradatasql.connect(
            host=_require("TERADATA_HOST"),
            user=_require("TERADATA_USER"),
            password=_require("TERADATA_PASSWORD"),
            logmech=os.getenv("TERADATA_LOGMECH", "LDAP"),
        ))


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

    def __init__(self, conn):
        self._conn = conn
        self._conn.autocommit = True   # read-only source queries; avoid open txns

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
        return cls(psycopg2.connect(
            host=_require("POSTGRES_HOST"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=_require("POSTGRES_DATABASE"),
            user=_require("POSTGRES_USER"),
            password=_require("POSTGRES_PASSWORD"),
            sslmode=os.getenv("POSTGRES_SSLMODE", "prefer"),
        ))


# =============================================================================
# Flat files (CSV/Excel/TSV/Parquet) + S3 -- DuckDB + pandas
# =============================================================================

try:
    import duckdb
    _DUCKDB_AVAILABLE = True
except ImportError:
    _DUCKDB_AVAILABLE = False
    logger.warning("duckdb not installed -- file/S3 source connections unavailable.")

try:
    import pandas as pd
    _PANDAS_AVAILABLE = True
except ImportError:
    _PANDAS_AVAILABLE = False
    logger.warning("pandas not installed -- file source connections unavailable.")

_MAX_FILE_SIZE_MB = int(os.getenv("GRE_MAX_FILE_SIZE_MB", "500"))
_FILE_ENCODING = os.getenv("GRE_FILE_ENCODING", "utf-8")


def _read_file(full_path: str):
    """Read one file into a pandas DataFrame. Supports .csv/.tsv/.xlsx/.xls/.parquet."""
    ext = Path(full_path).suffix.lower()

    if _MAX_FILE_SIZE_MB > 0:
        size_mb = Path(full_path).stat().st_size / (1024 * 1024)
        if size_mb > _MAX_FILE_SIZE_MB:
            raise ValueError(f"File '{full_path}' is {size_mb:.1f} MB, exceeds GRE_MAX_FILE_SIZE_MB={_MAX_FILE_SIZE_MB}.")

    if ext == ".csv":
        return pd.read_csv(full_path, encoding=_FILE_ENCODING)
    if ext == ".tsv":
        return pd.read_csv(full_path, sep="\t", encoding=_FILE_ENCODING)
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(full_path)
    if ext == ".parquet":
        return pd.read_parquet(full_path)
    raise ValueError(f"Unsupported file extension '{ext}'. Supported: .csv, .tsv, .xlsx, .xls, .parquet")


class FileAdapter(SourceAdapter):
    """
    DuckDB-backed adapter for CSV/Excel/TSV/Parquet files on local/mounted
    disk. A rule drives its own path entirely from the metadata table --
    no per-rule setup outside gre_rules is needed:
        database_name -> the directory the file lives in (absolute, or
                          relative to FILE_BASE_PATH if that's set)
        table_name    -> the filename (e.g. "claims.csv")
    prepare(rule) loads the file once (guarded by a lock) into a shared
    DataFrame registry, then registers it as a DuckDB view -- named via
    _view_name(table_name) -- in each thread's own connection (DuckDB
    connections aren't safe to share across threads). rule_sql references
    that same view name.
    """

    source_type = "file"

    def __init__(self, base_path: str = ""):
        if not _DUCKDB_AVAILABLE or not _PANDAS_AVAILABLE:
            raise ImportError("duckdb and pandas are required. Install with: pip install duckdb pandas openpyxl")
        self._base_path = base_path.rstrip("/") if base_path else ""
        self._df_registry: dict = {}
        self._registry_lock = threading.Lock()
        self._local = threading.local()

    def cursor(self):
        return self._get_thread_conn().cursor()

    def commit(self):
        pass   # read-only

    def close(self):
        if hasattr(self._local, "conn"):
            try:
                self._local.conn.close()
            except Exception:
                pass
            del self._local.conn

    def ping(self) -> bool:
        try:
            self._get_thread_conn().execute("SELECT 1")
            return True
        except Exception:
            return False

    def prepare(self, rule: dict) -> None:
        table_name = (rule.get("table_name") or "").strip()
        if not table_name:
            raise ValueError("table_name is empty -- cannot prepare file source.")
        view_name = _view_name(table_name)

        if view_name not in self._df_registry:
            with self._registry_lock:
                if view_name not in self._df_registry:
                    full_path = self._resolve_path(table_name, (rule.get("database_name") or "").strip())
                    df = _read_file(full_path)
                    df.columns = [c.lower() for c in df.columns]
                    self._df_registry[view_name] = df
                    logger.info("File loaded: %s -> view '%s' (%d rows)", full_path, view_name, len(df))

        conn = self._get_thread_conn()
        registered = getattr(self._local, "registered", set())
        if view_name not in registered:
            conn.register(view_name, self._df_registry[view_name])
            registered.add(view_name)
            self._local.registered = registered

    def qualified_name(self, rule: dict) -> str:
        return _view_name(rule["table_name"])

    @classmethod
    def build(cls) -> "FileAdapter":
        return cls(base_path=os.getenv("FILE_BASE_PATH", ""))

    def _get_thread_conn(self):
        if not hasattr(self._local, "conn"):
            conn = duckdb.connect(":memory:")
            registered = set()
            for vname, df in self._df_registry.items():
                conn.register(vname, df)
                registered.add(vname)
            self._local.conn = conn
            self._local.registered = registered
        return self._local.conn

    def _resolve_path(self, table_name: str, database_name: str) -> str:
        if os.path.isabs(table_name):
            return table_name
        base = database_name or self._base_path
        return f"{base.rstrip('/')}/{table_name}" if base else table_name


class S3Adapter(SourceAdapter):
    """
    DuckDB-over-S3: queries Parquet/CSV objects directly on S3 with full SQL
    (joins, aggregates, window functions). Same metadata-driven convention as
    FileAdapter -- no per-rule setup outside gre_rules:
        database_name -> the s3:// prefix/bucket (e.g. "s3://bucket/claims")
        table_name    -> the object key or glob under that prefix (e.g.
                          "pull_date=*/*.parquet") -- also names the view
                          rule_sql references, via _view_name(table_name)

    One DuckDB connection per thread (thread-local, mirrors FileAdapter),
    each configured with the httpfs extension and S3 credentials read from
    env vars AT CONNECT TIME (never cached), so rotated credentials take
    effect on the next new thread without a restart.
    """

    source_type = "s3"

    def __init__(self):
        if not _DUCKDB_AVAILABLE:
            raise ImportError("duckdb is required for S3 source connections. Install with: pip install duckdb")
        self._local = threading.local()

    def cursor(self):
        return self._get_thread_conn().cursor()

    def commit(self):
        pass   # read-only

    def close(self):
        if hasattr(self._local, "conn"):
            try:
                self._local.conn.close()
            except Exception:
                pass
            del self._local.conn

    def ping(self) -> bool:
        try:
            self._get_thread_conn().execute("SELECT 1")
            return True
        except Exception:
            return False

    def prepare(self, rule: dict) -> None:
        table_name = (rule.get("table_name") or "").strip()
        prefix = (rule.get("database_name") or "").strip()
        if not table_name or not prefix:
            raise ValueError("S3 rules require database_name (s3:// prefix) and table_name (key/glob).")

        view_name = _view_name(table_name)
        conn = self._get_thread_conn()
        registered = getattr(self._local, "registered", set())
        if view_name in registered:
            return

        uri = f"{prefix.rstrip('/')}/{table_name}"
        conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {self._reader_fn(uri)}")
        registered.add(view_name)
        self._local.registered = registered
        logger.info("S3 view prepared: %s -> %s", view_name, uri)

    def qualified_name(self, rule: dict) -> str:
        return _view_name(rule["table_name"])

    @classmethod
    def build(cls) -> "S3Adapter":
        if not (os.getenv("S3_REGION") or os.getenv("AWS_DEFAULT_REGION")):
            logger.warning(
                "Neither S3_REGION nor AWS_DEFAULT_REGION is set -- "
                "DuckDB httpfs will use its own default region resolution."
            )
        return cls()

    def _get_thread_conn(self):
        if not hasattr(self._local, "conn"):
            conn = duckdb.connect(":memory:")
            conn.execute("INSTALL httpfs")
            conn.execute("LOAD httpfs")
            self._configure_credentials(conn)
            self._local.conn = conn
            self._local.registered = set()
        return self._local.conn

    @staticmethod
    def _configure_credentials(conn) -> None:
        region = os.getenv("S3_REGION") or os.getenv("AWS_DEFAULT_REGION")
        if region:
            conn.execute(f"SET s3_region='{_escape(region)}'")

        access_key = os.getenv("S3_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("S3_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
        if access_key and secret_key:
            conn.execute(f"SET s3_access_key_id='{_escape(access_key)}'")
            conn.execute(f"SET s3_secret_access_key='{_escape(secret_key)}'")
        else:
            logger.debug("No explicit S3 access key/secret -- relying on IAM role / default credential chain.")

        session_token = os.getenv("S3_SESSION_TOKEN") or os.getenv("AWS_SESSION_TOKEN")
        if session_token:
            conn.execute(f"SET s3_session_token='{_escape(session_token)}'")

        endpoint = os.getenv("S3_ENDPOINT")
        if endpoint:
            conn.execute(f"SET s3_endpoint='{_escape(endpoint)}'")
            conn.execute("SET s3_url_style='path'")

    @staticmethod
    def _reader_fn(uri: str) -> str:
        lower, u = uri.lower(), _escape(uri)
        if lower.endswith(".csv") or lower.endswith(".tsv"):
            return f"read_csv_auto('{u}', union_by_name=true)"
        return f"read_parquet('{u}', union_by_name=true, hive_partitioning=1)"


# =============================================================================
# ConnectionFactory -- one adapter per source_type
# =============================================================================

_TYPE_MAP = {
    "teradata": TeradataAdapter,
    "postgres": PostgresAdapter,
    "s3": S3Adapter,
    "file": FileAdapter,
}


class ConnectionFactory:
    """
    Builds and caches at most one adapter per source_type ('teradata',
    'postgres', 's3', 'file'). A FileAdapter/S3Adapter is never
    "reconnected" -- DuckDB is always alive in-process; the other two
    support ping-based staleness detection.
    """

    def __init__(self):
        self._conns: Dict[str, SourceAdapter] = {}

    def load(self) -> None:
        """
        Build every source_type this environment has credentials for.
        A type that fails to build (not configured, driver missing, bad
        credentials) is logged and skipped -- not every deployment needs
        all four.
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
            return adapter   # DuckDB-in-process -- always alive, skip ping overhead

        if not adapter.ping():
            logger.warning("Connection '%s' is stale -- reconnecting.", source_type)
            try:
                adapter = self._build(source_type)
                self._conns[source_type] = adapter
                logger.info("Connection '%s' reconnected.", source_type)
            except Exception as exc:
                logger.error("Reconnect failed for '%s': %s", source_type, exc, exc_info=True)
                return None

        return adapter

    def new_connection(self, source_type: str) -> Optional[SourceAdapter]:
        """
        A FRESH, independent adapter for `source_type` (not cached) -- for
        giving each parallel worker its own session. File/S3 adapters are
        already thread-safe internally, so this returns the same shared
        instance for those rather than building a redundant copy.
        """
        adapter_cls = _TYPE_MAP.get(source_type)
        if adapter_cls in (FileAdapter, S3Adapter):
            return self._conns.get(source_type)

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
                f"Unknown source_type '{source_type}'. Supported: {', '.join(sorted(_TYPE_MAP))}"
            )
        return adapter_cls.build()
