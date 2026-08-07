"""
db/adapters.py
-----------------
Every source-system connector in one place.

Adapter interface (SourceAdapter ABC)
--------------------------------------
    cursor()           -> DB-API-compatible cursor, for running SELECTs
    commit()            -> commit the current transaction (no-op for read-only sources)
    close()              -> release the underlying connection
    ping() -> bool        -> lightweight liveness check (default: SELECT 1)
    prepare(rule)          -> source-specific setup before a rule's queries run
                              (no-op by default; FileAdapter/S3Adapter use it to
                              register a DuckDB view for the rule's table)
    source_type: str        -> class attribute identifying the SQL dialect the
                              adapter speaks; used by rules_engine/rule_sql.py to pick
                              dialect-correct SQL and to enforce the dialect
                              guard (a rule's declared sql_dialect vs. this).

Sanctioned for this engine instance (Section 2 of DESIGN.md): teradata,
postgresql, s3. The SQL Server adapter is included and fully functional,
but is not catalogued/tested for the current use case — it exists to
prove the interface is genuinely pluggable, not to be deployed yet.

Config vs. secrets
-------------------
Every `build()` classmethod below takes two things: `name` (the
connection's name, e.g. "teradata") and `config` (that connection's
entry from config/connections.yaml, parsed and validated by
config/connections.py -- host, port, database, region, etc.).

Credentials are handled completely separately and are NEVER read from
`config`: every build() reads them from DQ_<NAME>_* environment variables
via `_require()`/`os.getenv()` at CALL time (never at import), so
credential rotation takes effect on the next connect without a process
restart, and no secret ever needs to be written to a file that could end
up in source control.

Adding a new source
--------------------
Add one class implementing SourceAdapter to this file (import its driver
lazily, same pattern as every class below) and one line in
_TYPE_MAP in connection_factory.py. Nothing else changes — that's the
actual test of "pluggable."
"""

import logging
import os
import threading
from abc import ABC, abstractmethod
from pathlib import Path

logger = logging.getLogger(__name__)


def _require(key: str) -> str:
    """Read a required SECRET from an env var. Raises if unset/empty."""
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"Required env var '{key}' is not set.")
    return val


def _require_config(config: dict, key: str, name: str):
    """Read a required NON-SECRET field from a connections.yaml entry.
    Raises if missing/empty -- fails fast with a message pointing at the
    config file rather than surfacing a confusing KeyError/TypeError once
    it reaches the driver's connect() call."""
    val = config.get(key)
    if val in (None, ""):
        raise ValueError(
            f"Connection '{name}' (source_type={config.get('source_type')!r}) is "
            f"missing required field '{key}' in config/connections.yaml."
        )
    return val


# =============================================================================
# Base interface
# =============================================================================

class SourceAdapter(ABC):
    """Minimal DB-API 2.0-compatible wrapper around a source connection."""

    source_type: str = "unknown"   # every subclass overrides this

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
        """Default liveness check — override for a cheaper one where available."""
        try:
            cur = self.cursor()
            cur.execute("SELECT 1")
            cur.close()
            return True
        except Exception as exc:
            logger.debug("%s.ping() failed (connection considered stale): %s",
                        type(self).__name__, exc)
            return False

    def prepare(self, rule: dict) -> None:
        """Per-rule setup hook. Default: no-op. See FileAdapter/S3Adapter."""


# =============================================================================
# Teradata — teradatasql
# =============================================================================

try:
    import teradatasql
    _TERADATA_AVAILABLE = True
except ImportError:
    _TERADATA_AVAILABLE = False
    logger.warning("teradatasql not installed — Teradata connections unavailable.")


class TeradataAdapter(SourceAdapter):
    """
    Non-secret config (config/connections.yaml): host, logmech (default LDAP).
    Secrets (env, prefix DQ_<NAME>_): USER, PASSWORD.
    """

    source_type: str = "teradata"

    def __init__(self, conn):
        self._conn = conn

    def cursor(self):
        return self._conn.cursor()

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()

    @classmethod
    def build(cls, name: str, config: dict) -> "TeradataAdapter":
        if not _TERADATA_AVAILABLE:
            raise ImportError("teradatasql is required. Install with: pip install teradatasql")
        prefix = f"DQ_{name.upper()}"
        conn = teradatasql.connect(
            host=_require_config(config, "host", name),
            user=_require(f"{prefix}_USER"),
            password=_require(f"{prefix}_PASSWORD"),
            logmech=config.get("logmech", "LDAP"),
        )
        return cls(conn)


# =============================================================================
# PostgreSQL / AWS Aurora (PostgreSQL-compatible) — psycopg2
# =============================================================================

try:
    import psycopg2
    _POSTGRES_AVAILABLE = True
except ImportError:
    _POSTGRES_AVAILABLE = False
    logger.warning("psycopg2-binary not installed — PostgreSQL/Aurora connections unavailable.")


class PostgresAdapter(SourceAdapter):
    """
    Non-secret config (config/connections.yaml): host, port (default 5432),
    database, sslmode (default prefer). Same connection shape for Aurora PG.
    Secrets (env, prefix DQ_<NAME>_): USER, PASSWORD.
    """

    source_type: str = "postgresql"

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
    def build(cls, name: str, config: dict) -> "PostgresAdapter":
        if not _POSTGRES_AVAILABLE:
            raise ImportError("psycopg2-binary is required. Install with: pip install psycopg2-binary")
        prefix = f"DQ_{name.upper()}"
        conn = psycopg2.connect(
            host=_require_config(config, "host", name),
            port=int(config.get("port", 5432)),
            dbname=_require_config(config, "database", name),
            user=_require(f"{prefix}_USER"),
            password=_require(f"{prefix}_PASSWORD"),
            sslmode=config.get("sslmode", "prefer"),
        )
        return cls(conn)


# =============================================================================
# Microsoft SQL Server — pyodbc
# =============================================================================

try:
    import pyodbc
    _SQLSERVER_AVAILABLE = True
except ImportError:
    _SQLSERVER_AVAILABLE = False
    logger.warning("pyodbc not installed — SQL Server connections unavailable.")


class SqlServerAdapter(SourceAdapter):
    """
    Requires the Microsoft ODBC Driver for SQL Server on the host.
    Non-secret config (config/connections.yaml): host, port (default 1433),
    database, driver (default "ODBC Driver 18 for SQL Server"), trust_cert
    (default "yes"), trusted_connection (default "no" — set "yes" for
    Windows Auth, which then omits USER/PASSWORD entirely).
    Secrets (env, prefix DQ_<NAME>_): USER, PASSWORD (skipped when
    trusted_connection is "yes").
    """

    source_type: str = "sqlserver"

    def __init__(self, conn):
        self._conn = conn
        self._conn.autocommit = True

    def cursor(self):
        return self._conn.cursor()

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()

    @classmethod
    def build(cls, name: str, config: dict) -> "SqlServerAdapter":
        if not _SQLSERVER_AVAILABLE:
            raise ImportError("pyodbc is required. Install with: pip install pyodbc")
        prefix   = f"DQ_{name.upper()}"
        host     = _require_config(config, "host", name)
        port     = config.get("port", 1433)
        database = _require_config(config, "database", name)
        driver   = config.get("driver", "ODBC Driver 18 for SQL Server")
        trust    = config.get("trust_cert", "yes")

        if str(config.get("trusted_connection", "no")).lower() == "yes":
            conn_str = (f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};"
                       f"Trusted_Connection=yes;TrustServerCertificate={trust};")
        else:
            user     = _require(f"{prefix}_USER")
            password = _require(f"{prefix}_PASSWORD")
            conn_str = (f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};"
                       f"UID={user};PWD={password};TrustServerCertificate={trust};")

        return cls(pyodbc.connect(conn_str))


# =============================================================================
# Flat files (CSV/Excel/TSV/Parquet) — DuckDB + pandas
# =============================================================================

try:
    import duckdb
    _DUCKDB_AVAILABLE = True
except ImportError:
    _DUCKDB_AVAILABLE = False
    logger.warning("duckdb not installed — file/S3 source connections unavailable.")

try:
    import pandas as pd
    _PANDAS_AVAILABLE = True
except ImportError:
    _PANDAS_AVAILABLE = False
    logger.warning("pandas not installed — file source connections unavailable.")

_FILE_EXTENSIONS = {".csv", ".xlsx", ".xls", ".tsv", ".parquet"}
_MAX_FILE_SIZE_MB = int(os.getenv("DQ_MAX_FILE_SIZE_MB", "500"))
_FILE_ENCODING = os.getenv("DQ_FILE_ENCODING", "utf-8")


class FileAdapter(SourceAdapter):
    """
    DuckDB-backed adapter for CSV/Excel/TSV/Parquet files on local/mounted
    disk. Each pandas DataFrame is loaded once (guarded by a lock) into a
    shared registry, then registered as a DuckDB view in each thread's own
    connection (threading.local()) — DuckDB connections aren't safe to
    share across threads.

    Non-secret config (config/connections.yaml): base_path (optional —
    rules can also supply src_db_name directly). No secrets — local/mounted
    files have no credentials to read from env.

    Rule conventions: src_tbl_nm = filename (e.g. "claims.csv"), src_db_name
    = base directory (may contain {ENV}), source_system = this connection's
    name. The DuckDB view name is the filename stem.
    """

    source_type: str = "file"

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
            except Exception as exc:
                logger.debug("%s.close() error closing thread-local DuckDB connection: %s",
                            type(self).__name__, exc)
            del self._local.conn

    def ping(self) -> bool:
        try:
            self._get_thread_conn().execute("SELECT 1")
            return True
        except Exception as exc:
            logger.debug("%s.ping() failed (connection considered stale): %s",
                        type(self).__name__, exc)
            return False

    def prepare(self, rule: dict) -> None:
        src_tbl_nm  = (rule.get("src_tbl_nm") or "").strip()
        src_db_name = (rule.get("src_db_name") or "").strip()
        if not src_tbl_nm:
            raise ValueError("src_tbl_nm is empty — cannot prepare file source.")

        view_name = Path(src_tbl_nm).stem
        if view_name not in self._df_registry:
            with self._registry_lock:
                if view_name not in self._df_registry:
                    full_path = self._resolve_path(src_tbl_nm, src_db_name)
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

    @classmethod
    def build(cls, name: str, config: dict) -> "FileAdapter":
        return cls(base_path=config.get("base_path", ""))

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

    def _resolve_path(self, src_tbl_nm: str, src_db_name: str) -> str:
        if os.path.isabs(src_tbl_nm):
            return src_tbl_nm
        base = src_db_name or self._base_path
        return f"{base.rstrip('/')}/{src_tbl_nm}" if base else src_tbl_nm


def _read_file(full_path: str):
    """Read one file into a pandas DataFrame. Supports .csv/.tsv/.xlsx/.xls/.parquet."""
    p, ext = Path(full_path), Path(full_path).suffix.lower()

    if _MAX_FILE_SIZE_MB > 0:
        size_mb = p.stat().st_size / (1024 * 1024)
        if size_mb > _MAX_FILE_SIZE_MB:
            raise ValueError(f"File '{full_path}' is {size_mb:.1f} MB, exceeds DQ_MAX_FILE_SIZE_MB={_MAX_FILE_SIZE_MB}.")

    if ext == ".csv":
        return pd.read_csv(full_path, encoding=_FILE_ENCODING)
    if ext == ".tsv":
        return pd.read_csv(full_path, sep="\t", encoding=_FILE_ENCODING)
    if ext in (".xlsx", ".xls"):
        return pd.read_excel(full_path)
    if ext == ".parquet":
        return pd.read_parquet(full_path)
    raise ValueError(f"Unsupported file extension '{ext}'. Supported: .csv, .tsv, .xlsx, .xls, .parquet")


def is_file_source(src_tbl_nm: str) -> bool:
    return Path(src_tbl_nm).suffix.lower() in _FILE_EXTENSIONS


# =============================================================================
# S3 landed files (Parquet/CSV) — DuckDB + httpfs
# =============================================================================

class S3Adapter(SourceAdapter):
    """
    DuckDB-over-S3: queries Parquet/CSV objects directly on S3 with full SQL
    (joins, aggregates, window functions) — required because every DQ rule
    is SQL (see rules_engine/rule_sql.py), and a source that only returns file
    bytes can't be queried that way.

    Thread model mirrors FileAdapter: one DuckDB connection per thread, each
    configured with the httpfs extension and S3 credentials read from env
    vars AT CONNECT TIME (never cached), so rotated credentials take effect
    on the next new thread without a restart.

    Rule conventions: src_tbl_nm = the view name the rule's SQL references,
    src_db_name = the s3:// URI or glob (e.g. supports Hive-partitioned
    "pull_date=*/*.parquet" globs for reading dated snapshots), source_system
    = this connection's name.

    Non-secret config (config/connections.yaml): region, endpoint (optional
    — S3-compatible endpoint override).
    Secrets (env, prefix DQ_<NAME>_, all optional — omit to use the
    instance/task IAM role): ACCESS_KEY_ID, SECRET_ACCESS_KEY, SESSION_TOKEN.
    """

    source_type: str = "s3"

    def __init__(self, name: str, config: dict):
        if not _DUCKDB_AVAILABLE:
            raise ImportError("duckdb is required for S3 source connections. Install with: pip install duckdb")
        self._name = name
        self._config = config
        self._local = threading.local()

    def cursor(self):
        return self._get_thread_conn().cursor()

    def commit(self):
        pass   # read-only

    def close(self):
        if hasattr(self._local, "conn"):
            try:
                self._local.conn.close()
            except Exception as exc:
                logger.debug("%s.close() error closing thread-local DuckDB connection: %s",
                            type(self).__name__, exc)
            del self._local.conn

    def ping(self) -> bool:
        try:
            self._get_thread_conn().execute("SELECT 1")
            return True
        except Exception as exc:
            logger.debug("%s.ping() failed (connection considered stale): %s",
                        type(self).__name__, exc)
            return False

    def prepare(self, rule: dict) -> None:
        view_name = (rule.get("src_tbl_nm") or "").strip()
        uri       = (rule.get("src_db_name") or "").strip()
        if not view_name or not uri:
            raise ValueError("S3 rules require src_tbl_nm (view name) and src_db_name (s3:// URI or glob).")

        conn = self._get_thread_conn()
        registered = getattr(self._local, "registered", set())
        if view_name in registered:
            return

        conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {self._reader_fn(uri)}")
        registered.add(view_name)
        self._local.registered = registered
        logger.info("S3 view prepared: %s -> %s (connection '%s')", view_name, uri, self._name)

    @classmethod
    def build(cls, name: str, config: dict) -> "S3Adapter":
        if not (config.get("region") or os.getenv("AWS_DEFAULT_REGION")):
            logger.warning(
                "Neither 'region' in config/connections.yaml nor AWS_DEFAULT_REGION "
                "is set for '%s' — DuckDB httpfs will use its own default region "
                "resolution.", name,
            )
        return cls(name, config)

    def _get_thread_conn(self):
        if not hasattr(self._local, "conn"):
            conn = duckdb.connect(":memory:")
            conn.execute("INSTALL httpfs")
            conn.execute("LOAD httpfs")
            self._configure_credentials(conn)
            self._local.conn = conn
            self._local.registered = set()
        return self._local.conn

    def _configure_credentials(self, conn) -> None:
        prefix = f"DQ_{self._name.upper()}"

        region = self._config.get("region") or os.getenv("AWS_DEFAULT_REGION")
        if region:
            conn.execute(f"SET s3_region='{_escape(region)}'")

        access_key = os.getenv(f"{prefix}_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv(f"{prefix}_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
        if access_key and secret_key:
            conn.execute(f"SET s3_access_key_id='{_escape(access_key)}'")
            conn.execute(f"SET s3_secret_access_key='{_escape(secret_key)}'")
        else:
            logger.debug("No explicit access key/secret for '%s' — relying on IAM role / default credential chain.", self._name)

        session_token = os.getenv(f"{prefix}_SESSION_TOKEN") or os.getenv("AWS_SESSION_TOKEN")
        if session_token:
            conn.execute(f"SET s3_session_token='{_escape(session_token)}'")

        endpoint = self._config.get("endpoint")
        if endpoint:
            conn.execute(f"SET s3_endpoint='{_escape(endpoint)}'")
            conn.execute("SET s3_url_style='path'")

    @staticmethod
    def _reader_fn(uri: str) -> str:
        lower, u = uri.lower(), _escape(uri)
        if lower.endswith(".csv") or lower.endswith(".tsv"):
            return f"read_csv_auto('{u}', union_by_name=true)"
        return f"read_parquet('{u}', union_by_name=true, hive_partitioning=1)"


def _escape(value: str) -> str:
    return value.replace("'", "''")
