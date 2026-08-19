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
                              adapter speaks; used by core/rule_sql.py to pick
                              dialect-correct SQL and to enforce the dialect
                              guard (a rule's declared sql_dialect vs. this).

Sanctioned for this engine instance (Section 2 of DESIGN.md): teradata,
postgresql, s3. Databricks and SQL Server adapters are included and fully
functional, but are not catalogued/tested for the current use case — they
exist to prove the interface is genuinely pluggable, not to be deployed yet.

Adding a new source
--------------------
Add one class implementing SourceAdapter to this file (import its driver
lazily, same pattern as every class below) and one line in
_TYPE_MAP in connection_factory.py. Nothing else changes — that's the
actual test of "pluggable."

Every `build()` classmethod reads its env vars at CALL time (never at
import), so credential rotation takes effect on the next connect without a
process restart.
"""

import logging
import os
import threading
from abc import ABC, abstractmethod
from pathlib import Path

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
        except Exception:
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
    Env vars (prefix DQ_<NAME>_): HOST, USER, PASSWORD, LOGMECH (default LDAP).
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
    def build(cls, name: str) -> "TeradataAdapter":
        if not _TERADATA_AVAILABLE:
            raise ImportError("teradatasql is required. Install with: pip install teradatasql")
        prefix = f"DQ_{name.upper()}"
        conn = teradatasql.connect(
            host=_require(f"{prefix}_HOST"),
            user=_require(f"{prefix}_USER"),
            password=_require(f"{prefix}_PASSWORD"),
            logmech=os.getenv(f"{prefix}_LOGMECH", "LDAP"),
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
    Env vars (prefix DQ_<NAME>_): HOST, PORT (5432), DATABASE, USER,
    PASSWORD, SSLMODE (prefer). Same connection shape for Aurora PG.
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
    def build(cls, name: str) -> "PostgresAdapter":
        if not _POSTGRES_AVAILABLE:
            raise ImportError("psycopg2-binary is required. Install with: pip install psycopg2-binary")
        prefix = f"DQ_{name.upper()}"
        conn = psycopg2.connect(
            host=_require(f"{prefix}_HOST"),
            port=int(os.getenv(f"{prefix}_PORT", "5432")),
            dbname=_require(f"{prefix}_DATABASE"),
            user=_require(f"{prefix}_USER"),
            password=_require(f"{prefix}_PASSWORD"),
            sslmode=os.getenv(f"{prefix}_SSLMODE", "prefer"),
        )
        return cls(conn)


# =============================================================================
# Databricks SQL Warehouses — databricks-sql-connector
# =============================================================================

try:
    from databricks import sql as _databricks_sql
    _DATABRICKS_AVAILABLE = True
except ImportError:
    _DATABRICKS_AVAILABLE = False
    logger.warning("databricks-sql-connector not installed — Databricks connections unavailable.")


class DatabricksAdapter(SourceAdapter):
    """
    Env vars (prefix DQ_<NAME>_): HOST, HTTP_PATH, TOKEN, CATALOG (optional),
    SCHEMA (optional). Note: executemany() is unsupported by this driver —
    not a problem here since bulk_insert() only ever targets the metadata
    (Teradata) connection, never a Databricks source connection.
    """

    source_type: str = "databricks"

    def __init__(self, conn):
        self._conn = conn

    def cursor(self):
        return self._conn.cursor()

    def commit(self):
        pass   # Databricks SQL warehouses are read-committed; no explicit commit needed

    def close(self):
        self._conn.close()

    @classmethod
    def build(cls, name: str) -> "DatabricksAdapter":
        if not _DATABRICKS_AVAILABLE:
            raise ImportError("databricks-sql-connector is required. Install with: pip install databricks-sql-connector")
        prefix = f"DQ_{name.upper()}"
        kwargs = dict(
            server_hostname=_require(f"{prefix}_HOST"),
            http_path=_require(f"{prefix}_HTTP_PATH"),
            access_token=_require(f"{prefix}_TOKEN"),
        )
        if os.getenv(f"{prefix}_CATALOG"):
            kwargs["catalog"] = os.getenv(f"{prefix}_CATALOG")
        if os.getenv(f"{prefix}_SCHEMA"):
            kwargs["schema"] = os.getenv(f"{prefix}_SCHEMA")
        return cls(_databricks_sql.connect(**kwargs))


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
    Env vars (prefix DQ_<NAME>_): HOST, PORT (1433), DATABASE, USER,
    PASSWORD, DRIVER (default "ODBC Driver 18 for SQL Server"),
    TRUST_CERT (yes), TRUSTED_CONNECTION (no — set "yes" for Windows Auth,
    which then omits USER/PASSWORD).
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
    def build(cls, name: str) -> "SqlServerAdapter":
        if not _SQLSERVER_AVAILABLE:
            raise ImportError("pyodbc is required. Install with: pip install pyodbc")
        prefix   = f"DQ_{name.upper()}"
        host     = _require(f"{prefix}_HOST")
        port     = os.getenv(f"{prefix}_PORT", "1433")
        database = _require(f"{prefix}_DATABASE")
        driver   = os.getenv(f"{prefix}_DRIVER", "ODBC Driver 18 for SQL Server")
        trust    = os.getenv(f"{prefix}_TRUST_CERT", "yes")

        if os.getenv(f"{prefix}_TRUSTED_CONNECTION", "no").lower() == "yes":
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

_MAX_FILE_SIZE_MB = int(os.getenv("DQ_MAX_FILE_SIZE_MB", "500"))
_FILE_ENCODING = os.getenv("DQ_FILE_ENCODING", "utf-8")


class FileAdapter(SourceAdapter):
    """
    DuckDB-backed adapter for CSV/Excel/TSV/Parquet files on local/mounted
    disk. Each pandas DataFrame is loaded once (guarded by a lock) into a
    shared registry, then registered as a DuckDB view in each thread's own
    connection (threading.local()) — DuckDB connections aren't safe to
    share across threads.

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
    def build(cls, name: str) -> "FileAdapter":
        return cls(base_path=os.getenv(f"DQ_{name.upper()}_BASE_PATH", ""))

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


# =============================================================================
# S3 landed files (Parquet/CSV) — DuckDB + httpfs
# =============================================================================

class S3Adapter(SourceAdapter):
    """
    DuckDB-over-S3: queries Parquet/CSV objects directly on S3 with full SQL
    (joins, aggregates, window functions) — required because every DQ rule
    is SQL (see core/rule_sql.py), and a source that only returns file
    bytes can't be queried that way.

    Thread model mirrors FileAdapter: one DuckDB connection per thread, each
    configured with the httpfs extension and S3 credentials read from env
    vars AT CONNECT TIME (never cached), so rotated credentials take effect
    on the next new thread without a restart.

    Rule conventions: src_tbl_nm = the view name the rule's SQL references,
    src_db_name = the s3:// URI or glob (e.g. supports Hive-partitioned
    "pull_date=*/*.parquet" globs for reading dated snapshots), source_system
    = this connection's name.

    Env vars (prefix DQ_<NAME>_): REGION, ACCESS_KEY_ID (optional — omit to
    use the instance/task IAM role), SECRET_ACCESS_KEY, SESSION_TOKEN
    (optional), ENDPOINT (optional — S3-compatible endpoint override).
    """

    source_type: str = "s3"

    def __init__(self, name: str):
        if not _DUCKDB_AVAILABLE:
            raise ImportError("duckdb is required for S3 source connections. Install with: pip install duckdb")
        self._name = name
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
    def build(cls, name: str) -> "S3Adapter":
        prefix = f"DQ_{name.upper()}"
        if not (os.getenv(f"{prefix}_REGION") or os.getenv("AWS_DEFAULT_REGION")):
            logger.warning(
                "Neither %s_REGION nor AWS_DEFAULT_REGION is set for '%s' — "
                "DuckDB httpfs will use its own default region resolution.", prefix, name,
            )
        return cls(name)

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

        region = os.getenv(f"{prefix}_REGION") or os.getenv("AWS_DEFAULT_REGION")
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

        endpoint = os.getenv(f"{prefix}_ENDPOINT")
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
