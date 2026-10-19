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

FileAdapter (source_type 'file') is supported: CSV/TSV/Excel/Parquet read
in memory (DuckDB if installed, else built-in sqlite3), from local disk or downloaded from a NAS (SMB share, see
db/nas_store.py). A rule uses it by setting sql_dialect='file',
database_name=<folder> and src_tbl_nm=<filename>.

S3Adapter below is still a disabled stub (never registered in _TYPE_MAP;
.build() raises NotImplementedError), kept only because
rules_engine/parallel.py imports it by name for isinstance() checks.

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
import csv
import re
import shutil
import sqlite3
import tempfile
import threading
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Dict, Optional

from db import nas_store

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
# Flat files (CSV/TSV/Excel/Parquet) -- DuckDB + pandas. The file is either on
# local/mounted disk or on a NAS (SMB share), in which case it is downloaded
# to a temp folder first (see db/nas_store.py).
#
# S3Adapter below is still a disabled stub (kept only for parallel.py's import
# and isinstance checks).
# =============================================================================

# Engine that runs rule SQL against the in-memory file data. DuckDB if it is
# installed, else Python's built-in sqlite3 (no install needed). Force one with
# GRE_FILE_ENGINE=duckdb|sqlite. pandas is optional: only .xlsx/.xls/.parquet
# need it (CSV/TSV are read with the csv module).
try:
    import duckdb
except ImportError:
    duckdb = None
try:
    import pandas as pd
except ImportError:
    pd = None


def _file_engine() -> str:
    want = (os.getenv("GRE_FILE_ENGINE") or "").lower()
    if want == "duckdb" and (duckdb is None or pd is None):
        raise ImportError("GRE_FILE_ENGINE=duckdb needs duckdb and pandas installed.")
    if want in ("duckdb", "sqlite"):
        return want
    return "duckdb" if duckdb is not None and pd is not None else "sqlite"


_MAX_FILE_SIZE_MB = int(os.getenv("GRE_MAX_FILE_SIZE_MB", "500"))
_FILE_ENCODING = os.getenv("GRE_FILE_ENCODING", "utf-8")


def _view_name(src_tbl_nm: str) -> str:
    """Safe SQL identifier for a file's DuckDB view ("claims.csv" -> "claims"); rule_syntax references this name."""
    suffix = Path(src_tbl_nm).suffix
    base = src_tbl_nm[: -len(suffix)] if suffix else src_tbl_nm
    cleaned = re.sub(r"[^0-9a-zA-Z_]", "_", base) or "t"
    return f"t_{cleaned}" if cleaned[0].isdigit() else cleaned


def _convert(values: list):
    """int/float column if every non-empty value parses, else text; '' -> None."""
    vals = [v if v != "" else None for v in values]
    for cast in (int, float):
        try:
            return [None if v is None else cast(v) for v in vals]
        except ValueError:
            continue
    return vals


def _read_file(full_path: str):
    """Read one file. Returns (columns, column_lists) with lower-cased column names."""
    ext = Path(full_path).suffix.lower()
    if _MAX_FILE_SIZE_MB > 0:
        size_mb = Path(full_path).stat().st_size / (1024 * 1024)
        if size_mb > _MAX_FILE_SIZE_MB:
            raise ValueError(f"File '{full_path}' is {size_mb:.1f} MB, exceeds GRE_MAX_FILE_SIZE_MB={_MAX_FILE_SIZE_MB}.")
    if ext in (".csv", ".tsv"):
        with open(full_path, newline="", encoding=_FILE_ENCODING) as fh:
            reader = csv.reader(fh, delimiter="\t" if ext == ".tsv" else ",")
            header = [h.strip().lower() for h in next(reader)]
            data = [r + [""] * (len(header) - len(r)) for r in reader]
        return header, [_convert([r[i] for r in data]) for i in range(len(header))]
    if ext in (".xlsx", ".xls", ".parquet"):
        if pd is None:
            raise ImportError(f"pandas is required to read {ext} files (CSV/TSV need nothing extra).")
        df = pd.read_parquet(full_path) if ext == ".parquet" else pd.read_excel(full_path)
        df = df.astype(object).where(df.notna(), None)
        return [str(c).lower() for c in df.columns], [df[c].tolist() for c in df.columns]
    raise ValueError(f"Unsupported file extension '{ext}'. Supported: .csv, .tsv, .xlsx, .xls, .parquet")


class FileAdapter(SourceAdapter):
    """
    A rule with sql_dialect='file' drives its own path from gre_rules:
        database_name -> the folder the file is in: a local/mounted directory
                          (absolute, or relative to FILE_BASE_PATH), or a NAS
                          folder -- //server/share/folder, \\\\server\\share\\folder,
                          smb://server/share/folder or nas://share/folder
        src_tbl_nm    -> the filename (e.g. "claims.csv"). If the run sets "file_name"
                          (env GRE_FILE_NAME), that file is read instead and src_tbl_nm
                          is just the fixed view name rule_syntax uses.
    prepare(rule) loads the file once into a shared DataFrame registry, then
    registers it as a DuckDB view named _view_name(src_tbl_nm) in each
    thread's own connection. rule_syntax references that view name.
    """

    source_type = "file"

    def __init__(self, base_path: str = ""):
        self._engine = _file_engine()
        self._base_path = base_path.rstrip("/") if base_path else ""
        self._df_registry: dict = {}
        self._registry_lock = threading.Lock()
        self._local = threading.local()
        self._tmp_dir = None

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
        if self._tmp_dir:
            shutil.rmtree(self._tmp_dir, ignore_errors=True)
            self._tmp_dir = None

    def ping(self) -> bool:
        try:
            self._get_thread_conn().execute("SELECT 1")
            return True
        except Exception:
            return False

    def prepare(self, rule: dict) -> None:
        src_tbl_nm = (rule.get("src_tbl_nm") or "").strip()
        if not src_tbl_nm:
            raise ValueError("src_tbl_nm is empty -- cannot prepare file source.")
        view_name = _view_name(src_tbl_nm)

        if view_name not in self._df_registry:
            with self._registry_lock:
                if view_name not in self._df_registry:
                    # GRE_FILE_NAME (the run's "file_name") is the real file; src_tbl_nm is then only
                    # the fixed name rule_syntax reads FROM, so a changing file name needs no rule edit.
                    actual_name = (os.getenv("GRE_FILE_NAME") or "").strip() or src_tbl_nm
                    full_path = self._fetch(actual_name, (rule.get("database_name") or "").strip())
                    cols, data = _read_file(full_path)
                    self._df_registry[view_name] = (cols, data)
                    logger.info("File loaded: %s -> view '%s' (%d rows, engine=%s)", full_path, view_name,
                                len(data[0]) if data else 0, self._engine)

        self._materialize(view_name)

    def qualified_name(self, rule: dict) -> str:
        return _view_name((rule.get("src_tbl_nm") or "").strip())

    @classmethod
    def build(cls) -> "FileAdapter":
        return cls(base_path=os.getenv("FILE_BASE_PATH", ""))

    def _get_thread_conn(self):
        if not hasattr(self._local, "conn"):
            conn = duckdb.connect(":memory:") if self._engine == "duckdb" else sqlite3.connect(":memory:")
            self._local.conn = conn
            self._local.registered = set()
            for vname in self._df_registry:
                self._materialize(vname)
        return self._local.conn

    def _materialize(self, view_name: str) -> None:
        """
        Make the file's data a table in this thread's in-memory database. A table
        (not DuckDB's conn.register()) because cursor() hands out a duplicate
        connection, which cannot see another connection's registered DataFrames.
        """
        conn = self._get_thread_conn()
        if view_name in self._local.registered:
            return
        cols, data = self._df_registry[view_name]
        if self._engine == "duckdb":
            df = pd.DataFrame(dict(zip(cols, data)))   # noqa: F841 -- read by DuckDB's replacement scan
            conn.execute(f'CREATE OR REPLACE TABLE "{view_name}" AS SELECT * FROM df')
        else:
            conn.execute(f'DROP TABLE IF EXISTS "{view_name}"')
            conn.execute(f'CREATE TABLE "{view_name}" ({", ".join(chr(34) + c + chr(34) for c in cols)})')
            conn.executemany(f'INSERT INTO "{view_name}" VALUES ({",".join("?" * len(cols))})', zip(*data))
        self._local.registered.add(view_name)

    def _fetch(self, src_tbl_nm: str, database_name: str) -> str:
        """Local path of the file; a NAS file is downloaded into a temp folder first."""
        if nas_store.is_nas_uri(database_name):
            if self._tmp_dir is None:
                self._tmp_dir = tempfile.mkdtemp(prefix="gre_nas_")
            dest = os.path.join(self._tmp_dir, os.path.basename(src_tbl_nm))
            nas_store.download(database_name, src_tbl_nm, dest)
            logger.info("Downloaded from NAS: %s/%s", database_name, src_tbl_nm)
            return dest
        if os.path.isabs(src_tbl_nm):
            return src_tbl_nm
        base = database_name or self._base_path
        return f"{base.rstrip('/')}/{src_tbl_nm}" if base else src_tbl_nm


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
    "file": FileAdapter,
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
