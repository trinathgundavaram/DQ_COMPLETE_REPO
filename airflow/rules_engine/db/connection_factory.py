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


class SourceAdapter(ABC):

    source_type: str = "unknown"


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
        pass

    def qualified_name(self, rule: dict) -> str:
        return f"{rule['database_name']}.{rule['src_tbl_nm']}"


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


        logger.info("Connecting to Teradata: host=%s user=%s logmech=%s", host, user, logmech)
        return cls(teradatasql.connect(
            host=host,
            user=user,
            password=_require("TERADATA_PASSWORD"),
            logmech=logmech,
        ), host=host)


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
        self._conn.autocommit = True
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


        logger.info("Connecting to Postgres: host=%s port=%d dbname=%s user=%s", host, port, dbname, user)
        return cls(psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=_require("POSTGRES_PASSWORD"),
            sslmode=os.getenv("POSTGRES_SSLMODE", "prefer"),
        ), host=f"{host}:{port}/{dbname}")


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
_FILE_ENCODING = os.getenv("GRE_FILE_ENCODING", "utf-8-sig")


def _view_name(src_tbl_nm: str) -> str:
    suffix = Path(src_tbl_nm).suffix
    base = src_tbl_nm[: -len(suffix)] if suffix else src_tbl_nm
    cleaned = re.sub(r"[^0-9a-zA-Z_]", "_", base) or "t"
    return f"t_{cleaned}" if cleaned[0].isdigit() else cleaned


_DELIMITED_EXT = (".csv", ".tsv", ".txt", ".dat", ".psv")
_CANDIDATES = [",", "|", "\t", ";", "^", "~"]


def _delimiter(fh, ext: str) -> str:
    forced = os.getenv("GRE_FILE_DELIMITER")
    if forced:
        return "\t" if forced.lower() in ("\\t", "tab") else forced
    if ext == ".tsv":
        return "\t"
    if ext == ".psv":
        return "|"
    start = fh.tell()
    line = fh.readline()
    fh.seek(start)
    counts = {d: line.count(d) for d in _CANDIDATES}
    best = max(counts, key=counts.get)
    return best if counts[best] else ","


def _convert(values: list):
    vals = [v if v != "" else None for v in values]
    for cast in (int, float):
        try:
            return [None if v is None else cast(v) for v in vals]
        except ValueError:
            continue
    return vals


def _read_file(full_path: str):
    ext = Path(full_path).suffix.lower()
    if _MAX_FILE_SIZE_MB > 0:
        size_mb = Path(full_path).stat().st_size / (1024 * 1024)
        if size_mb > _MAX_FILE_SIZE_MB:
            raise ValueError(f"File '{full_path}' is {size_mb:.1f} MB, exceeds GRE_MAX_FILE_SIZE_MB={_MAX_FILE_SIZE_MB}.")
    if ext in _DELIMITED_EXT:


        tried = [_FILE_ENCODING] if os.getenv("GRE_FILE_ENCODING") else ["utf-8-sig"]
        for enc in tried + [e for e in ("utf-8-sig", "cp1252", "latin-1") if e not in tried]:
            try:
                with open(full_path, newline="", encoding=enc) as fh:
                    reader = csv.reader(fh, delimiter=_delimiter(fh, ext))
                    header = [h.strip().lower() for h in next(reader)]
                    data = [r + [""] * (len(header) - len(r)) for r in reader]
                break
            except UnicodeDecodeError:
                logger.warning("File %s is not %s -- trying the next encoding.", full_path, enc)
        logger.info("File %s read as %s.", full_path, enc)
        return header, [_convert([r[i] for r in data]) for i in range(len(header))]
    if ext in (".xlsx", ".xls", ".parquet"):
        if pd is None:
            raise ImportError(f"pandas is required to read {ext} files (CSV/TSV need nothing extra).")
        df = pd.read_parquet(full_path) if ext == ".parquet" else pd.read_excel(full_path, sheet_name=os.getenv("GRE_FILE_SHEET") or 0)
        df = df.astype(object).where(df.notna(), None)
        return [str(c).lower() for c in df.columns], [df[c].tolist() for c in df.columns]
    raise ValueError(f"Unsupported file extension '{ext}'. Supported: .csv .tsv .txt .dat .psv .xlsx .xls .parquet")


class FileAdapter(SourceAdapter):

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
        pass

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
        conn = self._get_thread_conn()
        if view_name in self._local.registered:
            return
        cols, data = self._df_registry[view_name]
        if self._engine == "duckdb":
            df = pd.DataFrame(dict(zip(cols, data)))
            conn.execute(f'CREATE OR REPLACE TABLE "{view_name}" AS SELECT * FROM df')
        else:
            conn.execute(f'DROP TABLE IF EXISTS "{view_name}"')
            conn.execute(f'CREATE TABLE "{view_name}" ({", ".join(chr(34) + c + chr(34) for c in cols)})')
            conn.executemany(f'INSERT INTO "{view_name}" VALUES ({",".join("?" * len(cols))})', zip(*data))
        self._local.registered.add(view_name)

    def _fetch(self, src_tbl_nm: str, database_name: str) -> str:
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


_TYPE_MAP = {
    "file": FileAdapter,
    "teradata": TeradataAdapter,
    "postgres": PostgresAdapter,
}


class ConnectionFactory:

    def __init__(self):
        self._conns: Dict[str, SourceAdapter] = {}

    def load(self) -> None:
        for source_type in _TYPE_MAP:
            try:
                self._conns[source_type] = self._build(source_type)
                logger.info("Connection '%s' established.", source_type)
            except Exception as exc:
                logger.warning("Connection '%s' not initialised: %s", source_type, exc)

    def get(self, source_type: str) -> Optional[SourceAdapter]:
        adapter = self._conns.get(source_type)
        if adapter is None:
            logger.error("Connection '%s' not found in pool.", source_type)
            return None

        if isinstance(adapter, (FileAdapter, S3Adapter)):
            return adapter

        if not adapter.ping():
            logger.warning("Connection '%s' is stale -- reconnecting.", source_type)
            try:
                new_adapter = self._build(source_type)
            except Exception as exc:
                logger.error("Reconnect failed for '%s': %s", source_type, exc, exc_info=True)
                return None


            try:
                adapter.close()
            except Exception as exc:
                logger.warning("Error closing stale connection '%s': %s", source_type, exc)
            adapter = new_adapter
            self._conns[source_type] = adapter
            logger.info("Connection '%s' reconnected.", source_type)

        return adapter

    def new_connection(self, source_type: str) -> Optional[SourceAdapter]:
        adapter_cls = _TYPE_MAP.get(source_type)
        if adapter_cls in (FileAdapter, S3Adapter):
            return self._conns.get(source_type)

        try:
            return self._build(source_type)
        except Exception as exc:
            logger.error("Failed to create fresh connection '%s': %s", source_type, exc, exc_info=True)
            return None

    def close_all(self) -> None:
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
    cf = ConnectionFactory()
    cf.load()
    return cf
