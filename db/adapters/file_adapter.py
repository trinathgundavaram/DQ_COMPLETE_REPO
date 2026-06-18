"""
db/adapters/file_adapter.py
----------------------------
Adapter for flat-file sources: CSV, Excel (.xlsx/.xls), TSV, and Parquet.

Architecture
------------
* pandas reads each file into a DataFrame (once per process — lazy on first use).
* DuckDB provides an in-memory SQL engine, with each loaded DataFrame registered
  as a named view.
* Because the ThreadPoolExecutor runs multiple rules concurrently, each worker
  thread gets its OWN DuckDB connection via threading.local().  A shared
  DataFrame registry (dict) lets threads reuse already-loaded data without
  re-reading the file from disk.  A threading.Lock protects the registry write.

Rule conventions for file sources
----------------------------------
    src_tbl_nm   = filename, e.g. "claims_2024.csv" or "members.xlsx"
    src_db_name  = base directory path (may contain {ENV} token via db_resolver),
                   e.g. "/data/{ENV}/inputs/"   or leave empty if src_tbl_nm
                   is already an absolute path.
    source_system = the connection name mapped to this FileAdapter in the factory,
                   e.g. "file_source"

The DuckDB view name is the filename stem (without extension):
    "claims_2024.csv"  →  view name "claims_2024"
    "members.xlsx"     →  view name "members"

Env vars (prefix = DQ_<NAME>_):
    DQ_<NAME>_TYPE         must be "file"
    DQ_<NAME>_BASE_PATH    (optional) default directory when src_db_name is empty
"""

import logging
import os
import threading
from pathlib import Path

from db.adapters.base import SourceAdapter

logger = logging.getLogger(__name__)

try:
    import duckdb
    _DUCKDB_AVAILABLE = True
except ImportError:
    _DUCKDB_AVAILABLE = False
    logger.warning("duckdb not installed — file source connections unavailable.")

try:
    import pandas as pd
    _PANDAS_AVAILABLE = True
except ImportError:
    _PANDAS_AVAILABLE = False
    logger.warning("pandas not installed — file source connections unavailable.")

_FILE_EXTENSIONS = {".csv", ".xlsx", ".xls", ".tsv", ".parquet"}


class FileAdapter(SourceAdapter):
    """
    Thread-safe DuckDB-backed adapter for CSV, Excel, TSV, and Parquet files.

    Thread safety model
    -------------------
    * _df_registry   : dict { view_name → pd.DataFrame }
                       Written under _registry_lock; read without lock once populated.
    * _local         : threading.local()
                       Each thread creates its own DuckDB connection and registers
                       DataFrames from _df_registry into it.
    * _registry_lock : threading.Lock protecting _df_registry writes.
    """

    def __init__(self, base_path: str = ""):
        if not _DUCKDB_AVAILABLE or not _PANDAS_AVAILABLE:
            raise ImportError(
                "duckdb and pandas are required for file source connections. "
                "Install with: pip install duckdb pandas openpyxl"
            )
        self._base_path = base_path.rstrip("/") if base_path else ""
        self._df_registry: dict = {}          # view_name → DataFrame
        self._registry_lock = threading.Lock()
        self._local = threading.local()       # per-thread DuckDB connections

    # ------------------------------------------------------------------
    # SourceAdapter interface
    # ------------------------------------------------------------------

    def cursor(self):
        """Return a DuckDB cursor from the current thread's connection."""
        return self._get_thread_conn().cursor()

    def commit(self):
        """No-op — file sources are read-only."""

    def close(self):
        """Close the current thread's DuckDB connection."""
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
        """
        Load the rule's source file into the shared DataFrame registry (once),
        then ensure the current thread's DuckDB connection has it as a view.
        """
        src_tbl_nm  = (rule.get("src_tbl_nm") or "").strip()
        src_db_name = (rule.get("src_db_name") or "").strip()

        if not src_tbl_nm:
            raise ValueError("src_tbl_nm is empty — cannot prepare file source.")

        view_name = Path(src_tbl_nm).stem

        # ── 1. Load into registry if not already there ──────────────────
        if view_name not in self._df_registry:
            with self._registry_lock:
                if view_name not in self._df_registry:   # double-check
                    full_path = self._resolve_path(src_tbl_nm, src_db_name)
                    df = _read_file(full_path)
                    # Normalise column names to lowercase for consistency
                    df.columns = [c.lower() for c in df.columns]
                    self._df_registry[view_name] = df
                    logger.info(
                        "File loaded: %s → view '%s' (%d rows, %d cols)",
                        full_path, view_name, len(df), len(df.columns),
                    )

        # ── 2. Register in the current thread's DuckDB connection ────────
        conn = self._get_thread_conn()
        registered = getattr(self._local, "registered", set())
        if view_name not in registered:
            conn.register(view_name, self._df_registry[view_name])
            registered.add(view_name)
            self._local.registered = registered
            logger.debug("Thread %s: registered view '%s' in DuckDB.",
                         threading.current_thread().name, view_name)

    # ------------------------------------------------------------------
    # Factory helper
    # ------------------------------------------------------------------

    @classmethod
    def build(cls, name: str) -> "FileAdapter":
        """Create a FileAdapter using env vars for `name`."""
        prefix    = f"DQ_{name.upper()}"
        base_path = os.getenv(f"{prefix}_BASE_PATH", "")
        return cls(base_path=base_path)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_thread_conn(self):
        """
        Return the DuckDB connection for the current thread.
        Creates one on first access and pre-registers all known DataFrames.
        """
        if not hasattr(self._local, "conn"):
            conn = duckdb.connect(":memory:")
            # Pre-register everything already in the registry
            registered = set()
            for vname, df in self._df_registry.items():
                conn.register(vname, df)
                registered.add(vname)
            self._local.conn = conn
            self._local.registered = registered
        return self._local.conn

    def _resolve_path(self, src_tbl_nm: str, src_db_name: str) -> str:
        """
        Build the full file path.

        Priority:
            1. src_tbl_nm is an absolute path  → use as-is
            2. src_db_name provided             → src_db_name / src_tbl_nm
               (src_db_name {ENV} already resolved by table_resolver)
            3. adapter's base_path set          → base_path / src_tbl_nm
            4. fallback                         → just src_tbl_nm (CWD-relative)
        """
        if os.path.isabs(src_tbl_nm):
            return src_tbl_nm

        base = src_db_name or self._base_path
        if base:
            return f"{base.rstrip('/')}/{src_tbl_nm}"
        return src_tbl_nm


# ---------------------------------------------------------------------------
# Standalone helpers
# ---------------------------------------------------------------------------

def _read_file(full_path: str):
    """
    Read a file into a pandas DataFrame.
    Supports: .csv, .tsv, .xlsx, .xls, .parquet
    """
    ext = Path(full_path).suffix.lower()

    if ext == ".csv":
        return pd.read_csv(full_path)

    if ext == ".tsv":
        return pd.read_csv(full_path, sep="\t")

    if ext in (".xlsx", ".xls"):
        try:
            return pd.read_excel(full_path)
        except ImportError as exc:
            raise ImportError(
                "openpyxl (for .xlsx) or xlrd (for .xls) is required. "
                "Install with: pip install openpyxl xlrd"
            ) from exc

    if ext == ".parquet":
        return pd.read_parquet(full_path)

    raise ValueError(
        f"Unsupported file extension '{ext}'. "
        f"Supported: .csv, .tsv, .xlsx, .xls, .parquet"
    )


def is_file_source(src_tbl_nm: str) -> bool:
    """True if src_tbl_nm has a recognised flat-file extension."""
    return Path(src_tbl_nm).suffix.lower() in _FILE_EXTENSIONS
