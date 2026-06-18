"""
db/adapters/sqlserver_adapter.py
---------------------------------
Adapter for Microsoft SQL Server via pyodbc.

Requires the Microsoft ODBC Driver for SQL Server to be installed on the host:
    macOS : brew install msodbcsql18
    Linux : https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver
    Windows: included with SQL Server tools

Env vars required (prefix = DQ_<NAME>_):
    DQ_<NAME>_HOST          — SQL Server host / instance
    DQ_<NAME>_PORT          (optional, default: 1433)
    DQ_<NAME>_DATABASE
    DQ_<NAME>_USER
    DQ_<NAME>_PASSWORD
    DQ_<NAME>_DRIVER        (optional, default: ODBC Driver 18 for SQL Server)
    DQ_<NAME>_TRUST_CERT    (optional, default: yes)
                            Set to 'no' for production with proper TLS cert.

Notes
-----
* pyodbc rows behave like tuples; cursor.description follows DB-API.
* For Windows Authentication (no user/password), set
  DQ_<NAME>_TRUSTED_CONNECTION=yes and omit USER/PASSWORD.
"""

import logging
import os

from db.adapters.base import SourceAdapter

logger = logging.getLogger(__name__)

try:
    import pyodbc
    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False
    logger.warning("pyodbc not installed — SQL Server connections unavailable.")


class SqlServerAdapter(SourceAdapter):
    """Wraps a pyodbc connection to SQL Server."""

    def __init__(self, conn):
        self._conn = conn
        self._conn.autocommit = True  # source queries only; avoids open txn

    # ------------------------------------------------------------------
    # SourceAdapter interface
    # ------------------------------------------------------------------

    def cursor(self):
        return self._conn.cursor()

    def commit(self):
        self._conn.commit()

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
    def build(cls, name: str) -> "SqlServerAdapter":
        """Open a pyodbc SQL Server connection using env vars for `name`."""
        if not _AVAILABLE:
            raise ImportError(
                "pyodbc is required for SQL Server connections. "
                "Install with: pip install pyodbc"
            )

        prefix    = f"DQ_{name.upper()}"
        host      = _require(f"{prefix}_HOST")
        port      = os.getenv(f"{prefix}_PORT", "1433")
        database  = _require(f"{prefix}_DATABASE")
        driver    = os.getenv(f"{prefix}_DRIVER", "ODBC Driver 18 for SQL Server")
        trust     = os.getenv(f"{prefix}_TRUST_CERT", "yes")
        trusted   = os.getenv(f"{prefix}_TRUSTED_CONNECTION", "no")

        if trusted.lower() == "yes":
            # Windows Authentication — no user/password
            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={host},{port};"
                f"DATABASE={database};"
                f"Trusted_Connection=yes;"
                f"TrustServerCertificate={trust};"
            )
        else:
            user     = _require(f"{prefix}_USER")
            password = _require(f"{prefix}_PASSWORD")
            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={host},{port};"
                f"DATABASE={database};"
                f"UID={user};"
                f"PWD={password};"
                f"TrustServerCertificate={trust};"
            )

        conn = pyodbc.connect(conn_str)
        logger.debug("pyodbc connected to %s:%s/%s", host, port, database)
        return cls(conn)


# ---------------------------------------------------------------------------

def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"Required env var '{key}' is not set.")
    return val
