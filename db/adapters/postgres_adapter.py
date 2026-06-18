"""
db/adapters/postgres_adapter.py
--------------------------------
Adapter for PostgreSQL and AWS Aurora (PostgreSQL-compatible endpoint).

Both use the same psycopg2 driver and identical connection parameters.
Aurora MySQL-compatible endpoints are NOT covered here.

Env vars required (prefix = DQ_<NAME>_):
    DQ_<NAME>_HOST
    DQ_<NAME>_PORT        (optional, default: 5432)
    DQ_<NAME>_DATABASE
    DQ_<NAME>_USER
    DQ_<NAME>_PASSWORD
    DQ_<NAME>_SSLMODE     (optional, default: prefer)

For AWS Aurora, set HOST to the Aurora cluster or reader endpoint.
No IAM auth is wired here — use a password or RDS Proxy with token if needed.
"""

import logging
import os

from db.adapters.base import SourceAdapter

logger = logging.getLogger(__name__)

try:
    import psycopg2
    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False
    logger.warning("psycopg2-binary not installed — PostgreSQL/Aurora connections unavailable.")


class PostgresAdapter(SourceAdapter):
    """Wraps a psycopg2 connection."""

    source_type: str = "postgresql"

    def __init__(self, conn):
        self._conn = conn
        # Use autocommit for read-only source queries; avoids open transactions.
        self._conn.autocommit = True

    # ------------------------------------------------------------------
    # SourceAdapter interface
    # ------------------------------------------------------------------

    def cursor(self):
        """Return a standard psycopg2 cursor (rows as tuples)."""
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
    def build(cls, name: str) -> "PostgresAdapter":
        """
        Open a psycopg2 connection using env vars for `name`.

        Supports both plain PostgreSQL and AWS Aurora PG-compatible endpoints;
        the connection string is identical.
        """
        if not _AVAILABLE:
            raise ImportError(
                "psycopg2-binary is required for PostgreSQL/Aurora connections. "
                "Install with: pip install psycopg2-binary"
            )

        prefix   = f"DQ_{name.upper()}"
        host     = _require(f"{prefix}_HOST")
        port     = int(os.getenv(f"{prefix}_PORT", "5432"))
        database = _require(f"{prefix}_DATABASE")
        user     = _require(f"{prefix}_USER")
        password = _require(f"{prefix}_PASSWORD")
        sslmode  = os.getenv(f"{prefix}_SSLMODE", "prefer")

        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
            sslmode=sslmode,
        )
        logger.debug("psycopg2 connected to %s:%s/%s", host, port, database)
        return cls(conn)


# ---------------------------------------------------------------------------

def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"Required env var '{key}' is not set.")
    return val
