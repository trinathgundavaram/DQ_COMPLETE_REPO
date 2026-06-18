"""
db/adapters/teradata_adapter.py
--------------------------------
Adapter for Teradata (teradatasql driver).

Env vars required (prefix = DQ_<NAME>_):
    DQ_<NAME>_HOST
    DQ_<NAME>_USER
    DQ_<NAME>_PASSWORD
    DQ_<NAME>_LOGMECH   (optional, default: LDAP)
"""

import logging
import os

from db.adapters.base import SourceAdapter

logger = logging.getLogger(__name__)

try:
    import teradatasql
    _AVAILABLE = True
except ImportError:
    _AVAILABLE = False
    logger.warning("teradatasql not installed — Teradata connections unavailable.")


class TeradataAdapter(SourceAdapter):
    """Thin wrapper around a teradatasql connection."""

    def __init__(self, conn):
        self._conn = conn

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
    def build(cls, name: str) -> "TeradataAdapter":
        """
        Open a new Teradata connection using env vars for `name`.
        Raises ImportError / EnvironmentError / teradatasql errors on failure.
        """
        if not _AVAILABLE:
            raise ImportError(
                "teradatasql is required for Teradata connections. "
                "Install with: pip install teradatasql"
            )

        prefix   = f"DQ_{name.upper()}"
        host     = _require(f"{prefix}_HOST")
        user     = _require(f"{prefix}_USER")
        password = _require(f"{prefix}_PASSWORD")
        logmech  = os.getenv(f"{prefix}_LOGMECH", "LDAP")

        conn = teradatasql.connect(
            host=host,
            user=user,
            password=password,
            logmech=logmech,
        )
        return cls(conn)


# ---------------------------------------------------------------------------

def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"Required env var '{key}' is not set.")
    return val
