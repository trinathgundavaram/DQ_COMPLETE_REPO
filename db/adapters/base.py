"""
db/adapters/base.py
-------------------
Abstract base class for all source-system adapters.

Every adapter must implement cursor(), commit(), close(), and ping().
prepare() is optional — the default is a no-op, used only by FileAdapter.
"""

from abc import ABC, abstractmethod


class SourceAdapter(ABC):
    """
    Minimal DB-API 2.0-compatible wrapper around a source connection.

    The executor only calls these methods on db_conn (the source connection):
        cursor()       — to run SELECT queries
        commit()       — after DML (not used on read-only sources)
        close()        — on cleanup
        ping()         — health check in ConnectionFactory.get()
        prepare(rule)  — file loading hook; no-op for DB adapters
    """

    @abstractmethod
    def cursor(self):
        """Return a DB-API cursor for the underlying connection."""
        ...

    @abstractmethod
    def commit(self):
        """Commit the current transaction."""
        ...

    @abstractmethod
    def close(self):
        """Close the underlying connection."""
        ...

    def ping(self) -> bool:
        """
        Lightweight liveness check.  Default: execute SELECT 1 via cursor.
        Override for sources that have a cheaper health check.
        """
        try:
            cur = self.cursor()
            cur.execute("SELECT 1")
            cur.close()
            return True
        except Exception:
            return False

    def prepare(self, rule: dict) -> None:
        """
        Source-specific setup called once per rule before any query runs.

        Default: no-op.  Override in FileAdapter to load CSV/Excel files
        into the in-memory DuckDB engine.
        """
