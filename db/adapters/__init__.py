"""
db/adapters
-----------
Source-system adapters.  Each adapter wraps a native driver connection and
exposes a minimal DB-API 2.0–compatible interface so the rest of the
framework never needs to import driver-specific code directly.

Adapter interface
-----------------
    cursor()           → DB-API-compatible cursor
    commit()           → commit the current transaction (no-op for read-only sources)
    close()            → release the underlying connection
    ping() → bool      → lightweight liveness check
    prepare(rule)      → source-specific setup before queries run
                         (loads CSV/Excel into DuckDB; no-op for DB adapters)

Supported sources
-----------------
    teradata    : TeradataAdapter   — teradatasql
    postgresql  : PostgresAdapter   — psycopg2   (also covers AWS Aurora PG-compatible)
    databricks  : DatabricksAdapter — databricks-sql-connector
    sqlserver   : SqlServerAdapter  — pyodbc
    file        : FileAdapter       — DuckDB + pandas (CSV, Excel, TSV, Parquet)
"""

from db.adapters.base import SourceAdapter
from db.adapters.teradata_adapter import TeradataAdapter
from db.adapters.postgres_adapter import PostgresAdapter
from db.adapters.databricks_adapter import DatabricksAdapter
from db.adapters.sqlserver_adapter import SqlServerAdapter
from db.adapters.file_adapter import FileAdapter

__all__ = [
    "SourceAdapter",
    "TeradataAdapter",
    "PostgresAdapter",
    "DatabricksAdapter",
    "SqlServerAdapter",
    "FileAdapter",
]
