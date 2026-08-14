"""
shared/db_ops.py tests: the low-level DB helpers used by BOTH
rules_engine/ and sampling/ -- bulk writes with duplicate-key tolerance,
the dialect guard, and {batch_id} token substitution. No live DB
connection required -- DuckDB stands in for the metadata store.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
import pytest

from shared.db_ops import (
    execute_query, bulk_insert, bulk_insert_or_skip,
    check_dialect, DialectMismatchError, _substitute_batch_id,
)


def _conn():
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE gre_exceptions (
            record_id BIGINT, run_id VARCHAR, rule_id INTEGER, table_name VARCHAR,
            element_name VARCHAR, source_name VARCHAR, issue_desc VARCHAR,
            exception_flag VARCHAR DEFAULT 'OPEN', exception_approver VARCHAR,
            batch_id VARCHAR, etl_is_curr_ind VARCHAR DEFAULT 'Y',
            etl_load_dt DATE, etl_last_updt_dt TIMESTAMP,
            natural_key_value VARCHAR, created_at TIMESTAMP DEFAULT current_timestamp
        )
    """)
    conn.execute("CREATE UNIQUE INDEX gre_exceptions_uix ON gre_exceptions(rule_id, batch_id, natural_key_value)")
    return conn


# ── batch-id substitution ────────────────────────────────────────────────

def test_substitute_batch_id_escapes_quotes():
    sql = "WHERE batch_id = '{batch_id}'"
    assert _substitute_batch_id(sql, "B1") == "WHERE batch_id = 'B1'"
    assert _substitute_batch_id(sql, "O'Brien") == "WHERE batch_id = 'O''Brien'"


# ── bulk writes ───────────────────────────────────────────────────────────

def test_bulk_insert_batches_across_multiple_chunks():
    conn = _conn()
    rows = [["RUN", i, "t", "e", "s", f"issue {i}", "B1", f"claim_id=C{i}"] for i in range(7)]
    sql = """
        INSERT INTO gre_exceptions (
            run_id, rule_id, table_name, element_name, source_name,
            issue_desc, batch_id, natural_key_value
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    bulk_insert(conn, sql, rows, chunk_size=2)   # 7 rows, chunk_size=2 -> 4 chunks
    written = execute_query(conn, "SELECT COUNT(*) AS cnt FROM gre_exceptions")[0]["cnt"]
    assert written == 7


def test_bulk_insert_or_skip_chunk_falls_back_on_duplicate():
    conn = _conn()
    sql = """
        INSERT INTO gre_exceptions (
            run_id, rule_id, table_name, element_name, source_name,
            issue_desc, batch_id, natural_key_value
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    first = [["RUN1", 1, "t", "e", "s", "d", "B1", "claim_id=C1"],
             ["RUN1", 1, "t", "e", "s", "d", "B1", "claim_id=C2"]]
    inserted1 = bulk_insert_or_skip(conn, sql, first, chunk_size=10)
    assert inserted1 == 2

    # One brand-new key plus one duplicate of an already-committed key, in
    # the SAME chunk -- the whole-chunk executemany collides, exercising
    # the row-by-row fallback path rather than the happy-path executemany.
    # (DuckDB applies C3 before hitting the C1 collision, so the fallback's
    # own count of "newly inserted" can undercount here -- see
    # bulk_insert_or_skip()'s docstring; what must hold is the DATA: no
    # duplicate row, no dropped row, and no exception escaping.)
    second = [["RUN2", 1, "t", "e", "s", "d", "B1", "claim_id=C3"],
              ["RUN2", 1, "t", "e", "s", "d", "B1", "claim_id=C1"]]
    bulk_insert_or_skip(conn, sql, second, chunk_size=10)

    total = execute_query(conn, "SELECT COUNT(*) AS cnt FROM gre_exceptions")[0]["cnt"]
    assert total == 3   # C1, C2, C3 -- no duplicate row, no lost row
    keys = {r["natural_key_value"] for r in execute_query(conn, "SELECT natural_key_value FROM gre_exceptions")}
    assert keys == {"claim_id=C1", "claim_id=C2", "claim_id=C3"}


# ── dialect guard ─────────────────────────────────────────────────────────

def test_check_dialect_ansi_accepted_everywhere():
    check_dialect({"rule_id": 1, "sql_dialect": "ansi"}, "postgresql")
    check_dialect({"rule_id": 1, "sql_dialect": "ansi"}, "teradata")
    check_dialect({"rule_id": 1, "sql_dialect": "ansi"}, "some_future_adapter")


def test_check_dialect_matching_dialect_passes():
    check_dialect({"rule_id": 1, "sql_dialect": "teradata"}, "teradata")
    check_dialect({"rule_id": 1, "sql_dialect": "postgres"}, "postgresql")
    check_dialect({"rule_id": 1, "sql_dialect": "postgres"}, "s3")   # DuckDB-backed


def test_check_dialect_mismatch_raises():
    with pytest.raises(DialectMismatchError):
        check_dialect({"rule_id": 1, "sql_dialect": "teradata"}, "postgresql")


def test_check_dialect_invalid_value_raises():
    with pytest.raises(DialectMismatchError):
        check_dialect({"rule_id": 1, "sql_dialect": "mysql"}, "teradata")


def test_check_dialect_unrecognised_source_type_is_a_no_op():
    # databricks/sqlserver are deliberately not in DIALECT_COMPATIBILITY --
    # skip with a warning rather than guess.
    check_dialect({"rule_id": 1, "sql_dialect": "teradata"}, "databricks")
