"""
shared/db_ops.py tests: the low-level DB helpers used by BOTH
rules_engine/ and sampling/ -- bulk writes with duplicate-key tolerance,
the dialect guard, and {key} run_params token substitution. No live DB
connection required -- DuckDB stands in for the metadata store.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
import pytest

from shared.db_ops import (
    execute_query, bulk_insert, bulk_insert_or_skip,
    check_dialect, DialectMismatchError, _substitute_params, build_run_params,
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


# ── run_params substitution ─────────────────────────────────────────────

def test_substitute_params_escapes_quotes():
    sql = "WHERE batch_id = '{batch_id}'"
    assert _substitute_params(sql, {"batch_id": "B1"}) == "WHERE batch_id = 'B1'"
    assert _substitute_params(sql, {"batch_id": "O'Brien"}) == "WHERE batch_id = 'O''Brien'"


def test_substitute_params_multi_key():
    sql = "WHERE batch_id = '{batch_id}' AND yr = {year} AND run_type = '{run_type}'"
    resolved = _substitute_params(sql, {"batch_id": "B1", "year": 2026, "run_type": "MONTHLY"})
    assert resolved == "WHERE batch_id = 'B1' AND yr = 2026 AND run_type = 'MONTHLY'"


def test_substitute_params_ignores_extra_keys_not_present_in_sql():
    sql = "WHERE batch_id = '{batch_id}'"
    assert _substitute_params(sql, {"batch_id": "B1", "unused": "x"}) == "WHERE batch_id = 'B1'"


def test_substitute_params_none_or_empty_sql_passthrough():
    assert _substitute_params(None, {"batch_id": "B1"}) is None
    assert _substitute_params("", {"batch_id": "B1"}) == ""


def test_substitute_params_none_value_becomes_empty_string():
    assert _substitute_params("v = '{x}'", {"x": None}) == "v = ''"


def test_substitute_params_unresolved_token_raises():
    sql = "WHERE batch_id = '{batch_id}' AND yr = {year}"
    with pytest.raises(ValueError) as exc_info:
        _substitute_params(sql, {"batch_id": "B1"})
    msg = str(exc_info.value)
    assert "year" in msg
    assert "batch_id" in msg  # lists what WAS supplied, for a fast diagnosis


def test_substitute_params_empty_params_dict_with_no_tokens_ok():
    assert _substitute_params("SELECT 1", {}) == "SELECT 1"
    assert _substitute_params("SELECT 1", None) == "SELECT 1"


# ── build_run_params ─────────────────────────────────────────────────────

def test_build_run_params_merges_extra_with_batch_id():
    assert build_run_params("B1", {"year": 2026}) == {"year": 2026, "batch_id": "B1"}


def test_build_run_params_no_extra():
    assert build_run_params("B1") == {"batch_id": "B1"}
    assert build_run_params("B1", None) == {"batch_id": "B1"}


def test_build_run_params_batch_id_always_wins_on_collision():
    # A stray "batch_id" key inside extra_params can never silently
    # override the dedicated, required batch_id argument.
    assert build_run_params("REAL", {"batch_id": "STRAY", "year": 2026}) == {
        "batch_id": "REAL", "year": 2026,
    }


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
