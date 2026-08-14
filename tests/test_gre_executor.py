"""
Generic Rules Engine (gre/) executor tests. Same style as test_core.py:
no live DB connection required -- DuckDB stands in for both the source
table and the gre_ metadata store (schema-qualified as "main", DuckDB's
default schema, so the f"{meta_db}.table" pattern the engine uses in
production works unchanged here).
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
import pytest

import gre.executor as gre_executor
from gre.executor import (
    evaluate_threshold, build_natural_key, _substitute_batch_id,
    execute_rule, execute_query, bulk_insert, bulk_insert_or_skip,
)

META_DB = "main"


def _conn():
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE claims (
            claim_id VARCHAR, denial_reason VARCHAR, batch_id VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO claims VALUES
            ('C1', NULL, 'B1'),
            ('C2', 'Not medically necessary', 'B1'),
            ('C3', NULL, 'B1'),
            ('C4', 'X', 'B1'),
            ('C5', NULL, 'B2')
    """)

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

    conn.execute("""
        CREATE TABLE gre_log (
            log_id BIGINT, run_id VARCHAR, rule_id INTEGER, rule_group VARCHAR,
            batch_id VARCHAR, seq_no INTEGER, start_time TIMESTAMP, end_time TIMESTAMP,
            status VARCHAR, rowcount BIGINT, error_message VARCHAR,
            created_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    conn.execute("""
        CREATE TABLE gre_errors (
            error_id BIGINT, run_id VARCHAR, rule_id INTEGER, rule_group VARCHAR,
            batch_id VARCHAR, error_type VARCHAR, error_message VARCHAR,
            error_detail VARCHAR, occurred_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    conn.execute("""
        CREATE TABLE gre_results (
            result_id BIGINT, rule_id INTEGER, batch_id VARCHAR, run_id VARCHAR,
            total_records BIGINT, failed_records BIGINT, failure_pct DOUBLE,
            threshold_pct_used DOUBLE, threshold_count_used INTEGER,
            threshold_operator_used VARCHAR, severity VARCHAR, status VARCHAR,
            evaluated_at TIMESTAMP DEFAULT current_timestamp
        )
    """)
    conn.execute("CREATE UNIQUE INDEX gre_results_uix ON gre_results(rule_id, batch_id)")

    return conn


def _rule(**overrides):
    rule = {
        "rule_id": 1,
        "rule_name": "Denied claim missing denial_reason",
        "table_name": "claims",
        "source_connection": "duckdb_test",
        "sql_dialect": "ansi",
        "rule_sql": "SELECT claim_id, denial_reason FROM claims "
                    "WHERE denial_reason IS NULL AND batch_id = '{batch_id}'",
        "scope_sql": None,
        "batch_id_column": "batch_id",
        "rule_group": "claims_dq",
        "seq_no": 10,
        "sequencing_mode": "independent",
        "on_failure": "skip_and_continue",
        "threshold_pct": None,
        "threshold_count": None,
        "threshold_operator": "OR",
        "severity": "Data Validation Error",
        "natural_key_columns": "claim_id",
        "element_name": "denial_reason",
    }
    rule.update(overrides)
    return rule


# ── evaluate_threshold ──────────────────────────────────────────────────

def test_threshold_pct_exactly_at_boundary_is_pass():
    # 750/1000 = 75.0%, threshold_pct=75 -> strictly-greater-than means PASS
    v = evaluate_threshold(1000, 750, threshold_pct=75, severity="Data Validation Error")
    assert v["status"] == "PASS" and v["write_result"] is True


def test_threshold_pct_just_over_boundary_is_fail():
    v = evaluate_threshold(1000, 751, threshold_pct=75, severity="Data Validation Error")
    assert v["status"] == "FAIL" and v["write_result"] is True


def test_threshold_count_breaches_independently_of_pct_under_or():
    # 750/1000=75% (not >75), but count=50 threshold with OR -> breach via count alone
    v = evaluate_threshold(1000, 750, threshold_pct=75, threshold_count=50,
                           threshold_operator="OR", severity="Data Validation Error")
    assert v["status"] == "FAIL"


def test_threshold_and_operator_requires_both():
    # pct breaches (75.1 > 75) but count does not (750 !> 800) -> AND -> PASS
    v = evaluate_threshold(1000, 751, threshold_pct=75, threshold_count=800,
                           threshold_operator="AND", severity="Data Validation Error")
    assert v["status"] == "PASS"
    # both breach -> FAIL
    v2 = evaluate_threshold(1000, 900, threshold_pct=75, threshold_count=800,
                            threshold_operator="AND", severity="Data Validation Error")
    assert v2["status"] == "FAIL"


def test_soft_severity_resolves_to_warn():
    v = evaluate_threshold(1000, 751, threshold_pct=75, severity="WARN")
    assert v["status"] == "WARN"


def test_zero_total_is_pass_no_row():
    v = evaluate_threshold(0, 0, threshold_pct=10)
    assert v["status"] == "PASS" and v["write_result"] is False


def test_no_threshold_fallback_requires_every_record_to_fail():
    # 3 of 4 failed, no threshold configured -> PASS, and NOT written
    v = evaluate_threshold(4, 3)
    assert v["status"] == "PASS" and v["write_result"] is False

    # 4 of 4 failed -> breach, IS written
    v2 = evaluate_threshold(4, 4)
    assert v2["status"] == "FAIL" and v2["write_result"] is True
    assert v2["threshold_pct_used"] is None and v2["threshold_count_used"] is None


# ── natural key / batch-id substitution ─────────────────────────────────

def test_build_natural_key():
    rule = _rule(natural_key_columns="claim_id, denial_reason")
    key = build_natural_key(rule, {"claim_id": "C1", "denial_reason": None})
    assert key == "claim_id=C1|denial_reason=NULL"


def test_build_natural_key_requires_columns():
    rule = _rule(natural_key_columns="")
    with pytest.raises(ValueError):
        build_natural_key(rule, {"claim_id": "C1"})


def test_substitute_batch_id_escapes_quotes():
    sql = "WHERE batch_id = '{batch_id}'"
    assert _substitute_batch_id(sql, "B1") == "WHERE batch_id = 'B1'"
    assert _substitute_batch_id(sql, "O'Brien") == "WHERE batch_id = 'O''Brien'"


# ── execute_rule end-to-end ──────────────────────────────────────────────

def test_execute_rule_writes_exceptions_and_result():
    conn = _conn()
    rule = _rule(threshold_pct=25)  # 2/4 = 50% > 25% -> FAIL

    status = execute_rule(rule, conn, conn, "RUN1", "B1", META_DB)
    assert status == "SUCCESS"

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND batch_id = 'B1'")
    assert len(exceptions) == 2
    assert {r["natural_key_value"] for r in exceptions} == {"claim_id=C1", "claim_id=C3"}

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND batch_id = 'B1'")
    assert len(results) == 1
    assert results[0]["status"] == "FAIL"
    assert results[0]["total_records"] == 4
    assert results[0]["failed_records"] == 2
    assert results[0]["threshold_pct_used"] == 25

    logs = execute_query(conn, "SELECT * FROM gre_log WHERE rule_id = 1 AND batch_id = 'B1'")
    assert len(logs) == 1
    assert logs[0]["status"] == "SUCCESS"
    assert logs[0]["rowcount"] == 2


def test_execute_rule_is_idempotent_on_rerun():
    conn = _conn()
    rule = _rule(threshold_pct=25)

    execute_rule(rule, conn, conn, "RUN1", "B1", META_DB)
    execute_rule(rule, conn, conn, "RUN2", "B1", META_DB)  # simulate a rerun of the same batch

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND batch_id = 'B1'")
    assert len(exceptions) == 2   # not duplicated

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND batch_id = 'B1'")
    assert len(results) == 1      # upserted in place, not a second row
    assert results[0]["run_id"] == "RUN2"   # reflects the latest run

    logs = execute_query(conn, "SELECT * FROM gre_log WHERE rule_id = 1 AND batch_id = 'B1'")
    assert len(logs) == 2         # both attempts are logged (attempt history, unlike results)


def test_execute_rule_batches_are_isolated():
    conn = _conn()
    rule = _rule(threshold_pct=25)

    execute_rule(rule, conn, conn, "RUN1", "B1", META_DB)
    execute_rule(rule, conn, conn, "RUN1", "B2", META_DB)

    b1 = execute_query(conn, "SELECT * FROM gre_exceptions WHERE batch_id = 'B1'")
    b2 = execute_query(conn, "SELECT * FROM gre_exceptions WHERE batch_id = 'B2'")
    assert len(b1) == 2
    assert len(b2) == 1   # only C5 is in B2


def test_execute_rule_no_threshold_fallback_not_written_when_partial_failure():
    conn = _conn()
    rule = _rule(threshold_pct=None, threshold_count=None)  # 2/4 fail, no threshold

    status = execute_rule(rule, conn, conn, "RUN1", "B1", META_DB)
    assert status == "SUCCESS"

    # Exceptions are still written regardless of any threshold...
    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND batch_id = 'B1'")
    assert len(exceptions) == 2

    # ...but gre_results gets no row, since not every in-scope record failed.
    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND batch_id = 'B1'")
    assert len(results) == 0


def test_execute_rule_sql_error_routes_to_errors_and_logs():
    conn = _conn()
    rule = _rule(rule_sql="SELECT * FROM no_such_table WHERE batch_id = '{batch_id}'")

    status = execute_rule(rule, conn, conn, "RUN1", "B1", META_DB)
    assert status == "ERROR"

    errors = execute_query(conn, "SELECT * FROM gre_errors WHERE rule_id = 1")
    assert len(errors) == 1
    assert errors[0]["error_type"] == "SQL_RUNTIME"

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1")
    assert len(exceptions) == 0   # a crash never writes partial findings

    logs = execute_query(conn, "SELECT * FROM gre_log WHERE rule_id = 1")
    assert len(logs) == 1 and logs[0]["status"] == "ERROR"


# ── big-dataset path: bulk writes, dedup, and the true-count/capped-fetch split ──

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


def test_write_exceptions_dedupes_natural_key_within_one_pull():
    conn = _conn()
    # A rule_sql that returns the SAME claim_id twice in one pull (e.g. a
    # join fan-out) should still only write ONE gre_exceptions row per
    # natural key. The true failed_records count (a COUNT(*) on rule_sql
    # itself) legitimately counts every row rule_sql returns, duplicates
    # included -- it's the exception-DETAIL rows that get deduplicated.
    rule = _rule(
        rule_sql="SELECT claim_id, denial_reason FROM claims "
                 "WHERE denial_reason IS NULL AND batch_id = '{batch_id}' "
                 "UNION ALL "
                 "SELECT claim_id, denial_reason FROM claims "
                 "WHERE denial_reason IS NULL AND batch_id = '{batch_id}'",
    )
    status = execute_rule(rule, conn, conn, "RUN1", "B1", META_DB)
    assert status == "SUCCESS"

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND batch_id = 'B1'")
    assert len(exceptions) == 2   # C1, C3 -- deduped, not 4, even though the pull returned each twice
    assert {r["natural_key_value"] for r in exceptions} == {"claim_id=C1", "claim_id=C3"}

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND batch_id = 'B1'")
    assert len(results) == 1
    assert results[0]["failed_records"] == 4   # true COUNT(*) on rule_sql counts every returned row


def test_max_exceptions_cap_keeps_failed_records_true_but_caps_detail_rows(monkeypatch):
    conn = _conn()
    monkeypatch.setattr(gre_executor, "MAX_EXCEPTIONS", 1)
    rule = _rule(threshold_pct=0)   # any failure breaches -> a gre_results row is written

    status = execute_rule(rule, conn, conn, "RUN1", "B1", META_DB)
    assert status == "SUCCESS"

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND batch_id = 'B1'")
    assert len(exceptions) == 1   # capped at MAX_EXCEPTIONS=1

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND batch_id = 'B1'")
    assert results[0]["failed_records"] == 2   # true count stays exact -- 2 of 4 actually failed

    logs = execute_query(conn, "SELECT * FROM gre_log WHERE rule_id = 1 AND batch_id = 'B1'")
    assert logs[0]["rowcount"] == 1   # "rows written to gre_exceptions this attempt" == the capped count
