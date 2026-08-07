"""
Core rule-execution tests: SQL-dialect enforcement (core/rule_sql.py),
raw-SQL rule authoring incl. cross-table joins, and AND/OR threshold
evaluation (core/executor.py). No live DB connection required — DuckDB
stands in for the target databases.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
import pytest

from core.rule_sql import (
    check_dialect, DialectMismatchError,
    build_rule_sql, validate_rule_params, is_raw_sql_rule, build_filter,
)
from core.executor import evaluate_rule


# ── Dialect enforcement ──────────────────────────────────────────────────

def test_legacy_rule_no_dialect_is_exempt():
    rule = {"rule_code": "R1", "sql_dialect": None}
    check_dialect(rule, "teradata")  # must not raise


def test_matching_dialect_passes():
    rule = {"rule_code": "R2", "sql_dialect": "teradata"}
    check_dialect(rule, "teradata")


def test_mismatched_dialect_raises():
    rule = {"rule_code": "R3", "sql_dialect": "postgres"}
    with pytest.raises(DialectMismatchError) as exc:
        check_dialect(rule, "teradata")
    assert "R3" in str(exc.value)
    assert "postgres" in str(exc.value)
    assert "teradata" in str(exc.value)


def test_ansi_dialect_always_allowed():
    rule = {"rule_code": "R4", "sql_dialect": "ansi"}
    check_dialect(rule, "teradata")
    check_dialect(rule, "postgresql")
    check_dialect(rule, "s3")


def test_s3_accepts_postgres_dialect():
    rule = {"rule_code": "R5", "sql_dialect": "postgres"}
    check_dialect(rule, "s3")


def test_s3_rejects_teradata_dialect():
    rule = {"rule_code": "R6", "sql_dialect": "teradata"}
    with pytest.raises(DialectMismatchError):
        check_dialect(rule, "s3")


def test_invalid_dialect_value_raises():
    rule = {"rule_code": "R7", "sql_dialect": "mysql"}
    with pytest.raises(DialectMismatchError):
        check_dialect(rule, "postgresql")


# ── Raw-SQL rule authoring (end-to-end against DuckDB) ──────────────────

def _conn():
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE um_universe (
            enrollee_id VARCHAR, authorization_number VARCHAR,
            request_disposition VARCHAR, denial_reason VARCHAR,
            pull_date DATE
        )
    """)
    conn.execute("""
        INSERT INTO um_universe VALUES
            ('E1','A1','Denied', NULL, '2026-08-01'),
            ('E2','A2','Denied', 'Not medically necessary', '2026-08-01'),
            ('E3','A3','Approved', NULL, '2026-08-01'),
            ('E4','A4','Denied', '', '2026-07-25')
    """)
    conn.execute("""
        CREATE TABLE shrpa_reference (
            authorization_number VARCHAR, requires_clinical_review VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO shrpa_reference VALUES ('A3', 'Y'), ('A1', 'N')
    """)
    return conn


def test_simple_negative_sql_rule_no_filter():
    conn = _conn()
    rule = {
        "rule_code": "RULE-014",
        "sql_dialect": "postgres",
        "check_type": "CONDITIONAL",
        "primary_key_columns": "enrollee_id, authorization_number",
        "rule_syntax": (
            "SELECT enrollee_id, authorization_number\n"
            "FROM um_universe\n"
            "WHERE request_disposition = 'Denied'\n"
            "  AND (denial_reason IS NULL OR denial_reason = '')"
        ),
    }
    assert is_raw_sql_rule(rule)
    assert validate_rule_params(rule) is None

    sql, level = build_rule_sql(rule, table="um_universe", filter_sql="1=1", source_type="postgresql")
    assert level == "ROW"
    rows = conn.execute(sql).fetchall()
    # E1 (NULL reason) and E4 (blank reason) should violate; E2 has a reason.
    assert len(rows) == 2
    ids = {r[0] for r in rows}
    assert ids == {"E1", "E4"}


def test_raw_sql_rule_with_run_scoped_filter():
    conn = _conn()
    rule = {
        "rule_code": "RULE-014",
        "sql_dialect": "postgres",
        "primary_key_columns": "enrollee_id",
        "filter_column": "pull_date",
        "filter_type": "DATE",
        "rule_syntax": (
            "SELECT enrollee_id, authorization_number, pull_date\n"
            "FROM um_universe\n"
            "WHERE request_disposition = 'Denied'\n"
            "  AND (denial_reason IS NULL OR denial_reason = '')"
        ),
    }
    run = {"run_mode": "DATE", "start_date": "2026-08-01", "end_date": "2026-08-01"}
    filter_sql = build_filter(rule, run)
    sql, level = build_rule_sql(rule, table="um_universe", filter_sql=filter_sql, source_type="postgresql")
    rows = conn.execute(sql).fetchall()
    # Only E1 falls in the 2026-08-01 window; E4 is 2026-07-25 and gets filtered out.
    assert len(rows) == 1
    assert rows[0][0] == "E1"


def test_raw_sql_rule_with_cross_table_join_shrpa_pattern():
    """
    Mirrors the SHRPA rule pattern: Approved determinations require checking
    a SEPARATE reference table for a 'requires clinical review' flag. The join
    is written directly in the rule's own SQL — no separate join-config needed.
    """
    conn = _conn()
    rule = {
        "rule_code": "RULE-SHRPA-01",
        "sql_dialect": "postgres",
        "primary_key_columns": "enrollee_id, authorization_number",
        "rule_syntax": (
            "SELECT u.enrollee_id, u.authorization_number\n"
            "FROM um_universe u\n"
            "JOIN shrpa_reference s ON s.authorization_number = u.authorization_number\n"
            "WHERE u.request_disposition = 'Approved'\n"
            "  AND s.requires_clinical_review = 'Y'"
        ),
    }
    sql, level = build_rule_sql(rule, table="um_universe", filter_sql="1=1", source_type="postgresql")
    rows = conn.execute(sql).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "E3"


# ── Threshold evaluation ─────────────────────────────────────────────────

def test_evaluate_rule_and_operator_requires_both_breaches():
    # count breached but pct not breached -> AND should still PASS
    status = evaluate_rule(
        total=10000, failed=50, threshold_pct=5.0, threshold_count=10,
        severity="ERROR", threshold_operator="AND",
    )
    assert status == "PASS"  # 0.5% < 5% even though 50 > 10

    status2 = evaluate_rule(
        total=100, failed=50, threshold_pct=5.0, threshold_count=10,
        severity="ERROR", threshold_operator="AND",
    )
    assert status2 == "FAIL"  # 50% > 5% AND 50 > 10
