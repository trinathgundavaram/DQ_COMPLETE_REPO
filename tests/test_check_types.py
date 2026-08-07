"""
Coverage for core/check_types.py's 23 built-in check_type generators.

Two layers per check type that takes required check_params:
  1. validate_rule_params() reports the missing-params error before any
     SQL is ever generated (static, no DB access).
  2. The generated SQL is actually run against DuckDB and produces the
     correct PASS/FAIL result -- not just "doesn't raise."

check_type is exercised through build_query() (core/rule_sql.py), the
same entry point core/engine.py uses, rather than calling the generator
functions directly, so this also proves the check_type authoring path
(path 2 of 3 in core/rule_sql.py) end to end.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json

import duckdb
import pytest

from core.check_types import CHECK_CATALOG, get_level, list_check_types, _column_exists
from core.rule_sql import build_query, validate_rule_params

SOURCE_TYPE = "file"   # hits the DuckDB/file branch in every generator


def _rule(check_type, check_column=None, table="t1", **check_params):
    r = {
        "rule_code":  f"R_{check_type}",
        "check_type": check_type,
        "src_tbl_nm": table,
    }
    if check_column is not None:
        r["check_column"] = check_column
    if check_params:
        r["check_params"] = json.dumps(check_params)
    return r


def _run():
    return {"run_mode": "FULL"}


def _failing_keys(conn, rule, key_col="id"):
    """ROW-level: run the built query, return the sorted set of key_col values it flags."""
    sql, level = build_query(rule, _run(), SOURCE_TYPE)
    assert level == "ROW"
    rows = conn.execute(sql).fetchall()
    cols = [d[0] for d in conn.description]
    idx = cols.index(key_col)
    return sorted(r[idx] for r in rows)


def _table_check_fails(conn, rule):
    """TABLE-level: 0 rows = PASS, >=1 row = FAIL (per check_types.py's own convention)."""
    sql, level = build_query(rule, _run(), SOURCE_TYPE)
    assert level == "TABLE"
    return len(conn.execute(sql).fetchall()) >= 1


# =============================================================================
# Catalog structure
# =============================================================================

def test_catalog_has_24_check_types():
    assert len(list_check_types()) == 24


def test_every_non_schema_type_has_a_callable_generator():
    for ct, spec in CHECK_CATALOG.items():
        if spec["level"] == "SCHEMA":
            continue
        assert callable(spec["fn"]), ct
        assert spec["level"] in ("ROW", "TABLE"), ct


def test_column_exists_is_schema_level_and_executor_handled():
    assert get_level("COLUMN_EXISTS") == "SCHEMA"
    with pytest.raises(NotImplementedError):
        _column_exists({}, "t", "t", "1=1", {}, SOURCE_TYPE)


@pytest.mark.parametrize("check_type,missing_key", [
    ("REGEX_MATCH", "pattern"),
    ("IN_LIST", "values"),
    ("NOT_IN_LIST", "values"),
    ("RANGE_CHECK", "min_value"),
    ("MIN_VALUE", "min_value"),
    ("MAX_VALUE", "max_value"),
    ("CROSS_COLUMN", "expression"),
    ("CONDITIONAL", "if_column"),
    ("REFERENTIAL_INTEGRITY", "ref_table"),
    ("FRESHNESS", "max_age_hours"),
    ("MIN_ROW_COUNT", "min_rows"),
    ("MAX_ROW_COUNT", "max_rows"),
    ("ROW_COUNT_RANGE", "min_rows"),
    ("AGGREGATE_RANGE", "aggregate"),
    ("SUM_MATCH", "ref_table"),
    ("COUNT_MATCH", "ref_table"),
])
def test_validate_rule_params_reports_missing_required_params(check_type, missing_key):
    rule = {"rule_code": "RX", "check_type": check_type, "check_column": "col1"}
    err = validate_rule_params(rule)
    assert err is not None
    assert missing_key in err


# =============================================================================
# Fixture tables
# =============================================================================

def _people_conn():
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE t1 (
            id INTEGER, email VARCHAR, age INTEGER, region VARCHAR,
            status VARCHAR, effective_date DATE, salary DOUBLE, dept VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO t1 VALUES
            (1, 'a@x.com',  30, 'EAST', 'ACTIVE',   '2026-01-01', 50000, 'ENG'),
            (2, NULL,       25, 'WEST', 'ACTIVE',   NULL,         52000, 'ENG'),
            (3, 'c@x.com',  NULL, 'EAST', 'INACTIVE', NULL,       48000, 'OPS'),
            (4, 'd@x.com',  40, 'NORTH', 'ACTIVE',  '2026-02-01', 51000, 'OPS'),
            (5, 'a@x.com',  35, 'SOUTH', 'CLOSED',  NULL,         1000000, 'ENG')
    """)
    return conn


# =============================================================================
# COMPLETENESS
# =============================================================================

def test_not_null():
    conn = _people_conn()
    rule = _rule("NOT_NULL", check_column="email")
    assert _failing_keys(conn, rule) == [2]


def test_not_empty():
    conn = _people_conn()
    conn.execute("UPDATE t1 SET email = '   ' WHERE id = 4")
    rule = _rule("NOT_EMPTY", check_column="email")
    assert _failing_keys(conn, rule) == [2, 4]


# =============================================================================
# UNIQUENESS
# =============================================================================

def test_unique():
    conn = _people_conn()
    rule = _rule("UNIQUE", check_column="email")
    # 'a@x.com' appears twice (id 1 and 5) -- both are "not unique"
    assert _failing_keys(conn, rule) == [1, 5]


def test_unique_combination():
    conn = _people_conn()
    conn.execute("INSERT INTO t1 VALUES (6, 'e@x.com', 30, 'EAST', 'ACTIVE', '2026-01-01', 50000, 'ENG')")
    rule = _rule("UNIQUE_COMBINATION", check_column="age, region")
    # (30, EAST) now appears for id 1 and id 6
    assert _failing_keys(conn, rule) == [1, 6]


# =============================================================================
# VALIDITY
# =============================================================================

def test_regex_match():
    conn = _people_conn()
    rule = _rule("REGEX_MATCH", check_column="email", pattern=r"^[a-z]@x\.com$")
    # NULL email (id 2) fails the regex too (NOT REGEXP_MATCHES(NULL,...) is NULL,
    # which is excluded by the WHERE — DuckDB NULL semantics), so only check the
    # non-NULL mismatches explicitly by giving id 2 a non-matching value instead.
    conn.execute("UPDATE t1 SET email = 'BAD_FORMAT' WHERE id = 2")
    assert _failing_keys(conn, rule) == [2]


def test_in_list():
    conn = _people_conn()
    rule = _rule("IN_LIST", check_column="region", values=["EAST", "WEST"])
    assert _failing_keys(conn, rule) == [4, 5]   # NORTH, SOUTH


def test_not_in_list():
    conn = _people_conn()
    rule = _rule("NOT_IN_LIST", check_column="status", values=["CLOSED"])
    assert _failing_keys(conn, rule) == [5]


def test_range_check():
    conn = _people_conn()
    rule = _rule("RANGE_CHECK", check_column="age", min_value=26, max_value=39)
    # age NULL (id 3) is excluded by NULL comparison semantics; 25 and 40 fail
    assert _failing_keys(conn, rule) == [2, 4]


def test_min_value():
    conn = _people_conn()
    rule = _rule("MIN_VALUE", check_column="age", min_value=30)
    assert _failing_keys(conn, rule) == [2]


def test_max_value():
    conn = _people_conn()
    rule = _rule("MAX_VALUE", check_column="age", max_value=35)
    assert _failing_keys(conn, rule) == [4]


def test_positive_value():
    conn = _people_conn()
    conn.execute("UPDATE t1 SET salary = -5 WHERE id = 3")
    conn.execute("UPDATE t1 SET salary = NULL WHERE id = 4")
    rule = _rule("POSITIVE_VALUE", check_column="salary")
    assert _failing_keys(conn, rule) == [3, 4]


def test_non_negative():
    conn = _people_conn()
    conn.execute("UPDATE t1 SET salary = -5 WHERE id = 3")
    conn.execute("UPDATE t1 SET salary = 0 WHERE id = 4")
    rule = _rule("NON_NEGATIVE", check_column="salary")
    assert _failing_keys(conn, rule) == [3]   # 0 is allowed, -5 is not, NULL n/a


# =============================================================================
# CONSISTENCY
# =============================================================================

def test_cross_column():
    conn = _people_conn()
    conn.execute("ALTER TABLE t1 ADD COLUMN term_date DATE")
    conn.execute("UPDATE t1 SET effective_date = '2026-03-01', term_date = '2026-01-01' WHERE id = 1")
    rule = _rule("CROSS_COLUMN", expression="term_date IS NULL OR term_date >= effective_date")
    assert _failing_keys(conn, rule) == [1]


def test_conditional_if_active_then_effective_date_not_null():
    conn = _people_conn()
    rule = _rule(
        "CONDITIONAL",
        if_column="status", if_value="ACTIVE",
        then_column="effective_date", then_operator="IS NOT NULL",
    )
    # ACTIVE rows: id 1 (has date, OK), id 2 (NULL date, FAIL), id 4 (has date, OK)
    assert _failing_keys(conn, rule) == [2]


def test_referential_integrity():
    conn = _people_conn()
    conn.execute("CREATE TABLE depts (dept_code VARCHAR)")
    conn.execute("INSERT INTO depts VALUES ('ENG')")
    rule = _rule("REFERENTIAL_INTEGRITY", check_column="dept",
                 ref_table="depts", ref_column="dept_code")
    assert _failing_keys(conn, rule) == [3, 4]   # OPS has no match in depts


# =============================================================================
# ACCURACY / STATISTICAL
# =============================================================================

def test_outlier_check():
    conn = _people_conn()
    rule = _rule("OUTLIER_CHECK", check_column="salary", n_stddev=1.5)
    # id 5's salary (1,000,000) is a massive outlier vs the other ~50k rows
    assert _failing_keys(conn, rule) == [5]


# =============================================================================
# TIMELINESS (TABLE)
# =============================================================================

def test_freshness_fails_when_stale():
    conn = _people_conn()
    conn.execute("ALTER TABLE t1 ADD COLUMN pulled_at TIMESTAMP")
    conn.execute("UPDATE t1 SET pulled_at = TIMESTAMP '2000-01-01 00:00:00'")
    rule = _rule("FRESHNESS", check_column="pulled_at", max_age_hours=24)
    assert _table_check_fails(conn, rule) is True


def test_freshness_passes_when_recent():
    conn = _people_conn()
    conn.execute("ALTER TABLE t1 ADD COLUMN pulled_at TIMESTAMP")
    conn.execute("UPDATE t1 SET pulled_at = CURRENT_TIMESTAMP")
    rule = _rule("FRESHNESS", check_column="pulled_at", max_age_hours=24)
    assert _table_check_fails(conn, rule) is False


# =============================================================================
# VOLUME (TABLE)
# =============================================================================

def test_min_row_count_fails():
    conn = _people_conn()
    rule = _rule("MIN_ROW_COUNT", min_rows=10)
    assert _table_check_fails(conn, rule) is True


def test_min_row_count_passes():
    conn = _people_conn()
    rule = _rule("MIN_ROW_COUNT", min_rows=3)
    assert _table_check_fails(conn, rule) is False


def test_max_row_count_fails():
    conn = _people_conn()
    rule = _rule("MAX_ROW_COUNT", max_rows=2)
    assert _table_check_fails(conn, rule) is True


def test_row_count_range():
    conn = _people_conn()
    rule = _rule("ROW_COUNT_RANGE", min_rows=10, max_rows=20)
    assert _table_check_fails(conn, rule) is True
    rule2 = _rule("ROW_COUNT_RANGE", min_rows=1, max_rows=10)
    assert _table_check_fails(conn, rule2) is False


def test_aggregate_range():
    conn = _people_conn()
    rule = _rule("AGGREGATE_RANGE", check_column="salary", aggregate="AVG",
                 min_value=0, max_value=100000)
    # id 5's 1,000,000 salary pulls AVG well above 100000
    assert _table_check_fails(conn, rule) is True


# =============================================================================
# CROSS-TABLE ACCURACY (TABLE)
# =============================================================================

def test_sum_match():
    conn = _people_conn()
    conn.execute("CREATE TABLE t1_ref (amt DOUBLE)")
    conn.execute("INSERT INTO t1_ref VALUES (1000000)")   # way off from SUM(t1.salary)
    rule = _rule("SUM_MATCH", check_column="salary", ref_table="t1_ref",
                 ref_column="amt", tolerance_pct=1)
    assert _table_check_fails(conn, rule) is True


def test_sum_match_within_tolerance_passes():
    conn = _people_conn()
    total = conn.execute("SELECT SUM(salary) FROM t1").fetchone()[0]
    conn.execute("CREATE TABLE t1_ref (amt DOUBLE)")
    conn.execute(f"INSERT INTO t1_ref VALUES ({total})")
    rule = _rule("SUM_MATCH", check_column="salary", ref_table="t1_ref",
                 ref_column="amt", tolerance_pct=1)
    assert _table_check_fails(conn, rule) is False


def test_count_match():
    conn = _people_conn()
    conn.execute("CREATE TABLE t1_ref (x INTEGER)")
    conn.execute("INSERT INTO t1_ref VALUES (1), (2), (3), (4), (5), (6), (7), (8)")
    rule = _rule("COUNT_MATCH", ref_table="t1_ref", tolerance_pct=1)
    assert _table_check_fails(conn, rule) is True   # 5 rows vs 8 rows
