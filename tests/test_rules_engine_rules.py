"""
rules_engine/rules.py tests: load_rules() -- active-flag/rule_group
filtering, seq_no/rule_id ordering, and the rule_variant selection level
(NULL = universal, non-NULL = only when explicitly requested). No live DB
connection required -- DuckDB stands in for the gre_ metadata store.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb

from rules_engine.rules import load_rules

META_DB = "main"


def _conn():
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE gre_rules (
            rule_id INTEGER, rule_name VARCHAR, database_name VARCHAR, table_name VARCHAR,
            sql_dialect VARCHAR, rule_sql VARCHAR,
            rule_group VARCHAR, rule_variant VARCHAR,
            seq_no INTEGER, sequencing_mode VARCHAR, on_failure VARCHAR,
            threshold_pct DOUBLE, threshold_count INTEGER, threshold_operator VARCHAR,
            severity VARCHAR, natural_key_columns VARCHAR, element_name VARCHAR,
            active_flag INTEGER, created_at TIMESTAMP DEFAULT current_timestamp,
            updated_at TIMESTAMP
        )
    """)
    return conn


def _insert(conn, rule_id, rule_group="claims_dq", seq_no=100, active_flag=1, rule_variant=None):
    conn.execute("""
        INSERT INTO gre_rules (
            rule_id, rule_name, database_name, table_name, sql_dialect, rule_sql,
            rule_group, rule_variant, seq_no, natural_key_columns, active_flag
        ) VALUES (?, ?, 'main', 't', 'teradata', 'SELECT 1', ?, ?, ?, 'k', ?)
    """, [rule_id, f"rule {rule_id}", rule_group, rule_variant, seq_no, active_flag])


def test_load_rules_filters_by_group_and_active_flag():
    conn = _conn()
    _insert(conn, 1, rule_group="claims_dq")
    _insert(conn, 2, rule_group="other_group")
    _insert(conn, 3, rule_group="claims_dq", active_flag=0)

    rows = load_rules(conn, META_DB, "claims_dq")
    assert [r["rule_id"] for r in rows] == [1]


def test_load_rules_orders_by_seq_no_then_rule_id():
    conn = _conn()
    _insert(conn, 3, seq_no=10)
    _insert(conn, 1, seq_no=10)
    _insert(conn, 2, seq_no=5)

    rows = load_rules(conn, META_DB, "claims_dq")
    assert [r["rule_id"] for r in rows] == [2, 1, 3]


def test_load_rules_no_variant_requested_loads_only_universal_rules():
    conn = _conn()
    _insert(conn, 1, rule_variant=None)
    _insert(conn, 2, rule_variant="2026")

    rows = load_rules(conn, META_DB, "claims_dq")
    assert [r["rule_id"] for r in rows] == [1]


def test_load_rules_variant_requested_loads_universal_plus_matching():
    conn = _conn()
    _insert(conn, 1, rule_variant=None)
    _insert(conn, 2, rule_variant="2026")
    _insert(conn, 3, rule_variant="2025")

    rows = load_rules(conn, META_DB, "claims_dq", rule_variant="2026")
    assert {r["rule_id"] for r in rows} == {1, 2}


def test_load_rules_variant_requested_but_no_rule_has_it_still_returns_universal():
    conn = _conn()
    _insert(conn, 1, rule_variant=None)

    rows = load_rules(conn, META_DB, "claims_dq", rule_variant="2099")
    assert [r["rule_id"] for r in rows] == [1]


def test_load_rules_composite_variant_string_is_just_a_value():
    # No special dimension parsing -- a project composing "year|run_type"
    # into one string is just an opaque match against rule_variant.
    conn = _conn()
    _insert(conn, 1, rule_variant="2026|MONTHLY")
    _insert(conn, 2, rule_variant="2026|WEEKLY")

    rows = load_rules(conn, META_DB, "claims_dq", rule_variant="2026|MONTHLY")
    assert [r["rule_id"] for r in rows] == [1]


def test_load_rules_empty_group_returns_empty_list():
    conn = _conn()
    assert load_rules(conn, META_DB, "no_such_group") == []
