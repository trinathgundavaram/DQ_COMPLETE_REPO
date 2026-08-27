"""
rules_engine/rules.py tests: load_rules() -- active-flag/rule_group
filtering, seq_no/rule_id ordering, and the rule_variant selection level.
rule_variant NOT passed means "no filter at all" (every active rule
regardless of its own rule_variant); rule_variant passed means "universal
(NULL) plus that exact value." No live DB connection required -- DuckDB
stands in for the gre_ metadata store.
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
            rule_id INTEGER, rule_nm VARCHAR, database_name VARCHAR, src_tbl_nm VARCHAR,
            sql_dialect VARCHAR, rule_syntax VARCHAR,
            rule_group VARCHAR, rule_variant VARCHAR,
            seq_no INTEGER, sequencing_mode VARCHAR, on_failure VARCHAR,
            threshold_pct DOUBLE, threshold_count INTEGER, threshold_operator VARCHAR,
            severity VARCHAR, src_key_cols VARCHAR, element_name VARCHAR,
            act_ind INTEGER, load_datetime TIMESTAMP DEFAULT current_timestamp,
            last_updated_datetime TIMESTAMP
        )
    """)
    return conn


def _insert(conn, rule_id, rule_group="claims_dq", seq_no=100, act_ind=1, rule_variant=None):
    conn.execute("""
        INSERT INTO gre_rules (
            rule_id, rule_nm, database_name, src_tbl_nm, sql_dialect, rule_syntax,
            rule_group, rule_variant, seq_no, src_key_cols, act_ind
        ) VALUES (?, ?, 'main', 't', 'teradata', 'SELECT 1', ?, ?, ?, 'k', ?)
    """, [rule_id, f"rule {rule_id}", rule_group, rule_variant, seq_no, act_ind])


def test_load_rules_filters_by_group_and_act_ind():
    conn = _conn()
    _insert(conn, 1, rule_group="claims_dq")
    _insert(conn, 2, rule_group="other_group")
    _insert(conn, 3, rule_group="claims_dq", act_ind=0)

    rows = load_rules(conn, META_DB, "claims_dq")
    assert [r["rule_id"] for r in rows] == [1]


def test_load_rules_orders_by_seq_no_then_rule_id():
    conn = _conn()
    _insert(conn, 3, seq_no=10)
    _insert(conn, 1, seq_no=10)
    _insert(conn, 2, seq_no=5)

    rows = load_rules(conn, META_DB, "claims_dq")
    assert [r["rule_id"] for r in rows] == [2, 1, 3]


def test_load_rules_no_variant_requested_loads_every_variant():
    # NOT passing rule_variant means "don't filter on it at all" -- every
    # active rule loads, universal or variant-tagged alike. See
    # load_rules()'s docstring for why this is deliberately NOT the same
    # as "rule_variant IS NULL".
    conn = _conn()
    _insert(conn, 1, rule_variant=None)
    _insert(conn, 2, rule_variant="2026")

    rows = load_rules(conn, META_DB, "claims_dq")
    assert {r["rule_id"] for r in rows} == {1, 2}


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


def test_load_rules_resolves_env_tokens_in_rule_syntax(monkeypatch):
    # rule_syntax can embed its own literal database.table references
    # (e.g. joining a second source database rule_syntax itself decided
    # to reference) -- $env/$ENV in that text gets the same environment
    # resolution as the row's own database_name column, via
    # rules_engine/config.py::resolve_env_tokens(). See that module's
    # tests for the token-matching mechanics themselves; this just
    # confirms load_rules() actually wires rule_syntax through it.
    monkeypatch.setenv("GRE_ENVIRONMENT", "QA")
    import importlib
    import rules_engine.config as shared_config
    importlib.reload(shared_config)
    try:
        conn = _conn()
        conn.execute("""
            INSERT INTO gre_rules (
                rule_id, rule_nm, database_name, src_tbl_nm, sql_dialect, rule_syntax,
                rule_group, seq_no, src_key_cols, act_ind
            ) VALUES (
                1, 'multi-db join', 'QNXT_core_$env_T', 't', 'teradata',
                'SELECT c.claim_id FROM QNXT_core_$env_T.claims c JOIN QNXT_ref_$env_T.codes r ON c.code_id = r.code_id',
                'claims_dq', 100, 'k', 1
            )
        """)

        rows = load_rules(conn, META_DB, "claims_dq")

        assert rows[0]["database_name"] == "QNXT_core_qa_T"
        assert rows[0]["rule_syntax"] == (
            "SELECT c.claim_id FROM QNXT_core_qa_T.claims c "
            "JOIN QNXT_ref_qa_T.codes r ON c.code_id = r.code_id"
        )
    finally:
        importlib.reload(shared_config)


def test_load_rules_rule_syntax_without_env_token_is_unaffected():
    conn = _conn()
    _insert(conn, 1)   # rule_syntax='SELECT 1', no $env/$ENV token

    rows = load_rules(conn, META_DB, "claims_dq")
    assert rows[0]["rule_syntax"] == "SELECT 1"
