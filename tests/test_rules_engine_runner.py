"""
rules_engine/runner.py orchestration tests: checkpoint/resume and sequencing_mode's
on_failure behaviour (halt_group vs skip_and_continue). Same DuckDB-fixture
approach as test_rules_engine_executor.py; a tiny fake ConnectionFactory stands in
for db.connection_factory.ConnectionFactory since every rule here reads
from the same in-memory DuckDB connection.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb

import rules_engine.executor as rules_engine_executor
from rules_engine.runner import run_rule_group
from shared.db_ops import execute_query

META_DB = "main"


class _FakeConnectionFactory:
    """Every named connection resolves to the same DuckDB connection."""
    def __init__(self, conn):
        self._conn = conn

    def get(self, name):
        return self._conn


def _conn():
    conn = duckdb.connect(":memory:")

    conn.execute("CREATE TABLE claims (claim_id VARCHAR, denial_reason VARCHAR, batch_id VARCHAR)")
    conn.execute("""
        INSERT INTO claims VALUES
            ('C1', NULL, 'B1'),
            ('C2', 'Not medically necessary', 'B1'),
            ('C3', NULL, 'B1'),
            ('C4', 'X', 'B1')
    """)

    conn.execute("""
        CREATE TABLE gre_rules (
            rule_id INTEGER, rule_name VARCHAR, database_name VARCHAR, table_name VARCHAR,
            source_connection VARCHAR, sql_dialect VARCHAR, rule_sql VARCHAR,
            rule_group VARCHAR, rule_variant VARCHAR,
            seq_no INTEGER, sequencing_mode VARCHAR, on_failure VARCHAR,
            threshold_pct DOUBLE, threshold_count INTEGER, threshold_operator VARCHAR,
            severity VARCHAR, natural_key_columns VARCHAR, element_name VARCHAR,
            active_flag INTEGER, created_at TIMESTAMP DEFAULT current_timestamp,
            updated_at TIMESTAMP
        )
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

    conn.execute("""
        CREATE TABLE gre_audit (
            run_id VARCHAR, rule_group VARCHAR, batch_id VARCHAR, rule_variant VARCHAR,
            started_at TIMESTAMP, ended_at TIMESTAMP, status VARCHAR,
            total_rules INTEGER, rules_succeeded INTEGER, rules_errored INTEGER,
            triggered_by VARCHAR, created_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    return conn


def _insert_rule(conn, rule_id, rule_sql, seq_no, sequencing_mode="independent",
                  on_failure="skip_and_continue", rule_group="claims_dq", rule_variant=None):
    conn.execute("""
        INSERT INTO gre_rules (
            rule_id, rule_name, database_name, table_name, source_connection, sql_dialect, rule_sql,
            rule_group, rule_variant, seq_no, sequencing_mode, on_failure, natural_key_columns,
            active_flag
        ) VALUES (?, ?, 'main', 'claims', 'duckdb_test', 'ansi', ?, ?, ?, ?, ?, ?, 'claim_id', 1)
    """, [rule_id, f"rule {rule_id}", rule_sql, rule_group, rule_variant, seq_no, sequencing_mode, on_failure])


_MISSING_REASON_SQL = "SELECT claim_id FROM claims WHERE denial_reason IS NULL AND batch_id = '{batch_id}'"
_BROKEN_SQL = "SELECT * FROM no_such_table WHERE batch_id = '{batch_id}'"


def test_checkpoint_resume_skips_already_succeeded_rules():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10)
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20)

    # Pre-seed gre_log as if rule 1 already succeeded in a prior (interrupted) run.
    conn.execute("""
        INSERT INTO gre_log (run_id, rule_id, rule_group, batch_id, status, rowcount)
        VALUES ('PRIOR_RUN', 1, 'claims_dq', 'B1', 'SUCCESS', 2)
    """)

    summary = run_rule_group("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "COMPLETED"
    assert 1 not in summary["results"]          # rule 1 was skipped, not re-run
    assert summary["results"][2] == "SUCCESS"

    # Rule 1 should have exactly the ONE log row from before (not re-attempted).
    logs_r1 = execute_query(conn, "SELECT * FROM gre_log WHERE rule_id = 1")
    assert len(logs_r1) == 1
    logs_r2 = execute_query(conn, "SELECT * FROM gre_log WHERE rule_id = 2")
    assert len(logs_r2) == 1


def test_sequential_halt_group_stops_before_next_rule():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _BROKEN_SQL, seq_no=10, sequencing_mode="sequential", on_failure="halt_group")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20, sequencing_mode="sequential", on_failure="halt_group")

    summary = run_rule_group("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "HALTED"
    assert summary["results"][1] == "ERROR"
    assert 2 not in summary["results"]          # rule 2 was never started

    logs_r2 = execute_query(conn, "SELECT * FROM gre_log WHERE rule_id = 2")
    assert len(logs_r2) == 0


def test_sequential_skip_and_continue_runs_next_rule_after_error():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _BROKEN_SQL, seq_no=10, sequencing_mode="sequential", on_failure="skip_and_continue")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20, sequencing_mode="sequential", on_failure="skip_and_continue")

    summary = run_rule_group("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "COMPLETED"
    assert summary["results"][1] == "ERROR"
    assert summary["results"][2] == "SUCCESS"   # rule 2 still ran

    errors = execute_query(conn, "SELECT * FROM gre_errors WHERE rule_id = 1")
    assert len(errors) == 1


def test_independent_mode_runs_every_rule_regardless_of_earlier_errors():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _BROKEN_SQL, seq_no=10, sequencing_mode="independent")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20, sequencing_mode="independent")

    summary = run_rule_group("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "COMPLETED"
    assert summary["results"][1] == "ERROR"
    assert summary["results"][2] == "SUCCESS"


def test_no_rules_returns_no_rules_status():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    summary = run_rule_group("empty_group", "B1", cf, meta_conn=conn, meta_db=META_DB)
    assert summary["status"] == "NO_RULES"


def test_shared_total_cache_avoids_redundant_count_queries_across_rules(monkeypatch):
    # Two rules in the same group, same database_name/table_name, same
    # run_params -- _compute_total() auto-builds the identical
    # database.table + WHERE batch_id = 'B1' query for both, so within one
    # run_rule_group() call that COUNT(*) should only actually run once.
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10)
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20)
    # Force a gre_results row for both rules (any failure breaches), so the
    # cached total_records is actually observable below.
    conn.execute("UPDATE gre_rules SET threshold_pct = 0")

    calls = {"n": 0}
    original = rules_engine_executor._run_source_query

    def counting(db_conn, sql):
        calls["n"] += 1
        return original(db_conn, sql)

    monkeypatch.setattr(rules_engine_executor, "_run_source_query", counting)

    summary = run_rule_group("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "COMPLETED"
    assert summary["results"][1] == "SUCCESS"
    assert summary["results"][2] == "SUCCESS"
    # _run_source_query is only used by _compute_total() in this flow (the
    # rule_sql/failed-count path uses execute_query/_count_failed directly)
    # -- one call total proves the second rule's total was served from cache.
    assert calls["n"] == 1

    # Both rules should still report the correct total, proving the cached
    # value is being reused correctly, not just skipped.
    results = execute_query(conn, "SELECT rule_id, total_records FROM gre_results WHERE batch_id = 'B1'")
    assert {r["rule_id"]: r["total_records"] for r in results} == {1: 4, 2: 4}


# ── rule_variant (additional level on top of rule_group/table) ──────────

def test_rule_variant_none_requested_runs_only_universal_rules():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, rule_variant=None)      # universal
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20, rule_variant="2026")    # variant-specific

    summary = run_rule_group("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "COMPLETED"
    assert summary["total_rules"] == 1
    assert 1 in summary["results"]
    assert 2 not in summary["results"]   # variant-specific rule not requested -> not loaded/run


def test_rule_variant_requested_runs_universal_plus_matching_variant():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, rule_variant=None)       # universal
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20, rule_variant="2026")     # matches
    _insert_rule(conn, 3, _MISSING_REASON_SQL, seq_no=30, rule_variant="2025")     # does NOT match

    summary = run_rule_group("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB, rule_variant="2026")

    assert summary["status"] == "COMPLETED"
    assert summary["total_rules"] == 2
    assert set(summary["results"].keys()) == {1, 2}

    audit = execute_query(conn, "SELECT rule_variant FROM gre_audit WHERE run_id = ?", [summary["run_id"]])
    assert audit[0]["rule_variant"] == "2026"


# ── run_params threading (v2 scoping) ────────────────────────────────────

def test_run_params_extra_key_is_available_to_every_rule_sql():
    conn = _conn()
    # run_type must be a real column -- every run_params key also becomes
    # an equality filter for the auto-generated total-record count (see
    # rules_engine/executor.py::_build_total_query()).
    conn.execute("ALTER TABLE claims ADD COLUMN run_type VARCHAR DEFAULT 'MONTHLY'")
    cf = _FakeConnectionFactory(conn)
    _insert_rule(
        conn, 1,
        "SELECT claim_id FROM claims WHERE denial_reason IS NULL "
        "AND batch_id = '{batch_id}' AND '{run_type}' = 'MONTHLY'",
        seq_no=10,
    )

    summary = run_rule_group("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB,
                             run_params={"run_type": "MONTHLY"})

    assert summary["status"] == "COMPLETED"
    assert summary["results"][1] == "SUCCESS"


def test_run_params_stray_batch_id_key_never_overrides_the_real_one():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10)

    summary = run_rule_group("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB,
                             run_params={"batch_id": "SOMETHING_ELSE"})

    assert summary["status"] == "COMPLETED"
    # Findings are still recorded under the REAL batch_id ('B1'), proving
    # build_run_params() let the dedicated batch_id argument win.
    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND batch_id = 'B1'")
    assert len(exceptions) == 2
