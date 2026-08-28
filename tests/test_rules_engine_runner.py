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
import pytest

import rules_engine.executor as rules_engine_executor
from rules_engine.runner import (
    run_rule_group, discover_rule_groups, run_all_active_groups, run_by_process_name,
    run_by_scope,
    generate_run_id,
)
from rules_engine.db_ops import execute_query

META_DB = "main"


class _Adapter:
    """
    Minimal SourceAdapter shim wrapping a raw DuckDB connection/cursor: adds
    the prepare()/qualified_name() surface execute_rule()/_compute_total()
    call directly (db.connection_factory.SourceAdapter's interface) -- see
    test_rules_engine_executor.py's twin for why a raw duckdb object can't
    be handed to execute_rule() as db_conn anymore.
    """
    def __init__(self, conn):
        self._conn = conn

    def cursor(self):
        return self._conn.cursor()

    def commit(self):
        self._conn.commit()

    def prepare(self, rule: dict) -> None:
        pass

    def qualified_name(self, rule: dict) -> str:
        return f"{rule['database_name']}.{rule['src_tbl_nm']}"


class _FakeConnectionFactory:
    """Every source_type resolves to the same underlying DuckDB connection."""
    def __init__(self, conn):
        self._conn = conn

    def get(self, name):
        return _Adapter(self._conn)

    def new_connection(self, name):
        # A fresh DuckDB cursor sharing the same in-memory database -- this
        # is DuckDB's own documented way to get independent,
        # concurrency-safe connections to one database from multiple
        # threads, standing in here for what ConnectionFactory.
        # new_connection() does for a real adapter (build a genuinely
        # separate connection, not hand back the single shared one).
        return _Adapter(self._conn.cursor())


class _CountingFakeConnectionFactory(_FakeConnectionFactory):
    """
    Tracks new_connection() calls per name (for asserting pool sizing),
    and can simulate a connection that's unavailable altogether
    (max_builds={"name": 0}) or has limited capacity (max_builds={"name": N})
    -- used by the parallel-execution tests below.
    """
    def __init__(self, conn, max_builds=None):
        super().__init__(conn)
        self.calls = {}
        self.max_builds = max_builds or {}

    def new_connection(self, name):
        n = self.calls.get(name, 0)
        self.calls[name] = n + 1
        limit = self.max_builds.get(name)
        if limit is not None and n >= limit:
            return None
        return super().new_connection(name)


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
            rule_id INTEGER, rule_nm VARCHAR, database_name VARCHAR, src_tbl_nm VARCHAR,
            sql_dialect VARCHAR, rule_syntax VARCHAR,
            project_name VARCHAR, process_name VARCHAR,
            rule_group VARCHAR, rule_variant VARCHAR,
            seq_no INTEGER, sequencing_mode VARCHAR, on_failure VARCHAR,
            threshold_pct DOUBLE, threshold_count INTEGER, threshold_operator VARCHAR,
            severity VARCHAR, src_key_cols VARCHAR, element_name VARCHAR,
            act_ind INTEGER,
            universe_version VARCHAR, universe_year INTEGER, dgr_nbr VARCHAR,
            issue_category_name VARCHAR, business_rule VARCHAR, rule_description VARCHAR,
            created_by VARCHAR, last_updated_by VARCHAR,
            load_datetime TIMESTAMP DEFAULT current_timestamp,
            last_updated_datetime TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE gre_exceptions (
            record_id BIGINT, run_id VARCHAR, rule_id INTEGER, database_name VARCHAR, src_tbl_nm VARCHAR,
            project_name VARCHAR, process_name VARCHAR, rule_group VARCHAR, rule_variant VARCHAR,
            element_name VARCHAR, source_name VARCHAR, issue_desc VARCHAR,
            exception_flag VARCHAR DEFAULT 'OPEN', exception_approver VARCHAR,
            run_key VARCHAR, etl_is_curr_ind VARCHAR DEFAULT 'Y',
            etl_load_dt DATE, etl_last_updt_dt TIMESTAMP,
            src_key_value VARCHAR,
            rule_nm VARCHAR, dgr_nbr VARCHAR, universe_version VARCHAR,
            run_type VARCHAR, batch_schedule VARCHAR,
            load_datetime TIMESTAMP DEFAULT current_timestamp,
            last_updated_by VARCHAR, last_updated_datetime TIMESTAMP
        )
    """)
    conn.execute("CREATE UNIQUE INDEX gre_exceptions_uix ON gre_exceptions(rule_id, run_key, src_key_value)")

    conn.execute("""
        CREATE TABLE gre_rule_errors (
            error_id BIGINT, run_id VARCHAR, rule_id INTEGER, rule_group VARCHAR, rule_variant VARCHAR,
            run_key VARCHAR, error_type VARCHAR, error_message VARCHAR,
            error_detail VARCHAR, active_ind VARCHAR DEFAULT 'Y',
            occurred_at TIMESTAMP DEFAULT current_timestamp,
            last_updated_datetime TIMESTAMP
        )
    """)

    # Consolidated gre_log + gre_results -- one row per rule PER EXECUTION
    # ATTEMPT (run_id), not per rule_id+run_key. See rules_engine/schema.sql's
    # "gre_results" section / rules_engine/executor.py::_write_result()'s
    # docstring for the full rationale.
    conn.execute("""
        CREATE TABLE gre_results (
            result_id BIGINT, run_id VARCHAR, rule_id INTEGER, rule_group VARCHAR, rule_variant VARCHAR,
            project_name VARCHAR, process_name VARCHAR, run_key VARCHAR, seq_no INTEGER,
            start_time TIMESTAMP, end_time TIMESTAMP,
            total_records BIGINT, failed_records BIGINT, failure_pct DOUBLE,
            threshold_pct_used DOUBLE, threshold_count_used INTEGER,
            threshold_operator_used VARCHAR, severity VARCHAR, status VARCHAR,
            error_message VARCHAR, executed_sql VARCHAR, source_tieback_sql VARCHAR, active_ind VARCHAR DEFAULT 'Y',
            load_datetime TIMESTAMP DEFAULT current_timestamp,
            last_updated_datetime TIMESTAMP
        )
    """)
    # NOT unique on (rule_id, run_key) -- a rerun keeps history, so multiple
    # rows (one per run_id attempt) can legitimately share a rule_id+run_key,
    # distinguished by active_ind. Mirrors schema.sql's non-unique
    # gre_results_rule_run_key_active_ix.
    conn.execute("CREATE INDEX gre_results_rule_run_key_active_ix ON gre_results(rule_id, run_key, active_ind)")

    conn.execute("""
        CREATE TABLE gre_rule_audit (
            run_id VARCHAR, rule_group VARCHAR, project_name VARCHAR, process_name VARCHAR,
            run_key VARCHAR, rule_variant VARCHAR, run_params VARCHAR, extra_filters VARCHAR,
            started_at TIMESTAMP, ended_at TIMESTAMP, status VARCHAR,
            total_rules INTEGER, rules_succeeded INTEGER, rules_errored INTEGER,
            triggered_by VARCHAR, load_datetime TIMESTAMP DEFAULT current_timestamp
        )
    """)

    return conn


def _insert_rule(conn, rule_id, rule_syntax, seq_no, sequencing_mode="independent",
                  on_failure="skip_and_continue", rule_group="claims_dq", rule_variant=None,
                  project_name="HEALTHSPRING_UM", process_name="UNIVERSE_VALIDATION",
                  sql_dialect="teradata"):
    conn.execute("""
        INSERT INTO gre_rules (
            rule_id, rule_nm, database_name, src_tbl_nm, sql_dialect, rule_syntax,
            project_name, process_name, rule_group, rule_variant, seq_no, sequencing_mode, on_failure,
            src_key_cols, act_ind
        ) VALUES (?, ?, 'main', 'claims', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'claim_id', 1)
    """, [rule_id, f"rule {rule_id}", sql_dialect, rule_syntax, project_name, process_name, rule_group,
          rule_variant, seq_no, sequencing_mode, on_failure])


_MISSING_REASON_SQL = "SELECT claim_id FROM claims WHERE denial_reason IS NULL AND batch_id = '{batch_id}'"
_BROKEN_SQL = "SELECT * FROM no_such_table WHERE batch_id = '{batch_id}'"


def _run(rule_group, run_key, cf, **kwargs):
    """
    Test convenience wrapper: run_key is no longer auto-injected into
    run_params (see rules_engine/runner.py::run_rule_group()'s docstring
    -- doing so would corrupt _compute_total()'s auto-generated filter for
    any table without a run_key column). _MISSING_REASON_SQL/_BROKEN_SQL
    reference the real claims.batch_id business column via a "{batch_id}"
    token, so default it to run_key's value here (matching the OLD
    build_run_params() behavior) unless the caller passes their own
    run_params, in which case merge rather than replace.
    """
    run_params = {"batch_id": run_key}
    run_params.update(kwargs.pop("run_params", None) or {})
    return run_rule_group(rule_group, run_key, cf, run_params=run_params, **kwargs)


def _run_all(meta_conn, meta_db, run_key, cf, **kwargs):
    """Same convenience as _run(), for run_all_active_groups()."""
    run_params = {"batch_id": run_key}
    run_params.update(kwargs.pop("run_params", None) or {})
    return run_all_active_groups(meta_conn, meta_db, run_key, cf, run_params=run_params, **kwargs)


# ── generate_run_id (thin wrapper over rules_engine/db_ops.py's) ─────────

def test_generate_run_id_delegates_to_shared_helper():
    """
    rules_engine.runner.generate_run_id() is a thin wrapper around
    rules_engine/db_ops.py::generate_run_id() -- the format/uniqueness
    guarantees themselves are covered exhaustively in
    tests/test_rules_engine_db_ops.py; this just confirms the wrapper
    actually delegates rather than keeping its own (possibly drifted)
    copy of the old second-precision "{rule_group}_{run_key}_{ts}" format.
    """
    run_id = generate_run_id("claims_dq", "BATCH_2026_08_19")
    assert run_id.startswith("claims_dq::BATCH_2026_08_19::")
    parts = run_id.split("::")
    assert len(parts) == 4   # rule_group, run_key, timestamp, uniqueness suffix
    assert generate_run_id("claims_dq", "BATCH_2026_08_19") != run_id   # never collides


# ── run_rule_group()'s own richer run_id (project_name.rule_group,
#    attempt-N, triggered_by folded in -- see runner.py::_build_group_run_id()) ──

def test_run_rule_group_run_id_embeds_project_attempt_and_triggered_by():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=1, project_name="HEALTHSPRING_UM")

    summary = _run("claims_dq", "BATCH_2026_08_19", cf, meta_conn=conn, meta_db=META_DB, triggered_by="jsmith")

    # project_name.rule_group prefix, attempt-1 (first attempt at this
    # run_key), and triggered_by all visible directly in the id -- no join
    # back to gre_rule_audit needed to answer "what/who/which attempt was this."
    assert summary["run_id"].startswith("HEALTHSPRING_UM.claims_dq::BATCH_2026_08_19::attempt-1::jsmith::")

    audit = execute_query(conn, "SELECT triggered_by FROM gre_rule_audit WHERE run_id = ?", [summary["run_id"]])
    assert audit[0]["triggered_by"] == "jsmith"


def test_run_rule_group_run_id_attempt_number_increments_on_rerun():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=1)

    first = _run("claims_dq", "BATCH_2026_08_19", cf, meta_conn=conn, meta_db=META_DB)
    second = _run("claims_dq", "BATCH_2026_08_19", cf, meta_conn=conn, meta_db=META_DB)   # deliberate rerun, same run_key
    third = _run("claims_dq", "BATCH_2026_08_19", cf, meta_conn=conn, meta_db=META_DB)

    assert "::attempt-1::" in first["run_id"]
    assert "::attempt-2::" in second["run_id"]
    assert "::attempt-3::" in third["run_id"]

    # A different run_key starts its own attempt count from 1.
    other_key = _run("claims_dq", "BATCH_2026_08_20", cf, meta_conn=conn, meta_db=META_DB)
    assert "::attempt-1::" in other_key["run_id"]


def test_run_rule_group_run_id_omits_project_prefix_when_project_name_unset():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=1, project_name=None)

    summary = _run("claims_dq", "BATCH_2026_08_19", cf, meta_conn=conn, meta_db=META_DB)
    assert summary["run_id"].startswith("claims_dq::BATCH_2026_08_19::attempt-1::")


def test_rerun_always_re_executes_already_succeeded_rules():
    """
    run_rule_group() no longer skips a rule just because it already has a
    SUCCESS attempt on file for this run_key -- every rule always
    re-executes, every call (see runner.py's module docstring: this is
    what lets executor.py::_write_exceptions() reconcile
    etl_is_curr_ind against each attempt's TRUE current violation set,
    instead of an already-succeeded rule being silently frozen in place
    forever from its first run).
    """
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10)
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20)

    # Pre-seed gre_results as if rule 1 already had a PASS verdict in a
    # prior run (active_ind='Y' -- this attempt's write will deactivate it).
    conn.execute("""
        INSERT INTO gre_results (run_id, rule_id, rule_group, run_key, status, failed_records, active_ind)
        VALUES ('PRIOR_RUN', 1, 'claims_dq', 'B1', 'PASS', 2, 'Y')
    """)

    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "COMPLETED"
    assert summary["results"][1] == "SUCCESS"   # rule 1 re-ran, not skipped
    assert summary["results"][2] == "SUCCESS"

    # Rule 1 now has the pre-seeded row (deactivated) PLUS this attempt's new one.
    logs_r1 = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1")
    assert len(logs_r1) == 2
    assert {r["active_ind"] for r in logs_r1} == {"Y", "N"}
    logs_r2 = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 2")
    assert len(logs_r2) == 1



def test_rerun_deactivates_stale_rows_for_a_rule_no_longer_in_scope():
    """
    The gap the old per-rule-id deactivation could never close: rule_id 99
    has active gre_results/gre_rule_errors/gre_exceptions rows for
    (rule_group='claims_dq', run_key='B1') from some earlier attempt, but
    rule_id 99 is NOT part of THIS attempt's active rule set at all (it's
    simply not inserted into gre_rules below -- same observable effect as
    act_ind=0, removed from the group, or narrowed out by rule_variant).

    The old _deactivate_prior_results()/_deactivate_prior_errors()
    functions only ever ran right before a rule wrote ITS OWN new row, so
    a rule_id that never executes again in a later attempt kept its stale
    active_ind='Y'/etl_is_curr_ind='Y' rows forever -- exactly the
    "anything for same run_key should be set to N" complaint. The new
    runner.py::_deactivate_all_active_for_run() call, made once up front
    for the whole (rule_group, run_key) before any rule in the attempt
    executes, closes this: rule_id 99's rows get deactivated even though
    rule_id 99 never runs in this attempt.
    """
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10)

    # Pre-seed stale active rows for rule_id 99 -- a rule that is NOT
    # inserted into gre_rules for this attempt, so it can never re-execute
    # and re-deactivate its own prior rows the old way.
    conn.execute("""
        INSERT INTO gre_results (run_id, rule_id, rule_group, run_key, status, failed_records, active_ind)
        VALUES ('PRIOR_RUN', 99, 'claims_dq', 'B1', 'FAIL', 3, 'Y')
    """)
    conn.execute("""
        INSERT INTO gre_rule_errors (run_id, rule_id, rule_group, run_key, error_type, error_message, active_ind)
        VALUES ('PRIOR_RUN', 99, 'claims_dq', 'B1', 'RUNTIME', 'boom', 'Y')
    """)
    conn.execute("""
        INSERT INTO gre_exceptions (run_id, run_key, rule_id, rule_group, src_key_value, etl_is_curr_ind)
        VALUES ('PRIOR_RUN', 'B1', 99, 'claims_dq', 'REC1', 'Y')
    """)

    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "COMPLETED"
    assert summary["results"][1] == "SUCCESS"

    stale_results = execute_query(conn, "SELECT active_ind FROM gre_results WHERE rule_id = 99")
    assert len(stale_results) == 1
    assert stale_results[0]["active_ind"] == "N"

    stale_errors = execute_query(conn, "SELECT active_ind FROM gre_rule_errors WHERE rule_id = 99")
    assert len(stale_errors) == 1
    assert stale_errors[0]["active_ind"] == "N"

    stale_exceptions = execute_query(conn, "SELECT etl_is_curr_ind FROM gre_exceptions WHERE rule_id = 99")
    assert len(stale_exceptions) == 1
    assert stale_exceptions[0]["etl_is_curr_ind"] == "N"



def test_run_rule_group_text_params_reach_execute_rule_without_scoping_total():
    """
    End-to-end proof that text_params is plumbed all the way from
    run_rule_group() down to execute_rule(): a rule_syntax token that
    ISN'T a real column (RUNTYPE) substitutes correctly via text_params,
    while the total-record count stays scoped by run_params (batch_id)
    alone -- see tests/test_rules_engine_executor.py's twin unit tests
    for the underlying mechanics this exercises through the full
    orchestration path instead of calling execute_rule() directly.
    """
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(
        conn, 1,
        "SELECT claim_id FROM claims WHERE denial_reason IS NULL "
        "AND batch_id = '{batch_id}' AND '{RUNTYPE}' = 'MNT'",
        seq_no=10,
    )

    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB,
                    text_params={"RUNTYPE": "MNT"})

    assert summary["status"] == "COMPLETED"
    assert summary["results"][1] == "SUCCESS"

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1")
    assert len(results) == 1
    assert "'MNT' = 'MNT'" in results[0]["executed_sql"]


def test_sequential_halt_group_stops_before_next_rule():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _BROKEN_SQL, seq_no=10, sequencing_mode="sequential", on_failure="halt_group")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20, sequencing_mode="sequential", on_failure="halt_group")

    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "HALTED"
    assert summary["results"][1] == "ERROR"
    assert 2 not in summary["results"]          # rule 2 was never started

    logs_r2 = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 2")
    assert len(logs_r2) == 0


def test_sequential_skip_and_continue_runs_next_rule_after_error():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _BROKEN_SQL, seq_no=10, sequencing_mode="sequential", on_failure="skip_and_continue")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20, sequencing_mode="sequential", on_failure="skip_and_continue")

    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "COMPLETED"
    assert summary["results"][1] == "ERROR"
    assert summary["results"][2] == "SUCCESS"   # rule 2 still ran

    errors = execute_query(conn, "SELECT * FROM gre_rule_errors WHERE rule_id = 1")
    assert len(errors) == 1


def test_independent_mode_runs_every_rule_regardless_of_earlier_errors():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _BROKEN_SQL, seq_no=10, sequencing_mode="independent")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20, sequencing_mode="independent")

    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "COMPLETED"
    assert summary["results"][1] == "ERROR"
    assert summary["results"][2] == "SUCCESS"


def test_no_rules_returns_no_rules_status():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    summary = _run("empty_group", "B1", cf, meta_conn=conn, meta_db=META_DB)
    assert summary["status"] == "NO_RULES"


def test_shared_total_cache_avoids_redundant_count_queries_across_rules(monkeypatch):
    # Two rules in the same group, same database_name/src_tbl_nm, same
    # run_params -- _compute_total() auto-builds the identical
    # database.table + WHERE batch_id = '{run_key}' query for both, so within one
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

    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "COMPLETED"
    assert summary["results"][1] == "SUCCESS"
    assert summary["results"][2] == "SUCCESS"
    # _run_source_query is only used by _compute_total() in this flow (the
    # rule_syntax/failed-count path uses execute_query/_count_failed directly)
    # -- one call total proves the second rule's total was served from cache.
    assert calls["n"] == 1

    # Both rules should still report the correct total, proving the cached
    # value is being reused correctly, not just skipped.
    results = execute_query(conn, "SELECT rule_id, total_records FROM gre_results WHERE run_key = 'B1'")
    assert {r["rule_id"]: r["total_records"] for r in results} == {1: 4, 2: 4}


# ── rule_variant (additional level on top of rule_group/table) ──────────

def test_rule_variant_none_requested_runs_every_variant():
    """
    rule_variant NOT passed means "don't filter on rule_variant at all" --
    every active rule in the group runs regardless of its own
    rule_variant, universal (NULL) or not. This is a deliberate behavior
    change from an earlier version of load_rules() that treated "not
    passed" as "only the universal (NULL) rules" -- that stricter default
    silently dropped every variant-tagged rule for a caller who never
    intended to use variant scoping at all, producing a confusing
    NO_RULES/0-rules-loaded result for a rule_group that plainly had
    active rows. See rules_engine/rules.py::load_rules()'s docstring.
    """
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, rule_variant=None)      # universal
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20, rule_variant="2026")    # variant-specific

    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "COMPLETED"
    assert summary["total_rules"] == 2
    assert set(summary["results"].keys()) == {1, 2}   # both run -- no variant filter applied at all


def test_rule_variant_requested_runs_universal_plus_matching_variant():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, rule_variant=None)       # universal
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20, rule_variant="2026")     # matches
    _insert_rule(conn, 3, _MISSING_REASON_SQL, seq_no=30, rule_variant="2025")     # does NOT match

    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB, rule_variant="2026")

    assert summary["status"] == "COMPLETED"
    assert summary["total_rules"] == 2
    assert set(summary["results"].keys()) == {1, 2}

    audit = execute_query(conn, "SELECT rule_variant FROM gre_rule_audit WHERE run_id = ?", [summary["run_id"]])
    assert audit[0]["rule_variant"] == "2026"


# ── run_params threading (v2 scoping) ────────────────────────────────────

def test_run_params_extra_key_is_available_to_every_rule_syntax():
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

    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB,
                    run_params={"run_type": "MONTHLY"})

    assert summary["status"] == "COMPLETED"
    assert summary["results"][1] == "SUCCESS"


def test_run_params_batch_id_key_is_an_ordinary_key_not_reserved():
    # run_key is now fully decoupled from run_params: passing a
    # run_params["batch_id"] that differs from run_key doesn't collide with
    # anything, because run_key is never auto-merged into run_params (see
    # rules_engine/runner.py::run_rule_group()'s docstring). Tracking
    # (gre_exceptions/gre_results) is keyed by run_key ("TRACKING_KEY"
    # here); the business filter used by _MISSING_REASON_SQL's {batch_id}
    # token is driven independently by run_params["batch_id"] ("B1").
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10)

    summary = run_rule_group("claims_dq", "TRACKING_KEY", cf, meta_conn=conn, meta_db=META_DB,
                              run_params={"batch_id": "B1"})

    assert summary["status"] == "COMPLETED"
    # Findings are recorded under run_key, NOT the business batch_id value.
    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'TRACKING_KEY'")
    assert len(exceptions) == 2
    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'TRACKING_KEY'")
    assert len(results) == 1 and results[0]["status"] in ("PASS", "FAIL", "WARN")


# ── Parallel execution (opt-in via GRE_MAX_PARALLEL_RULES) ──────────────
# All sql_dialect values below are still "teradata" -- _insert_rule's
# default -- so _CountingFakeConnectionFactory.calls["teradata"] counts
# every new_connection() call made for the SOURCE side across a run; the
# meta side is counted separately under get_meta_connection_name()'s value
# ("teradata", rules_engine.config's default -- these tests never override
# GRE_META_CONNECTION).
from rules_engine import config as gre_config  # noqa: E402  (grouped with this section deliberately)

_META_NAME = gre_config.get_meta_connection_name()


def test_parallel_disabled_by_default_uses_sequential_path(monkeypatch):
    # No GRE_MAX_PARALLEL_RULES set -- default is 1, so even 3 independent
    # rules must take the existing single-threaded loop untouched. Proven
    # here by new_connection() never being called at all: the sequential
    # path only ever uses cf.get(), never cf.new_connection().
    conn = _conn()
    cf = _CountingFakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, sequencing_mode="independent")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20, sequencing_mode="independent")
    _insert_rule(conn, 3, _MISSING_REASON_SQL, seq_no=30, sequencing_mode="independent")

    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "COMPLETED"
    assert summary["succeeded"] == 3
    assert cf.calls == {}   # new_connection() never touched -- confirms sequential path


def test_parallel_enabled_runs_every_independent_rule(monkeypatch):
    monkeypatch.setenv("GRE_MAX_PARALLEL_RULES", "4")
    conn = _conn()
    cf = _CountingFakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, sequencing_mode="independent")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20, sequencing_mode="independent")
    _insert_rule(conn, 3, _BROKEN_SQL, seq_no=30, sequencing_mode="independent")

    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "COMPLETED"
    assert summary["results"] == {1: "SUCCESS", 2: "SUCCESS", 3: "ERROR"}
    assert summary["succeeded"] == 2
    assert summary["errored"] == 1
    # Findings actually landed -- the parallel path commits through real
    # (pooled) connections, not a no-op.
    exceptions = execute_query(conn, "SELECT rule_id FROM gre_exceptions WHERE run_key = 'B1'")
    assert {r["rule_id"] for r in exceptions} == {1, 2}
    # The pool did open independent connections this time (both source and
    # meta side), proving the parallel path was actually taken.
    assert cf.calls.get("teradata", 0) > 0
    assert cf.calls.get(_META_NAME, 0) > 0


def test_parallel_never_applies_to_sequential_mode_even_when_enabled(monkeypatch):
    # Same halt_group scenario as test_sequential_halt_group_stops_before_next_rule,
    # but with parallelism turned way up -- must behave IDENTICALLY, proving
    # sequencing_mode='sequential' never takes the parallel path.
    monkeypatch.setenv("GRE_MAX_PARALLEL_RULES", "8")
    conn = _conn()
    cf = _CountingFakeConnectionFactory(conn)
    _insert_rule(conn, 1, _BROKEN_SQL, seq_no=10, sequencing_mode="sequential", on_failure="halt_group")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20, sequencing_mode="sequential", on_failure="halt_group")

    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "HALTED"
    assert summary["results"][1] == "ERROR"
    assert 2 not in summary["results"]          # rule 2 was never started
    assert cf.calls == {}                        # sequential path -- pools never built


def test_parallel_sql_dialect_unavailable_marks_rule_error_without_deadlock(monkeypatch):
    monkeypatch.setenv("GRE_MAX_PARALLEL_RULES", "4")
    conn = _conn()
    # teradata can never build even one connection -- every rule using it
    # must come back ERROR/CONNECTION_UNAVAILABLE, same as the sequential
    # path's db_conn is None branch, and the run must still complete (not hang).
    cf = _CountingFakeConnectionFactory(conn, max_builds={"teradata": 0})
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, sequencing_mode="independent")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20, sequencing_mode="independent")

    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "COMPLETED"
    assert summary["results"] == {1: "ERROR", 2: "ERROR"}
    errors = execute_query(conn, "SELECT * FROM gre_rule_errors WHERE error_type = 'CONNECTION_UNAVAILABLE'")
    assert len(errors) == 2


def test_parallel_meta_connection_unavailable_fails_every_rule_without_deadlock(monkeypatch):
    monkeypatch.setenv("GRE_MAX_PARALLEL_RULES", "4")
    conn = _conn()
    # The metadata connection itself can't be pooled -- every pending rule
    # must fail closed (via the top-level shared meta_conn, which is still
    # fine to use for THIS particular write) rather than block forever
    # waiting on an empty meta connection pool.
    cf = _CountingFakeConnectionFactory(conn, max_builds={_META_NAME: 0})
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, sequencing_mode="independent")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20, sequencing_mode="independent")

    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "COMPLETED"
    assert summary["results"] == {1: "ERROR", 2: "ERROR"}


def test_parallel_respects_per_connection_max_parallel_cap(monkeypatch):
    monkeypatch.setenv("GRE_MAX_PARALLEL_RULES", "6")
    monkeypatch.setenv("GRE_TERADATA_MAX_PARALLEL", "2")   # cap this source at 2 concurrent sessions
    conn = _conn()
    cf = _CountingFakeConnectionFactory(conn)
    for rule_id, seq_no in [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]:
        _insert_rule(conn, rule_id, _MISSING_REASON_SQL, seq_no=seq_no, sequencing_mode="independent")

    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "COMPLETED"
    assert summary["succeeded"] == 5
    # Only 2 connections were ever built for 'teradata' -- even though it
    # serves BOTH roles here (every rule's sql_dialect AND the default meta
    # connection name; see rules_engine/config.py's META_CONNECTION default), a
    # single shared pool is built per connection NAME now (runner.py::
    # _run_pending_parallel()), not once per role. Before that fix, source
    # and meta pools were built independently for the same name and this
    # assertion (==2) passed for the wrong reason -- it was really 1 (source)
    # + 1 (meta) each capped at the *default* limit of 1, since the env var
    # here used to be misspelled and never actually applied. Now it's one
    # pool of size min(GRE_TERADATA_MAX_PARALLEL, GRE_MAX_PARALLEL_RULES) =
    # min(2, 6) = 2, proving GRE_<NAME>_MAX_PARALLEL bounds the real number
    # of concurrent sessions against one connection, not per-role double
    # counting.
    assert cf.calls["teradata"] == 2


def test_parallel_closes_pooled_connections_after_the_run(monkeypatch):
    monkeypatch.setenv("GRE_MAX_PARALLEL_RULES", "4")
    conn = _conn()
    cf = _CountingFakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, sequencing_mode="independent")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20, sequencing_mode="independent")

    # Wrap new_connection() to track every cursor it hands out, so we can
    # assert they were all closed once the run finishes.
    issued = []
    original_new_connection = cf.new_connection

    def tracking(name):
        adapter = original_new_connection(name)
        if adapter is not None:
            issued.append(adapter)
        return adapter

    cf.new_connection = tracking

    _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert len(issued) > 0
    for cursor in issued:
        # A closed DuckDB cursor raises on further use -- this is the
        # simplest reliable "was it actually closed" check available.
        with pytest.raises(Exception):
            cursor.execute("SELECT 1")


def test_parallel_shared_total_cache_still_avoids_redundant_count_queries(monkeypatch):
    # Same scenario as test_shared_total_cache_avoids_redundant_count_queries_across_rules,
    # run through the parallel path instead -- the shared total_cache dict
    # is passed to every concurrent execute_rule() call exactly as it is
    # sequentially (see _run_pending_parallel()'s docstring for the
    # accepted narrow-race trade-off this makes).
    monkeypatch.setenv("GRE_MAX_PARALLEL_RULES", "4")
    conn = _conn()
    cf = _CountingFakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, sequencing_mode="independent")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20, sequencing_mode="independent")
    conn.execute("UPDATE gre_rules SET threshold_pct = 0")

    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "COMPLETED"
    results = execute_query(conn, "SELECT rule_id, total_records FROM gre_results WHERE run_key = 'B1'")
    # Regardless of any redundant-query race, both rules must still report
    # the correct total -- that's the actual correctness guarantee; the
    # redundant-query count itself isn't asserted since it's timing-dependent.
    assert {r["rule_id"]: r["total_records"] for r in results} == {1: 4, 2: 4}


# ── run_params/extra_filters captured on gre_rule_audit (run-level) ───────
# Separate from gre_results.executed_sql (per-rule, fully-resolved SQL) --
# this is the run-level record of what run_rule_group() was actually
# called with, so a reviewer can backtrack a run_id's inputs directly.

def test_gre_rule_audit_captures_run_params_and_extra_filters_as_json():
    import json

    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10)

    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB,
                   run_params={"year": 2026, "month": 8}, extra_filters={"run_ty": "MNT"})
    assert summary["status"] == "COMPLETED"

    audit = execute_query(conn, "SELECT * FROM gre_rule_audit WHERE run_id = ?", [summary["run_id"]])
    assert len(audit) == 1
    # _run() merges {"batch_id": run_key} into run_params (see _run()'s docstring) --
    # confirm the merged dict, not just the caller-supplied keys, was captured.
    captured_params = json.loads(audit[0]["run_params"])
    assert captured_params == {"batch_id": "B1", "year": 2026, "month": 8}
    assert json.loads(audit[0]["extra_filters"]) == {"run_ty": "MNT"}


def test_gre_rule_audit_run_params_and_extra_filters_null_when_not_passed():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10)

    # _run() always injects {"batch_id": run_key} (see its docstring), so
    # run_params is never actually empty here -- but extra_filters is
    # never passed at all, and must come back NULL, not "{}" or "null".
    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)
    assert summary["status"] == "COMPLETED"

    audit = execute_query(conn, "SELECT * FROM gre_rule_audit WHERE run_id = ?", [summary["run_id"]])
    assert audit[0]["extra_filters"] is None


def test_run_rule_group_invalid_extra_filters_fails_whole_run_before_any_rule():
    # extra_filters is now validated/built ONCE, up front in
    # run_rule_group() (rules_engine/runner.py), before any rule executes
    # or gre_rule_audit is even written -- instead of the old per-rule
    # validation inside execute_rule(), which would have logged the same
    # PARAM_SUBSTITUTION_ERROR once per rule in the group. An invalid key
    # (not a valid SQL identifier) must fail the WHOLE run immediately,
    # with status="INVALID_EXTRA_FILTERS" and zero gre_results/gre_rule_audit
    # rows written for any rule.
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10)
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20)

    bad_key = "run_ty = 'x'; DROP TABLE claims; --"
    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB,
                   extra_filters={bad_key: "MNT"})
    assert summary["status"] == "INVALID_EXTRA_FILTERS"
    assert summary["run_id"] is None
    assert summary["results"] == {}

    assert execute_query(conn, "SELECT * FROM gre_rule_audit") == []
    assert execute_query(conn, "SELECT * FROM gre_results") == []


def test_run_rule_group_valid_extra_filters_applies_to_every_rule_by_default():
    # A run_rule_group()-level extra_filters, with no rule embedding the
    # {extra_filters}/$extra_filters marker, still narrows every rule in
    # the group by default (the hoisted extra_filters_sql is threaded down
    # to execute_rule() for each rule) -- confirms the fast-path threading
    # through run_rule_group() -> the sequential loop -> execute_rule()
    # produces the identical end result as passing extra_filters directly
    # to execute_rule().
    # SELECT * (rather than _MISSING_REASON_SQL's narrow claim_id-only
    # projection) so run_ty is visible to the derived-table wrap's outer
    # WHERE -- see executor.py's extra_filters STEP 0b docstring for why
    # a no-marker rule now needs the filtered column in its own SELECT
    # list.
    conn = _conn()
    conn.execute("ALTER TABLE claims ADD COLUMN run_ty VARCHAR")
    conn.execute("UPDATE claims SET run_ty = 'MNT' WHERE batch_id = 'B1'")
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, "SELECT * FROM claims WHERE denial_reason IS NULL AND batch_id = '{batch_id}'",
                 seq_no=10)

    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB,
                   extra_filters={"run_ty": "MNT"})
    assert summary["status"] == "COMPLETED"
    assert summary["succeeded"] == 1

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    assert "run_ty = 'MNT'" in results[0]["executed_sql"]


# ── project_name/process_name propagation ─────────────────────────────────

def test_gre_rule_audit_carries_project_and_process_name_from_rules():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10,
                 project_name="HEALTHSPRING_UM", process_name="UNIVERSE_VALIDATION")

    summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)
    assert summary["status"] == "COMPLETED"

    audit = execute_query(conn, "SELECT * FROM gre_rule_audit WHERE run_id = ?", [summary["run_id"]])
    assert len(audit) == 1
    assert audit[0]["project_name"] == "HEALTHSPRING_UM"
    assert audit[0]["process_name"] == "UNIVERSE_VALIDATION"


def test_gre_rule_audit_warns_and_picks_lowest_seq_no_on_mixed_project_name(caplog):
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10,
                 project_name="HEALTHSPRING_UM", process_name="UNIVERSE_VALIDATION")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20,
                 project_name="OTHER_PROJECT", process_name="OTHER_PROCESS")

    with caplog.at_level("WARNING"):
        summary = _run("claims_dq", "B1", cf, meta_conn=conn, meta_db=META_DB)

    assert summary["status"] == "COMPLETED"
    audit = execute_query(conn, "SELECT * FROM gre_rule_audit WHERE run_id = ?", [summary["run_id"]])
    assert audit[0]["project_name"] == "HEALTHSPRING_UM"      # rule 1, lowest seq_no
    assert audit[0]["process_name"] == "UNIVERSE_VALIDATION"
    assert any("mixed project_name/process_name" in r.message for r in caplog.records)


# ── multi-group orchestration (project_name/process_name fan-out) ─────────

def test_discover_rule_groups_filters_by_project_and_process():
    conn = _conn()
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, rule_group="group_a",
                 project_name="PROJECT_A", process_name="PROC_A")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=10, rule_group="group_b",
                 project_name="PROJECT_B", process_name="PROC_B")

    assert discover_rule_groups(conn, META_DB) == ["group_a", "group_b"]
    assert discover_rule_groups(conn, META_DB, project_name="PROJECT_A") == ["group_a"]
    assert discover_rule_groups(conn, META_DB, process_name="PROC_B") == ["group_b"]
    assert discover_rule_groups(conn, META_DB, project_name="PROJECT_A", process_name="PROC_B") == []


def test_run_all_active_groups_runs_one_group_per_rule_group():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, rule_group="group_a",
                 project_name="PROJECT_A", process_name="PROC_A")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=10, rule_group="group_b",
                 project_name="PROJECT_B", process_name="PROC_B")

    outcome = _run_all(conn, META_DB, "B1", cf)

    assert set(outcome["rule_groups"].keys()) == {"group_a", "group_b"}
    assert outcome["rule_groups"]["group_a"]["status"] == "COMPLETED"
    assert outcome["rule_groups"]["group_b"]["status"] == "COMPLETED"
    # Each group gets its OWN gre_rule_audit row / run_id -- not a merged run.
    assert outcome["rule_groups"]["group_a"]["run_id"] != outcome["rule_groups"]["group_b"]["run_id"]


def test_run_all_active_groups_scoped_to_one_project():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, rule_group="group_a",
                 project_name="PROJECT_A", process_name="PROC_A")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=10, rule_group="group_b",
                 project_name="PROJECT_B", process_name="PROC_B")

    outcome = _run_all(conn, META_DB, "B1", cf, project_name="PROJECT_A")

    assert set(outcome["rule_groups"].keys()) == {"group_a"}


# ── run_by_process_name(): convenience wrapper over run_all_active_groups ──

def test_run_by_process_name_runs_every_group_for_that_process():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, rule_group="group_a",
                 project_name="PROJECT_A", process_name="UNIVERSE_VALIDATION")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=10, rule_group="group_b",
                 project_name="PROJECT_B", process_name="UNIVERSE_VALIDATION")
    _insert_rule(conn, 3, _MISSING_REASON_SQL, seq_no=10, rule_group="group_c",
                 project_name="PROJECT_A", process_name="OTHER_PROCESS")

    outcome = run_by_process_name("UNIVERSE_VALIDATION", "B1", cf, meta_conn=conn, meta_db=META_DB,
                                   run_params={"batch_id": "B1"})

    # group_c belongs to a different process_name -- excluded.
    assert set(outcome["rule_groups"].keys()) == {"group_a", "group_b"}
    assert outcome["rule_groups"]["group_a"]["status"] == "COMPLETED"
    assert outcome["rule_groups"]["group_b"]["status"] == "COMPLETED"


def test_run_by_process_name_scoped_to_one_project():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, rule_group="group_a",
                 project_name="PROJECT_A", process_name="UNIVERSE_VALIDATION")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=10, rule_group="group_b",
                 project_name="PROJECT_B", process_name="UNIVERSE_VALIDATION")

    outcome = run_by_process_name("UNIVERSE_VALIDATION", "B1", cf, meta_conn=conn, meta_db=META_DB,
                                   project_name="PROJECT_A", run_params={"batch_id": "B1"})

    assert set(outcome["rule_groups"].keys()) == {"group_a"}


def test_run_by_process_name_resolves_meta_conn_and_db_from_cf_when_omitted(monkeypatch):
    # meta_conn/meta_db aren't passed explicitly here -- the wrapper must
    # resolve them itself via cf.get(...)/rules_engine.config, the whole point of
    # this convenience function over calling run_all_active_groups() directly.
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, rule_group="group_a",
                 project_name="PROJECT_A", process_name="UNIVERSE_VALIDATION")

    import rules_engine.config as shared_config
    monkeypatch.setattr(shared_config, "get_meta_db", lambda: META_DB)

    outcome = run_by_process_name("UNIVERSE_VALIDATION", "B1", cf, run_params={"batch_id": "B1"})

    assert set(outcome["rule_groups"].keys()) == {"group_a"}
    assert outcome["rule_groups"]["group_a"]["status"] == "COMPLETED"


def test_run_by_process_name_raises_clearly_when_no_match():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, rule_group="group_a",
                 project_name="PROJECT_A", process_name="UNIVERSE_VALIDATION")

    with pytest.raises(ValueError, match="NO_SUCH_PROCESS"):
        run_by_process_name("NO_SUCH_PROCESS", "B1", cf, meta_conn=conn, meta_db=META_DB)


# ── run_by_scope(): one entry point for every level (project / process /
# rule_group / rule_variant), driven by which inputs are actually passed ──

def test_run_by_scope_rule_group_level_runs_that_group_directly():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, rule_group="group_a",
                 project_name="PROJECT_A", process_name="PROC_A")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=10, rule_group="group_b",
                 project_name="PROJECT_A", process_name="PROC_A")

    outcome = run_by_scope(run_key="B1", cf=cf, meta_conn=conn, meta_db=META_DB,
                            rule_group="group_a", run_params={"batch_id": "B1"})

    assert set(outcome["rule_groups"].keys()) == {"group_a"}
    assert outcome["rule_groups"]["group_a"]["status"] == "COMPLETED"


def test_run_by_scope_process_level_runs_every_group_under_it():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, rule_group="group_a",
                 project_name="PROJECT_A", process_name="UNIVERSE_VALIDATION")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=10, rule_group="group_b",
                 project_name="PROJECT_B", process_name="UNIVERSE_VALIDATION")
    _insert_rule(conn, 3, _MISSING_REASON_SQL, seq_no=10, rule_group="group_c",
                 project_name="PROJECT_A", process_name="OTHER_PROCESS")

    outcome = run_by_scope(run_key="B1", cf=cf, meta_conn=conn, meta_db=META_DB,
                            process_name="UNIVERSE_VALIDATION", run_params={"batch_id": "B1"})

    assert set(outcome["rule_groups"].keys()) == {"group_a", "group_b"}


def test_run_by_scope_project_level_runs_every_process_under_it():
    """
    Project level -- only project_name given -- must run every process
    (and every rule_group under each of them), not just one process.
    """
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, rule_group="group_a",
                 project_name="PROJECT_A", process_name="PROC_1")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=10, rule_group="group_b",
                 project_name="PROJECT_A", process_name="PROC_2")
    _insert_rule(conn, 3, _MISSING_REASON_SQL, seq_no=10, rule_group="group_c",
                 project_name="PROJECT_B", process_name="PROC_1")

    outcome = run_by_scope(run_key="B1", cf=cf, meta_conn=conn, meta_db=META_DB,
                            project_name="PROJECT_A", run_params={"batch_id": "B1"})

    # Both PROC_1 and PROC_2 under PROJECT_A ran; PROJECT_B's group did not.
    assert set(outcome["rule_groups"].keys()) == {"group_a", "group_b"}


def test_run_by_scope_project_and_process_level_narrows_further():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, rule_group="group_a",
                 project_name="PROJECT_A", process_name="PROC_1")
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=10, rule_group="group_b",
                 project_name="PROJECT_A", process_name="PROC_2")

    outcome = run_by_scope(run_key="B1", cf=cf, meta_conn=conn, meta_db=META_DB,
                            project_name="PROJECT_A", process_name="PROC_1",
                            run_params={"batch_id": "B1"})

    assert set(outcome["rule_groups"].keys()) == {"group_a"}


def test_run_by_scope_rule_variant_narrows_within_whatever_level_is_run():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, rule_group="group_a",
                 project_name="PROJECT_A", process_name="PROC_A", rule_variant=None)
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20, rule_group="group_a",
                 project_name="PROJECT_A", process_name="PROC_A", rule_variant="2026")
    _insert_rule(conn, 3, _MISSING_REASON_SQL, seq_no=30, rule_group="group_a",
                 project_name="PROJECT_A", process_name="PROC_A", rule_variant="2025")

    # process level + rule_variant="2026" -> universal (1) + matching variant (2), not 2025's (3)
    outcome = run_by_scope(run_key="B1", cf=cf, meta_conn=conn, meta_db=META_DB,
                            process_name="PROC_A", rule_variant="2026",
                            run_params={"batch_id": "B1"})

    assert outcome["rule_groups"]["group_a"]["total_rules"] == 2
    assert set(outcome["rule_groups"]["group_a"]["results"].keys()) == {1, 2}


def test_run_by_scope_no_rule_variant_runs_every_variant_at_any_level():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, rule_group="group_a",
                 project_name="PROJECT_A", process_name="PROC_A", rule_variant=None)
    _insert_rule(conn, 2, _MISSING_REASON_SQL, seq_no=20, rule_group="group_a",
                 project_name="PROJECT_A", process_name="PROC_A", rule_variant="2026")

    outcome = run_by_scope(run_key="B1", cf=cf, meta_conn=conn, meta_db=META_DB,
                            project_name="PROJECT_A", run_params={"batch_id": "B1"})

    assert outcome["rule_groups"]["group_a"]["total_rules"] == 2   # both -- no variant filter at all


def test_run_by_scope_requires_at_least_one_scoping_input():
    cf = _FakeConnectionFactory(_conn())
    with pytest.raises(ValueError, match="at least one"):
        run_by_scope(run_key="B1", cf=cf)


def test_run_by_scope_raises_clearly_when_scope_matches_nothing():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    _insert_rule(conn, 1, _MISSING_REASON_SQL, seq_no=10, rule_group="group_a",
                 project_name="PROJECT_A", process_name="PROC_A")

    with pytest.raises(ValueError, match="NO_SUCH_PROCESS"):
        run_by_scope(run_key="B1", cf=cf, meta_conn=conn, meta_db=META_DB,
                     process_name="NO_SUCH_PROCESS")
