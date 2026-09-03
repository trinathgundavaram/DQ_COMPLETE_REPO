"""
rules_engine/executor.py tests: threshold evaluation, natural-key
building, and execute_rule() end-to-end, including the big-dataset path
(single-scan evaluation, memoized total counts). No live DB connection
required -- DuckDB stands in for both the source table and the gre_
metadata store (schema-qualified as "main", DuckDB's default schema, so
the f"{meta_db}.table" pattern the engine uses in production works
unchanged here).

Generic DB-helper behavior (bulk writes, {key} run_params substitution) is
covered in tests/test_rules_engine_db_ops.py instead -- this file only
exercises rule-specific behavior built on top of those primitives.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging

import duckdb
import pytest

import rules_engine.executor as rules_engine_executor
from rules_engine.executor import (
    evaluate_threshold, build_src_key, split_src_key_cols,
    execute_rule, _compute_total, _scan_violations,
    build_source_tieback_sql,
)
from rules_engine.db_ops import execute_query
from rules_engine.runner import _deactivate_all_active_for_run

META_DB = "main"


class _Adapter:
    """
    Minimal SourceAdapter shim wrapping a raw DuckDB connection: adds the
    prepare()/qualified_name() surface execute_rule()/_compute_total() call
    directly (db.connection_factory.SourceAdapter's interface). A raw
    duckdb.Connection can't be passed as db_conn anymore -- it has its own
    unrelated .prepare() (for prepared statements) and no qualified_name()
    at all.
    """
    def __init__(self, conn):
        self._conn = conn

    def cursor(self):
        return self._conn.cursor()

    def commit(self):
        self._conn.commit()

    def prepare(self, rule: dict) -> None:
        pass   # no-op, same default as SourceAdapter -- these tests use real tables

    def qualified_name(self, rule: dict) -> str:
        return f"{rule['database_name']}.{rule['src_tbl_nm']}"


class _CursorCountingWrapper(_Adapter):
    """
    Wraps a DuckDB connection and counts how many times .cursor() is
    called on it -- one call per distinct query execution issued through
    this connection -- so a test can prove a code path issues exactly the
    number of source-side queries it claims to, not more.
    """
    def __init__(self, conn):
        super().__init__(conn)
        self.cursor_calls = 0

    def cursor(self):
        self.cursor_calls += 1
        return self._conn.cursor()


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

    # record_id needs a real, stable, auto-generated value (matching
    # production's GENERATED ALWAYS AS IDENTITY) -- _write_exceptions()'s
    # reactivate/deactivate UPDATEs target rows by record_id, which is
    # NULL for every row without this (a plain BIGINT column with no
    # default silently inserts NULL, and "WHERE record_id = NULL" matches
    # nothing under normal SQL NULL semantics).
    conn.execute("CREATE SEQUENCE gre_exceptions_seq START 1")
    conn.execute("""
        CREATE TABLE gre_exceptions (
            record_id BIGINT DEFAULT nextval('gre_exceptions_seq'),
            run_id VARCHAR, rule_id INTEGER, database_name VARCHAR, src_tbl_nm VARCHAR,
            project_name VARCHAR, process_name VARCHAR, rule_group VARCHAR, rule_variant VARCHAR,
            element_name VARCHAR, source_name VARCHAR,
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
    # ATTEMPT (run_id), not per rule_id+run_key. See
    # rules_engine/schema.sql's "gre_results" section / executor.py::
    # _write_result()'s docstring for the full rationale.
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

    return conn


def _rule(**overrides):
    rule = {
        "rule_id": 1,
        "rule_nm": "Denied claim missing denial_reason",
        "database_name": "main",   # DuckDB's default schema -- see _conn()
        "src_tbl_nm": "claims",
        "sql_dialect": "teradata",   # also picks the one connection this rule runs against
        "rule_syntax": "SELECT claim_id, denial_reason FROM claims "
                    "WHERE denial_reason IS NULL AND batch_id = '{batch_id}'",
        # No scope_sql column anymore -- _compute_total() auto-builds the
        # total-record count from database_name.src_tbl_nm filtered by
        # every key in run_params (batch_id included), so passing
        # run_params={"batch_id": "B1"} alone is enough to batch-scope the
        # total the same way the old explicit scope_sql override used to.
        "project_name": "HEALTHSPRING_UM",
        "process_name": "UNIVERSE_VALIDATION",
        "rule_group": "claims_dq",
        "rule_variant": None,
        "seq_no": 10,
        "sequencing_mode": "independent",
        "on_failure": "skip_and_continue",
        "threshold_pct": None,
        "threshold_count": None,
        "threshold_operator": "OR",
        "severity": "Data Validation Error",
        "src_key_cols": "claim_id",
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


def test_no_threshold_fallback_breaches_on_any_single_failure():
    # No threshold configured means "never any tolerance was set up" --
    # treated as an effective threshold_count=0, not 100% failure required.
    # 1 of 4 failed is enough to breach.
    v = evaluate_threshold(4, 1)
    assert v["status"] == "FAIL" and v["write_result"] is True
    assert v["threshold_pct_used"] is None
    assert v["threshold_count_used"] == 0   # effective value actually applied, reported for auditability
    assert v["threshold_operator_used"] == "OR"   # the default operator, even though only count is "in play"

    # 4 of 4 failed -> still breaches (a strict superset of "any failure")
    v2 = evaluate_threshold(4, 4)
    assert v2["status"] == "FAIL" and v2["write_result"] is True

    # 0 of 4 failed -> genuinely clean, no threshold configured -> PASS,
    # and still not written (unchanged from before: only a breach writes
    # a row when no threshold was ever configured).
    v3 = evaluate_threshold(4, 0)
    assert v3["status"] == "PASS" and v3["write_result"] is False
    assert v3["threshold_pct_used"] is None and v3["threshold_count_used"] is None


def test_no_threshold_fallback_respects_soft_severity_for_warn():
    v = evaluate_threshold(4, 1, severity="WARN")
    assert v["status"] == "WARN" and v["write_result"] is True


# ── src key ────────────────────────────────────────────────────────

def test_build_src_key():
    rule = _rule(src_key_cols="claim_id, denial_reason")
    key = build_src_key(rule, {"claim_id": "C1", "denial_reason": None})
    assert key == "claim_id=C1|denial_reason=NULL"


def test_build_src_key_requires_columns():
    rule = _rule(src_key_cols="")
    with pytest.raises(ValueError):
        build_src_key(rule, {"claim_id": "C1"})


# ── execute_rule end-to-end ──────────────────────────────────────────────

def test_execute_rule_writes_exceptions_and_result():
    conn = _conn()
    rule = _rule(threshold_pct=25)  # 2/4 = 50% > 25% -> FAIL

    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(exceptions) == 2
    assert {r["src_key_value"] for r in exceptions} == {"claim_id=C1", "claim_id=C3"}
    assert all(r["project_name"] == "HEALTHSPRING_UM" and r["process_name"] == "UNIVERSE_VALIDATION"
               for r in exceptions)

    # gre_results is now the consolidated table -- one row per rule per
    # execution attempt, carrying both the PASS/FAIL/WARN verdict AND the
    # attempt-level bookkeeping that used to live in a separate gre_log
    # row (project_name/process_name/failed_records do double duty here).
    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(results) == 1
    assert results[0]["status"] == "FAIL"
    assert results[0]["total_records"] == 4
    assert results[0]["failed_records"] == 2
    assert results[0]["threshold_pct_used"] == 25
    assert results[0]["project_name"] == "HEALTHSPRING_UM"
    assert results[0]["process_name"] == "UNIVERSE_VALIDATION"


def test_execute_rule_stamps_etl_load_and_last_updt_dt():
    # etl_load_dt/etl_last_updt_dt (the legacy-vocabulary siblings of
    # load_datetime/last_updated_datetime) must actually get populated at
    # INSERT, not stay NULL forever -- see _write_exceptions()'s docstring.
    conn = _conn()
    rule = _rule(threshold_pct=25)

    execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(exceptions) == 2
    assert all(r["etl_load_dt"] is not None for r in exceptions)
    assert all(r["etl_last_updt_dt"] is not None for r in exceptions)


def test_execute_rule_updates_etl_last_updt_dt_on_reactivate_and_deactivate():
    conn = _conn()
    rule = _rule(threshold_pct=25)

    execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)

    # C1 fixed upstream -- rerun deactivates it.
    conn.execute("UPDATE claims SET denial_reason = 'Fixed' WHERE claim_id = 'C1'")
    execute_rule(rule, _Adapter(conn), conn, "RUN2", "B1", {"batch_id": "B1"}, META_DB)
    deactivated = execute_query(
        conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1' "
              "AND src_key_value = 'claim_id=C1'",
    )[0]
    assert deactivated["etl_is_curr_ind"] == "N"
    assert deactivated["etl_last_updt_dt"] is not None

    # C1 breaks again -- rerun reactivates it.
    conn.execute("UPDATE claims SET denial_reason = NULL WHERE claim_id = 'C1'")
    execute_rule(rule, _Adapter(conn), conn, "RUN3", "B1", {"batch_id": "B1"}, META_DB)
    reactivated = execute_query(
        conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1' "
              "AND src_key_value = 'claim_id=C1'",
    )[0]
    assert reactivated["etl_is_curr_ind"] == "Y"
    assert reactivated["etl_last_updt_dt"] is not None


# ── build_source_tieback_sql: generated (never executed) join SQL ────────

def test_build_source_tieback_sql_teradata_single_column_key():
    rule = _rule(rule_id=7, database_name="CMSUNIV_FILELAND_DEV_T", src_tbl_nm="claims_universe",
                src_key_cols="claim_id", sql_dialect="teradata")
    sql = build_source_tieback_sql(rule, "B1", "CMSUNIV_FILELAND_DEV_T")

    assert "FROM CMSUNIV_FILELAND_DEV_T.claims_universe s" in sql
    assert "JOIN CMSUNIV_FILELAND_DEV_T.gre_exceptions e" in sql
    assert "STRTOK(e.src_key_value, '=', 2)" in sql
    assert "s.claim_id = STRTOK(e.src_key_value, '=', 2)" in sql
    assert "e.rule_id = 7" in sql
    assert "e.run_key = 'B1'" in sql
    assert "e.etl_is_curr_ind = 'Y'" in sql
    # Never touches the DB -- this is text generation only.
    assert isinstance(sql, str)


def test_build_source_tieback_sql_teradata_composite_key_uses_pipe_split():
    rule = _rule(database_name="db", src_tbl_nm="order_lines",
                src_key_cols="order_id, line_no", sql_dialect="teradata")
    sql = build_source_tieback_sql(rule, "B1", "META_DB")

    assert "STRTOK(STRTOK(e.src_key_value, '|', 1), '=', 2)" in sql
    assert "STRTOK(STRTOK(e.src_key_value, '|', 2), '=', 2)" in sql
    assert "s.order_id = STRTOK(STRTOK(e.src_key_value, '|', 1), '=', 2)" in sql
    assert "s.line_no = STRTOK(STRTOK(e.src_key_value, '|', 2), '=', 2)" in sql


def test_build_source_tieback_sql_postgres_uses_split_part():
    rule = _rule(database_name="db", src_tbl_nm="claims", src_key_cols="claim_id", sql_dialect="postgres")
    sql = build_source_tieback_sql(rule, "B1", "META_DB")

    assert "split_part(e.src_key_value, '=', 2)" in sql
    assert "STRTOK" not in sql


def test_build_source_tieback_sql_handles_null_sentinel():
    rule = _rule(database_name="db", src_tbl_nm="claims", src_key_cols="region", sql_dialect="teradata")
    sql = build_source_tieback_sql(rule, "B1", "META_DB")

    # Not a CASE expression -- CASE WHEN...THEN s.region IS NULL ELSE...END
    # is invalid SQL (a boolean predicate can't be a THEN/ELSE value); this
    # is the OR-of-ANDs replacement that's actually valid, portable SQL.
    assert "((STRTOK(e.src_key_value, '=', 2) = 'NULL' AND s.region IS NULL) " \
           "OR (STRTOK(e.src_key_value, '=', 2) <> 'NULL' AND s.region = " \
           "STRTOK(e.src_key_value, '=', 2)))" in sql
    assert "CASE" not in sql


def test_build_source_tieback_sql_none_for_file_and_s3_dialects():
    for dialect in ("file", "s3"):
        rule = _rule(src_key_cols="claim_id", sql_dialect=dialect)
        assert build_source_tieback_sql(rule, "B1", "META_DB") is None


def test_build_source_tieback_sql_none_when_no_src_key_cols():
    rule = _rule(sql_dialect="teradata", src_key_cols="")
    assert build_source_tieback_sql(rule, "B1", "META_DB") is None


def test_build_source_tieback_sql_escapes_run_key():
    rule = _rule(src_key_cols="claim_id", sql_dialect="teradata")
    sql = build_source_tieback_sql(rule, "O'BRIEN", "META_DB")
    assert "run_key = 'O''BRIEN'" in sql   # single quote doubled, not left unescaped


def test_build_source_tieback_sql_applies_scope_params():
    # scope_params reproduces the run_params/extra_filters scope that
    # actually bounded this attempt (execute_rule()'s STEP 2 total_params)
    # -- without this, the src-key join alone could tie back to a
    # DIFFERENT row sharing the same src_key_value outside that scope
    # (e.g. the same claim_id in a different batch_id).
    rule = _rule(database_name="db", src_tbl_nm="claims", src_key_cols="claim_id", sql_dialect="teradata")
    sql = build_source_tieback_sql(rule, "B1", "META_DB",
                                    scope_params={"batch_id": "B1", "run_ty": "MNT"})
    assert "s.batch_id = 'B1'" in sql
    assert "s.run_ty = 'MNT'" in sql


def test_build_source_tieback_sql_scope_params_escaped_and_optional():
    rule = _rule(database_name="db", src_tbl_nm="claims", src_key_cols="claim_id", sql_dialect="teradata")

    sql_escaped = build_source_tieback_sql(rule, "B1", "META_DB", scope_params={"region": "O'BRIEN"})
    assert "s.region = 'O''BRIEN'" in sql_escaped

    # No scope_params (or empty) -- behavior unchanged from before this
    # parameter existed: src-key join only, no extra AND conditions.
    sql_none = build_source_tieback_sql(rule, "B1", "META_DB")
    sql_empty = build_source_tieback_sql(rule, "B1", "META_DB", scope_params={})
    assert sql_none == sql_empty
    assert "s.claim_id = STRTOK" in sql_none


def test_execute_rule_writes_source_tieback_sql_onto_gre_results():
    conn = _conn()
    rule = _rule(threshold_pct=25)   # 2/4 = 50% > 25% -> FAIL, write_result=True

    execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(results) == 1
    sql = results[0]["source_tieback_sql"]
    assert sql is not None
    assert "e.rule_id = 1" in sql
    assert "e.run_key = 'B1'" in sql
    assert "s.claim_id = STRTOK(e.src_key_value, '=', 2)" in sql
    # run_params (batch_id) is a real column and scopes this attempt's
    # total-record count -- the tie-back SQL must reproduce that same
    # scope, not just the bare src-key join, so an analyst re-running it
    # gets exactly the rows this rule actually evaluated.
    assert "s.batch_id = 'B1'" in sql


def test_execute_rule_source_tieback_sql_includes_extra_filters_scope():
    # Same scenario as test_execute_rule_extra_filters_brace_marker_narrows_results
    # (claims has no run_ty column of its own -- proves the marker-splice
    # path works), but asserting on source_tieback_sql instead of
    # executed_sql: the generated tie-back SQL must carry the SAME
    # run_ty='MNT' scope the scan itself was narrowed by, or re-running it
    # later could match a since-added row outside that scope.
    conn = _conn()
    conn.execute("ALTER TABLE claims ADD COLUMN run_ty VARCHAR")
    conn.execute("UPDATE claims SET run_ty = 'MNT' WHERE batch_id = 'B1'")

    rule = _rule(
        rule_syntax="SELECT claim_id, denial_reason FROM claims "
                    "WHERE denial_reason IS NULL AND batch_id = '{batch_id}' {extra_filters}",
        threshold_pct=25,
    )
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB,
                          extra_filters={"run_ty": "MNT"})
    assert status == "SUCCESS"

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    sql = results[0]["source_tieback_sql"]
    assert sql is not None
    assert "s.batch_id = 'B1'" in sql
    assert "s.run_ty = 'MNT'" in sql


def test_execute_rule_keeps_attempt_history_on_rerun():
    """
    gre_results now keeps one row per rule PER EXECUTION ATTEMPT (like the
    retired gre_log used to), not one upserted-in-place summary row --
    see rules_engine/executor.py::_write_result()'s docstring. gre_exceptions
    stays idempotent (no duplicate detail rows on a rerun); gre_results
    instead accumulates one row per run_id, with active_ind marking which
    attempt is current.

    Deactivating a rerun's prior rows is now an orchestration-level step
    (rules_engine/runner.py::run_rule_group() calls
    _deactivate_all_active_for_run() ONCE, up front, before any rule
    executes -- see that function's docstring) rather than something
    execute_rule()/_write_result() do themselves per rule_id. This test
    calls execute_rule() directly (bypassing run_rule_group()), so it
    reproduces that same upfront deactivation explicitly between the two
    "attempts" below, exactly as run_rule_group() would.
    """
    conn = _conn()
    rule = _rule(threshold_pct=25)

    execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    _deactivate_all_active_for_run(conn, META_DB, rule["rule_group"], "B1", "RUN2")
    execute_rule(rule, _Adapter(conn), conn, "RUN2", "B1", {"batch_id": "B1"}, META_DB)  # simulate a rerun of the same batch

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(exceptions) == 2   # not duplicated

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(results) == 2      # one row per attempt, not upserted in place
    by_run = {r["run_id"]: r for r in results}
    assert by_run["RUN1"]["active_ind"] == "N"   # superseded by the rerun
    assert by_run["RUN2"]["active_ind"] == "Y"   # the latest run_id takes precedence
    assert by_run["RUN2"]["status"] == "FAIL"    # both attempts still carry the real data verdict
    assert by_run["RUN1"]["status"] == "FAIL"    # -- never deleted, so the FAIL history stays on file too


def test_execute_rule_gre_results_failed_records_matches_active_exceptions_on_unchanged_rerun():
    """
    Regression for the old gre_log.rowcount inconsistency (now
    gre_results.failed_records, the only count that exists post-
    consolidation): it used to be reconcile["inserted"] + reconcile["reactivated"]
    (rows that CHANGED state this attempt), not the true violating-row
    count. On a rerun where nothing changed (no new violations, nothing
    fixed), that delta is 0/0 -- so the old gre_log reported rowcount=0
    for an attempt that still had genuinely-open, currently-active
    violations, silently disagreeing with a COUNT(*) against
    gre_exceptions itself. failed_records must always equal the actual
    currently-active gre_exceptions count for the same rule_id/run_id.
    """
    conn = _conn()
    rule = _rule(threshold_pct=10)   # any nonzero failure_pct breaches -> gre_results row written

    execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    execute_rule(rule, _Adapter(conn), conn, "RUN2", "B1", {"batch_id": "B1"}, META_DB)   # nothing changed

    result_run2 = execute_query(conn, "SELECT failed_records FROM gre_results WHERE rule_id = 1 AND run_id = 'RUN2'")[0]
    active_count = execute_query(
        conn, "SELECT COUNT(*) AS c FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1' AND etl_is_curr_ind = 'Y'",
    )[0]["c"]

    assert result_run2["failed_records"] == 2          # true violating count (C1, C3), not 0
    assert result_run2["failed_records"] == active_count   # must always agree with the actual active gre_exceptions count


def test_execute_rule_deactivates_prior_result_attempts_on_rerun():
    """
    active_ind reconciliation regression for gre_results (the retired
    gre_log's equivalent behavior): a rerun of the same run_key under a
    new run_id must deactivate every earlier run_id's gre_results row for
    this (rule_id, run_key) -- including an ERROR attempt, not just a
    PASS/FAIL/WARN one -- so a reader filtering active_ind='Y' always
    sees exactly the latest attempt, never a stale one left behind.

    That deactivation is now an orchestration-level step
    (run_rule_group()'s upfront _deactivate_all_active_for_run() call, not
    something execute_rule()/_write_result() do per rule_id) -- see
    test_execute_rule_keeps_attempt_history_on_rerun()'s docstring above
    for why this test reproduces it explicitly between the two attempts.
    """
    conn = _conn()
    broken_rule = _rule(rule_syntax="SELECT * FROM no_such_table WHERE batch_id = '{batch_id}'")

    # RUN1: errors out (bad SQL) -- logged as status=ERROR, active_ind='Y'.
    status1 = execute_rule(broken_rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status1 == "ERROR"

    # RUN2: same run_key, rule now fixed -- succeeds (verdict FAIL: no
    # threshold configured on the default _rule() means the no-threshold
    # fallback breaches on ANY failure, and 2/4 rows fail here -- see
    # evaluate_threshold()'s no-threshold fallback).
    _deactivate_all_active_for_run(conn, META_DB, broken_rule["rule_group"], "B1", "RUN2")
    fixed_rule = _rule()
    status2 = execute_rule(fixed_rule, _Adapter(conn), conn, "RUN2", "B1", {"batch_id": "B1"}, META_DB)
    assert status2 == "SUCCESS"

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(results) == 2   # both attempts kept for history
    by_run = {r["run_id"]: r for r in results}
    assert by_run["RUN1"]["status"] == "ERROR"
    assert by_run["RUN1"]["active_ind"] == "N"    # deactivated even though it never produced a verdict
    assert by_run["RUN2"]["status"] == "FAIL"
    assert by_run["RUN2"]["active_ind"] == "Y"

    active = execute_query(
        conn, "SELECT run_id FROM gre_results WHERE rule_id = 1 AND run_key = 'B1' AND active_ind = 'Y'",
    )
    assert {r["run_id"] for r in active} == {"RUN2"}


def test_execute_rule_deactivates_prior_errors_on_rerun():
    """
    active_ind reconciliation regression for gre_rule_errors: two consecutive
    failing reruns of the same run_key must leave only the LATEST run_id's
    error row active, with the earlier one deactivated (not deleted).

    That deactivation is now an orchestration-level step
    (run_rule_group()'s upfront _deactivate_all_active_for_run() call, not
    something log_error() does per rule_id) -- see
    test_execute_rule_keeps_attempt_history_on_rerun()'s docstring above
    for why this test reproduces it explicitly between the two attempts.
    """
    conn = _conn()
    broken_rule = _rule(rule_syntax="SELECT * FROM no_such_table WHERE batch_id = '{batch_id}'")

    execute_rule(broken_rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    _deactivate_all_active_for_run(conn, META_DB, broken_rule["rule_group"], "B1", "RUN2")
    execute_rule(broken_rule, _Adapter(conn), conn, "RUN2", "B1", {"batch_id": "B1"}, META_DB)

    errors = execute_query(conn, "SELECT * FROM gre_rule_errors WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(errors) == 2
    by_run = {r["run_id"]: r for r in errors}
    assert by_run["RUN1"]["active_ind"] == "N"
    assert by_run["RUN2"]["active_ind"] == "Y"


def test_execute_rule_deactivates_exceptions_that_no_longer_violate():
    """
    Reconciliation regression: a rerun (runner.py no longer skips
    already-succeeded rules -- see runner.py's module docstring) must
    deactivate (etl_is_curr_ind='N') an exception whose underlying record
    has since been fixed, not leave it marked current forever from the
    first attempt.
    """
    conn = _conn()
    rule = _rule(threshold_pct=25)

    execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    before = {r["src_key_value"]: r for r in
              execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1'")}
    assert set(before) == {"claim_id=C1", "claim_id=C3"}
    assert all(r["etl_is_curr_ind"] == "Y" for r in before.values())
    c1_record_id = before["claim_id=C1"]["record_id"]

    # C1 gets fixed upstream (no longer NULL) -- rerun the SAME run_key.
    conn.execute("UPDATE claims SET denial_reason = 'Fixed' WHERE claim_id = 'C1'")
    execute_rule(rule, _Adapter(conn), conn, "RUN2", "B1", {"batch_id": "B1"}, META_DB)

    after = {r["src_key_value"]: r for r in
             execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1'")}
    assert set(after) == {"claim_id=C1", "claim_id=C3"}   # soft-deactivated, not deleted
    assert after["claim_id=C1"]["etl_is_curr_ind"] == "N"
    assert after["claim_id=C1"]["record_id"] == c1_record_id   # same row, updated in place
    assert after["claim_id=C3"]["etl_is_curr_ind"] == "Y"      # still genuinely violating

    # reporting.py's etl_is_curr_ind='Y' filter now correctly excludes C1
    active = execute_query(
        conn, "SELECT src_key_value FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1' "
              "AND etl_is_curr_ind = 'Y'",
    )
    assert {r["src_key_value"] for r in active} == {"claim_id=C3"}


def test_execute_rule_reactivates_exception_that_violates_again():
    """
    The inverse of the deactivate case: a record that was fixed (soft-
    deactivated) and later breaks again must be reactivated in place
    (same record_id, etl_is_curr_ind flipped back to 'Y'), not inserted
    as a brand new row (gre_exceptions_uix would reject that anyway).
    """
    conn = _conn()
    rule = _rule(threshold_pct=25)

    execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    conn.execute("UPDATE claims SET denial_reason = 'Fixed' WHERE claim_id = 'C1'")
    execute_rule(rule, _Adapter(conn), conn, "RUN2", "B1", {"batch_id": "B1"}, META_DB)

    deactivated_id = execute_query(
        conn, "SELECT record_id FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1' "
              "AND src_key_value = 'claim_id=C1'",
    )[0]["record_id"]

    # C1 breaks again
    conn.execute("UPDATE claims SET denial_reason = NULL WHERE claim_id = 'C1'")
    execute_rule(rule, _Adapter(conn), conn, "RUN3", "B1", {"batch_id": "B1"}, META_DB)

    rows = execute_query(
        conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1' "
              "AND src_key_value = 'claim_id=C1'",
    )
    assert len(rows) == 1                              # reactivated, not duplicated
    assert rows[0]["record_id"] == deactivated_id       # same row throughout
    assert rows[0]["etl_is_curr_ind"] == "Y"
    assert rows[0]["run_id"] == "RUN3"                  # stamped with the reactivating run


def test_execute_rule_clean_rerun_deactivates_all_prior_exceptions():
    """
    A rule that used to fail and is now fully clean (zero violations) must
    still deactivate its previously-active exceptions -- this only works
    because _write_exceptions() is called unconditionally, even when
    violating_rows is empty (see execute_rule()'s STEP 3 comment); the old
    `if violating_rows:` guard would have skipped this entirely.
    """
    conn = _conn()
    rule = _rule(threshold_pct=25)

    execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    conn.execute("UPDATE claims SET denial_reason = 'Fixed' WHERE claim_id IN ('C1', 'C3')")
    status = execute_rule(rule, _Adapter(conn), conn, "RUN2", "B1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"

    active = execute_query(
        conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1' AND etl_is_curr_ind = 'Y'",
    )
    assert active == []
    all_rows = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(all_rows) == 2   # both rows kept, both deactivated
    assert all(r["etl_is_curr_ind"] == "N" for r in all_rows)


def test_write_exceptions_deactivation_always_runs_no_cap_param():
    """
    Regression for the MAX_EXCEPTIONS removal: _write_exceptions() no
    longer takes a `capped` parameter at all, and deactivation always
    runs unconditionally -- `rows` is now always this attempt's COMPLETE
    violation set (see _scan_violations()'s docstring), so there is no
    longer a "partial view, might incorrectly deactivate a still-open
    exception" case to guard against. A src_key_value with an existing
    active row that is genuinely absent from this attempt's (complete)
    `rows` list is always safe to deactivate.
    """
    from rules_engine.executor import _write_exceptions

    conn = _conn()
    rule = _rule(threshold_pct=25)

    conn.execute("""
        INSERT INTO gre_exceptions (record_id, rule_id, run_key, src_key_value, etl_is_curr_ind)
        VALUES (999, 1, 'B1', 'claim_id=PRIOR', 'Y')
    """)

    summary = _write_exceptions(
        conn, META_DB, rule, "RUN1", "B1",
        rows=[{"claim_id": "C1"}],   # does NOT include claim_id=PRIOR -- it's genuinely fixed
    )
    assert summary["deactivated"] == 1

    prior = execute_query(
        conn, "SELECT etl_is_curr_ind FROM gre_exceptions WHERE record_id = 999",
    )[0]
    assert prior["etl_is_curr_ind"] == "N"


def test_execute_rule_batches_are_isolated():
    conn = _conn()
    rule = _rule(threshold_pct=25)

    execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    execute_rule(rule, _Adapter(conn), conn, "RUN1", "B2", {"batch_id": "B2"}, META_DB)

    b1 = execute_query(conn, "SELECT * FROM gre_exceptions WHERE run_key = 'B1'")
    b2 = execute_query(conn, "SELECT * FROM gre_exceptions WHERE run_key = 'B2'")
    assert len(b1) == 2
    assert len(b2) == 1   # only C5 is in B2


def test_execute_rule_no_threshold_fallback_breaches_on_partial_failure():
    # evaluate_threshold()'s no-threshold fallback now breaches on ANY
    # failure (treats threshold_count as an effective 0) -- a 2/4 partial
    # failure with no threshold configured IS a breach, not a PASS.
    conn = _conn()
    rule = _rule(threshold_pct=None, threshold_count=None)  # 2/4 fail, no threshold

    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"

    # Exceptions are still written regardless of any threshold...
    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(exceptions) == 2

    # ...and gre_results gets one attempt-history row, verdict FAIL since
    # even a single failure now breaches the no-threshold fallback.
    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(results) == 1
    assert results[0]["status"] == "FAIL"
    assert results[0]["failed_records"] == 2
    assert results[0]["threshold_count_used"] == 0
    assert results[0]["threshold_pct_used"] is None


def test_execute_rule_no_threshold_fallback_writes_pass_when_zero_failures():
    # A genuinely clean attempt (0 failures) with no threshold configured
    # is still a PASS -- gre_results still gets ONE attempt-history row
    # regardless (via _write_result_safe()'s always-write path), just with
    # status="PASS" instead of FAIL/WARN.
    conn = _conn()
    rule = _rule(
        rule_syntax="SELECT claim_id, denial_reason FROM claims "
                    "WHERE denial_reason IS NULL AND batch_id = '{batch_id}' AND 1 = 0",
        threshold_pct=None, threshold_count=None,
    )

    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(results) == 1
    assert results[0]["status"] == "PASS"
    assert results[0]["failed_records"] == 0


def test_execute_rule_sql_error_routes_to_errors_and_logs():
    conn = _conn()
    rule = _rule(rule_syntax="SELECT * FROM no_such_table WHERE batch_id = '{batch_id}'")

    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status == "ERROR"

    errors = execute_query(conn, "SELECT * FROM gre_rule_errors WHERE rule_id = 1")
    assert len(errors) == 1
    assert errors[0]["error_type"] == "SQL_RUNTIME"

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1")
    assert len(exceptions) == 0   # a crash never writes partial findings

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1")
    assert len(results) == 1 and results[0]["status"] == "ERROR"


# ── big-dataset path: dedup + uncapped detail capture ─────────────────────

def test_write_exceptions_dedupes_src_key_within_one_pull():
    conn = _conn()
    # A rule_syntax that returns the SAME claim_id twice in one pull (e.g. a
    # join fan-out) should still only write ONE gre_exceptions row per
    # src key. The true failed_records count (a COUNT(*) on rule_syntax
    # itself) legitimately counts every row rule_syntax returns, duplicates
    # included -- it's the exception-DETAIL rows that get deduplicated.
    rule = _rule(
        rule_syntax="SELECT claim_id, denial_reason FROM claims "
                 "WHERE denial_reason IS NULL AND batch_id = '{batch_id}' "
                 "UNION ALL "
                 "SELECT claim_id, denial_reason FROM claims "
                 "WHERE denial_reason IS NULL AND batch_id = '{batch_id}'",
    )
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(exceptions) == 2   # C1, C3 -- deduped, not 4, even though the pull returned each twice
    assert {r["src_key_value"] for r in exceptions} == {"claim_id=C1", "claim_id=C3"}

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(results) == 1
    assert results[0]["failed_records"] == 4   # true COUNT(*) on rule_syntax counts every returned row


# ── _write_exceptions(): structural fail-fast, per-row resilience, and
# duplicate-key visibility (missing src_key_cols column / colliding keys) ──

def test_write_exceptions_missing_key_column_fails_fast_before_any_db_call():
    """
    3a: a src_key_cols column absent from EVERY row (rule_syntax never
    SELECTs it) is a structural, query-wide misconfiguration -- it must be
    caught once, up front, before the existing-rows query even runs, not
    discovered mid-loop. Proven here by dropping gre_exceptions first: if
    _write_exceptions() ever reached its `existing = execute_query(...)`
    call, it would raise a DuckDB catalog error instead of the expected
    KeyError.
    """
    from rules_engine.executor import _write_exceptions

    conn = _conn()
    conn.execute("DROP TABLE gre_exceptions")
    rule = _rule(src_key_cols="claim_id")

    with pytest.raises(KeyError, match="claim_id"):
        _write_exceptions(
            conn, META_DB, rule, "RUN1", "B1",
            rows=[{"other_col": "x"}],   # claim_id missing from every row
        )


def test_write_exceptions_partial_key_build_failure_skips_only_bad_rows():
    """
    3b: a row missing the key column only manifests as a per-row failure
    when row[0] itself HAS the column (so 3a's fail-fast doesn't fire) but
    a later row doesn't -- a genuinely per-row anomaly, not a structural
    one. The good row must still be reconciled; exactly one aggregated
    KEY_BUILD_FAILURE row must land in gre_rule_errors, not one per bad row.
    """
    from rules_engine.executor import _write_exceptions

    conn = _conn()
    rule = _rule()

    summary = _write_exceptions(
        conn, META_DB, rule, "RUN1", "B1",
        rows=[{"claim_id": "C1"}, {"other_col": "bad"}],
    )
    assert summary["inserted"] == 1   # the good row still made it through

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1'")
    assert {e["src_key_value"] for e in exceptions} == {"claim_id=C1"}

    errors = execute_query(conn, "SELECT * FROM gre_rule_errors WHERE rule_id = 1")
    assert len(errors) == 1   # aggregated, not one row per bad row
    assert errors[0]["error_type"] == "KEY_BUILD_FAILURE"
    assert "1/2" in errors[0]["error_message"]


def test_write_exceptions_duplicate_keys_collapse_and_are_logged():
    """
    3c: rows sharing one src_key_value (e.g. src_key_cols isn't a true
    natural key, or several rows are all-NULL in every key column and
    collapse onto the literal "NULL" encoding) still merge
    first-encountered-wins via setdefault() -- unchanged behavior -- but
    the collapse must now be visible as one aggregated KEY_NOT_DISTINCT
    row instead of vanishing silently.
    """
    from rules_engine.executor import _write_exceptions

    conn = _conn()
    rule = _rule()

    summary = _write_exceptions(
        conn, META_DB, rule, "RUN1", "B1",
        rows=[{"claim_id": "C1"}] * 10,   # 10 rows, all the same key
    )
    assert summary["inserted"] == 1   # existing dedup behavior unchanged -- still just 1 row

    errors = execute_query(conn, "SELECT * FROM gre_rule_errors WHERE rule_id = 1")
    assert len(errors) == 1
    assert errors[0]["error_type"] == "KEY_NOT_DISTINCT"
    assert "9" in errors[0]["error_message"]   # 9 of the 10 collapsed into the 1 that survived


def test_write_exceptions_key_build_failures_and_duplicates_dont_double_count():
    """
    3b + 3c together: a mix of one key-build failure and one duplicate-key
    collision among the rows that DID build a key. The two counts must not
    double-count each other -- this is why 3c's condition subtracts
    key_build_errors before computing the collapsed count.
    """
    from rules_engine.executor import _write_exceptions

    conn = _conn()
    rule = _rule()

    rows = [
        {"claim_id": "C1"},
        {"claim_id": "C1"},      # duplicate of the row above
        {"claim_id": "C2"},
        {"other_col": "bad"},    # fails key-building
    ]
    summary = _write_exceptions(conn, META_DB, rule, "RUN1", "B1", rows=rows)
    assert summary["inserted"] == 2   # C1 (deduped) + C2

    errors = execute_query(conn, "SELECT * FROM gre_rule_errors WHERE rule_id = 1 ORDER BY error_type")
    assert len(errors) == 2
    by_type = {e["error_type"]: e["error_message"] for e in errors}
    assert set(by_type) == {"KEY_BUILD_FAILURE", "KEY_NOT_DISTINCT"}
    assert "1/4" in by_type["KEY_BUILD_FAILURE"]      # 1 of the 4 rows failed key-building
    assert "1 duplicate-key row" in by_type["KEY_NOT_DISTINCT"]   # only the genuine dup, not the failed row too


def test_execute_rule_structural_key_failure_still_reports_success_and_a_verdict():
    """
    Rule status is unaffected by any of the above: a structural
    src_key_cols misconfiguration still routes through execute_rule()'s
    existing STEP 3 try/except (WRITE_FAILURE, unchanged) -- the rule
    itself still reports "SUCCESS" and gre_results still gets a normal
    verdict from the STEP 1/2 scan, exactly as before this change.
    """
    conn = _conn()
    rule = _rule(threshold_pct=None, threshold_count=None, src_key_cols="nonexistent_col")

    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(results) == 1
    assert results[0]["status"] == "FAIL"          # 2/4 fail, no threshold -> breaches (unchanged verdict logic)
    assert results[0]["failed_records"] == 2

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(exceptions) == 0   # reconciliation never got a valid key to write

    errors = execute_query(conn, "SELECT * FROM gre_rule_errors WHERE rule_id = 1")
    assert len(errors) == 1
    assert errors[0]["error_type"] == "WRITE_FAILURE"   # STEP 3's existing catch-all, unchanged


def test_no_max_exceptions_cap_captures_every_violating_row():
    """
    Regression for the removal of GRE_MAX_EXCEPTIONS: gre_exceptions
    detail capture is uncapped. This seeds a source table with far more
    violating rows than the OLD default cap (10000) to prove there is no
    ceiling left anywhere in the path -- gre_exceptions and
    gre_results.failed_records must agree on the full, true count.
    """
    conn = _conn()
    n = 12000   # comfortably past the old default GRE_MAX_EXCEPTIONS=10000
    conn.execute("CREATE TABLE big_claims (claim_id VARCHAR, denial_reason VARCHAR, batch_id VARCHAR)")
    conn.executemany(
        "INSERT INTO big_claims VALUES (?, NULL, 'B1')",
        [[f"C{i}"] for i in range(n)],
    )
    rule = _rule(
        database_name="main", src_tbl_nm="big_claims",
        rule_syntax="SELECT claim_id, denial_reason FROM big_claims "
                    "WHERE denial_reason IS NULL AND batch_id = '{batch_id}'",
        threshold_pct=0,   # any failure breaches -> a gre_results row is written
    )

    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"

    exceptions = execute_query(conn, "SELECT COUNT(*) AS c FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1'")
    assert exceptions[0]["c"] == n   # every row captured -- no cap

    results = execute_query(conn, "SELECT failed_records FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    assert results[0]["failed_records"] == n   # gre_results agrees with gre_exceptions, no cap anywhere

    # confirm the cap-related knobs are actually gone, not just unused
    assert not hasattr(rules_engine_executor, "MAX_EXCEPTIONS")


# ── source prepare (STEP 0 of execute_rule) ──────────────────────────────

def test_execute_rule_prepare_failure_routes_to_errors_before_any_query():
    # A file/S3 rule whose prepare() fails (e.g. missing src_tbl_nm) must
    # fail BEFORE any query runs -- same fail-fast contract the old dialect
    # guard had, now covering source setup instead of a dialect mismatch.
    conn = _conn()

    class _FailingAdapter(_Adapter):
        def prepare(self, rule):
            raise ValueError("src_tbl_nm is empty -- cannot prepare file source.")

    rule = _rule(threshold_pct=25)
    status = execute_rule(rule, _FailingAdapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status == "ERROR"

    errors = execute_query(conn, "SELECT * FROM gre_rule_errors WHERE rule_id = 1")
    assert len(errors) == 1
    assert errors[0]["error_type"] == "SOURCE_PREPARE_ERROR"

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1")
    assert len(results) == 1 and results[0]["status"] == "ERROR"

    # Caught before ANY query ran -- nothing written to gre_exceptions, and
    # the one gre_results row above is the ERROR attempt marker, not a verdict.
    assert execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1") == []


def test_execute_rule_calls_prepare_before_scanning():
    # prepare() is a no-op for a real-database rule (see _Adapter), but
    # must still be called -- proves execute_rule() doesn't skip STEP 0
    # just because the adapter happens not to need it.
    conn = _conn()

    calls = []

    class _TrackingAdapter(_Adapter):
        def prepare(self, rule):
            calls.append(rule["rule_id"])

    rule = _rule(threshold_pct=25)
    status = execute_rule(rule, _TrackingAdapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"
    assert calls == [1]


# ── _compute_total: auto-derived from database_name.src_tbl_nm + run_params ──

def test_compute_total_auto_filters_by_every_run_params_key():
    # No scope_sql anywhere -- _compute_total() builds
    # "SELECT COUNT(*) FROM main.claims WHERE batch_id = '...'" straight
    # from database_name/src_tbl_nm + run_params, same as the old explicit
    # scope_sql override used to, with nothing hand-written.
    conn = _conn()
    rule = _rule()
    assert _compute_total(_Adapter(conn), rule, {"batch_id": "B1"}, total_cache=None) == 4
    assert _compute_total(_Adapter(conn), rule, {"batch_id": "B2"}, total_cache=None) == 1


def test_compute_total_multiple_run_params_keys_are_anded_together():
    conn = _conn()
    rule = _rule()
    # denial_reason isn't a real scoping dimension for this fixture, but
    # proves every key in run_params becomes its own AND'd equality filter,
    # not just batch_id.
    assert _compute_total(_Adapter(conn), rule, {"batch_id": "B1", "denial_reason": "X"}, total_cache=None) == 1
    assert _compute_total(_Adapter(conn), rule, {"batch_id": "B1", "claim_id": "C1"}, total_cache=None) == 1
    assert _compute_total(_Adapter(conn), rule, {"batch_id": "B1", "claim_id": "NO_SUCH_ID"}, total_cache=None) == 0


def test_compute_total_uses_database_name_and_src_tbl_nm():
    conn = _conn()
    rule = _rule(database_name="main", src_tbl_nm="claims")
    assert _compute_total(_Adapter(conn), rule, {"batch_id": "B1"}, total_cache=None) == 4

    # A wrong database_name/src_tbl_nm should surface as a real query
    # failure, not silently return something -- proves the auto-built
    # query actually uses the fields, not a hardcoded table reference.
    bad_rule = _rule(database_name="main", src_tbl_nm="no_such_table")
    with pytest.raises(Exception):
        _compute_total(_Adapter(conn), bad_rule, {"batch_id": "B1"}, total_cache=None)


def test_compute_total_is_memoized_within_a_shared_cache():
    conn = _conn()
    rule = _rule()
    cache = {}

    total1 = _compute_total(_Adapter(conn), rule, {"batch_id": "B1"}, total_cache=cache)
    assert total1 == 4
    assert len(cache) == 1

    # Mutate the underlying table -- a fresh (uncached) count would change.
    conn.execute("INSERT INTO claims VALUES ('C99', NULL, 'B1')")

    total2 = _compute_total(_Adapter(conn), rule, {"batch_id": "B1"}, total_cache=cache)
    assert total2 == 4   # served from cache, not re-queried -- proves memoization

    total3 = _compute_total(_Adapter(conn), rule, {"batch_id": "B1"}, total_cache=None)
    assert total3 == 5   # no cache passed -> fresh query, reflects the mutation


def test_compute_total_cache_is_keyed_by_sql_dialect_and_query():
    # Different run_params -> different auto-built query text -> different
    # cache key, not collapsed together.
    conn = _conn()
    cache = {}
    rule = _rule()

    assert _compute_total(_Adapter(conn), rule, {"batch_id": "B1"}, total_cache=cache) == 4
    assert _compute_total(_Adapter(conn), rule, {"batch_id": "B2"}, total_cache=cache) == 1   # only C5 is in B2
    assert len(cache) == 2   # different resolved query -> different cache key, not collapsed together


def test_compute_total_query_text_is_deterministic_regardless_of_dict_order():
    # sorted(run_params) inside _build_total_query() means the SAME
    # cache_key is produced no matter what order the caller's dict was
    # built in -- otherwise two logically-identical calls could miss the
    # cache purely because of dict insertion order.
    conn = _conn()
    rule = _rule()
    cache = {}

    _compute_total(_Adapter(conn), rule, {"batch_id": "B1", "claim_id": "C1"}, total_cache=cache)
    _compute_total(_Adapter(conn), rule, {"claim_id": "C1", "batch_id": "B1"}, total_cache=cache)
    assert len(cache) == 1   # same effective filters, same cache entry


# ── single-scan evaluation (_scan_violations) ────────────────────────────

def test_scan_violations_returns_true_count_and_all_rows_uncapped():
    conn = _conn()
    query = "SELECT claim_id, denial_reason FROM claims WHERE denial_reason IS NULL AND batch_id = 'B1'"
    failed, rows = _scan_violations(conn, query)
    assert failed == 2
    assert len(rows) == 2   # every violating row returned -- no cap of any kind
    assert {r["claim_id"] for r in rows} == {"C1", "C3"}
    assert failed == len(rows)   # always equal now -- there is no partial-capture case


def test_scan_violations_issues_exactly_one_query():
    conn = _conn()
    wrapped = _CursorCountingWrapper(conn)
    query = "SELECT claim_id, denial_reason FROM claims WHERE denial_reason IS NULL AND batch_id = 'B1'"
    failed, rows = _scan_violations(wrapped, query)
    assert failed == 2
    assert wrapped.cursor_calls == 1   # ONE execution -- not a separate COUNT query plus a fetch query


# ── src_key_cols memory projection ────────────────────────────────────────
# _write_exceptions() (the only consumer of _scan_violations()'s rows) only
# ever touches a row through build_src_key(rule, row), which only reads
# src_key_cols -- so a wide rule_syntax can retain just those columns
# instead of every SELECTed column, without changing correctness.

def test_scan_violations_projects_to_src_key_cols_when_given():
    conn = _conn()
    # denial_reason is SELECTed but NOT part of src_key_cols -- it must be
    # dropped from what's retained, proving the projection actually
    # narrows the row instead of just being accepted and ignored.
    query = "SELECT claim_id, denial_reason FROM claims WHERE denial_reason IS NULL AND batch_id = 'B1'"
    failed, rows = _scan_violations(conn, query, src_key_cols=["claim_id"])
    assert failed == 2
    assert len(rows) == 2
    for row in rows:
        assert set(row.keys()) == {"claim_id"}   # denial_reason projected away
    assert {r["claim_id"] for r in rows} == {"C1", "C3"}


def test_scan_violations_projection_result_still_builds_correct_src_key():
    # The whole point: a projected row must still produce the IDENTICAL
    # build_src_key() output a full row would -- _write_exceptions()'s
    # dedup/insert/reconcile logic depends entirely on that string.
    conn = _conn()
    query = "SELECT claim_id, denial_reason FROM claims WHERE denial_reason IS NULL AND batch_id = 'B1'"
    rule = _rule(src_key_cols="claim_id")

    _, full_rows = _scan_violations(conn, query, src_key_cols=None)
    _, projected_rows = _scan_violations(conn, query, src_key_cols=split_src_key_cols(rule))

    full_keys = sorted(build_src_key(rule, r) for r in full_rows)
    projected_keys = sorted(build_src_key(rule, r) for r in projected_rows)
    assert full_keys == projected_keys == ["claim_id=C1", "claim_id=C3"]


def test_scan_violations_no_src_key_cols_keeps_full_row_unchanged():
    # None (the default) preserves the OLD full-row behavior exactly --
    # a caller that doesn't pass src_key_cols is unaffected.
    conn = _conn()
    query = "SELECT claim_id, denial_reason FROM claims WHERE denial_reason IS NULL AND batch_id = 'B1'"
    failed, rows = _scan_violations(conn, query)
    assert failed == 2
    for row in rows:
        assert set(row.keys()) == {"claim_id", "denial_reason"}


def test_scan_violations_src_key_cols_not_in_result_falls_back_to_full_row():
    # A misconfigured src_key_cols (naming a column rule_syntax doesn't
    # SELECT at all) matches nothing -- key_idx ends up empty, so every
    # row is kept in full, same as the no-projection case. This preserves
    # _write_exceptions()'s existing "src_key_cols column not found" error
    # behavior unchanged (the column is equally absent from a full row).
    conn = _conn()
    query = "SELECT claim_id, denial_reason FROM claims WHERE denial_reason IS NULL AND batch_id = 'B1'"
    failed, rows = _scan_violations(conn, query, src_key_cols=["nonexistent_column"])
    assert failed == 2
    for row in rows:
        assert set(row.keys()) == {"claim_id", "denial_reason"}


def test_execute_rule_scans_with_src_key_cols_projection_end_to_end():
    # execute_rule() itself must actually pass src_key_cols through --
    # this is a regression guard for the STEP 1 call site, not just the
    # _scan_violations() unit above.
    conn = _conn()
    rule = _rule(
        rule_syntax="SELECT claim_id, denial_reason FROM claims "
                    "WHERE denial_reason IS NULL AND batch_id = '{batch_id}'",
        src_key_cols="claim_id",
        threshold_pct=25,
    )
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1'")
    assert {e["src_key_value"] for e in exceptions} == {"claim_id=C1", "claim_id=C3"}


def test_execute_rule_issues_two_source_queries_not_three():
    # Old design per rule: a COUNT(*)-wrapped query + a detail-row fetch
    # query (both running rule_syntax) + the total-record query = 3 source-side
    # queries. New design: one merged scan (_scan_violations) + the total
    # query = 2. (A rule_group with several rules sharing the same table
    # drops this further via total_cache -- see test_rules_engine_runner.py's
    # shared-cache test -- but for a single rule on its own, 2 is the count.)
    conn = _conn()
    wrapped_db = _CursorCountingWrapper(conn)
    rule = _rule(threshold_pct=25)

    status = execute_rule(rule, wrapped_db, conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"
    assert wrapped_db.cursor_calls == 2


# ── run_params substitution (v2 scoping) ─────────────────────────────────

def test_execute_rule_uses_extra_run_params_key_beyond_batch_id():
    conn = _conn()
    # run_type must be a REAL column: every key in run_params also becomes
    # an equality filter for the auto-generated total-record count (see
    # _build_total_query()), so an extra run_params key has to name an
    # actual column on the rule's table, not just something rule_syntax
    # happens to reference inline.
    conn.execute("ALTER TABLE claims ADD COLUMN run_type VARCHAR DEFAULT 'MONTHLY'")
    rule = _rule(
        rule_syntax="SELECT claim_id, denial_reason FROM claims "
                 "WHERE denial_reason IS NULL AND batch_id = '{batch_id}' AND '{run_type}' = 'MONTHLY'",
    )
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1", "run_type": "MONTHLY"}, META_DB)
    assert status == "SUCCESS"

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(exceptions) == 2   # C1, C3 -- the extra {run_type} token resolved and matched


def test_execute_rule_unresolved_token_fails_fast_before_any_query():
    # rule_syntax references {run_type}, but the caller's run_params doesn't
    # supply it -- must fail BEFORE the scan/count queries run, logged as
    # PARAM_SUBSTITUTION_ERROR, never as a confusing SQL syntax error from
    # the source database.
    conn = _conn()
    wrapped_db = _CursorCountingWrapper(conn)
    rule = _rule(
        rule_syntax="SELECT claim_id, denial_reason FROM claims "
                 "WHERE denial_reason IS NULL AND batch_id = '{batch_id}' AND run_type = '{run_type}'",
    )

    status = execute_rule(rule, wrapped_db, conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status == "ERROR"
    assert wrapped_db.cursor_calls == 0   # caught before any source query ran

    errors = execute_query(conn, "SELECT * FROM gre_rule_errors WHERE rule_id = 1")
    assert len(errors) == 1
    assert errors[0]["error_type"] == "PARAM_SUBSTITUTION_ERROR"
    assert "run_type" in errors[0]["error_message"]

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1")
    assert len(results) == 1 and results[0]["status"] == "ERROR"
    # Substitution itself failed -- executed_sql holds the RAW, unsubstituted
    # rule_syntax, so the unresolved {run_type} token is still visible for
    # a reviewer, instead of being empty/NULL just because nothing ran.
    assert "{run_type}" in results[0]["executed_sql"]
    assert "{batch_id}" in results[0]["executed_sql"]   # never substituted either -- raised before either token resolved

    assert execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1") == []



# ── text_params: substitution WITHOUT total-count scoping ────────────────

def test_execute_rule_run_params_key_that_is_not_a_real_column_falls_back_gracefully():
    # Reproduces the reported production scenario: a run_params key
    # (RUNTYPE) that doesn't name a real column on the table is initially
    # forced into the auto-generated total-record count's WHERE clause
    # (see _build_total_query()) -- "unresolved/unknown column" from the
    # source database. Rather than failing the whole rule over an
    # unrelated denominator-query problem, execute_rule() retries the
    # total-count query ONCE with run_params dropped entirely (extra_filters
    # only, none here), succeeds, and the rule still produces a real
    # verdict -- a caller shouldn't have to know in advance whether a
    # --param's key happens to be a real column just to avoid an ERROR.
    conn = _conn()
    rule = _rule(
        rule_syntax="SELECT claim_id, denial_reason FROM claims "
                 "WHERE denial_reason IS NULL AND batch_id = '{batch_id}' AND '{RUNTYPE}' = 'MNT'",
    )
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1",
                           {"batch_id": "B1", "RUNTYPE": "MNT"}, META_DB)
    assert status == "SUCCESS"

    assert execute_query(conn, "SELECT * FROM gre_rule_errors WHERE rule_id = 1") == []

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1")
    assert len(results) == 1
    # Fallback drops run_params (batch_id included) from the denominator
    # entirely, not just the offending key -- total reflects the WHOLE
    # table (5 rows) rather than the batch_id='B1' subset it would have
    # used had the total-count query succeeded on the first try. This is
    # the accepted trade-off of not requiring every --param to name a
    # real column: the fallback denominator is coarser, never wrong in a
    # way that hides a real breach (it only ever widens scope, never
    # narrows it), but a caller who wants a precisely-scoped denominator
    # should use text_params for a non-column value instead (see the
    # sibling test below) to avoid the fallback entirely.
    assert results[0]["total_records"] == 5


def test_execute_rule_total_count_still_errors_when_fallback_also_fails(monkeypatch):
    # If the fallback retry ALSO fails (a genuinely broken total-count
    # query, not just a run_params key that isn't a column), the rule
    # still errors -- the fallback softens "a run_params key isn't a
    # column," it doesn't mask every possible total-count failure.
    # _compute_total() is forced to always raise here to simulate that,
    # since a bare "COUNT(*) FROM <the same table the scan just
    # succeeded against>" fallback query has no realistic way to fail on
    # its own in this test's in-memory DuckDB setup.
    conn = _conn()
    rule = _rule(
        rule_syntax="SELECT claim_id, denial_reason FROM claims "
                 "WHERE denial_reason IS NULL AND batch_id = '{batch_id}' AND '{RUNTYPE}' = 'MNT'",
    )

    def _always_raise(*args, **kwargs):
        raise RuntimeError("simulated total-count failure")

    monkeypatch.setattr(rules_engine_executor, "_compute_total", _always_raise)

    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1",
                           {"batch_id": "B1", "RUNTYPE": "MNT"}, META_DB)
    assert status == "ERROR"

    errors = execute_query(conn, "SELECT * FROM gre_rule_errors WHERE rule_id = 1")
    assert len(errors) == 1
    assert errors[0]["error_type"] == "SCOPE_QUERY_FAILURE"
    assert "simulated total-count failure" in errors[0]["error_message"]


def test_compute_total_caches_failure_avoiding_redundant_source_queries():
    # Reproduces the reported production scenario: many rules in a group
    # share the identical (sql_dialect, total-count query) key, and that
    # query FAILS (e.g. a run_params key like RUNTYPE that isn't a real
    # column on the table). Before this fix, total_cache only memoized
    # SUCCESSFUL results -- a failing query was re-issued against the
    # source database, and re-failed, once per rule sharing that key, even
    # though every one of those round trips was guaranteed to fail
    # identically. total_cache now caches the failure too: the first call
    # issues one real query (and one real cursor()/network round trip);
    # every subsequent call with the identical cache key re-raises the
    # SAME cached exception immediately, with NO additional query.
    conn = _conn()
    wrapped_db = _CursorCountingWrapper(conn)
    rule = _rule(
        rule_syntax="SELECT claim_id, denial_reason FROM claims "
                 "WHERE denial_reason IS NULL AND batch_id = '{batch_id}'",
    )
    run_params = {"batch_id": "B1", "RUNTYPE": "MNT"}   # RUNTYPE: not a real column
    total_cache = {}

    with pytest.raises(Exception):
        _compute_total(wrapped_db, rule, run_params, total_cache=total_cache)
    assert wrapped_db.cursor_calls == 1   # one real round trip for the first, failing call

    # Second call, identical rule/run_params -> identical cache_key. Must
    # raise the cached exception WITHOUT issuing a second source query.
    with pytest.raises(Exception):
        _compute_total(wrapped_db, rule, run_params, total_cache=total_cache)
    assert wrapped_db.cursor_calls == 1   # unchanged -- served from the cached failure

    # A third, unrelated rule sharing the same table+run_params also hits
    # the cache, not the source.
    other_rule = _rule(rule_id=2)
    with pytest.raises(Exception):
        _compute_total(wrapped_db, other_rule, run_params, total_cache=total_cache)
    assert wrapped_db.cursor_calls == 1


def test_execute_rule_text_params_substitute_without_scoping_total_count():
    # The fix: the SAME RUNTYPE value, passed via text_params instead of
    # run_params, still substitutes into rule_syntax ("{RUNTYPE}" resolves
    # to 'MNT' below) but is NEVER folded into the total-count WHERE
    # clause -- only run_params (batch_id here) is. No "unknown column"
    # error, because the total query only ever references batch_id.
    conn = _conn()
    rule = _rule(
        rule_syntax="SELECT claim_id, denial_reason FROM claims "
                 "WHERE denial_reason IS NULL AND batch_id = '{batch_id}' AND '{RUNTYPE}' = 'MNT'",
    )
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1",
                           {"batch_id": "B1"}, META_DB,
                           text_params={"RUNTYPE": "MNT"})
    assert status == "SUCCESS"

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1")
    assert len(results) == 1
    assert "'MNT' = 'MNT'" in results[0]["executed_sql"]   # {RUNTYPE} resolved via text_params
    assert results[0]["total_records"] == 4   # same denominator as batch_id='B1' alone -- RUNTYPE never scoped it

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(exceptions) == 2   # C1, C3 -- unaffected, same as run_params-only equivalents elsewhere


def test_execute_rule_text_params_key_collision_with_run_params_favors_text_params():
    # Documented precedence: when the same key appears in both dicts,
    # text_params wins for SUBSTITUTION -- but run_params (not the merged
    # value) is still what's used for total-count scoping, since
    # batch_id must remain a real column filter regardless.
    conn = _conn()
    rule = _rule(
        rule_syntax="SELECT claim_id, denial_reason FROM claims "
                 "WHERE denial_reason IS NULL AND batch_id = '{batch_id}'",
    )
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1",
                           {"batch_id": "B1"}, META_DB,
                           text_params={"batch_id": "OVERRIDE_NOT_A_REAL_BATCH"})
    assert status == "SUCCESS"

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1")
    assert len(results) == 1
    # rule_syntax substitution used text_params' value (collision winner) --
    # no row in `claims` has this batch_id, so the scan itself finds nothing.
    assert "OVERRIDE_NOT_A_REAL_BATCH" in results[0]["executed_sql"]
    assert results[0]["total_records"] == 4   # total-count still scoped by run_params' real batch_id='B1'


# ── run_key genericity: no "batch_id" concept required ───────────────────

def test_execute_rule_writes_resolved_sql_to_executed_sql_on_success():
    # On a successful attempt (any status), gre_results.executed_sql holds
    # the FULLY RESOLVED SQL that actually ran -- run_params tokens already
    # substituted to their literal values -- not the raw, templated
    # rule_syntax off gre_rules. batch_id is referenced via BOTH styles
    # ("{batch_id}" braces AND "$batch_id" dollar) in the same rule_syntax
    # to prove both resolve identically (must ALSO be a real run_params key
    # here, since it doubles as the auto total-record-count's equality
    # filter -- see _build_total_query()'s docstring).
    conn = _conn()
    rule = _rule(
        rule_syntax="SELECT claim_id, denial_reason FROM claims "
                    "WHERE denial_reason IS NULL AND batch_id = '{batch_id}' AND batch_id = '$batch_id'",
        threshold_pct=25,
    )
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(results) == 1
    executed = results[0]["executed_sql"]
    assert "{batch_id}" not in executed and "$batch_id" not in executed   # both tokens resolved
    assert executed.count("'B1'") == 2   # each style resolved to the same literal value


# ── extra_filters -- runtime AND conditions on top of run_params ────────
# A mechanism SEPARATE from run_params substitution: a rule opts in by
# embedding the literal marker "{extra_filters}"/"$extra_filters"
# somewhere in its rule_syntax; a caller then passes extra_filters=... at
# run time. See rules_engine/db_ops.py::build_extra_filters_clause()'s
# docstring for the full design rationale.

def test_execute_rule_extra_filters_brace_marker_narrows_results():
    # Only batch_id='B1' rows are visible to the scan at all once run_ty
    # is spliced in as an extra_filters condition on a column rule_syntax
    # never mentions on its own -- claims has no run_ty column, so if the
    # marker weren't spliced correctly this would fail to bind, not just
    # return the wrong count.
    conn = _conn()
    conn.execute("ALTER TABLE claims ADD COLUMN run_ty VARCHAR")
    conn.execute("UPDATE claims SET run_ty = 'MNT' WHERE batch_id = 'B1'")
    conn.execute("UPDATE claims SET run_ty = 'ADHOC' WHERE batch_id = 'B2'")

    rule = _rule(
        rule_syntax="SELECT claim_id, denial_reason FROM claims "
                    "WHERE denial_reason IS NULL AND batch_id = '{batch_id}' {extra_filters}",
        threshold_pct=100,
    )
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB,
                          extra_filters={"run_ty": "MNT"})
    assert status == "SUCCESS"

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(results) == 1
    executed = results[0]["executed_sql"]
    assert "{extra_filters}" not in executed
    assert "run_ty = 'MNT'" in executed
    # total_records is the DENOMINATOR (every batch_id='B1' row, regardless
    # of denial_reason -- 4 of claims' 5 rows: C1-C4), narrowed by the SAME
    # extra_filters as the scan (all 4 also have run_ty='MNT' here) -- see
    # _build_total_query()'s docstring. failed_records is the violation
    # count from the actual scan: denial_reason IS NULL AND batch_id='B1'
    # (C1, C3) -- 2.
    assert results[0]["total_records"] == 4
    assert results[0]["failed_records"] == 2


def test_execute_rule_extra_filters_dollar_marker_also_works():
    conn = _conn()
    conn.execute("ALTER TABLE claims ADD COLUMN run_ty VARCHAR")
    conn.execute("UPDATE claims SET run_ty = 'MNT' WHERE batch_id = 'B1'")

    rule = _rule(
        rule_syntax="SELECT claim_id, denial_reason FROM claims "
                    "WHERE denial_reason IS NULL AND batch_id = '{batch_id}' $extra_filters",
        threshold_pct=100,
    )
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB,
                          extra_filters={"run_ty": "MNT"})
    assert status == "SUCCESS"

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    executed = results[0]["executed_sql"]
    assert "$extra_filters" not in executed
    assert "run_ty = 'MNT'" in executed


def test_execute_rule_extra_filters_multiple_filters_all_applied():
    conn = _conn()
    conn.execute("ALTER TABLE claims ADD COLUMN run_ty VARCHAR")
    conn.execute("ALTER TABLE claims ADD COLUMN region VARCHAR")
    conn.execute("UPDATE claims SET run_ty = 'MNT', region = 'EAST' WHERE batch_id = 'B1'")

    rule = _rule(
        rule_syntax="SELECT claim_id, denial_reason FROM claims "
                    "WHERE denial_reason IS NULL AND batch_id = '{batch_id}' {extra_filters}",
        threshold_pct=100,
    )
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB,
                          extra_filters={"run_ty": "MNT", "region": "EAST"})
    assert status == "SUCCESS"

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    executed = results[0]["executed_sql"]
    assert "region = 'EAST'" in executed
    assert "run_ty = 'MNT'" in executed


def test_execute_rule_extra_filters_no_marker_derived_table_wrap(caplog):
    # A rule that never embeds "{extra_filters}"/"$extra_filters" now gets
    # rule_syntax wrapped as a derived table and the filter applied on
    # the OUTER query, instead of being silently ignored -- extra_filters
    # applies to EVERY rule in a run by default now (see execute_rule()'s
    # STEP 0b). SELECT * projects every column (including run_ty), so the
    # outer WHERE can see it -- run_ty='MNT' narrows both the scan and
    # the total-count denominator, and a DEBUG line names the rule and
    # explains the wrap.
    conn = _conn()
    conn.execute("ALTER TABLE claims ADD COLUMN run_ty VARCHAR")
    conn.execute("UPDATE claims SET run_ty = 'MNT' WHERE batch_id = 'B1'")
    conn.execute("UPDATE claims SET run_ty = 'ADHOC' WHERE batch_id = 'B2'")
    rule = _rule(
        rule_syntax="SELECT * FROM claims "
                    "WHERE denial_reason IS NULL AND batch_id = '{batch_id}'",
        threshold_pct=100,
    )
    with caplog.at_level(logging.DEBUG):
        status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB,
                              extra_filters={"run_ty": "MNT"})
    assert status == "SUCCESS"

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    executed = results[0]["executed_sql"]
    assert "run_ty = 'MNT'" in executed
    assert "FROM (" in executed   # derived-table wrap, not a bare textual append
    assert results[0]["total_records"] == 4
    assert results[0]["failed_records"] == 2

    debugs = [r.message for r in caplog.records if r.levelno == logging.DEBUG]
    assert any("DERIVED-TABLE WRAP" in msg and "Rule 1" in msg for msg in debugs)


def test_execute_rule_extra_filters_no_marker_top_level_or_applies_to_every_branch():
    # THE bug this wrap fixes, reproducing a real production report: a
    # rule_syntax with a top-level OR in its WHERE clause (two independent
    # qualifying conditions) used to have extra_filters silently attach
    # to ONLY the last OR branch when naively text-appended as
    # "<rule_syntax> AND col = 'val'" -- SQL's "AND binds tighter than
    # OR" precedence means "... WHERE A OR B AND run_ty='MNT'" parses as
    # "... WHERE A OR (B AND run_ty='MNT')", leaving every row matching
    # branch A completely unfiltered by run_ty. The derived-table wrap
    # sidesteps this: the filter applies to the WHOLE result set of
    # "(A OR B)", regardless of how many top-level branches it has.
    #
    # claims: C1 (denial_reason NULL, batch_id B1), C2 ('Not medically
    # necessary', B1), C3 (NULL, B1), C4 ('X', B1), C5 (NULL, B2).
    # Branch A: denial_reason IS NULL -- matches C1, C3, C5.
    # Branch B: batch_id = 'B2' -- matches C5.
    # Union (no filter): C1, C3, C5. C5 is set to run_ty='ADHOC' below,
    # everything else 'MNT' -- a correct run_ty='MNT' filter must exclude
    # C5 via BOTH branches, leaving only C1, C3.
    conn = _conn()
    conn.execute("ALTER TABLE claims ADD COLUMN run_ty VARCHAR")
    conn.execute("UPDATE claims SET run_ty = 'MNT'")
    conn.execute("UPDATE claims SET run_ty = 'ADHOC' WHERE claim_id = 'C5'")
    rule = _rule(
        # Top-level OR: branch A (denial_reason IS NULL) OR branch B (batch_id='B2').
        rule_syntax="SELECT * FROM claims "
                    "WHERE denial_reason IS NULL OR batch_id = 'B2'",
        threshold_pct=100,
    )
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "ALL", {}, META_DB,
                          extra_filters={"run_ty": "MNT"})
    assert status == "SUCCESS"

    # C5 matches branch A (denial_reason IS NULL) but has run_ty='ADHOC' --
    # a correct filter excludes it from the violation set entirely. The
    # old naive-append bug would have left C5 in (branch A was never
    # filtered by run_ty at all), reporting a false violation.
    exceptions = execute_query(
        conn, "SELECT src_key_value FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'ALL'"
    )
    keys = {row["src_key_value"] for row in exceptions}
    assert keys == {"claim_id=C1", "claim_id=C3"}


def test_execute_rule_extra_filters_no_marker_missing_column_fails_loudly():
    # The one real limitation of the derived-table wrap: if rule_syntax's
    # own SELECT list doesn't project the filtered column at all, the
    # outer query can't see it -- an ordinary "column not found" error,
    # caught and logged the same as any other broken rule_syntax. Per the
    # user's explicit instruction ("Donot worry about rule breaking. if
    # it breaks its expected to log that error"), this is expected: add
    # the {extra_filters}/$extra_filters marker (or widen the SELECT
    # list) to fix a rule that hits this.
    conn = _conn()
    rule = _rule(
        rule_syntax="SELECT claim_id, denial_reason FROM claims "
                    "WHERE denial_reason IS NULL",
        threshold_pct=100,
    )
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {}, META_DB,
                          extra_filters={"run_ty": "MNT"})
    assert status == "ERROR"

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(results) == 1
    assert results[0]["status"] == "ERROR"
    assert "run_ty" in results[0]["executed_sql"]   # the (invalid) wrapped SQL, for diagnosis


def test_execute_rule_extra_filters_no_marker_group_by_still_works_when_column_projected():
    # A rule_syntax with GROUP BY (content after its WHERE clause) is no
    # longer inherently incompatible with a no-marker extra_filters --
    # the derived-table wrap doesn't care what's between WHERE and the
    # end of rule_syntax, only whether the filtered column ends up in the
    # projected result. Here run_ty is part of the GROUP BY/SELECT list,
    # so the wrap succeeds cleanly (this used to be an unconditional
    # SQL-syntax failure under the old textual-append design).
    conn = _conn()
    conn.execute("ALTER TABLE claims ADD COLUMN run_ty VARCHAR")
    conn.execute("UPDATE claims SET run_ty = 'MNT'")
    rule = _rule(
        rule_syntax="SELECT run_ty, COUNT(*) AS cnt FROM claims "
                    "WHERE denial_reason IS NULL GROUP BY run_ty",
        threshold_pct=100,
    )
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {}, META_DB,
                          extra_filters={"run_ty": "MNT"})
    assert status == "SUCCESS"


def test_execute_rule_extra_filters_with_marker_still_spliced_precisely():
    # A rule that DOES embed the marker still gets the precise,
    # author-controlled splice at that exact position -- unaffected by the
    # default-append fallback used only when the marker is absent.
    conn = _conn()
    conn.execute("ALTER TABLE claims ADD COLUMN run_ty VARCHAR")
    conn.execute("UPDATE claims SET run_ty = 'MNT'")
    rule = _rule(
        rule_syntax="SELECT claim_id, denial_reason FROM claims "
                    "WHERE denial_reason IS NULL AND batch_id = '{batch_id}' {extra_filters}",
    )
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB,
                          extra_filters={"run_ty": "MNT"})
    assert status == "SUCCESS"

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    executed = results[0]["executed_sql"]
    assert "{extra_filters}" not in executed
    assert "run_ty = 'MNT'" in executed


def test_execute_rule_extra_filters_invalid_identifier_key_writes_error():
    # build_extra_filters_clause() rejects a non-identifier key before any
    # query runs -- executor.py must catch that ValueError, log it, and
    # write an ERROR row (executed_sql falling back to the raw rule_syntax,
    # since substitution never even started), not let it propagate and
    # crash the whole run.
    conn = _conn()
    rule = _rule(
        rule_syntax="SELECT claim_id, denial_reason FROM claims "
                    "WHERE denial_reason IS NULL AND batch_id = '{batch_id}' {extra_filters}",
    )
    bad_key = "run_ty = 'x'; DROP TABLE claims; --"
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB,
                          extra_filters={bad_key: "MNT"})
    assert status == "ERROR"

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(results) == 1
    assert results[0]["status"] == "ERROR"
    assert "not valid SQL identifiers" in results[0]["error_message"]
    assert "{extra_filters}" in results[0]["executed_sql"]   # raw rule_syntax, splice never happened


def test_execute_rule_with_year_month_run_key_not_batch_id():
    # run_key doesn't have to be a "batch" at all -- a year+month composite
    # (built via rules_engine/db_ops.py::build_run_key()) works identically, and
    # run_params contains NO "batch_id" key anywhere -- rule_syntax here
    # doesn't reference {batch_id}, proving run_key is fully decoupled
    # from run_params.
    from rules_engine.db_ops import build_run_key
    conn = _conn()
    run_key = build_run_key(2026, 8)
    assert run_key == "2026_8"

    rule = _rule(
        rule_syntax="SELECT claim_id, denial_reason FROM claims WHERE denial_reason IS NULL",
        threshold_pct=25,
    )
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", run_key, {}, META_DB)
    assert status == "SUCCESS"

    exceptions = execute_query(conn, f"SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = '{run_key}'")
    assert len(exceptions) == 3   # C1, C3, C5 -- no batch_id filter in rule_syntax this time

    results = execute_query(conn, f"SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = '{run_key}'")
    assert len(results) == 1 and results[0]["status"] in ("PASS", "FAIL", "WARN")


# ── descriptive/reporting columns: rule_nm/dgr_nbr/universe_version, ───
# ── run_type/batch_schedule ───────────────────────────────────────────────

def test_execute_rule_copies_rule_name_dgr_nbr_universe_version_onto_exceptions():
    # rule_nm/dgr_nbr/universe_version are copied straight from the rule
    # row onto every gre_exceptions row it writes -- purely descriptive,
    # never read by engine logic (execute_rule() doesn't branch on them).
    conn = _conn()
    rule = _rule(dgr_nbr="CDAG1V22R4", universe_version="V22")
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1")
    assert len(exceptions) == 2
    for exc in exceptions:
        assert exc["rule_nm"] == rule["rule_nm"]
        assert exc["dgr_nbr"] == "CDAG1V22R4"
        assert exc["universe_version"] == "V22"


def test_execute_rule_copies_rule_group_and_rule_variant_onto_exceptions_results_and_errors():
    """
    rule_group/rule_variant are copied straight from the rule row onto
    gre_exceptions and gre_results (rule_variant only -- gre_results
    already had rule_group), and onto gre_rule_errors on an error path --
    purely descriptive, so any of these three tables can be
    filtered/reported on by rule_group or rule_variant without a join
    back to gre_rules (which may have since changed). This is the RULE's
    OWN rule_variant value, not the run's requested rule_variant filter
    (see rules_engine/runner.py::run_by_scope()'s docstring for that
    distinction).
    """
    conn = _conn()
    rule = _rule(rule_group="claims_dq", rule_variant="2026")
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1")
    assert len(exceptions) == 2
    for exc in exceptions:
        assert exc["rule_group"] == "claims_dq"
        assert exc["rule_variant"] == "2026"

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1")
    assert len(results) == 1
    assert results[0]["rule_variant"] == "2026"

    # Error path: gre_rule_errors also carries this rule's own rule_variant.
    broken_rule = _rule(rule_group="claims_dq", rule_variant="2026", rule_id=2,
                         rule_syntax="SELECT * FROM no_such_table WHERE batch_id = '{batch_id}'")
    status2 = execute_rule(broken_rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status2 == "ERROR"

    errors = execute_query(conn, "SELECT * FROM gre_rule_errors WHERE rule_id = 2")
    assert len(errors) == 1
    assert errors[0]["rule_variant"] == "2026"


def test_execute_rule_dgr_nbr_and_universe_version_null_when_rule_doesnt_set_them():
    # A rule that never sets dgr_nbr/universe_version (most rules, and
    # every rule written before this feature existed) gets NULL there --
    # this is purely additive, not a new requirement on every rule.
    conn = _conn()
    rule = _rule()
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1")
    assert len(exceptions) == 2
    for exc in exceptions:
        assert exc["dgr_nbr"] is None
        assert exc["universe_version"] is None


def test_execute_rule_copies_run_type_and_batch_schedule_from_run_params_when_present():
    # run_type/batch_schedule are copied from run_params onto
    # gre_exceptions IF the caller happens to supply those exact keys --
    # they are NOT reserved/required (run_params still has no reserved
    # key), just a courtesy landing spot for these two particular values.
    # (Every run_params key doubles as an equality filter for the
    # auto-generated total-record count -- see _compute_total() -- so the
    # source table needs matching columns here, same as any other
    # run_params key.)
    conn = _conn()
    conn.execute("ALTER TABLE claims ADD COLUMN run_type VARCHAR DEFAULT 'MONTHLY'")
    conn.execute("ALTER TABLE claims ADD COLUMN batch_schedule VARCHAR DEFAULT 'WEEKDAYS_0600'")
    rule = _rule()
    status = execute_rule(
        rule, _Adapter(conn), conn, "RUN1", "B1",
        {"batch_id": "B1", "run_type": "MONTHLY", "batch_schedule": "WEEKDAYS_0600"},
        META_DB,
    )
    assert status == "SUCCESS"

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1")
    assert len(exceptions) == 2
    for exc in exceptions:
        assert exc["run_type"] == "MONTHLY"
        assert exc["batch_schedule"] == "WEEKDAYS_0600"


def test_execute_rule_run_type_and_batch_schedule_null_when_run_params_omits_them():
    conn = _conn()
    rule = _rule()
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1")
    assert len(exceptions) == 2
    for exc in exceptions:
        assert exc["run_type"] is None
        assert exc["batch_schedule"] is None
