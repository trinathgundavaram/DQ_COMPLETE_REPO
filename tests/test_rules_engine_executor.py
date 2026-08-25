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

import duckdb
import pytest

import rules_engine.executor as rules_engine_executor
from rules_engine.executor import (
    evaluate_threshold, build_src_key,
    execute_rule, _compute_total, _scan_violations,
    build_source_tieback_sql,
)
from rules_engine.db_ops import execute_query

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
            project_name VARCHAR, process_name VARCHAR,
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
        CREATE TABLE gre_log (
            log_id BIGINT, run_id VARCHAR, rule_id INTEGER, rule_group VARCHAR,
            project_name VARCHAR, process_name VARCHAR,
            run_key VARCHAR, seq_no INTEGER, start_time TIMESTAMP, end_time TIMESTAMP,
            status VARCHAR, rowcount BIGINT, error_message VARCHAR,
            active_ind VARCHAR DEFAULT 'Y',
            created_at TIMESTAMP DEFAULT current_timestamp,
            last_updated_datetime TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE gre_rule_errors (
            error_id BIGINT, run_id VARCHAR, rule_id INTEGER, rule_group VARCHAR,
            run_key VARCHAR, error_type VARCHAR, error_message VARCHAR,
            error_detail VARCHAR, active_ind VARCHAR DEFAULT 'Y',
            occurred_at TIMESTAMP DEFAULT current_timestamp,
            last_updated_datetime TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE gre_results (
            result_id BIGINT, rule_id INTEGER, run_key VARCHAR, run_id VARCHAR,
            project_name VARCHAR, process_name VARCHAR,
            total_records BIGINT, failed_records BIGINT, failure_pct DOUBLE,
            threshold_pct_used DOUBLE, threshold_count_used INTEGER,
            threshold_operator_used VARCHAR, severity VARCHAR, status VARCHAR,
            source_tieback_sql VARCHAR, active_ind VARCHAR DEFAULT 'Y',
            evaluated_at TIMESTAMP DEFAULT current_timestamp
        )
    """)
    conn.execute("CREATE UNIQUE INDEX gre_results_uix ON gre_results(rule_id, run_key)")

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


def test_no_threshold_fallback_requires_every_record_to_fail():
    # 3 of 4 failed, no threshold configured -> PASS, and NOT written
    v = evaluate_threshold(4, 3)
    assert v["status"] == "PASS" and v["write_result"] is False

    # 4 of 4 failed -> breach, IS written
    v2 = evaluate_threshold(4, 4)
    assert v2["status"] == "FAIL" and v2["write_result"] is True
    assert v2["threshold_pct_used"] is None and v2["threshold_count_used"] is None


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

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(results) == 1
    assert results[0]["status"] == "FAIL"
    assert results[0]["total_records"] == 4
    assert results[0]["failed_records"] == 2
    assert results[0]["threshold_pct_used"] == 25
    assert results[0]["project_name"] == "HEALTHSPRING_UM"
    assert results[0]["process_name"] == "UNIVERSE_VALIDATION"

    logs = execute_query(conn, "SELECT * FROM gre_log WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(logs) == 1
    assert logs[0]["status"] == "SUCCESS"
    assert logs[0]["rowcount"] == 2
    assert logs[0]["project_name"] == "HEALTHSPRING_UM"
    assert logs[0]["process_name"] == "UNIVERSE_VALIDATION"


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


def test_execute_rule_is_idempotent_on_rerun():
    conn = _conn()
    rule = _rule(threshold_pct=25)

    execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    execute_rule(rule, _Adapter(conn), conn, "RUN2", "B1", {"batch_id": "B1"}, META_DB)  # simulate a rerun of the same batch

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(exceptions) == 2   # not duplicated

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(results) == 1      # upserted in place, not a second row
    assert results[0]["run_id"] == "RUN2"   # reflects the latest run

    logs = execute_query(conn, "SELECT * FROM gre_log WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(logs) == 2         # both attempts are logged (attempt history, unlike results)
    by_run = {r["run_id"]: r for r in logs}
    assert by_run["RUN1"]["active_ind"] == "N"   # superseded by the rerun
    assert by_run["RUN2"]["active_ind"] == "Y"   # the latest run_id takes precedence


def test_execute_rule_gre_log_rowcount_matches_failed_records_on_unchanged_rerun():
    """
    Regression for the gre_log.rowcount inconsistency: rowcount used to be
    reconcile["inserted"] + reconcile["reactivated"] (rows that CHANGED
    state this attempt), not the true violating-row count. On a rerun
    where nothing changed (no new violations, nothing fixed), that delta
    is 0/0 -- so gre_log reported rowcount=0 for an attempt that still had
    genuinely-open, currently-active violations, silently disagreeing with
    gre_results.failed_records and with a COUNT(*) against gre_exceptions
    itself. rowcount must always equal gre_results.failed_records for the
    same rule_id/run_id.
    """
    conn = _conn()
    rule = _rule(threshold_pct=10)   # any nonzero failure_pct breaches -> gre_results row written

    execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    execute_rule(rule, _Adapter(conn), conn, "RUN2", "B1", {"batch_id": "B1"}, META_DB)   # nothing changed

    log_run2 = execute_query(conn, "SELECT rowcount FROM gre_log WHERE rule_id = 1 AND run_id = 'RUN2'")[0]
    result_run2 = execute_query(conn, "SELECT failed_records FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")[0]
    active_count = execute_query(
        conn, "SELECT COUNT(*) AS c FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1' AND etl_is_curr_ind = 'Y'",
    )[0]["c"]

    assert log_run2["rowcount"] == 2          # true violating count (C1, C3), not 0
    assert log_run2["rowcount"] == result_run2["failed_records"]   # must always agree with gre_results
    assert log_run2["rowcount"] == active_count   # and with the actual active gre_exceptions count


def test_execute_rule_deactivates_prior_log_attempts_on_rerun():
    """
    active_ind reconciliation regression for gre_log: a rerun of the same
    run_key under a new run_id must deactivate every earlier run_id's
    gre_log row for this (rule_id, run_key) -- including an ERROR attempt,
    not just a SUCCESS one -- so a reader filtering active_ind='Y' always
    sees exactly the latest attempt, never a stale one left behind.
    """
    conn = _conn()
    broken_rule = _rule(rule_syntax="SELECT * FROM no_such_table WHERE batch_id = '{batch_id}'")

    # RUN1: errors out (bad SQL) -- logged as ERROR, active_ind='Y'.
    status1 = execute_rule(broken_rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status1 == "ERROR"

    # RUN2: same run_key, rule now fixed -- succeeds.
    fixed_rule = _rule()
    status2 = execute_rule(fixed_rule, _Adapter(conn), conn, "RUN2", "B1", {"batch_id": "B1"}, META_DB)
    assert status2 == "SUCCESS"

    logs = execute_query(conn, "SELECT * FROM gre_log WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(logs) == 2   # both attempts kept for history
    by_run = {r["run_id"]: r for r in logs}
    assert by_run["RUN1"]["status"] == "ERROR"
    assert by_run["RUN1"]["active_ind"] == "N"    # deactivated even though it never succeeded
    assert by_run["RUN2"]["status"] == "SUCCESS"
    assert by_run["RUN2"]["active_ind"] == "Y"

    active = execute_query(
        conn, "SELECT run_id FROM gre_log WHERE rule_id = 1 AND run_key = 'B1' AND active_ind = 'Y'",
    )
    assert {r["run_id"] for r in active} == {"RUN2"}


def test_execute_rule_deactivates_prior_errors_on_rerun():
    """
    active_ind reconciliation regression for gre_rule_errors: two consecutive
    failing reruns of the same run_key must leave only the LATEST run_id's
    error row active, with the earlier one deactivated (not deleted).
    """
    conn = _conn()
    broken_rule = _rule(rule_syntax="SELECT * FROM no_such_table WHERE batch_id = '{batch_id}'")

    execute_rule(broken_rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
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


def test_execute_rule_no_threshold_fallback_not_written_when_partial_failure():
    conn = _conn()
    rule = _rule(threshold_pct=None, threshold_count=None)  # 2/4 fail, no threshold

    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", "B1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"

    # Exceptions are still written regardless of any threshold...
    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(exceptions) == 2

    # ...but gre_results gets no row, since not every in-scope record failed.
    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND run_key = 'B1'")
    assert len(results) == 0


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

    logs = execute_query(conn, "SELECT * FROM gre_log WHERE rule_id = 1")
    assert len(logs) == 1 and logs[0]["status"] == "ERROR"


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


def test_no_max_exceptions_cap_captures_every_violating_row():
    """
    Regression for the removal of GRE_MAX_EXCEPTIONS: gre_exceptions
    detail capture is uncapped. This seeds a source table with far more
    violating rows than the OLD default cap (10000) to prove there is no
    ceiling left anywhere in the path -- gre_exceptions,
    gre_results.failed_records, and gre_log.rowcount must all agree on
    the full, true count.
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
    assert results[0]["failed_records"] == n

    logs = execute_query(conn, "SELECT rowcount FROM gre_log WHERE rule_id = 1 AND run_key = 'B1'")
    assert logs[0]["rowcount"] == n   # gre_log agrees with gre_results and gre_exceptions, no cap anywhere

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

    logs = execute_query(conn, "SELECT * FROM gre_log WHERE rule_id = 1")
    assert len(logs) == 1 and logs[0]["status"] == "ERROR"

    # Caught before ANY query ran -- nothing written to gre_exceptions/gre_results.
    assert execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1") == []
    assert execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1") == []


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

    logs = execute_query(conn, "SELECT * FROM gre_log WHERE rule_id = 1")
    assert len(logs) == 1 and logs[0]["status"] == "ERROR"

    assert execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1") == []
    assert execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1") == []


# ── run_key genericity: no "batch_id" concept required ───────────────────

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
    assert len(results) == 1

    logs = execute_query(conn, f"SELECT * FROM gre_log WHERE rule_id = 1 AND run_key = '{run_key}'")
    assert len(logs) == 1 and logs[0]["status"] == "SUCCESS"


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
