"""
rules_engine/executor.py tests: threshold evaluation, natural-key
building, and execute_rule() end-to-end, including the big-dataset path
(single-scan evaluation, memoized total counts). No live DB connection
required -- DuckDB stands in for both the source table and the gre_
metadata store (schema-qualified as "main", DuckDB's default schema, so
the f"{meta_db}.table" pattern the engine uses in production works
unchanged here).

Generic DB-helper behavior (bulk writes, {key} run_params substitution) is
covered in tests/test_shared_db_ops.py instead -- this file only exercises
rule-specific behavior built on top of those primitives.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
import pytest

import rules_engine.executor as rules_engine_executor
from rules_engine.executor import (
    evaluate_threshold, build_natural_key,
    execute_rule, _compute_total, _scan_violations,
)
from shared.db_ops import execute_query

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
        return f"{rule['database_name']}.{rule['table_name']}"


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

    conn.execute("""
        CREATE TABLE gre_exceptions (
            record_id BIGINT, run_id VARCHAR, rule_id INTEGER, database_name VARCHAR, table_name VARCHAR,
            project_name VARCHAR, process_name VARCHAR,
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
            project_name VARCHAR, process_name VARCHAR,
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
            project_name VARCHAR, process_name VARCHAR,
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
        "database_name": "main",   # DuckDB's default schema -- see _conn()
        "table_name": "claims",
        "sql_dialect": "teradata",   # also picks the one connection this rule runs against
        "rule_sql": "SELECT claim_id, denial_reason FROM claims "
                    "WHERE denial_reason IS NULL AND batch_id = '{batch_id}'",
        # No scope_sql column anymore -- _compute_total() auto-builds the
        # total-record count from database_name.table_name filtered by
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


# ── natural key ────────────────────────────────────────────────────────

def test_build_natural_key():
    rule = _rule(natural_key_columns="claim_id, denial_reason")
    key = build_natural_key(rule, {"claim_id": "C1", "denial_reason": None})
    assert key == "claim_id=C1|denial_reason=NULL"


def test_build_natural_key_requires_columns():
    rule = _rule(natural_key_columns="")
    with pytest.raises(ValueError):
        build_natural_key(rule, {"claim_id": "C1"})


# ── execute_rule end-to-end ──────────────────────────────────────────────

def test_execute_rule_writes_exceptions_and_result():
    conn = _conn()
    rule = _rule(threshold_pct=25)  # 2/4 = 50% > 25% -> FAIL

    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND batch_id = 'B1'")
    assert len(exceptions) == 2
    assert {r["natural_key_value"] for r in exceptions} == {"claim_id=C1", "claim_id=C3"}
    assert all(r["project_name"] == "HEALTHSPRING_UM" and r["process_name"] == "UNIVERSE_VALIDATION"
               for r in exceptions)

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND batch_id = 'B1'")
    assert len(results) == 1
    assert results[0]["status"] == "FAIL"
    assert results[0]["total_records"] == 4
    assert results[0]["failed_records"] == 2
    assert results[0]["threshold_pct_used"] == 25
    assert results[0]["project_name"] == "HEALTHSPRING_UM"
    assert results[0]["process_name"] == "UNIVERSE_VALIDATION"

    logs = execute_query(conn, "SELECT * FROM gre_log WHERE rule_id = 1 AND batch_id = 'B1'")
    assert len(logs) == 1
    assert logs[0]["status"] == "SUCCESS"
    assert logs[0]["rowcount"] == 2
    assert logs[0]["project_name"] == "HEALTHSPRING_UM"
    assert logs[0]["process_name"] == "UNIVERSE_VALIDATION"


def test_execute_rule_is_idempotent_on_rerun():
    conn = _conn()
    rule = _rule(threshold_pct=25)

    execute_rule(rule, _Adapter(conn), conn, "RUN1", {"batch_id": "B1"}, META_DB)
    execute_rule(rule, _Adapter(conn), conn, "RUN2", {"batch_id": "B1"}, META_DB)  # simulate a rerun of the same batch

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

    execute_rule(rule, _Adapter(conn), conn, "RUN1", {"batch_id": "B1"}, META_DB)
    execute_rule(rule, _Adapter(conn), conn, "RUN1", {"batch_id": "B2"}, META_DB)

    b1 = execute_query(conn, "SELECT * FROM gre_exceptions WHERE batch_id = 'B1'")
    b2 = execute_query(conn, "SELECT * FROM gre_exceptions WHERE batch_id = 'B2'")
    assert len(b1) == 2
    assert len(b2) == 1   # only C5 is in B2


def test_execute_rule_no_threshold_fallback_not_written_when_partial_failure():
    conn = _conn()
    rule = _rule(threshold_pct=None, threshold_count=None)  # 2/4 fail, no threshold

    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", {"batch_id": "B1"}, META_DB)
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

    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", {"batch_id": "B1"}, META_DB)
    assert status == "ERROR"

    errors = execute_query(conn, "SELECT * FROM gre_errors WHERE rule_id = 1")
    assert len(errors) == 1
    assert errors[0]["error_type"] == "SQL_RUNTIME"

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1")
    assert len(exceptions) == 0   # a crash never writes partial findings

    logs = execute_query(conn, "SELECT * FROM gre_log WHERE rule_id = 1")
    assert len(logs) == 1 and logs[0]["status"] == "ERROR"


# ── big-dataset path: dedup + true-count/capped-fetch split ──────────────

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
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND batch_id = 'B1'")
    assert len(exceptions) == 2   # C1, C3 -- deduped, not 4, even though the pull returned each twice
    assert {r["natural_key_value"] for r in exceptions} == {"claim_id=C1", "claim_id=C3"}

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND batch_id = 'B1'")
    assert len(results) == 1
    assert results[0]["failed_records"] == 4   # true COUNT(*) on rule_sql counts every returned row


def test_max_exceptions_cap_keeps_failed_records_true_but_caps_detail_rows(monkeypatch):
    conn = _conn()
    monkeypatch.setattr(rules_engine_executor, "MAX_EXCEPTIONS", 1)
    rule = _rule(threshold_pct=0)   # any failure breaches -> a gre_results row is written

    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND batch_id = 'B1'")
    assert len(exceptions) == 1   # capped at MAX_EXCEPTIONS=1

    results = execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1 AND batch_id = 'B1'")
    assert results[0]["failed_records"] == 2   # true count stays exact -- 2 of 4 actually failed

    logs = execute_query(conn, "SELECT * FROM gre_log WHERE rule_id = 1 AND batch_id = 'B1'")
    assert logs[0]["rowcount"] == 1   # "rows written to gre_exceptions this attempt" == the capped count


# ── source prepare (STEP 0 of execute_rule) ──────────────────────────────

def test_execute_rule_prepare_failure_routes_to_errors_before_any_query():
    # A file/S3 rule whose prepare() fails (e.g. missing table_name) must
    # fail BEFORE any query runs -- same fail-fast contract the old dialect
    # guard had, now covering source setup instead of a dialect mismatch.
    conn = _conn()

    class _FailingAdapter(_Adapter):
        def prepare(self, rule):
            raise ValueError("table_name is empty -- cannot prepare file source.")

    rule = _rule(threshold_pct=25)
    status = execute_rule(rule, _FailingAdapter(conn), conn, "RUN1", {"batch_id": "B1"}, META_DB)
    assert status == "ERROR"

    errors = execute_query(conn, "SELECT * FROM gre_errors WHERE rule_id = 1")
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
    status = execute_rule(rule, _TrackingAdapter(conn), conn, "RUN1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"
    assert calls == [1]


# ── _compute_total: auto-derived from database_name.table_name + run_params ──

def test_compute_total_auto_filters_by_every_run_params_key():
    # No scope_sql anywhere -- _compute_total() builds
    # "SELECT COUNT(*) FROM main.claims WHERE batch_id = '...'" straight
    # from database_name/table_name + run_params, same as the old explicit
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


def test_compute_total_uses_database_name_and_table_name():
    conn = _conn()
    rule = _rule(database_name="main", table_name="claims")
    assert _compute_total(_Adapter(conn), rule, {"batch_id": "B1"}, total_cache=None) == 4

    # A wrong database_name/table_name should surface as a real query
    # failure, not silently return something -- proves the auto-built
    # query actually uses the fields, not a hardcoded table reference.
    bad_rule = _rule(database_name="main", table_name="no_such_table")
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
    assert len(rows) == 2   # default MAX_EXCEPTIONS (10000) doesn't cap 2 rows
    assert {r["claim_id"] for r in rows} == {"C1", "C3"}


def test_scan_violations_keeps_true_count_exact_past_the_cap(monkeypatch):
    conn = _conn()
    monkeypatch.setattr(rules_engine_executor, "MAX_EXCEPTIONS", 1)
    query = "SELECT claim_id, denial_reason FROM claims WHERE denial_reason IS NULL AND batch_id = 'B1'"
    failed, rows = _scan_violations(conn, query)
    assert failed == 2      # true count, uncapped
    assert len(rows) == 1   # detail rows capped


def test_scan_violations_issues_exactly_one_query():
    conn = _conn()
    wrapped = _CursorCountingWrapper(conn)
    query = "SELECT claim_id, denial_reason FROM claims WHERE denial_reason IS NULL AND batch_id = 'B1'"
    failed, rows = _scan_violations(wrapped, query)
    assert failed == 2
    assert wrapped.cursor_calls == 1   # ONE execution -- not a separate COUNT query plus a fetch query


def test_execute_rule_issues_two_source_queries_not_three():
    # Old design per rule: a COUNT(*)-wrapped query + a detail-row fetch
    # query (both running rule_sql) + the total-record query = 3 source-side
    # queries. New design: one merged scan (_scan_violations) + the total
    # query = 2. (A rule_group with several rules sharing the same table
    # drops this further via total_cache -- see test_rules_engine_runner.py's
    # shared-cache test -- but for a single rule on its own, 2 is the count.)
    conn = _conn()
    wrapped_db = _CursorCountingWrapper(conn)
    rule = _rule(threshold_pct=25)

    status = execute_rule(rule, wrapped_db, conn, "RUN1", {"batch_id": "B1"}, META_DB)
    assert status == "SUCCESS"
    assert wrapped_db.cursor_calls == 2


# ── run_params substitution (v2 scoping) ─────────────────────────────────

def test_execute_rule_uses_extra_run_params_key_beyond_batch_id():
    conn = _conn()
    # run_type must be a REAL column: every key in run_params also becomes
    # an equality filter for the auto-generated total-record count (see
    # _build_total_query()), so an extra run_params key has to name an
    # actual column on the rule's table, not just something rule_sql
    # happens to reference inline.
    conn.execute("ALTER TABLE claims ADD COLUMN run_type VARCHAR DEFAULT 'MONTHLY'")
    rule = _rule(
        rule_sql="SELECT claim_id, denial_reason FROM claims "
                 "WHERE denial_reason IS NULL AND batch_id = '{batch_id}' AND '{run_type}' = 'MONTHLY'",
    )
    status = execute_rule(rule, _Adapter(conn), conn, "RUN1", {"batch_id": "B1", "run_type": "MONTHLY"}, META_DB)
    assert status == "SUCCESS"

    exceptions = execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1 AND batch_id = 'B1'")
    assert len(exceptions) == 2   # C1, C3 -- the extra {run_type} token resolved and matched


def test_execute_rule_unresolved_token_fails_fast_before_any_query():
    # rule_sql references {run_type}, but the caller's run_params doesn't
    # supply it -- must fail BEFORE the scan/count queries run, logged as
    # PARAM_SUBSTITUTION_ERROR, never as a confusing SQL syntax error from
    # the source database.
    conn = _conn()
    wrapped_db = _CursorCountingWrapper(conn)
    rule = _rule(
        rule_sql="SELECT claim_id, denial_reason FROM claims "
                 "WHERE denial_reason IS NULL AND batch_id = '{batch_id}' AND run_type = '{run_type}'",
    )

    status = execute_rule(rule, wrapped_db, conn, "RUN1", {"batch_id": "B1"}, META_DB)
    assert status == "ERROR"
    assert wrapped_db.cursor_calls == 0   # caught before any source query ran

    errors = execute_query(conn, "SELECT * FROM gre_errors WHERE rule_id = 1")
    assert len(errors) == 1
    assert errors[0]["error_type"] == "PARAM_SUBSTITUTION_ERROR"
    assert "run_type" in errors[0]["error_message"]

    logs = execute_query(conn, "SELECT * FROM gre_log WHERE rule_id = 1")
    assert len(logs) == 1 and logs[0]["status"] == "ERROR"

    assert execute_query(conn, "SELECT * FROM gre_exceptions WHERE rule_id = 1") == []
    assert execute_query(conn, "SELECT * FROM gre_results WHERE rule_id = 1") == []
