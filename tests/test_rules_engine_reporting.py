"""
rules_engine/reporting.py tests: get_breaches()/get_records_for_result()
(the existing gre_results -> gre_exceptions drill-down), and
get_source_records_for_rule() -- the natural-key tie-back from a
gre_exceptions row all the way back to its actual source record, at
report/analysis time. No live DB connection required -- DuckDB stands in
for both the source table(s) and the gre_ metadata store.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
import pytest

import rules_engine.reporting as rules_engine_reporting
from rules_engine.reporting import (
    get_breaches, get_records_for_result, get_source_records_for_rule,
)
from shared.db_ops import execute_dml

META_DB = "main"


class _Adapter:
    """
    Minimal SourceAdapter shim wrapping a raw DuckDB connection: adds the
    prepare()/qualified_name() surface get_source_records_for_rule() calls
    directly (db.connection_factory.SourceAdapter's interface).
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
        return f"{rule['database_name']}.{rule['table_name']}"


class _FakeConnectionFactory:
    """Every named connection resolves to the same DuckDB connection, unless overridden to return None."""
    def __init__(self, conn, missing_names=()):
        self._conn = conn
        self._missing_names = set(missing_names)

    def get(self, name):
        return None if name in self._missing_names else _Adapter(self._conn)


def _conn():
    conn = duckdb.connect(":memory:")

    # Single-column-key source table.
    conn.execute("CREATE TABLE claims (claim_id VARCHAR, denial_reason VARCHAR, region VARCHAR)")
    conn.execute("""
        INSERT INTO claims VALUES
            ('C1', NULL, 'EAST'),
            ('C2', NULL, NULL),
            ('C3', 'X', 'WEST')
    """)

    # Composite-key source table.
    conn.execute("CREATE TABLE order_lines (order_id VARCHAR, line_no INTEGER, qty INTEGER)")
    conn.execute("""
        INSERT INTO order_lines VALUES
            ('O1', 1, -5),
            ('O1', 2, 10),
            ('O2', 1, -1)
    """)

    conn.execute("""
        CREATE TABLE gre_exceptions (
            record_id BIGINT, run_id VARCHAR, rule_id INTEGER, database_name VARCHAR, table_name VARCHAR,
            element_name VARCHAR, source_name VARCHAR, issue_desc VARCHAR,
            exception_flag VARCHAR DEFAULT 'OPEN', exception_approver VARCHAR,
            run_key VARCHAR, etl_is_curr_ind VARCHAR DEFAULT 'Y',
            etl_load_dt DATE, etl_last_updt_dt TIMESTAMP,
            natural_key_value VARCHAR, created_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    conn.execute("""
        CREATE TABLE gre_results (
            result_id BIGINT, rule_id INTEGER, run_key VARCHAR, run_id VARCHAR,
            total_records BIGINT, failed_records BIGINT, failure_pct DOUBLE,
            threshold_pct_used DOUBLE, threshold_count_used INTEGER,
            threshold_operator_used VARCHAR, severity VARCHAR, status VARCHAR,
            evaluated_at TIMESTAMP DEFAULT current_timestamp
        )
    """)

    return conn


_record_id_seq = [0]


def _insert_exception(conn, rule_id, natural_key_value, database_name="main", table_name="claims",
                      source_name="duckdb_test", run_key="B1", issue_desc=None,
                      exception_flag="OPEN", etl_is_curr_ind="Y"):
    _record_id_seq[0] += 1
    execute_dml(conn, """
        INSERT INTO gre_exceptions (
            record_id, run_id, rule_id, database_name, table_name, source_name,
            issue_desc, exception_flag, run_key, etl_is_curr_ind, natural_key_value
        ) VALUES (?, 'RUN1', ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [_record_id_seq[0], rule_id, database_name, table_name, source_name,
          issue_desc or f"rule {rule_id} violated", exception_flag, run_key, etl_is_curr_ind, natural_key_value])


def _insert_result(conn, rule_id, run_key, run_id, status):
    execute_dml(conn, """
        INSERT INTO gre_results (result_id, rule_id, run_key, run_id, total_records, failed_records, status)
        VALUES (?, ?, ?, ?, 10, 1, ?)
    """, [rule_id, rule_id, run_key, run_id, status])


# ── get_breaches / get_records_for_result (existing, previously untested) ──

def test_get_breaches_filters_to_fail_and_warn_for_the_run():
    conn = _conn()
    _insert_result(conn, 1, "B1", "RUN1", "FAIL")
    _insert_result(conn, 2, "B1", "RUN1", "PASS")
    _insert_result(conn, 3, "B1", "RUN1", "WARN")
    _insert_result(conn, 1, "B2", "RUN2", "FAIL")   # different run -- excluded

    breaches = get_breaches(conn, META_DB, "RUN1")
    assert {b["rule_id"] for b in breaches} == {1, 3}


def test_get_records_for_result_filters_current_version_only():
    conn = _conn()
    _insert_exception(conn, 1, "claim_id=C1", run_key="B1")
    _insert_exception(conn, 1, "claim_id=C2", run_key="B1", etl_is_curr_ind="N")
    _insert_exception(conn, 2, "claim_id=C3", run_key="B1")   # different rule -- excluded

    records = get_records_for_result(conn, META_DB, 1, "B1")
    assert len(records) == 1
    assert records[0]["natural_key_value"] == "claim_id=C1"


# ── get_source_records_for_rule: single-column natural key ──────────────

def test_get_source_records_pulls_matching_rows_with_finding_context():
    conn = _conn()
    _insert_exception(conn, 1, "claim_id=C1", issue_desc="missing denial_reason")
    _insert_exception(conn, 1, "claim_id=C2", issue_desc="missing denial_reason")
    cf = _FakeConnectionFactory(conn)

    records = get_source_records_for_rule(cf, conn, META_DB, 1, "B1")

    assert {r["claim_id"] for r in records} == {"C1", "C2"}
    by_id = {r["claim_id"]: r for r in records}
    assert by_id["C1"]["region"] == "EAST"            # the actual source columns are there
    assert by_id["C1"]["_rule_id"] == 1
    assert by_id["C1"]["_natural_key_value"] == "claim_id=C1"
    assert by_id["C1"]["_issue_desc"] == "missing denial_reason"
    assert by_id["C1"]["_exception_flag"] == "OPEN"
    assert by_id["C1"]["_record_id"] is not None


def test_get_source_records_handles_null_key_column_value():
    # C2's region is a genuine SQL NULL -- exercised via a rule keyed on region.
    conn = _conn()
    _insert_exception(conn, 5, "region=NULL")
    cf = _FakeConnectionFactory(conn)

    records = get_source_records_for_rule(cf, conn, META_DB, 5, "B1")
    assert len(records) == 1
    assert records[0]["claim_id"] == "C2"
    assert records[0]["region"] is None


def test_get_source_records_no_exceptions_returns_empty_list():
    conn = _conn()
    cf = _FakeConnectionFactory(conn)
    assert get_source_records_for_rule(cf, conn, META_DB, 999, "B1") == []


def test_get_source_records_scoped_to_rule_and_run_key():
    conn = _conn()
    _insert_exception(conn, 1, "claim_id=C1", run_key="B1")
    _insert_exception(conn, 1, "claim_id=C3", run_key="B2")   # different run_key -- excluded
    _insert_exception(conn, 2, "claim_id=C2", run_key="B1")   # different rule -- excluded
    cf = _FakeConnectionFactory(conn)

    records = get_source_records_for_rule(cf, conn, META_DB, 1, "B1")
    assert {r["claim_id"] for r in records} == {"C1"}


def test_get_source_records_current_version_only():
    conn = _conn()
    _insert_exception(conn, 1, "claim_id=C1")
    _insert_exception(conn, 1, "claim_id=C3", etl_is_curr_ind="N")   # superseded -- excluded
    cf = _FakeConnectionFactory(conn)

    records = get_source_records_for_rule(cf, conn, META_DB, 1, "B1")
    assert {r["claim_id"] for r in records} == {"C1"}


# ── one row failing multiple rules: no duplicated storage, independent tie-back ──

def test_one_row_failing_two_rules_is_not_duplicated_and_ties_back_independently():
    # The user's exact scenario: a row that fails rule 1 AND rule 2 gets ONE
    # gre_exceptions row per rule (not the full record captured twice) --
    # and each rule's tie-back independently resolves back to the SAME
    # single source record, not a duplicated copy.
    conn = _conn()
    _insert_exception(conn, 1, "claim_id=C1", issue_desc="rule 1 violated")
    _insert_exception(conn, 2, "claim_id=C1", issue_desc="rule 2 violated")
    cf = _FakeConnectionFactory(conn)

    rule1_records = get_source_records_for_rule(cf, conn, META_DB, 1, "B1")
    rule2_records = get_source_records_for_rule(cf, conn, META_DB, 2, "B1")

    assert len(rule1_records) == 1 and len(rule2_records) == 1
    assert rule1_records[0]["claim_id"] == rule2_records[0]["claim_id"] == "C1"
    assert rule1_records[0]["_issue_desc"] == "rule 1 violated"
    assert rule2_records[0]["_issue_desc"] == "rule 2 violated"


# ── composite (multi-column) natural key ─────────────────────────────────

def test_get_source_records_composite_natural_key():
    conn = _conn()
    _insert_exception(conn, 10, "order_id=O1|line_no=1", table_name="order_lines")
    _insert_exception(conn, 10, "order_id=O2|line_no=1", table_name="order_lines")
    cf = _FakeConnectionFactory(conn)

    records = get_source_records_for_rule(cf, conn, META_DB, 10, "B1")

    assert len(records) == 2
    keys = {(r["order_id"], r["line_no"]) for r in records}
    assert keys == {("O1", 1), ("O2", 1)}
    # O1/line_no=2 (qty=10, not negative) was never flagged -- proves the
    # composite-key match doesn't accidentally pull in every row for O1.
    assert all(r["qty"] < 0 for r in records)


# ── chunking (EXCEPTION_CHUNK) ────────────────────────────────────────────

def test_get_source_records_chunks_large_key_sets(monkeypatch):
    conn = _conn()
    conn.execute("CREATE TABLE big (id VARCHAR, val INTEGER)")
    conn.executemany("INSERT INTO big VALUES (?, ?)", [[f"K{i}", i] for i in range(25)])
    for i in range(25):
        _insert_exception(conn, 20, f"id=K{i}", table_name="big")

    monkeypatch.setattr(rules_engine_reporting, "EXCEPTION_CHUNK", 7)   # 25 keys -> 4 chunks
    cf = _FakeConnectionFactory(conn)

    records = get_source_records_for_rule(cf, conn, META_DB, 20, "B1")
    assert len(records) == 25
    assert {r["id"] for r in records} == {f"K{i}" for i in range(25)}


# ── record deleted/changed upstream since the rule ran ───────────────────

def test_get_source_records_missing_upstream_row_is_skipped_not_raised(caplog):
    conn = _conn()
    _insert_exception(conn, 1, "claim_id=C1")
    _insert_exception(conn, 1, "claim_id=NO_LONGER_THERE")   # deleted from claims since the rule ran
    cf = _FakeConnectionFactory(conn)

    with caplog.at_level("INFO"):
        records = get_source_records_for_rule(cf, conn, META_DB, 1, "B1")

    assert {r["claim_id"] for r in records} == {"C1"}   # only the still-present row comes back
    assert "1 of 2" in caplog.text


# ── missing source connection ─────────────────────────────────────────────

def test_get_source_records_raises_clearly_when_connection_unavailable():
    conn = _conn()
    _insert_exception(conn, 1, "claim_id=C1", source_name="gone")
    cf = _FakeConnectionFactory(conn, missing_names=["gone"])

    with pytest.raises(RuntimeError, match="gone"):
        get_source_records_for_rule(cf, conn, META_DB, 1, "B1")
