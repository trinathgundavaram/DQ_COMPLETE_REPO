"""
End-to-end integration test for core/engine.py::run_engine().

Everything else in tests/ exercises one function/module in isolation
(dialect enforcement, raw-SQL generation, the write-statement guard,
threshold evaluation, check_types.py generators). This file is the one
place that drives the REAL orchestration path top to bottom: dq_scope
resolution, rule loading, pre-validation, the ThreadPoolExecutor + rule
dependency graph (fix #12), metadata writes, metrics, and the final
run-control status -- against a stand-in database, no live
Teradata/Postgres connection required.

DuckDB stands in for BOTH the metadata store and the rule's source table
in one shared in-memory database: metadata tables live in a `{meta_db}`
schema (mirroring the "{meta_db}.dq_rules" qualification core/ uses
everywhere), the source table lives unqualified in `main`. See
_SharedDuckDBAdapter below for why a single shared connection (not one
`:memory:` per adapter) is required for engine.py's per-thread
`cf.new_connection()` calls to all see the same data.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import threading

import duckdb
import pytest

import core.engine as engine
import db.connection_factory as connection_factory
from db.adapters import SourceAdapter

META_DB = "dqmeta"


# =============================================================================
# Test-only adapter: one shared in-memory DuckDB database standing in for
# every named connection the engine asks for (metadata AND rule source).
# =============================================================================

class _SharedDuckDBCursor:
    """
    Adapts a raw duckdb cursor to the execute()/description/fetchall()
    shape core/executor.py's execute_query/execute_dml/bulk_insert expect.

    Each individual call (execute/executemany/fetchall/close) independently
    acquires the adapter-level lock via `with`, rather than the cursor
    holding the lock for its whole open->close lifetime. That matters
    because execute_query()/execute_dml() in core/executor.py don't wrap
    their cursor.execute() call in try/finally -- a query that raises
    (e.g. a rule with a genuine SQL error) would otherwise leave the lock
    permanently held with no code path left to release it, wedging every
    other thread. Per-call locking closes that gap: a lock is never held
    longer than one real duckdb operation, so a mid-operation exception
    can't strand it.
    """

    def __init__(self, conn, lock):
        self._lock = lock
        with lock:
            self._c = conn.cursor()

    def execute(self, query, params=None):
        with self._lock:
            return self._c.execute(query, params) if params is not None else self._c.execute(query)

    def executemany(self, query, data):
        with self._lock:
            return self._c.executemany(query, data)

    @property
    def description(self):
        with self._lock:
            return self._c.description

    def fetchall(self):
        with self._lock:
            return self._c.fetchall()

    def fetchmany(self, size=None):
        with self._lock:
            return self._c.fetchmany(size) if size is not None else self._c.fetchmany()

    def close(self):
        with self._lock:
            try:
                self._c.close()
            except Exception:
                pass


class _SharedDuckDBAdapter(SourceAdapter):
    """
    Every adapter instance -- the main-thread one from cf.get(), and one
    fresh instance per ThreadPoolExecutor worker from cf.new_connection()
    (see core/engine.py::run_single) -- wraps the SAME class-level shared
    duckdb connection, so writes from any thread are visible to every
    other thread and to the main thread's post-run metrics/reporting
    reads. A lock serializes one logical DB operation (open cursor ->
    execute -> fetch -> close) at a time, since DuckDB's Python bindings
    aren't documented as safe for fully concurrent access the way the
    real network-socket adapters (Teradata/Postgres) are.
    """
    source_type = "duckdb"   # DIALECT_COMPATIBILITY['duckdb'] = {'postgres', 'ansi'}

    _shared_conn = None
    _lock = threading.Lock()

    def cursor(self):
        return _SharedDuckDBCursor(type(self)._shared_conn, type(self)._lock)

    def commit(self):
        pass   # each statement is already visible on the shared connection
               # the instant cursor.execute() returns -- nothing to flush.

    def close(self):
        pass   # the shared connection is owned by the test, not any one
               # adapter instance; torn down explicitly at the end of the test.

    def ping(self) -> bool:
        return True

    @classmethod
    def build(cls, name: str) -> "_SharedDuckDBAdapter":
        return cls()


# =============================================================================
# Metadata schema (DuckDB-flavoured subset of ddl.sql v7)
# =============================================================================

def _create_schema(conn):
    conn.execute(f"CREATE SCHEMA {META_DB}")
    conn.execute(f"CREATE SEQUENCE {META_DB}.seq_scope START 1")
    conn.execute(f"""
        CREATE TABLE {META_DB}.dq_scope (
            scope_id      INTEGER PRIMARY KEY DEFAULT nextval('{META_DB}.seq_scope'),
            project_name  VARCHAR NOT NULL,
            process_name  VARCHAR,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute(f"""
        CREATE TABLE {META_DB}.dq_rules (
            rule_id              INTEGER NOT NULL,
            rule_code            VARCHAR NOT NULL,
            scope_id             INTEGER NOT NULL,
            src_tbl_nm           VARCHAR NOT NULL,
            src_db_name          VARCHAR,
            src_schema           VARCHAR,
            rule_name             VARCHAR,
            rule_description      VARCHAR,
            rule_syntax           VARCHAR,
            join_sql               VARCHAR,
            source_system          VARCHAR,
            filter_column           VARCHAR,
            filter_type             VARCHAR,
            filter_sql               VARCHAR,
            primary_key_columns      VARCHAR,
            severity                 VARCHAR,
            threshold_pct            DOUBLE,
            threshold_count          INTEGER,
            threshold_operator       VARCHAR DEFAULT 'OR',
            require_rows             INTEGER DEFAULT 0,
            priority                 INTEGER DEFAULT 100,
            depends_on_rule_id       INTEGER,
            rule_group               VARCHAR,
            table_group              VARCHAR,
            active_flag              INTEGER,
            created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at                TIMESTAMP,
            check_type                VARCHAR,
            check_column               VARCHAR,
            check_params                VARCHAR,
            sql_dialect                 VARCHAR,
            business_correctable        INTEGER DEFAULT 0
        )
    """)
    conn.execute(f"""
        CREATE TABLE {META_DB}.dq_run_control (
            run_id        VARCHAR NOT NULL,
            scope_id      INTEGER NOT NULL,
            run_type      VARCHAR,
            run_mode      VARCHAR,
            batch_id      VARCHAR,
            dataset_id    VARCHAR,
            start_date    DATE,
            end_date      DATE,
            triggered_by  VARCHAR,
            start_time    TIMESTAMP,
            end_time      TIMESTAMP,
            status        VARCHAR,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute(f"""
        CREATE TABLE {META_DB}.dq_rule_execution (
            run_id          VARCHAR,
            rule_id         INTEGER,
            rule_code       VARCHAR,
            table_name      VARCHAR,
            total_records   BIGINT,
            failed_records  BIGINT,
            passed_records  BIGINT,
            failure_pct     DOUBLE,
            pass_pct        DOUBLE,
            severity        VARCHAR,
            status          VARCHAR,
            execution_time  DOUBLE,
            run_timestamp   TIMESTAMP,
            run_date        DATE,
            run_month       DATE,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute(f"CREATE SEQUENCE {META_DB}.seq_exception START 1")
    conn.execute(f"""
        CREATE TABLE {META_DB}.dq_exceptions (
            exception_id     INTEGER PRIMARY KEY DEFAULT nextval('{META_DB}.seq_exception'),
            run_id           VARCHAR,
            rule_id          INTEGER,
            rule_code        VARCHAR,
            table_name       VARCHAR,
            key_json         VARCHAR,
            primary_key_str  VARCHAR,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute(f"""
        CREATE TABLE {META_DB}.dq_metrics_summary (
            scope_id         INTEGER,
            run_type         VARCHAR,
            batch_id         VARCHAR,
            dataset_id       VARCHAR,
            run_month        DATE,
            total_runs       INTEGER,
            total_rules      INTEGER,
            failed_rules     INTEGER,
            passed_rules     INTEGER,
            total_records    BIGINT,
            failed_records   BIGINT,
            avg_failure_pct  DOUBLE,
            dq_score         DOUBLE,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute(f"CREATE SEQUENCE {META_DB}.seq_log START 1")
    conn.execute(f"""
        CREATE TABLE {META_DB}.dq_run_logs (
            log_id        INTEGER PRIMARY KEY DEFAULT nextval('{META_DB}.seq_log'),
            run_id        VARCHAR,
            rule_id       INTEGER,
            rule_code     VARCHAR,
            log_level     VARCHAR,
            message       VARCHAR,
            error_code    VARCHAR,
            error_detail  VARCHAR,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute(f"CREATE SEQUENCE {META_DB}.seq_issue START 1")
    conn.execute(f"""
        CREATE TABLE {META_DB}.dq_rule_issues (
            issue_id      INTEGER PRIMARY KEY DEFAULT nextval('{META_DB}.seq_issue'),
            run_id        VARCHAR,
            rule_id       INTEGER,
            rule_code     VARCHAR,
            table_name    VARCHAR,
            issue_type    VARCHAR,
            issue_message VARCHAR,
            error_detail  VARCHAR,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute(f"""
        CREATE TABLE {META_DB}.dq_rule_suppressions (
            suppression_id  INTEGER NOT NULL,
            rule_id         INTEGER NOT NULL,
            rule_code       VARCHAR,
            reason          VARCHAR,
            suppressed_by   VARCHAR,
            suppressed_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at      TIMESTAMP,
            lifted_at       TIMESTAMP,
            lifted_by       VARCHAR,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute(f"CREATE SEQUENCE {META_DB}.seq_version START 1")
    conn.execute(f"""
        CREATE TABLE {META_DB}.dq_rule_versions (
            version_id          INTEGER PRIMARY KEY DEFAULT nextval('{META_DB}.seq_version'),
            rule_id             INTEGER,
            rule_code           VARCHAR,
            version_num         INTEGER,
            change_type         VARCHAR,
            rule_syntax         VARCHAR,
            check_type          VARCHAR,
            check_column        VARCHAR,
            check_params        VARCHAR,
            filter_sql          VARCHAR,
            join_sql            VARCHAR,
            threshold_pct       DOUBLE,
            threshold_count     INTEGER,
            threshold_operator  VARCHAR,
            severity            VARCHAR,
            active_flag         INTEGER,
            changed_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            change_reason       VARCHAR
        )
    """)
    conn.execute(f"""
        CREATE TABLE {META_DB}.dq_profile_config (
            config_id       INTEGER NOT NULL,
            project_name    VARCHAR,
            process_name    VARCHAR,
            table_name      VARCHAR NOT NULL,
            enabled         INTEGER DEFAULT 1,
            columns_include VARCHAR,
            columns_exclude VARCHAR,
            top_n_values    INTEGER DEFAULT 10,
            run_frequency   VARCHAR DEFAULT 'ALWAYS',
            last_profiled   TIMESTAMP,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute(f"""
        CREATE TABLE {META_DB}.dq_anomaly_config (
            config_id           INTEGER NOT NULL,
            project_name        VARCHAR,
            process_name        VARCHAR,
            run_type             VARCHAR,
            method                VARCHAR DEFAULT 'ZSCORE',
            zscore_threshold      DOUBLE DEFAULT 3.0,
            iqr_multiplier        DOUBLE DEFAULT 1.5,
            min_history_runs      INTEGER DEFAULT 10,
            alert_on_anomaly      INTEGER DEFAULT 1,
            created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute(f"CREATE SEQUENCE {META_DB}.seq_anomaly START 1")
    conn.execute(f"""
        CREATE TABLE {META_DB}.dq_anomaly_log (
            anomaly_id          INTEGER PRIMARY KEY DEFAULT nextval('{META_DB}.seq_anomaly'),
            run_id              VARCHAR,
            metric_name         VARCHAR,
            current_value       DOUBLE,
            historical_mean     DOUBLE,
            historical_std      DOUBLE,
            z_score             DOUBLE,
            iqr_lower_bound     DOUBLE,
            iqr_upper_bound     DOUBLE,
            is_anomaly          INTEGER,
            detection_method    VARCHAR,
            severity            VARCHAR,
            created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute(f"""
        CREATE TABLE {META_DB}.dq_notification_routes (
            route_id          INTEGER NOT NULL,
            project_name      VARCHAR,
            process_name      VARCHAR,
            finding_class     VARCHAR NOT NULL,
            audience          VARCHAR NOT NULL,
            channel_type      VARCHAR NOT NULL,
            destination       VARCHAR NOT NULL,
            business_correctable_only INTEGER DEFAULT 0,
            active_flag       INTEGER DEFAULT 1,
            created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # -- Source data: the table every rule below is actually evaluated against --
    conn.execute("""
        CREATE TABLE claims (
            claim_id  VARCHAR,
            amount    DOUBLE,
            pull_date DATE
        )
    """)
    conn.execute("""
        INSERT INTO claims VALUES
            ('C1', 100.0, '2026-08-01'),
            ('C2',  50.0, '2026-08-01'),
            ('C3', -10.0, '2026-08-01'),
            ('C4', -20.0, '2026-08-01'),
            ('C5',  30.0, '2026-08-01')
    """)


def _seed_rules(conn, scope_id: int):
    """
    Three rules exercising every part of fix #12's dependency graph in one
    chain:
        Rule 1 (NOT_NULL check_type, no dependency)      -> PASS
        Rule 2 (raw-SQL, depends_on_rule_id=1)            -> FAIL (2 negative amounts)
        Rule 3 (raw-SQL, depends_on_rule_id=2)            -> auto-SKIPPED (parent failed)
    Rule 1 also exercises the check_type-generator authoring path (path 2
    in core/rule_sql.py); rules 2/3 exercise the raw-SQL path (path 1).
    """
    conn.execute(f"""
        INSERT INTO {META_DB}.dq_rules (
            rule_id, rule_code, scope_id, src_tbl_nm, primary_key_columns,
            check_type, check_column, priority, depends_on_rule_id,
            severity, active_flag
        ) VALUES
            (1, 'AMOUNT_NOT_NULL',    ?, 'claims', 'claim_id',
             'NOT_NULL', 'amount', 1, NULL, 'HIGH', 1),
            (2, 'AMOUNT_NON_NEGATIVE', ?, 'claims', 'claim_id',
             NULL, NULL, 2, 1, 'HIGH', 1),
            (3, 'AMOUNT_UPPER_BOUND',  ?, 'claims', 'claim_id',
             NULL, NULL, 3, 2, 'MEDIUM', 1)
    """, [scope_id, scope_id, scope_id])

    conn.execute(f"""
        UPDATE {META_DB}.dq_rules SET rule_syntax = ?, sql_dialect = 'postgres'
        WHERE rule_id = 2
    """, ["SELECT claim_id, amount FROM claims WHERE amount < 0"])
    conn.execute(f"""
        UPDATE {META_DB}.dq_rules SET rule_syntax = ?, sql_dialect = 'postgres'
        WHERE rule_id = 3
    """, ["SELECT claim_id, amount FROM claims WHERE amount > 1000000"])


# =============================================================================
# The test
# =============================================================================

def test_run_engine_end_to_end(monkeypatch):
    conn = duckdb.connect(":memory:")
    _create_schema(conn)

    _SharedDuckDBAdapter._shared_conn = conn
    monkeypatch.setitem(connection_factory._TYPE_MAP, "teradata", _SharedDuckDBAdapter)

    # core/engine.py reads these three as module-level constants at import
    # time (os.getenv() executes once, at import) -- env vars set here
    # would be invisible to already-imported code, so patch the module
    # attributes directly instead. DQ_META_DB, by contrast, is read lazily
    # inside get_meta_db() on every call, so an env var is enough for it.
    monkeypatch.setenv("DQ_META_DB", META_DB)
    monkeypatch.setenv("DQ_CONNECTION_NAMES", "teradata")
    monkeypatch.setenv("DQ_TERADATA_TYPE", "teradata")
    monkeypatch.setattr(engine, "META_CONNECTION", "teradata")
    monkeypatch.setattr(engine, "MAX_WORKERS", 2)   # >1 to genuinely exercise the pool
    monkeypatch.setattr(engine, "PREVALIDATE_ABORT", False)

    try:
        scope_id_seed = 1   # dq_scope is empty; get_scope_id() will INSERT and get 1 back
        _seed_rules(conn, scope_id_seed)

        summary = engine.run_engine(
            project="claims_audit",
            process="monthly_review",
            run_type="MONTHLY",
            run_mode="FULL",
        )

        # -- Top-level run outcome --
        assert summary["total_rules"] == 3
        assert summary["status"] == "COMPLETED_WITH_ISSUES"
        assert summary["results"] == {
            "AMOUNT_NOT_NULL":     "PASS",
            "AMOUNT_NON_NEGATIVE": "FAIL",
            "AMOUNT_UPPER_BOUND":  "SKIP",
        }
        assert summary["data_issue_rules"]   == 1   # rule 2: FAIL
        assert summary["engine_issue_rules"] == 1   # rule 3: auto-skipped
        assert summary["issue_count"]        == 0   # no dialect/SQL-syntax/unsafe-SQL issues

        run_id = summary["run_id"]

        # -- dq_run_control: exactly one row, correctly finalised --
        rc = conn.execute(
            f"SELECT scope_id, status, run_type, run_mode FROM {META_DB}.dq_run_control WHERE run_id = ?",
            [run_id],
        ).fetchall()
        assert len(rc) == 1
        assert rc[0][0] == scope_id_seed
        assert rc[0][1] == "COMPLETED_WITH_ISSUES"
        assert rc[0][2] == "MONTHLY"
        assert rc[0][3] == "FULL"

        # -- dq_scope: created exactly once for this project/process --
        scope_rows = conn.execute(f"SELECT project_name, process_name FROM {META_DB}.dq_scope").fetchall()
        assert scope_rows == [("claims_audit", "monthly_review")]

        # -- dq_rule_execution: one row per rule, correct status/counts --
        exec_rows = conn.execute(
            f"SELECT rule_code, status, total_records, failed_records "
            f"FROM {META_DB}.dq_rule_execution WHERE run_id = ? ORDER BY rule_id",
            [run_id],
        ).fetchall()
        assert exec_rows == [
            ("AMOUNT_NOT_NULL",     "PASS", 5, 0),
            ("AMOUNT_NON_NEGATIVE", "FAIL", 5, 2),
            ("AMOUNT_UPPER_BOUND",  "SKIP", 0, 0),
        ]

        # -- dq_exceptions: the 2 negative-amount rows, and only those --
        exc_rows = conn.execute(
            f"SELECT rule_code FROM {META_DB}.dq_exceptions WHERE run_id = ?",
            [run_id],
        ).fetchall()
        assert len(exc_rows) == 2
        assert all(r[0] == "AMOUNT_NON_NEGATIVE" for r in exc_rows)

        # -- dq_metrics_summary: post-run upsert landed --
        metrics_rows = conn.execute(
            f"SELECT scope_id, run_type, total_rules, failed_rules, passed_rules "
            f"FROM {META_DB}.dq_metrics_summary WHERE scope_id = ?",
            [scope_id_seed],
        ).fetchall()
        assert len(metrics_rows) == 1
        assert metrics_rows[0] == (scope_id_seed, "MONTHLY", 3, 2, 1)

        # -- dq_rule_issues: none -- confirms the write-statement guard and
        #    dialect checks didn't misfire against legitimate rules --
        issues = conn.execute(f"SELECT COUNT(*) FROM {META_DB}.dq_rule_issues WHERE run_id = ?", [run_id]).fetchall()
        assert issues[0][0] == 0

        # -- get_scope_id() is get-or-create, not create-always: resolving
        #    the SAME project/process again must reuse the existing scope
        #    row rather than inserting a second one (a second full
        #    run_engine() call isn't used here since generate_run_id()'s
        #    second-granularity timestamp can collide within the same
        #    wall-clock second and that's an orthogonal concern from what
        #    this assertion targets). --
        from utils.db_helpers import get_scope_id
        reused_scope_id = get_scope_id(_SharedDuckDBAdapter(), "claims_audit", "monthly_review", META_DB)
        assert reused_scope_id == scope_id_seed
        scope_rows_after = conn.execute(f"SELECT scope_id FROM {META_DB}.dq_scope").fetchall()
        assert scope_rows_after == [(scope_id_seed,)]   # still exactly one scope row

    finally:
        _SharedDuckDBAdapter._shared_conn = None
        conn.close()
