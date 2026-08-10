-- ============================================================
-- Data Quality Framework DDL — RULES ENGINE (rules_engine/)
-- Schema : CMSUNIV_FILELAND_DEV_T  (DEV)
-- DB     : Teradata  (metadata store)
-- ============================================================
-- Run ddl_shared.sql FIRST — every table below references dq_scope
-- (project/process dimension) defined there.
--
-- Design rationale (why scope_id was introduced, why certain columns are
-- frozen execution-time snapshots instead of live joins, etc.) lives in
-- ddl_shared.sql's header — this file holds the CREATE TABLE statements
-- for the rules engine's own tables: dq_rules through dq_notification_routes.
-- ============================================================

-- ============================================================
-- RULES ENGINE FRAMEWORK (rules_engine/) — dq_rules through dq_anomaly_log.
-- ============================================================


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_rules (
    rule_id              INTEGER NOT NULL,
    rule_code            VARCHAR(200) NOT NULL,
    scope_id             BIGINT NOT NULL,                  -- FK -> dq_scope
    src_tbl_nm           VARCHAR(200) NOT NULL,
    src_db_name          VARCHAR(200),
    src_schema           VARCHAR(100),
    rule_name            VARCHAR(500),
    rule_description     VARCHAR(1000),
    rule_syntax          CLOB,
    source_system        VARCHAR(50),
    filter_column        VARCHAR(100),
    filter_type          VARCHAR(20),
    filter_sql           CLOB,                            -- verbatim WHERE clause
    primary_key_columns  VARCHAR(500),
    severity             VARCHAR(20),
    threshold_pct        FLOAT,
    threshold_count      INTEGER,
    threshold_operator   CHAR(3) DEFAULT 'OR',            -- 'OR' | 'AND'
    require_rows         BYTEINT DEFAULT 0,               -- 1 = fail on empty table
    priority             INTEGER DEFAULT 100,             -- lower = runs first
    depends_on_rule_id   INTEGER,                         -- skip if parent fails
    rule_group           VARCHAR(100),
    table_group          VARCHAR(100),
    active_flag          BYTEINT,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP,                           -- audit trail for rule edits
    check_type           VARCHAR(50),                         -- free-text classification tag (dashboard grouping only — not consumed by the engine)
    sql_dialect          VARCHAR(10) NOT NULL,                -- 'teradata'|'postgres'|'ansi' — every rule is raw negative-SQL
    business_correctable BYTEINT DEFAULT 0                    -- drives notification routing
)
UNIQUE PRIMARY INDEX (rule_id);

-- No secondary index: rules_engine/engine.py's rule-load query (WHERE
-- scope_id = ? AND active_flag = 1) is the only read pattern here, but
-- dq_rules' row count tracks how many rules a team has WRITTEN, not data
-- volume -- even with dozens of projects onboarded this stays in the
-- tens-to-low-hundreds of rows. An all-AMP scan of a table that size is
-- effectively free; a secondary index would only add write-side subtable
-- maintenance on every rule insert/update with no measurable read benefit.


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_run_control (
    run_id        VARCHAR(200) NOT NULL,
    run_seq_id    BIGINT GENERATED ALWAYS AS IDENTITY,
    scope_id      BIGINT NOT NULL,                        -- FK -> dq_scope
    run_type      VARCHAR(50),
    run_mode      VARCHAR(20),
    batch_id      VARCHAR(100),
    dataset_id    VARCHAR(200),
    start_date    DATE,
    end_date      DATE,
    triggered_by  VARCHAR(100),
    start_time    TIMESTAMP,
    end_time      TIMESTAMP,
    status        VARCHAR(20),
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
UNIQUE PRIMARY INDEX (run_id);

-- No secondary index here: nothing in the codebase filters dq_run_control
-- by scope_id/run_type/start_time. Every real access is either
-- WHERE run_id = ? (the PI, direct single-AMP retrieve) or
-- WHERE status = 'RUNNING' AND start_time < ... (stale-run cleanup, a
-- once-per-engine-startup scan of a small table -- not worth an index).



-- run_type/run_mode/batch_id/dataset_id/dates/project/process are all
-- fixed once at run start and available via a JOIN to dq_run_control on
-- run_id — repeating them on every rule-execution row was pure
-- duplication (a run typically has dozens to hundreds of rules). Kept:
-- rule_code, table_name, severity — frozen snapshots of what a MUTABLE
-- dq_rules row said at execution time (see ddl_shared.sql's header for
-- why that one stays).
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_execution (
    run_id          VARCHAR(200),
    rule_id         INTEGER,
    rule_code       VARCHAR(200),
    table_name      VARCHAR(200),
    total_records   BIGINT,
    failed_records  BIGINT,
    passed_records  BIGINT,
    failure_pct     FLOAT,
    pass_pct        FLOAT,
    severity        VARCHAR(20),
    status          VARCHAR(20),
    execution_time  FLOAT,
    run_timestamp   TIMESTAMP,
    run_date        DATE,
    run_month       DATE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
UNIQUE PRIMARY INDEX (run_id, rule_id);   -- one row per rule per run


-- same reasoning as dq_rule_execution above — project/process/run_type/
-- run_mode/batch_id/dataset_id dropped, derivable via run_id -> dq_run_control.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_exceptions (
    exception_id     BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id           VARCHAR(200),
    rule_id          INTEGER,
    rule_code        VARCHAR(200),
    table_name       VARCHAR(200),
    key_json         CLOB,
    primary_key_str  VARCHAR(500),
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
UNIQUE PRIMARY INDEX (exception_id);


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_metrics_summary (
    scope_id         BIGINT,                  -- FK -> dq_scope
    run_type         VARCHAR(50),
    batch_id         VARCHAR(100),
    dataset_id       VARCHAR(200),
    run_month        DATE,
    total_runs       INTEGER,
    total_rules      INTEGER,
    failed_rules     INTEGER,
    passed_rules     INTEGER,
    total_records    BIGINT,
    failed_records   BIGINT,
    avg_failure_pct  FLOAT,
    dq_score         FLOAT,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (scope_id, run_type, run_month);   -- NUPI on purpose: chosen for
                                                   -- AMP-distribution/grouping,
                                                   -- not identity -- the real
                                                   -- uniqueness key is wider
                                                   -- (see the USI right below)

-- UNIQUE INDEX prevents double-INSERT when two runs MERGE concurrently
CREATE UNIQUE INDEX dq_metrics_summary_uix
    (scope_id, run_type, batch_id, dataset_id, run_month)
ON CMSUNIV_FILELAND_DEV_T.dq_metrics_summary;

-- Secondary index on dq_exceptions for fast lookup by run_id / rule_id.
-- Kept after re-evaluating every secondary index against this project's
-- real scale: PI is exception_id (a random identity, zero correlation
-- with run_id), and unlike the other rules-engine tables dq_exceptions
-- is the one that genuinely grows -- RETENTION.md estimates tens of
-- thousands to low hundreds of thousands of rows over a multi-year
-- retention window. rules_engine/reporting.py's "WHERE run_id = ?" read
-- (and its JOIN to dq_rules on rule_id) is an interactive dashboard path
-- -- scanning that whole table on every page load is a real, avoidable
-- cost this index removes.
CREATE INDEX dq_exceptions_run_rule_ix (run_id, rule_id)
ON CMSUNIV_FILELAND_DEV_T.dq_exceptions;

-- No secondary index: dq_rule_execution writes ~one row per rule per run
-- (~40 rows/week for a typical project -- tens of thousands of rows even
-- after years of weekly runs, further pruned by the monthly partitioning
-- below per RETENTION.md). Every real read already filters WHERE run_id = ?
-- (rules_engine/reporting.py, rules_engine/metrics.py,
-- dashboard/streamlit_app.py) -- run_id is also this table's PI's leading
-- column, and no query anywhere filters on status alone. At this table's
-- realistic size an all-AMP scan is negligible; a status-inclusive
-- secondary index would only add write overhead on every execution row
-- with no read pattern that needs it.


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_run_logs (
    log_id        BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id        VARCHAR(200),
    rule_id       INTEGER,
    rule_code     VARCHAR(200),
    log_level     VARCHAR(20),
    message       CLOB,
    error_code    VARCHAR(50),
    error_detail  CLOB,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (run_id);   -- NUPI on purpose: many log lines per run.
                          -- log_id (identity) is the real per-row identifier.


-- project_name/process_name dropped — derivable via run_id -> dq_run_control.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_issues (
    issue_id      BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id        VARCHAR(200),
    rule_id       INTEGER,
    rule_code     VARCHAR(200),
    table_name    VARCHAR(200),
    issue_type    VARCHAR(50),
    issue_message CLOB,
    error_detail  CLOB,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (run_id);   -- NUPI on purpose: many issues per run.
                          -- issue_id (identity) is the real per-row identifier.


-- ── Rule suppression, versioning, profiling, anomaly detection ─────────────

-- Temporarily suppress a known-failing rule without touching its definition.
-- A suppression is active when lifted_at IS NULL AND (expires_at IS NULL OR > NOW).
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_suppressions (
    suppression_id  INTEGER NOT NULL,
    rule_id         INTEGER NOT NULL,
    rule_code       VARCHAR(200),
    reason          VARCHAR(1000),          -- e.g. "upstream incident TICKET-1234"
    suppressed_by   VARCHAR(100),           -- username / service account
    suppressed_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at      TIMESTAMP,              -- NULL = no automatic expiry
    lifted_at       TIMESTAMP,              -- NULL = still active
    lifted_by       VARCHAR(100),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (rule_id);      -- NUPI on purpose: many suppressions per rule
                               -- over time; rule_id lookups (is_suppressed())
                               -- already get single-AMP access from this PI,
                               -- so no secondary index needed for that path.

-- suppression_id is the natural key ops uses to lift a specific
-- suppression (see rule_lifecycle.py's docstring: "WHERE suppression_id
-- = ?") but it isn't the PI (rule_id was chosen instead, for locality
-- with is_suppressed()'s per-rule lookups) -- this is the only thing
-- enforcing it can't collide.
CREATE UNIQUE INDEX dq_rule_suppressions_id_uix (suppression_id)
ON CMSUNIV_FILELAND_DEV_T.dq_rule_suppressions;


-- Snapshots of dq_rules fields that matter for forensic analysis.
-- A new version row is written automatically whenever a tracked field changes.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_versions (
    version_id          BIGINT GENERATED ALWAYS AS IDENTITY,
    rule_id             INTEGER,
    rule_code           VARCHAR(200),
    version_num         INTEGER,            -- auto-incremented per rule
    change_type         VARCHAR(20),        -- CREATED | MODIFIED
    rule_syntax         CLOB,
    check_type          VARCHAR(50),
    filter_sql          CLOB,
    threshold_pct       FLOAT,
    threshold_count     INTEGER,
    threshold_operator  CHAR(3),
    severity            VARCHAR(20),
    active_flag         BYTEINT,
    changed_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    change_reason       VARCHAR(500)
)
UNIQUE PRIMARY INDEX (rule_id, version_num);   -- version_num never repeats per rule

-- No secondary index: every read in rules_engine/rule_lifecycle.py
-- filters WHERE rule_id = ? (get_version_at_run, _write_snapshot),
-- never rule_code -- the PI's leading column already serves those.


-- Per-column statistical profile snapshots.
-- project_name/process_name dropped — derivable via run_id -> dq_run_control.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_column_profile (
    profile_id      BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id          VARCHAR(200),
    table_name      VARCHAR(200),
    column_name     VARCHAR(200),
    total_rows      BIGINT,
    null_count      BIGINT,
    null_pct        FLOAT,
    distinct_count  BIGINT,
    distinct_pct    FLOAT,
    min_value       VARCHAR(500),
    max_value       VARCHAR(500),
    mean_value      FLOAT,
    stddev_value    FLOAT,
    top_values      CLOB,               -- JSON: [{"value":"X","count":N}, ...]
    profile_date    DATE,
    source_type     VARCHAR(50),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
UNIQUE PRIMARY INDEX (run_id, table_name, column_name);   -- one profile row
                                                            -- per column per
                                                            -- table per run

-- No secondary index: dq_column_profile is currently write-only (nothing
-- in rules_engine/ or dashboard/ reads it back yet) -- an index with no
-- query to serve is pure overhead. Add one, matched to the real WHERE
-- clause, if/when a reader is built.


-- Controls which tables are profiled and with what settings (opt-in).
-- Match rules: project_name + process_name + table_name.
-- NULL in project_name or process_name = wildcard (matches any).
-- Deliberately NOT normalized to scope_id — see ddl_shared.sql's header
-- (low-cardinality wildcard config table).
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_profile_config (
    config_id       INTEGER NOT NULL,
    project_name    VARCHAR(100),       -- NULL = all projects
    process_name    VARCHAR(100),       -- NULL = all processes
    table_name      VARCHAR(200) NOT NULL,  -- fully-qualified table name
    active          BYTEINT DEFAULT 1,      -- 'enabled' is a Teradata reserved word
    columns_include VARCHAR(2000),      -- CSV of columns; NULL = all columns
    columns_exclude VARCHAR(2000),      -- CSV of columns to skip
    top_n_values    INTEGER DEFAULT 10,
    run_frequency   VARCHAR(20) DEFAULT 'ALWAYS',  -- ALWAYS | DAILY | WEEKLY | MANUAL
    last_profiled   TIMESTAMP,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
UNIQUE PRIMARY INDEX (config_id);

-- No secondary index: rules_engine/profiler.py's only read is
-- "WHERE active = 1 AND (project_name IS NULL OR project_name = ?)
-- AND (process_name IS NULL OR process_name = ?)" -- an OR-NULL wildcard
-- match a standard index can't serve well, against a low-row-count
-- table (same reasoning as dq_anomaly_config below, which also has none).


-- Controls anomaly-detection sensitivity per project / process / run_type.
-- NULL fields act as wildcards; most-specific matching row wins.
-- Deliberately NOT normalized to scope_id — see ddl_shared.sql's header
-- (low-cardinality wildcard config table).
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_anomaly_config (
    config_id           INTEGER NOT NULL,
    project_name        VARCHAR(100),   -- NULL = global default
    process_name        VARCHAR(100),   -- NULL = all processes in project
    run_type            VARCHAR(50),    -- NULL = all run types
    process             VARCHAR(10) DEFAULT 'ZSCORE',  -- detection algorithm: ZSCORE | IQR | BOTH
                                                          -- ('method' is a Teradata reserved word)
    zscore_threshold    FLOAT DEFAULT 3.0,
    iqr_multiplier      FLOAT DEFAULT 1.5,
    min_history_runs    INTEGER DEFAULT 10, -- skip detection if < N history points
    alert_on_anomaly    BYTEINT DEFAULT 1,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
UNIQUE PRIMARY INDEX (config_id);


-- Log of detected anomalies — one row per metric per run.
-- project_name/process_name/run_type dropped — derivable via run_id ->
-- dq_run_control.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_anomaly_log (
    anomaly_id          BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id              VARCHAR(200),
    metric_name         VARCHAR(100),   -- dq_score | avg_failure_pct | failed_rule_pct
    current_value       FLOAT,
    historical_mean     FLOAT,
    historical_std      FLOAT,
    z_score             FLOAT,
    iqr_lower_bound     FLOAT,
    iqr_upper_bound     FLOAT,
    is_anomaly          BYTEINT,        -- 1 = detected anomaly
    detection_method    VARCHAR(10),    -- ZSCORE | IQR
    severity            VARCHAR(20),    -- INFO | LOW | MEDIUM | HIGH | CRITICAL
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
UNIQUE PRIMARY INDEX (run_id, metric_name, detection_method);
    -- Real grain is finer than the table's header comment suggests:
    -- rules_engine/metrics.py's detect_and_log() calls evaluate_metric_drift()
    -- once per metric with method="BOTH" by default (see dq_anomaly_config
    -- seed data), which can return one ZSCORE row AND one IQR row for the
    -- SAME metric_name in the SAME run -- (run_id, metric_name) alone would
    -- collide on the second INSERT. (Was just run_id, non-unique -- didn't
    -- enforce any of this.)

-- No secondary index: dq_anomaly_log is currently write-only (nothing
-- reads it back yet) -- an index with no query to serve is pure overhead.


-- ============================================================
-- SQL-dialect enforcement, case-level disposition, config-driven
-- stratified sampling, and notification routing. Every table below
-- is project-agnostic — no project's vocabulary is baked into the
-- schema; see config/seed/ for one project's config.
-- ============================================================

-- ── dq_rules: sql_dialect ─────────────────────────────────────────────────
-- Required on every rule (NOT NULL) — every rule is a complete, self-contained
-- negative-SQL SELECT (see rules_engine/rule_sql.py's module docstring), so the
-- engine always needs to know which dialect that SQL is written in.
--   Allowed values: 'teradata' | 'postgres' | 'ansi'
--   'ansi'  = confirmed portable across every supported source_type — use
--             ONLY for syntax with no dialect-specific date/window functions.
--
-- The engine (rules_engine/rule_sql.py) refuses to execute a rule whose
-- sql_dialect is incompatible with its target connection's source_type,
-- both at pre-validation time (rules_engine/engine.py::_pre_validate_rules) and
-- immediately before execution (rules_engine/executor.py::execute_rule) as a
-- defense-in-depth guard. A mismatch is logged to dq_rule_issues with
-- issue_type='DIALECT_MISMATCH' and the rule is recorded as status='ERROR'
-- in dq_rule_execution — it NEVER writes to dq_exceptions, and it NEVER
-- looks like a clean PASS.

-- Case-level disposition — layered ON TOP of an immutable dq_exceptions row.
-- A finding is NEVER updated or deleted. Waiving/resolving/dismissing a case
-- inserts a NEW disposition row; the most recent (effective_flag=1) row per
-- exception_id is the current state. Joined at read time by the dashboard
-- and the static audit report — dq_exceptions itself never changes.
--
-- Trimmed of run_id, rule_id, rule_code, project_name, process_name,
-- primary_key_str — every one of those already lives on
-- dq_exceptions.exception_id, which is itself immutable, so there's no
-- point-in-time-snapshot reason to repeat them here (unlike
-- dq_rule_execution/dq_exceptions denormalizing FROM the MUTABLE dq_rules
-- — this table denormalizes from an already-immutable row, which is pure
-- duplication). Join to dq_exceptions for everything else.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_exception_dispositions (
    disposition_id      BIGINT GENERATED ALWAYS AS IDENTITY,
    exception_id         BIGINT NOT NULL,        -- FK -> dq_exceptions.exception_id
    disposition_type     VARCHAR(30),            -- WAIVED | RESOLVED | FALSE_POSITIVE |
                                                  -- CORRECTED | UNDER_REVIEW | REOPENED
    disposition_reason   VARCHAR(1000),
    disposed_by          VARCHAR(100),
    disposed_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_flag       BYTEINT DEFAULT 1,      -- 1 = current state; superseded rows
                                                  -- get a NEW row with effective_flag=1
                                                  -- and the prior row's effective_flag
                                                  -- is set to 0 in the SAME transaction
                                                  -- (still never UPDATEs the finding itself)
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (exception_id);   -- NUPI on purpose: additive-only, a new row
                                 -- per disposition change shares exception_id
                                 -- with every prior one (see comment above)

-- No secondary index: dashboard/streamlit_app.py's only read is
-- "WHERE effective_flag = 1" inside a derived table, joined to
-- dq_exceptions afterward by exception_id -- exception_id (this table's
-- PI) never appears as a predicate in that query, so an index led by it
-- wouldn't help. effective_flag alone is too low-cardinality to be a
-- useful index key.


-- Notification routing — decouples "who gets told what" from rule logic.
-- audience: free-text label, e.g. ROAR | BUSINESS | ENGINEERING | QA for
--   HealthSpring UM — the engine never branches on this value, see
--   rules_engine/reporting.py.
-- finding_class: DATA_VIOLATION | ENGINE_FAILURE  (never both on one route —
--   this table is exactly what prevents the two audiences from being
--   collapsed onto the same channel).
-- Deliberately NOT normalized to scope_id — see ddl_shared.sql's header
-- (low-cardinality wildcard config table).
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_notification_routes (
    route_id          INTEGER NOT NULL,
    project_name      VARCHAR(100),     -- NULL = applies to all projects
    process_name      VARCHAR(100),     -- NULL = applies to all processes
    finding_class     VARCHAR(20) NOT NULL,   -- DATA_VIOLATION | ENGINE_FAILURE
    audience          VARCHAR(20) NOT NULL,
    channel_type      VARCHAR(20) NOT NULL,   -- EMAIL | TEAMS
    destination       VARCHAR(1000) NOT NULL, -- webhook URL or comma-sep emails
    business_correctable_only BYTEINT DEFAULT 0, -- 1 = only send rows where
                                                  -- dq_rules.business_correctable = 1
    active_flag       BYTEINT DEFAULT 1,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
UNIQUE PRIMARY INDEX (route_id);

-- No secondary index: rules_engine/reporting.py's only read is
-- "WHERE finding_class = ? AND active_flag = 1 AND (project_name IS NULL
-- OR project_name = ?) AND (process_name IS NULL OR process_name = ?)" --
-- audience is SELECTed, never filtered -- against a low-row-count table
-- (same reasoning as dq_anomaly_config and dq_profile_config above).
