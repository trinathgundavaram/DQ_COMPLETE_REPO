-- ============================================================
-- Data Quality Framework DDL  (v7 — schema normalization)
-- Schema : CMSUNIV_FILELAND_DEV_T  (DEV)
-- DB     : Teradata  (metadata store)
-- ============================================================
-- NOTE: The metadata store is always Teradata.
-- Source systems (PostgreSQL, Databricks, SQL Server, file)
-- are configured via environment variables — see
-- db/connection_factory.py and db/adapters.py for details.
-- The dq_connections table below is a catalogue / reference;
-- runtime credentials are read from env vars, NOT from this table.
-- ============================================================

-- ── New columns added in v2 ────────────────────────────────
-- dq_rules:
--   priority           INTEGER  DEFAULT 100   — lower = runs first
--   depends_on_rule_id INTEGER  DEFAULT NULL  — skip this rule if parent fails
--   require_rows       BYTEINT  DEFAULT 0     — 1 = FAIL when total_records = 0
--   threshold_operator CHAR(3)  DEFAULT 'OR'  — 'OR'|'AND' for pct+count thresholds
--   filter_sql         CLOB     DEFAULT NULL  — verbatim WHERE fragment (highest prio)
-- dq_metrics_summary:
--   UNIQUE INDEX to prevent MERGE race-condition double-insert
-- ── New in v3 ──────────────────────────────────────────────
-- dq_rules:
--   updated_at         TIMESTAMP  DEFAULT NULL  — audit trail for rule edits
-- dq_exceptions:
--   Secondary index on (run_id, rule_id) for fast exception lookups
-- dq_rule_execution:
--   Secondary index on (run_id, status) for dashboard status filtering
-- ── New in v4 ──────────────────────────────────────────────
-- dq_rules:
--   check_type   VARCHAR(50)   — built-in type (NOT_NULL, FRESHNESS, etc.)
--   check_column VARCHAR(500)  — column(s) the check applies to
--   check_params CLOB          — JSON dict of check-type parameters
-- dq_check_catalog:
--   Reference table listing all built-in check types (auto-populated)
-- ── New in v5 ──────────────────────────────────────────────
-- Rule suppression, versioning, column profiling, anomaly detection —
-- see dq_rule_suppressions / dq_rule_versions / dq_column_profile /
-- dq_profile_config / dq_anomaly_config / dq_anomaly_log below.
-- ── New in v6 ──────────────────────────────────────────────
-- SQL-dialect enforcement (dq_rules.sql_dialect), case-level disposition
-- (dq_case_dispositions), config-driven stratified sampling
-- (dq_sampling_config / dq_sample_selections), notification routing
-- (dq_notification_routes / dq_rules.business_correctable).
-- ── New in v7 (this file) ───────────────────────────────────
-- Schema normalization — removes duplicated columns in favor of keys:
--
-- 1. dq_scope(scope_id, project_name, process_name) — a single dimension
--    table. Every table that used to carry its own raw
--    project_name/process_name pair now carries a scope_id FK instead
--    (dq_rules, dq_run_control, dq_metrics_summary, dq_sampling_config).
--    Resolved via utils/db_helpers.py::get_scope_id() (get-or-create) at
--    write time, or find_scope_id() (lookup-only) at read/filter time.
--
-- 2. dq_rule_execution, dq_exceptions, dq_rule_issues, dq_column_profile,
--    dq_anomaly_log all carry run_id, and run_id already maps 1:1 to a
--    dq_run_control row that has scope_id (and, for dq_rule_execution /
--    dq_exceptions, also run_type/run_mode/batch_id/dataset_id/dates).
--    Repeating project_name/process_name/run_type/run_mode/batch_id/
--    dataset_id/start_date/end_date on every one of those child rows was
--    pure duplication with no audit-fidelity purpose (dq_run_control's
--    values are set once at run start and never change) — all of it is
--    now dropped from these five tables and read back via a JOIN to
--    dq_run_control on run_id.
--
--    dq_sample_selections carries config_id, which already maps 1:1 to a
--    dq_sampling_config row with scope_id — its own project_name/
--    process_name columns are dropped the same way.
--
--    NOT touched by this: dq_rule_execution.rule_code/severity/table_name
--    and dq_exceptions.rule_code/table_name. Those are frozen SNAPSHOTS
--    of what a mutable dq_rules row said AT EXECUTION TIME — dq_rules can
--    be edited later (severity reclassified, table renamed), and a CMS
--    audit record must show what was judged at the time, not what a live
--    join to dq_rules says today. That's a deliberate design constraint,
--    not accidental redundancy — see DESIGN.md.
--
-- 3. dq_case_dispositions (renamed dq_exception_dispositions) duplicated
--    run_id, rule_id, rule_code, project_name, process_name, and
--    primary_key_str — all of which already live on dq_exceptions.
--    exception_id, which is ITSELF immutable (never updated), so there is
--    no point-in-time-snapshot argument for repeating them here. Trimmed
--    to just the disposition fields, joined to dq_exceptions by
--    exception_id.
--
-- 4. dq_profile_config, dq_anomaly_config, dq_notification_routes are
--    DELIBERATELY NOT normalized to scope_id. These are low-row-count,
--    NULL-wildcard config tables ("NULL project_name = applies to every
--    project") matched by a most-specific-row-wins rule at read time.
--    They're not high-volume fact tables, so the duplication-avoidance
--    payoff is negligible, and collapsing project_name+process_name into
--    a single scope_id FK would still need to represent three distinct
--    wildcard levels (fully global / project-wide / exact project+
--    process), which only adds indirection to the specificity-matching
--    logic in core/metrics.py, core/reporting.py, and core/profiler.py
--    for no real benefit. Left as plain nullable columns on purpose.
-- ============================================================

-- ============================================================
-- SHARED FOUNDATION — used by both frameworks below.
--
-- This repo hosts TWO SEPARATE FRAMEWORKS against one metadata store:
--   1. The DQ RULES ENGINE       (core/)     -- dq_rules through
--      dq_anomaly_log below.
--   2. The SAMPLING FRAMEWORK    (sampling/) -- dq_sampling_config and
--      dq_sample_selections below.
-- Neither framework's Python code imports the other (sampling/ uses
-- core.executor/core.rule_sql as a plain library — see sampling/engine.py's
-- module docstring — but core/ has zero awareness sampling/ exists). The
-- two are independently deployable; dq_scope, dq_connections, and the
-- connector layer in db/ are the only things they share.
-- ============================================================

-- ── dq_scope: project/process dimension (v7) — SHARED ─────────────────────
-- Every other table that needs to say "this row belongs to project X,
-- process Y" references this table by scope_id instead of repeating the
-- two VARCHAR columns. process_name may be NULL (a scope can represent
-- "project X, no sub-process concept"). Both frameworks use this same
-- table — a sampling config and a rule set for the same project/process
-- resolve to the same scope_id.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_scope (
    scope_id      BIGINT GENERATED ALWAYS AS IDENTITY,
    project_name  VARCHAR(100) NOT NULL,
    process_name  VARCHAR(100),
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (scope_id);

CREATE UNIQUE INDEX dq_scope_lookup_uix (project_name, process_name)
ON CMSUNIV_FILELAND_DEV_T.dq_scope;


-- ============================================================
-- RULES ENGINE FRAMEWORK (core/) — dq_rules through dq_anomaly_log.
-- ============================================================

CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_rules (
    rule_id              INTEGER NOT NULL,
    rule_code            VARCHAR(200) NOT NULL,
    scope_id             BIGINT NOT NULL,                  -- v7: FK -> dq_scope
    src_tbl_nm           VARCHAR(200) NOT NULL,
    src_db_name          VARCHAR(200),
    src_schema           VARCHAR(100),
    rule_name            VARCHAR(500),
    rule_description     VARCHAR(1000),
    rule_syntax          CLOB,
    join_sql             CLOB,
    source_system        VARCHAR(50),
    filter_column        VARCHAR(100),
    filter_type          VARCHAR(20),
    filter_sql           CLOB,                            -- v2: verbatim WHERE clause
    primary_key_columns  VARCHAR(500),
    severity             VARCHAR(20),
    threshold_pct        FLOAT,
    threshold_count      INTEGER,
    threshold_operator   CHAR(3) DEFAULT 'OR',            -- v2: 'OR' | 'AND'
    require_rows         BYTEINT DEFAULT 0,               -- v2: 1 = fail on empty table
    priority             INTEGER DEFAULT 100,             -- v2: lower = runs first
    depends_on_rule_id   INTEGER,                         -- v2: skip if parent fails
    rule_group           VARCHAR(100),
    table_group          VARCHAR(100),
    active_flag          BYTEINT,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP,                           -- v3: audit trail for rule edits
    check_type           VARCHAR(50),                         -- v4: built-in check type code
    check_column         VARCHAR(500),                        -- v4: column(s) the check applies to
    check_params         CLOB,                                -- v4: JSON dict of check-type params
    sql_dialect          VARCHAR(10),                         -- v6: 'teradata'|'postgres'|'ansi'
    business_correctable BYTEINT DEFAULT 0                    -- v6: drives notification routing
)
PRIMARY INDEX (rule_id);

CREATE INDEX dq_rules_scope_ix (scope_id, active_flag)
ON CMSUNIV_FILELAND_DEV_T.dq_rules;

-- v6: constrain source_type to the 3 supported adapters (Teradata/
-- Postgres/S3 for this instance — Databricks/SqlServer adapters remain in
-- code, just not catalogued here). See db/adapters.py.
-- ALTER TABLE ... ADD CONSTRAINT dq_connections_source_type_ck
--   CHECK (source_type IN ('teradata', 'postgresql', 's3'));


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_run_control (
    run_id        VARCHAR(200) NOT NULL,
    run_seq_id    BIGINT GENERATED ALWAYS AS IDENTITY,
    scope_id      BIGINT NOT NULL,                        -- v7: FK -> dq_scope
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
PRIMARY INDEX (run_id);

CREATE INDEX dq_run_control_scope_ix (scope_id, run_type, start_time)
ON CMSUNIV_FILELAND_DEV_T.dq_run_control;


-- Connection catalogue (reference only — credentials stored in env vars).
-- SHARED: both frameworks pull their source data through these same
-- connection entries via db/connection_factory.py.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_connections (
    connection_id    INTEGER NOT NULL,
    connection_name  VARCHAR(100) NOT NULL,    -- matches DQ_CONNECTION_NAMES entry
    source_type      VARCHAR(50) NOT NULL,     -- teradata | postgresql | s3
                                               -- (databricks | sqlserver adapters exist
                                               --  in code but are uncatalogued here)
    host             VARCHAR(500),
    port             INTEGER,
    database_name    VARCHAR(200),
    description      VARCHAR(500),
    active_flag      BYTEINT DEFAULT 1,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (connection_name);


-- v7: run_type/run_mode/batch_id/dataset_id/dates/project/process are all
-- fixed once at run start and available via a JOIN to dq_run_control on
-- run_id — repeating them on every rule-execution row was pure
-- duplication (a run typically has dozens to hundreds of rules). Kept:
-- rule_code, table_name, severity — frozen snapshots of what a MUTABLE
-- dq_rules row said at execution time (see the v7 note at the top of
-- this file for why that one stays).
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
PRIMARY INDEX (run_id, rule_id);


-- v7: same reasoning as dq_rule_execution above — project/process/run_type/
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
PRIMARY INDEX (exception_id);


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_metrics_summary (
    scope_id         BIGINT,                  -- v7: FK -> dq_scope
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
PRIMARY INDEX (scope_id, run_type, run_month);

-- v2: UNIQUE INDEX prevents double-INSERT when two runs MERGE concurrently
CREATE UNIQUE INDEX dq_metrics_summary_uix
    (scope_id, run_type, batch_id, dataset_id, run_month)
ON CMSUNIV_FILELAND_DEV_T.dq_metrics_summary;

-- v3: Secondary index on dq_exceptions for fast lookup by run_id / rule_id
--     (PI is exception_id/identity — run-based queries would be full-table scans)
CREATE INDEX dq_exceptions_run_rule_ix (run_id, rule_id)
ON CMSUNIV_FILELAND_DEV_T.dq_exceptions;

-- v3: Secondary index on dq_rule_execution for dashboard status filtering
--     (e.g. WHERE run_id = ? AND status = 'FAIL')
CREATE INDEX dq_rule_execution_status_ix (run_id, status)
ON CMSUNIV_FILELAND_DEV_T.dq_rule_execution;


-- ── v4: Built-in check type reference table ──────────────────────────────────
-- Populated once via INSERT statements generated by core/check_types.py.
-- Acts as a catalogue / documentation table — not queried at runtime.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_check_catalog (
    check_type        VARCHAR(50)   NOT NULL,
    dimension         VARCHAR(50),          -- COMPLETENESS | UNIQUENESS | VALIDITY | ...
    check_level       VARCHAR(10),          -- ROW | TABLE | SCHEMA
    description       VARCHAR(500),
    required_params   VARCHAR(500),         -- comma-separated list of required param keys
    optional_params   VARCHAR(500),         -- comma-separated list of optional param keys
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (check_type);

-- Seed data for dq_check_catalog (run once after table creation)
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('NOT_NULL',               'COMPLETENESS',  'ROW',   'Column must not contain NULL values',                                           '',                                    '',                    CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('NOT_EMPTY',              'COMPLETENESS',  'ROW',   'Column must not be NULL or blank/whitespace',                                   '',                                    '',                    CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('UNIQUE',                 'UNIQUENESS',    'ROW',   'Column values must be unique within the scoped dataset',                        '',                                    '',                    CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('UNIQUE_COMBINATION',     'UNIQUENESS',    'ROW',   'Combination of check_columns must be unique (natural key)',                      '',                                    '',                    CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('REGEX_MATCH',            'VALIDITY',      'ROW',   'Column must match the given regular expression pattern',                         'pattern',                             '',                    CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('IN_LIST',                'VALIDITY',      'ROW',   'Column value must be one of the allowed values',                                'values',                              '',                    CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('NOT_IN_LIST',            'VALIDITY',      'ROW',   'Column value must NOT be any of the forbidden values',                          'values',                              '',                    CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('RANGE_CHECK',            'VALIDITY',      'ROW',   'Column must be within [min_value, max_value] (inclusive)',                      'min_value,max_value',                 '',                    CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('MIN_VALUE',              'VALIDITY',      'ROW',   'Column must be >= min_value',                                                   'min_value',                           '',                    CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('MAX_VALUE',              'VALIDITY',      'ROW',   'Column must be <= max_value',                                                   'max_value',                           '',                    CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('POSITIVE_VALUE',         'VALIDITY',      'ROW',   'Column must be > 0 (NULL also fails)',                                          '',                                    '',                    CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('NON_NEGATIVE',           'VALIDITY',      'ROW',   'Column must be >= 0 (NULL also fails)',                                         '',                                    '',                    CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('CROSS_COLUMN',           'CONSISTENCY',   'ROW',   'A SQL boolean expression across columns must be TRUE',                          'expression',                          '',                    CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('CONDITIONAL',            'CONSISTENCY',   'ROW',   'IF column A = X THEN column B must satisfy a condition',                        'if_column,if_value,then_column,then_operator', 'then_value', CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('REFERENTIAL_INTEGRITY',  'CONSISTENCY',   'ROW',   'Column value must exist in a reference table (foreign key)',                    'ref_table,ref_column',                '',                    CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('OUTLIER_CHECK',          'ACCURACY',      'ROW',   'Fails rows deviating > N standard deviations from the in-scope mean',          '',                                    'n_stddev',            CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('FRESHNESS',              'TIMELINESS',    'TABLE', 'MAX(check_column) must be within max_age_hours of current time',                'max_age_hours',                       '',                    CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('MIN_ROW_COUNT',          'VOLUME',        'TABLE', 'Row count must be >= min_rows',                                                 'min_rows',                            '',                    CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('MAX_ROW_COUNT',          'VOLUME',        'TABLE', 'Row count must be <= max_rows',                                                 'max_rows',                            '',                    CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('ROW_COUNT_RANGE',        'VOLUME',        'TABLE', 'Row count must be within [min_rows, max_rows]',                                 'min_rows,max_rows',                   '',                    CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('AGGREGATE_RANGE',        'ACCURACY',      'TABLE', 'Aggregate(check_column) must be within [min_value, max_value]',                 'aggregate',                           'min_value,max_value', CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('SUM_MATCH',              'ACCURACY',      'TABLE', 'SUM(check_column) must match SUM(ref_column) in ref_table within tolerance_pct %', 'ref_table',                       'ref_column,tolerance_pct,ref_filter', CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('COUNT_MATCH',            'CONSISTENCY',   'TABLE', 'COUNT(*) must match COUNT(*) in ref_table within tolerance_pct %',              'ref_table',                           'tolerance_pct,ref_filter', CURRENT_TIMESTAMP);
INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_check_catalog VALUES ('COLUMN_EXISTS',          'SCHEMA',        'SCHEMA','Verifies that check_column exists in the source table',                         '',                                    '',                    CURRENT_TIMESTAMP);


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
PRIMARY INDEX (run_id);


-- v7: project_name/process_name dropped — derivable via run_id -> dq_run_control.
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
PRIMARY INDEX (run_id);


-- ── v5: Rule suppression, versioning, profiling, anomaly detection ──────────

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
PRIMARY INDEX (rule_id);

CREATE INDEX dq_rule_suppressions_active_ix (rule_id, lifted_at, expires_at)
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
    check_column        VARCHAR(500),
    check_params        CLOB,
    filter_sql          CLOB,
    join_sql            CLOB,
    threshold_pct       FLOAT,
    threshold_count     INTEGER,
    threshold_operator  CHAR(3),
    severity            VARCHAR(20),
    active_flag         BYTEINT,
    changed_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    change_reason       VARCHAR(500)
)
PRIMARY INDEX (rule_id, version_num);

CREATE INDEX dq_rule_versions_code_ix (rule_code, changed_at)
ON CMSUNIV_FILELAND_DEV_T.dq_rule_versions;


-- Per-column statistical profile snapshots.
-- v7: project_name/process_name dropped — derivable via run_id -> dq_run_control.
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
PRIMARY INDEX (run_id, table_name, column_name);

CREATE INDEX dq_column_profile_table_ix (table_name, profile_date)
ON CMSUNIV_FILELAND_DEV_T.dq_column_profile;


-- Controls which tables are profiled and with what settings (opt-in).
-- Match rules: project_name + process_name + table_name.
-- NULL in project_name or process_name = wildcard (matches any).
-- Deliberately NOT normalized to scope_id — see the v7 note at the top of
-- this file (low-cardinality wildcard config table).
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_profile_config (
    config_id       INTEGER NOT NULL,
    project_name    VARCHAR(100),       -- NULL = all projects
    process_name    VARCHAR(100),       -- NULL = all processes
    table_name      VARCHAR(200) NOT NULL,  -- fully-qualified table name
    enabled         BYTEINT DEFAULT 1,
    columns_include VARCHAR(2000),      -- CSV of columns; NULL = all columns
    columns_exclude VARCHAR(2000),      -- CSV of columns to skip
    top_n_values    INTEGER DEFAULT 10,
    run_frequency   VARCHAR(20) DEFAULT 'ALWAYS',  -- ALWAYS | DAILY | WEEKLY | MANUAL
    last_profiled   TIMESTAMP,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (config_id);

CREATE INDEX dq_profile_config_lookup_ix (project_name, process_name, table_name)
ON CMSUNIV_FILELAND_DEV_T.dq_profile_config;


-- Controls anomaly-detection sensitivity per project / process / run_type.
-- NULL fields act as wildcards; most-specific matching row wins.
-- Deliberately NOT normalized to scope_id — see the v7 note at the top of
-- this file (low-cardinality wildcard config table).
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_anomaly_config (
    config_id           INTEGER NOT NULL,
    project_name        VARCHAR(100),   -- NULL = global default
    process_name        VARCHAR(100),   -- NULL = all processes in project
    run_type            VARCHAR(50),    -- NULL = all run types
    method               VARCHAR(10) DEFAULT 'ZSCORE',  -- ZSCORE | IQR | BOTH
    zscore_threshold    FLOAT DEFAULT 3.0,
    iqr_multiplier      FLOAT DEFAULT 1.5,
    min_history_runs    INTEGER DEFAULT 10, -- skip detection if < N history points
    alert_on_anomaly    BYTEINT DEFAULT 1,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (config_id);


-- Log of detected anomalies — one row per metric per run.
-- v7: project_name/process_name/run_type dropped — derivable via run_id ->
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
PRIMARY INDEX (run_id);

CREATE INDEX dq_anomaly_log_metric_ix (metric_name, created_at)
ON CMSUNIV_FILELAND_DEV_T.dq_anomaly_log;


-- ============================================================
-- v6: SQL-dialect enforcement, case-level disposition, config-driven
--     stratified sampling, and notification routing. Every table below
--     is project-agnostic — no project's vocabulary is baked into the
--     schema; see config/seed/ for one project's config.
-- ============================================================

-- ── dq_rules: sql_dialect ─────────────────────────────────────────────────
-- Every rule authored as raw negative-SQL (Section 4 of the design) MUST set
-- this.  Legacy check_type-generated rules may leave it NULL — the check_type
-- generators in core/check_types.py already emit dialect-correct SQL per
-- source_type, so no dialect-mismatch risk exists for that path.
--   Allowed values: 'teradata' | 'postgres' | 'ansi'
--   'ansi'  = confirmed portable across every supported source_type — use
--             ONLY for syntax with no dialect-specific date/window functions.
--
-- The engine (core/rule_sql.py) refuses to execute a rule whose
-- sql_dialect is incompatible with its target connection's source_type,
-- both at pre-validation time (core/engine.py::_pre_validate_rules) and
-- immediately before execution (core/executor.py::execute_rule) as a
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
-- v7: renamed from dq_case_dispositions and trimmed of run_id, rule_id,
-- rule_code, project_name, process_name, primary_key_str — every one of
-- those already lives on dq_exceptions.exception_id, which is itself
-- immutable, so there's no point-in-time-snapshot reason to repeat them
-- here (unlike dq_rule_execution/dq_exceptions denormalizing FROM the
-- MUTABLE dq_rules — this table denormalized from an already-immutable
-- row, which is pure duplication). Join to dq_exceptions for everything
-- else.
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
PRIMARY INDEX (exception_id);

CREATE INDEX dq_exception_dispositions_lookup_ix (exception_id, effective_flag)
ON CMSUNIV_FILELAND_DEV_T.dq_exception_dispositions;


-- Notification routing — decouples "who gets told what" from rule logic.
-- audience: free-text label, e.g. ROAR | BUSINESS | ENGINEERING | QA for
--   HealthSpring UM — the engine never branches on this value, see
--   core/reporting.py.
-- finding_class: DATA_VIOLATION | ENGINE_FAILURE  (never both on one route —
--   this table is exactly what prevents the two audiences from being
--   collapsed onto the same channel).
-- Deliberately NOT normalized to scope_id — see the v7 note at the top of
-- this file (low-cardinality wildcard config table).
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
PRIMARY INDEX (route_id);

CREATE INDEX dq_notification_routes_lookup_ix
    (project_name, process_name, finding_class, audience)
ON CMSUNIV_FILELAND_DEV_T.dq_notification_routes;


-- ============================================================
-- SAMPLING FRAMEWORK (sampling/) — a separate framework from the rules
-- engine above; see the note at the top of this file. dq_sampling_config
-- and dq_sample_selections are its only two tables.
-- ============================================================

-- ── Config-driven stratified sampling ─────────────────────────────────
-- Config: target mix %, exclusion rules, priority order — all JSON so a
-- different project/process can define a completely different sampling
-- scheme without touching sampling/engine.py.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_sampling_config (
    config_id            INTEGER NOT NULL,
    scope_id              BIGINT NOT NULL,        -- v7: FK -> dq_scope
    sample_name           VARCHAR(100) NOT NULL,   -- e.g. 'WEEKLY_CLINICAL_REVIEW_SAMPLE'
    connection_name       VARCHAR(100) NOT NULL,   -- which dq_connections entry to pull from
    universe_table         VARCHAR(200) NOT NULL,
    key_columns             VARCHAR(500) NOT NULL,   -- entity key column(s), CSV
    scope_column             VARCHAR(100),           -- e.g. 'pull_date' — scopes to the run's week
    target_volume            INTEGER NOT NULL DEFAULT 150,
    determination_column     VARCHAR(100),         -- e.g. 'request_disposition'
    determination_mix_json   CLOB,                  -- {"Denied":0.80,"Withdrawn":0.10,...}
    functional_area_column   VARCHAR(100),
    functional_area_mix_json CLOB,                  -- {"Part B":0.13,"Behavioral Health":0.08,...}
    exclusion_sql            CLOB,                   -- WHERE-fragment: rows matching are EXCLUDED
    priority_rank_sql        CLOB,                   -- ORDER BY expression (lowest = highest priority)
    schedule_cron            VARCHAR(50),            -- e.g. '0 8 * * FRI' — gates when this runs
    active_flag              BYTEINT DEFAULT 1,
    created_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (config_id);

-- Immutable output: every candidate case considered, scored, and whether it
-- was selected — not just the final 150. Retained 10y per Section 3.7;
-- never updated after a run completes (a re-run writes a new sample_run_id).
-- v7: project_name/process_name dropped — derivable via config_id ->
-- dq_sampling_config.scope_id.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_sample_selections (
    sample_row_id      BIGINT GENERATED ALWAYS AS IDENTITY,
    sample_run_id       VARCHAR(200) NOT NULL,   -- one per sampling execution
    config_id            INTEGER,
    sample_cycle           DATE,                  -- the pull/period this sample was drawn from
    case_key               VARCHAR(500),          -- entity key (matches key_columns)
    determination_type     VARCHAR(100),
    functional_area         VARCHAR(100),
    priority_rank             INTEGER,             -- 1 = highest priority
    excluded_flag              BYTEINT DEFAULT 0,
    exclusion_reason            VARCHAR(500),
    selected_flag                BYTEINT DEFAULT 0,  -- 1 = part of the final target-volume sample
    strata_json                   CLOB,               -- snapshot of the row's stratification attrs
    created_at                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (sample_run_id);

CREATE INDEX dq_sample_selections_lookup_ix (config_id, sample_cycle, selected_flag)
ON CMSUNIV_FILELAND_DEV_T.dq_sample_selections;


-- ============================================================
-- ALTER TABLE scripts for existing deployments
-- Run the block matching your current version once, in order.
-- ============================================================

/*  ── v1 → v2 ──────────────────────────────────────────────
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD filter_sql           CLOB;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD threshold_operator   CHAR(3) DEFAULT 'OR';
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD require_rows         BYTEINT DEFAULT 0;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD priority             INTEGER DEFAULT 100;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD depends_on_rule_id   INTEGER;

CREATE UNIQUE INDEX dq_metrics_summary_uix
    (project_name, process_name, run_type, batch_id, dataset_id, run_month)
ON CMSUNIV_FILELAND_DEV_T.dq_metrics_summary;
*/

/*  ── v2 → v3 ──────────────────────────────────────────────
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD updated_at TIMESTAMP;

CREATE INDEX dq_exceptions_run_rule_ix (run_id, rule_id)
ON CMSUNIV_FILELAND_DEV_T.dq_exceptions;

CREATE INDEX dq_rule_execution_status_ix (run_id, status)
ON CMSUNIV_FILELAND_DEV_T.dq_rule_execution;
*/

/*  ── v3 → v4 (check type system) ─────────────────────────
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD check_type   VARCHAR(50);
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD check_column VARCHAR(500);
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD check_params CLOB;

-- Create and seed the catalog table (run once) — see CREATE + INSERT
-- statements above.
*/

/*  ── v4 → v5 (suppression, versioning, profiling, anomaly) ───
-- New tables (see CREATE statements above):
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_suppressions ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_versions ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_column_profile ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_profile_config ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_anomaly_config ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_anomaly_log ...
-- No changes to existing tables in v5.
*/

/*  ── v5 → v6 (dialect enforcement, disposition, routing, sampling) ──────
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD sql_dialect VARCHAR(10);
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD business_correctable BYTEINT DEFAULT 0;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_connections
  ADD CONSTRAINT dq_connections_source_type_ck
  CHECK (source_type IN ('teradata', 'postgresql', 's3'));

-- New tables (see CREATE statements above):
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_case_dispositions ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_notification_routes ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_sampling_config ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_sample_selections ...
*/

-- ── v6 → v7 (schema normalization: dq_scope + trimmed columns) ─────────
-- Runnable migration script: migrations/v6_to_v7.sql (not illustrative
-- comments like the blocks above -- this one is meant to be executed,
-- phase by phase, against a real v6 deployment). See that file for the
-- full backfill/ALTER/DROP sequence, verification queries between
-- phases, and pre-migration backup guidance.
