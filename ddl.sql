-- ============================================================
-- Data Quality Framework DDL  (v3 — functional improvements)
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
-- ============================================================

CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_rules (
    rule_id              INTEGER NOT NULL,
    rule_code            VARCHAR(200) NOT NULL,
    project_name         VARCHAR(100) NOT NULL,
    process_name         VARCHAR(100),
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
    check_params         CLOB                                 -- v4: JSON dict of check-type params
)
PRIMARY INDEX (rule_id);


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_run_control (
    run_id        VARCHAR(200) NOT NULL,
    run_seq_id    BIGINT GENERATED ALWAYS AS IDENTITY,
    project_name  VARCHAR(100),
    process_name  VARCHAR(100),
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


-- Connection catalogue (reference only — credentials stored in env vars)
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_connections (
    connection_id    INTEGER NOT NULL,
    connection_name  VARCHAR(100) NOT NULL,    -- matches DQ_CONNECTION_NAMES entry
    source_type      VARCHAR(50) NOT NULL,     -- teradata | postgresql | aurora |
                                               -- databricks | sqlserver | file
    host             VARCHAR(500),
    port             INTEGER,
    database_name    VARCHAR(200),
    description      VARCHAR(500),
    active_flag      BYTEINT DEFAULT 1,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (connection_name);


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_execution (
    run_id          VARCHAR(200),
    rule_id         INTEGER,
    rule_code       VARCHAR(200),
    project_name    VARCHAR(100),
    process_name    VARCHAR(100),
    table_name      VARCHAR(200),
    run_type        VARCHAR(50),
    run_mode        VARCHAR(20),
    batch_id        VARCHAR(100),
    dataset_id      VARCHAR(200),
    start_date      DATE,
    end_date        DATE,
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


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_exceptions (
    exception_id     BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id           VARCHAR(200),
    rule_id          INTEGER,
    rule_code        VARCHAR(200),
    project_name     VARCHAR(100),
    process_name     VARCHAR(100),
    table_name       VARCHAR(200),
    run_type         VARCHAR(50),
    run_mode         VARCHAR(20),
    batch_id         VARCHAR(100),
    dataset_id       VARCHAR(200),
    key_json         CLOB,
    primary_key_str  VARCHAR(500),
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (exception_id);


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_metrics_summary (
    project_name     VARCHAR(100),
    process_name     VARCHAR(100),
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
PRIMARY INDEX (project_name, process_name, run_type, run_month);

-- v2: UNIQUE INDEX prevents double-INSERT when two runs MERGE concurrently
CREATE UNIQUE INDEX dq_metrics_summary_uix
    (project_name, process_name, run_type, batch_id, dataset_id, run_month)
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


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_issues (
    issue_id      BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id        VARCHAR(200),
    rule_id       INTEGER,
    rule_code     VARCHAR(200),
    project_name  VARCHAR(100),
    process_name  VARCHAR(100),
    table_name    VARCHAR(200),
    issue_type    VARCHAR(50),
    issue_message CLOB,
    error_detail  CLOB,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (run_id);


-- ============================================================
-- ALTER TABLE scripts for existing deployments (v1 → v2)
-- Run these once against DEV/QA/UAT/PROD schemas.
-- ============================================================

-- ============================================================
-- ALTER TABLE scripts for existing deployments
-- v1 → v2: run these once to add the new columns / indexes
-- v2 → v3: run the v3 block below
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

-- Create and seed the catalog table (run once)
-- CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_check_catalog ...
-- (see full DDL above for CREATE + INSERT statements)
*/

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
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_column_profile (
    profile_id      BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id          VARCHAR(200),
    project_name    VARCHAR(100),
    process_name    VARCHAR(100),
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

CREATE INDEX dq_column_profile_table_ix (project_name, process_name, table_name, profile_date)
ON CMSUNIV_FILELAND_DEV_T.dq_column_profile;


-- Controls which tables are profiled and with what settings (opt-in).
-- Match rules: project_name + process_name + table_name.
-- NULL in project_name or process_name = wildcard (matches any).
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
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_anomaly_config (
    config_id           INTEGER NOT NULL,
    project_name        VARCHAR(100),   -- NULL = global default
    process_name        VARCHAR(100),   -- NULL = all processes in project
    run_type            VARCHAR(50),    -- NULL = all run types
    method              VARCHAR(10) DEFAULT 'ZSCORE',  -- ZSCORE | IQR | BOTH
    zscore_threshold    FLOAT DEFAULT 3.0,
    iqr_multiplier      FLOAT DEFAULT 1.5,
    min_history_runs    INTEGER DEFAULT 10, -- skip detection if < N history points
    alert_on_anomaly    BYTEINT DEFAULT 1,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (config_id);


-- Log of detected anomalies — one row per metric per run.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_anomaly_log (
    anomaly_id          BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id              VARCHAR(200),
    project_name        VARCHAR(100),
    process_name        VARCHAR(100),
    run_type            VARCHAR(50),
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

CREATE INDEX dq_anomaly_log_project_ix (project_name, process_name, run_type, created_at)
ON CMSUNIV_FILELAND_DEV_T.dq_anomaly_log;


/*  ── v4 → v5 (suppression, versioning, profiling, anomaly) ───
-- Run once per environment schema after applying v4 migration.

-- New tables (see CREATE statements above):
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_suppressions ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_versions ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_column_profile ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_profile_config ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_anomaly_config ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_anomaly_log ...

-- No changes to existing tables in v5.
*/


-- ============================================================
-- v6: SQL-dialect enforcement, case-level disposition, config-driven
--     stratified sampling, and notification routing. Every table below
--     is project-agnostic and keyed by project_name/process_name like
--     everything else in this schema — no project's vocabulary is
--     baked into the schema; see config/seed/ for one project's config.
-- ============================================================

-- ── dq_rules: sql_dialect ─────────────────────────────────────────────────
-- Every rule authored as raw negative-SQL (Section 4 of the design) MUST set
-- this.  Legacy check_type-generated rules may leave it NULL — the check_type
-- generators in core/check_types.py already emit dialect-correct SQL per
-- source_type, so no dialect-mismatch risk exists for that path.
--
-- ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD sql_dialect VARCHAR(10);
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

-- ── dq_connections: constrain source_type to the 3 supported adapters ─────
-- ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_connections
--   ADD CONSTRAINT dq_connections_source_type_ck
--   CHECK (source_type IN ('teradata', 'postgresql', 's3'));
--
-- NOTE: the connector layer (db/adapters.py) remains pluggable — Databricks
-- and SQL Server adapters still exist in code — but only these three are
-- catalogued/tested for this engine instance. Adding a 4th source later is
-- a new adapter file + one new _TYPE_MAP entry; no engine-core change.


-- Case-level disposition — layered ON TOP of an immutable dq_exceptions row.
-- A finding is NEVER updated or deleted. Waiving/resolving/dismissing a case
-- inserts a NEW disposition row; the most recent (effective_flag=1) row per
-- exception_id is the current state. Joined at read time by the dashboard
-- and the static audit report — dq_exceptions itself never changes.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_case_dispositions (
    disposition_id     BIGINT GENERATED ALWAYS AS IDENTITY,
    exception_id        BIGINT NOT NULL,        -- FK -> dq_exceptions.exception_id
    run_id               VARCHAR(200),
    rule_id              INTEGER,
    rule_code            VARCHAR(200),
    project_name         VARCHAR(100),
    process_name         VARCHAR(100),
    primary_key_str      VARCHAR(500),           -- denormalised for fast lookup
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

CREATE INDEX dq_case_dispositions_lookup_ix (exception_id, effective_flag)
ON CMSUNIV_FILELAND_DEV_T.dq_case_dispositions;


-- Notification routing — decouples "who gets told what" from rule logic.
-- audience: ROAR | BUSINESS | ENGINEERING | QA
-- finding_class: DATA_VIOLATION | ENGINE_FAILURE  (never both on one route —
--   this table is exactly what prevents the two audiences from being
--   collapsed onto the same channel).
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_notification_routes (
    route_id          INTEGER NOT NULL,
    project_name      VARCHAR(100),     -- NULL = applies to all projects
    process_name      VARCHAR(100),     -- NULL = applies to all processes
    finding_class     VARCHAR(20) NOT NULL,   -- DATA_VIOLATION | ENGINE_FAILURE
    audience          VARCHAR(20) NOT NULL,   -- ROAR | BUSINESS | ENGINEERING | QA
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

-- dq_rules: business_correctable — is this finding type something the
-- BUSINESS audience can act on directly (vs. purely a compliance/audit
-- record for ROAR)?  Drives the notification split in Section 3.5.
-- ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD business_correctable BYTEINT DEFAULT 0;


-- ── Config-driven stratified sampling ─────────────────────────────────
-- Config: target mix %, exclusion rules, priority order — all JSON so a
-- different project/process can define a completely different sampling
-- scheme without touching core/stratified_sampling.py.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_sampling_config (
    config_id            INTEGER NOT NULL,
    project_name         VARCHAR(100) NOT NULL,
    process_name         VARCHAR(100) NOT NULL,
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
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_sample_selections (
    sample_row_id      BIGINT GENERATED ALWAYS AS IDENTITY,
    sample_run_id       VARCHAR(200) NOT NULL,   -- one per sampling execution
    config_id            INTEGER,
    project_name         VARCHAR(100),
    process_name         VARCHAR(100),
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

CREATE INDEX dq_sample_selections_lookup_ix (project_name, process_name, sample_cycle, selected_flag)
ON CMSUNIV_FILELAND_DEV_T.dq_sample_selections;


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
