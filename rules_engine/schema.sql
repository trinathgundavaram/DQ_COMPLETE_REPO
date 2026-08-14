-- ============================================================
-- Generic Rules Engine (GRE) DDL
-- Schema : CMSUNIV_FILELAND_DEV_T  (DEV)  -- same metadata store as dq_*,
--          reached at runtime via the same "teradata" connection name and
--          shared/config.py's get_meta_db() resolution (GRE_META_DB
--          overrides it independently if these tables ever need to move to
--          their own schema without a code change).
-- DB     : Teradata  (metadata store)
-- ============================================================
-- Deploy AFTER shared/schema.sql -- gre_log/gre_exceptions/gre_errors below
-- reference run_id values that come from gre_audit (shared/schema.sql),
-- and gre_exceptions/gre_results are written to by rules_engine/executor.py
-- via shared/db_ops.py's writers.
--
-- Standalone from ddl.sql on purpose: this file creates ONLY gre_*-prefixed
-- objects specific to rule evaluation. It never touches, renames, or
-- alters a single dq_* table, index, or column, and it does not create
-- gre_audit/gre_errors (those are shared with sampling/ -- see
-- shared/schema.sql) or any gre_sampling_*/gre_sample_* table (see
-- sampling/schema.sql).
--
-- Design notes (see rules_engine/config usage via shared/config.py,
-- rules_engine/executor.py docstrings for the code that relies on these
-- shapes):
--   * rule_sql / scope_sql may embed a literal "{batch_id}" token. The
--     engine string-substitutes it (quoted, escaped) before running the
--     query -- see shared/db_ops.py::_substitute_batch_id(). There is no
--     filter_column/filter_sql system like dq_rules has; SQL-authoring
--     rules are expected to be fully self-contained.
--   * source_connection names a connection already configured via
--     DQ_CONNECTION_NAMES / db/connection_factory.py -- the SAME connector
--     layer dq_* uses, imported directly, not reimplemented.
--   * natural_key_columns is this engine's analog of dq_rules'
--     primary_key_columns: a comma-separated list of column names present
--     in the rule's own SELECT output, used to build a deterministic
--     natural_key_value for each violating row so reruns are idempotent
--     (UNIQUE INDEX below), mirroring the dq_metrics_summary_uix pattern:
--     catch the duplicate-key error and skip/update rather than
--     delete-then-insert, which leaves a crash-mid-delete window.
--   * batch_id_column is only consulted when scope_sql is NULL: the
--     engine's default total-record count becomes
--       SELECT COUNT(*) AS total_count FROM {table_name}
--       WHERE {batch_id_column} = '{batch_id}'
--     defaulting batch_id_column itself to 'batch_id' when not set.
-- ============================================================


-- ── 1. gre_rules -- one row per rule ──────────────────────────────────────
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_rules (
    rule_id              INTEGER NOT NULL,
    rule_name            VARCHAR(500) NOT NULL,
    table_name           VARCHAR(200) NOT NULL,
    source_connection    VARCHAR(100) NOT NULL,   -- named connection, see db/connection_factory.py
    sql_dialect          VARCHAR(20)  NOT NULL,   -- 'teradata' | 'postgres' | 'ansi'
    rule_sql             CLOB NOT NULL,            -- the negative SELECT; never mutates data
    scope_sql            CLOB,                     -- optional override for the total-record count
    batch_id_column      VARCHAR(100) DEFAULT 'batch_id',  -- used only when scope_sql IS NULL
    rule_group           VARCHAR(100) NOT NULL,    -- groups rules for one use case / table pipeline
    seq_no               INTEGER DEFAULT 100,      -- run order within a group (sequential mode only)
    sequencing_mode      VARCHAR(20) DEFAULT 'independent',  -- 'independent' | 'sequential'
    on_failure           VARCHAR(20) DEFAULT 'skip_and_continue',  -- 'halt_group' | 'skip_and_continue'
                                                    -- meaningful only when sequencing_mode='sequential'
    threshold_pct        FLOAT,                    -- % of in-scope records that must fail to breach
    threshold_count      INTEGER,                  -- raw count of failed records that must be exceeded
    threshold_operator   CHAR(3) DEFAULT 'OR',      -- 'OR' | 'AND' -- only relevant if both are set
    severity             VARCHAR(50) DEFAULT 'Data Validation Error',  -- free string, project-defined
    natural_key_columns  VARCHAR(500) NOT NULL,    -- comma-separated cols from the rule's own SELECT
    element_name         VARCHAR(200),             -- optional; copied straight into gre_exceptions
    active_flag          BYTEINT DEFAULT 1,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMP
)
PRIMARY INDEX (rule_id);


-- ── 2. gre_log -- one row per rule execution attempt ──────────────────────
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_log (
    log_id           BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id           VARCHAR(200) NOT NULL,
    rule_id          INTEGER NOT NULL,
    rule_group       VARCHAR(100),
    batch_id         VARCHAR(100) NOT NULL,
    seq_no           INTEGER,
    start_time       TIMESTAMP,
    end_time         TIMESTAMP,
    status           VARCHAR(20),          -- 'SUCCESS' | 'ERROR' -- attempt-level, not the verdict
    rowcount         BIGINT,               -- violating rows written to gre_exceptions this attempt
    error_message    VARCHAR(2000),
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (run_id, rule_id);

-- Checkpoint/resume reads this per (rule_group, batch_id) to find the first
-- rule with no SUCCESS row yet -- see rules_engine/runner.py::_resume_point().
CREATE INDEX gre_log_group_batch_ix (rule_group, batch_id, status)
ON CMSUNIV_FILELAND_DEV_T.gre_log;


-- ── 3. gre_exceptions -- data findings, engine-populated only ─────────────
-- Column shape is the legacy INSERT list verbatim (record_id, rule_id,
-- table_name, element_name, source_name, issue_desc, exception_flag,
-- exception_approver, batch_id, etl_is_curr_ind, etl_load_dt,
-- etl_last_updt_dt) plus run_id and natural_key_value, which the legacy
-- shape didn't need but this engine's idempotency and traceability do.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_exceptions (
    record_id            BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id                VARCHAR(200),
    rule_id               INTEGER NOT NULL,
    table_name            VARCHAR(200),
    element_name          VARCHAR(200),
    source_name           VARCHAR(100),
    issue_desc            VARCHAR(2000),
    exception_flag        VARCHAR(20) DEFAULT 'OPEN',   -- compliance disposition
    exception_approver     VARCHAR(100),
    batch_id              VARCHAR(100) NOT NULL,
    etl_is_curr_ind       CHAR(1) DEFAULT 'Y',
    etl_load_dt           DATE,
    etl_last_updt_dt      TIMESTAMP,
    natural_key_value     VARCHAR(1000) NOT NULL,       -- built from rule.natural_key_columns
    created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (record_id);

-- v1: UNIQUE INDEX is the idempotency mechanism -- catch the duplicate-key
-- error on rerun and skip that row rather than delete-then-insert (same
-- pattern as dq_metrics_summary_uix).
CREATE UNIQUE INDEX gre_exceptions_uix (rule_id, batch_id, natural_key_value)
ON CMSUNIV_FILELAND_DEV_T.gre_exceptions;

-- v1: fast lookup by rule_id/batch_id -- this is exactly how gre_results
-- rows are joined back to their underlying records (see
-- rules_engine/reporting.py), mirroring dq_exceptions_run_rule_ix.
CREATE INDEX gre_exceptions_rule_batch_ix (rule_id, batch_id)
ON CMSUNIV_FILELAND_DEV_T.gre_exceptions;


-- ── 4. gre_case -- reference table for shared entity identifiers ──────────
-- Pure lookup data: rules correlate findings across tables by joining to
-- this on case_id. Populated/maintained outside the engine; the engine
-- itself never writes to this table.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_case (
    case_id          VARCHAR(100) NOT NULL,
    case_type        VARCHAR(50),          -- e.g. member | claim | provider | authorization
    source_system    VARCHAR(100),
    source_key       VARCHAR(200),
    description      VARCHAR(500),
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (case_id);


-- ── 5. gre_results -- one row per (rule_id, batch_id): rule-LEVEL verdict ──
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_results (
    result_id                 BIGINT GENERATED ALWAYS AS IDENTITY,
    rule_id                    INTEGER NOT NULL,
    batch_id                   VARCHAR(100) NOT NULL,
    run_id                     VARCHAR(200) NOT NULL,
    total_records               BIGINT,
    failed_records              BIGINT,
    failure_pct                 FLOAT,
    threshold_pct_used          FLOAT,      -- effective value actually applied (even if defaulted)
    threshold_count_used        INTEGER,
    threshold_operator_used     CHAR(3),
    severity                    VARCHAR(50),
    status                      VARCHAR(10),  -- 'PASS' | 'FAIL' | 'WARN'
    evaluated_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (rule_id, batch_id);

-- v1: UNIQUE INDEX so a rerun of a batch UPDATEs this summary row in place
-- (upsert) instead of expire-and-insert-new -- modeled directly on
-- dq_metrics_summary_uix.
CREATE UNIQUE INDEX gre_results_uix (rule_id, batch_id)
ON CMSUNIV_FILELAND_DEV_T.gre_results;
