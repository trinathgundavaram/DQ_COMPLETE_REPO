-- ============================================================
-- Generic Rules Engine (GRE) DDL  (v1)
-- Schema : CMSUNIV_FILELAND_DEV_T  (DEV)  -- same metadata store as dq_*,
--          reached at runtime via the same "teradata" connection name and
--          the same config/env_config.get_meta_db() resolution (GRE_META_DB
--          overrides it independently if these tables ever need to move to
--          their own schema without a code change).
-- DB     : Teradata  (metadata store)
-- ============================================================
-- Standalone from ddl.sql on purpose: this file creates ONLY gre_*-prefixed
-- objects. It never touches, renames, or alters a single dq_* table, index,
-- or column. See DESIGN.md / DQ_COMPLETE_REPO's core/ for the engine this
-- one deliberately does not replace.
--
-- Design notes (see gre/config.py, gre/executor.py docstrings for the code
-- that relies on these shapes):
--   * rule_sql / scope_sql may embed a literal "{batch_id}" token. The
--     engine string-substitutes it (quoted, escaped) before running the
--     query -- see gre/executor.py::_substitute_batch_id(). There is no
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
-- rule with no SUCCESS row yet -- see gre/runner.py::_resume_point().
CREATE INDEX gre_log_group_batch_ix (rule_group, batch_id, status)
ON CMSUNIV_FILELAND_DEV_T.gre_log;


-- ── 3. gre_audit -- durable, one row per run ───────────────────────────────
-- v2 (sampling): shared by BOTH rule_group runs and sampling runs, per the
-- prompt's explicit "reuse gre_audit rather than inventing a parallel
-- run-log table just for sampling." run_type discriminates the two; the
-- rule-run columns (rule_group, batch_id, total_rules, ...) are only
-- meaningful when run_type='RULE_GROUP' and are NULL for a sampling row,
-- and vice versa for the sampling-only columns below. Nothing here has
-- been deployed yet, so this is a straight edit of the v3 DDL, not a live
-- migration -- see gre/README.md for the full note.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_audit (
    run_id                 VARCHAR(200) NOT NULL,
    run_type               VARCHAR(20) DEFAULT 'RULE_GROUP',  -- 'RULE_GROUP' | 'SAMPLING'
    rule_group              VARCHAR(100),        -- RULE_GROUP runs only
    batch_id                 VARCHAR(100),        -- RULE_GROUP runs only
    started_at                TIMESTAMP,
    ended_at                   TIMESTAMP,
    status                      VARCHAR(20),      -- 'RUNNING' | 'COMPLETED' | 'HALTED'
    total_rules                  INTEGER,          -- RULE_GROUP runs only
    rules_succeeded                INTEGER,        -- RULE_GROUP runs only
    rules_errored                    INTEGER,      -- RULE_GROUP runs only
    sample_config_id                  INTEGER,     -- SAMPLING runs only (-> gre_sampling_config)
    sampling_method                     VARCHAR(20),  -- SAMPLING runs only: 'RANKED'|'RANDOM'|'SYSTEMATIC'
    random_seed                           BIGINT,   -- SAMPLING runs only, RANDOM/SYSTEMATIC: the ONE
                                                     -- seed used for this whole run -- see
                                                     -- gre/sampling.py's module docstring for how a
                                                     -- single seed plus deterministic (sorted) bucket
                                                     -- processing order reproduces every per-bucket
                                                     -- offset/draw without storing them separately
    target_volume                          INTEGER, -- SAMPLING runs only
    total_candidates                        INTEGER,-- SAMPLING runs only
    total_selected                           INTEGER,-- SAMPLING runs only
    triggered_by                              VARCHAR(100),
    created_at                                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (run_id);


-- ── 4. gre_exceptions -- data findings, engine-populated only ─────────────
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
-- rows are joined back to their underlying records (see gre/reporting.py),
-- mirroring dq_exceptions_run_rule_ix.
CREATE INDEX gre_exceptions_rule_batch_ix (rule_id, batch_id)
ON CMSUNIV_FILELAND_DEV_T.gre_exceptions;


-- ── 5. gre_errors -- SQL/execution failures, routed to engineering ────────
-- Kept fully separate from gre_exceptions: a rule crashing is never the
-- same row as a rule finding a violation.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_errors (
    error_id         BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id           VARCHAR(200),
    rule_id          INTEGER,
    rule_group       VARCHAR(100),
    batch_id         VARCHAR(100),
    error_type       VARCHAR(50),          -- e.g. SQL_SYNTAX | CONNECTION | RUNTIME
    error_message    VARCHAR(2000),
    error_detail     CLOB,
    occurred_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (run_id, rule_id);


-- ── 6. gre_case -- reference table for shared entity identifiers ──────────
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


-- ── 7. gre_results -- one row per (rule_id, batch_id): rule-LEVEL verdict ──
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


-- ============================================================
-- GENERIC STRATIFIED SAMPLING (v4) -- 5 tables, a separate concern from
-- rule evaluation. Generalizes core/stratified_sampling.py's proven shape
-- (filter -> bucket -> quota -> select -> persist), which hardcodes
-- exactly two stratification levels and one selection method (ranked
-- top-N), to: any number of levels and three selection methods. See
-- gre/sampling.py's module docstring for the algorithm; see
-- gre/seed/um_sample.sql for the existing dq_sampling_config config_id=1
-- (COMO weekly UM sample) re-expressed in this shape, proving the
-- redesign reproduces it.
-- ============================================================

-- ── 8. gre_sampling_config -- one row per sampling definition ─────────────
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_sampling_config (
    config_id            INTEGER NOT NULL,
    project_name         VARCHAR(100) NOT NULL,
    process_name         VARCHAR(100) NOT NULL,
    sample_name          VARCHAR(100) NOT NULL,
    connection_name      VARCHAR(100) NOT NULL,   -- see db/connection_factory.py
    universe_table       VARCHAR(200) NOT NULL,
    key_columns          VARCHAR(500) NOT NULL,   -- entity key column(s), CSV
    scope_sql            CLOB,                    -- WHERE-fragment; may embed a literal
                                                    -- {batch_id} token, substituted the same
                                                    -- way gre_rules.rule_sql is -- see
                                                    -- gre/executor.py::_substitute_batch_id.
                                                    -- Defaults to '1=1' (whole table) if unset.
                                                    -- NOTE: unlike gre_rules.scope_sql (a COUNT
                                                    -- query), this scope_sql is a WHERE-fragment
                                                    -- -- same field name, different shape,
                                                    -- because it answers a different question
                                                    -- here ("which rows are this cycle's
                                                    -- candidates" vs. "what's the denominator").
    exclusion_sql        CLOB,                    -- WHERE-fragment; matching rows are EXCLUDED
    target_volume        INTEGER NOT NULL DEFAULT 150,
    sampling_method       VARCHAR(20) DEFAULT 'RANKED',  -- 'RANKED'|'RANDOM'|'SYSTEMATIC'
    priority_rank_sql     CLOB,                    -- required for RANKED/SYSTEMATIC; ORDER BY
                                                    -- expression, lowest = highest priority
    rounding_mode          VARCHAR(10) DEFAULT 'FLOOR',  -- 'FLOOR'|'ROUND'|'CEIL'
    schedule_cron            VARCHAR(50),
    active_flag                BYTEINT DEFAULT 1,
    created_at                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (config_id);


-- ── 9. gre_sampling_strata -- one row per stratification LEVEL, in order ──
-- Replaces dq_sampling_config's two hardcoded columns (determination_column,
-- functional_area_column) with however many levels a project needs. Zero
-- rows for a config = no stratification -- straight to select() on the
-- whole candidate pool.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_sampling_strata (
    strata_id            INTEGER NOT NULL,
    config_id            INTEGER NOT NULL,
    level_order           INTEGER NOT NULL,        -- recursion order, 0-based
    level_name             VARCHAR(100),            -- e.g. 'request_disposition'
    stratify_expr             VARCHAR(1000) NOT NULL   -- column name OR any SQL expression
                                                    -- (e.g. a CASE statement) -- a bucket
                                                    -- doesn't have to already exist as a column
)
PRIMARY INDEX (strata_id);

CREATE INDEX gre_sampling_strata_config_ix (config_id, level_order)
ON CMSUNIV_FILELAND_DEV_T.gre_sampling_strata;


-- ── 10. gre_sampling_mix -- one row per named bucket value, per level ─────
-- Replaces dq_sampling_config's JSON mix columns. A bucket_value present in
-- the data but NOT listed here for that level absorbs the remainder
-- fraction (1 - sum(named fractions)) -- same rule as the proven dq_*
-- pattern (core/stratified_sampling.py::_target_for_bucket), just as rows.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_sampling_mix (
    mix_id               INTEGER NOT NULL,
    strata_id            INTEGER NOT NULL,
    bucket_value          VARCHAR(200) NOT NULL,
    target_fraction         FLOAT NOT NULL
)
PRIMARY INDEX (mix_id);

CREATE INDEX gre_sampling_mix_strata_ix (strata_id)
ON CMSUNIV_FILELAND_DEV_T.gre_sampling_mix;


-- ── 11. gre_sample_selections -- one row per candidate CONSIDERED ─────────
-- Every candidate, selected or not (audit defensibility). Never updated
-- after the run -- a rerun uses a fresh sample_run_id, unlike
-- gre_exceptions/gre_results which DO get updated on rerun: a sample is a
-- point-in-time draw, not a re-evaluated verdict.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_sample_selections (
    sample_run_id         VARCHAR(200) NOT NULL,
    config_id             INTEGER,
    project_name          VARCHAR(100),
    process_name          VARCHAR(100),
    sample_cycle           DATE,
    case_key                 VARCHAR(500) NOT NULL,   -- entity key, matches key_columns
    priority_rank              INTEGER,                -- 1 = highest priority; NULL for RANDOM
                                                        -- (meaningless for that method)
    excluded_flag                 BYTEINT DEFAULT 0,
    exclusion_reason                 VARCHAR(500),
    selected_flag                       BYTEINT DEFAULT 0,
    created_at                             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (sample_run_id);

CREATE INDEX gre_sample_selections_lookup_ix (project_name, process_name, sample_cycle, selected_flag)
ON CMSUNIV_FILELAND_DEV_T.gre_sample_selections;


-- ── 12. gre_sample_selection_attrs -- which bucket, at each level ─────────
-- One row per (sample_run_id, case_key, strata_id) -- deliberately keyed by
-- this natural composite rather than a fetched-back gre_sample_selections
-- surrogate id: Teradata has no RETURNING clause, so getting an
-- identity value back from the gre_sample_selections insert to use as a
-- foreign key here would need an extra round-trip query per row. case_key
-- is already unique within one sample_run_id, so this composite is just as
-- precise a join key. Replaces a JSON snapshot column with plain rows,
-- same no-JSON rule as everywhere else in this schema.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_sample_selection_attrs (
    sample_run_id         VARCHAR(200) NOT NULL,
    case_key              VARCHAR(500) NOT NULL,
    strata_id             INTEGER NOT NULL,
    level_order            INTEGER,          -- denormalized for ordered reads without a join
    bucket_value              VARCHAR(200),
    created_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (sample_run_id, case_key);
