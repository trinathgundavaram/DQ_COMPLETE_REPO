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
--   * rule_sql may embed any number of "{key}" tokens (e.g. "{run_date}",
--     "{year}", "{run_type}"). The engine string-substitutes each one
--     (quoted, escaped) from the run_params dict passed to this run --
--     see shared/db_ops.py::_substitute_params(). run_params has NO
--     reserved/required key -- entirely up to the rule author what it
--     contains. The one value the tracking/idempotency schema
--     (gre_exceptions_uix, gre_log, gre_results, gre_audit) keys off is
--     `run_key`, a SEPARATE explicit parameter passed alongside
--     run_params to rules_engine/runner.py's entry points -- see
--     shared/db_ops.py::build_run_key() for a convenience way to build one
--     out of a batch id, a year+month pair, a specific date, or any other
--     column/combination. There is no filter_column/filter_sql system like
--     dq_rules has; SQL-authoring rules are expected to be fully
--     self-contained -- an unresolved "{token}" fails the rule attempt
--     immediately (PARAM_SUBSTITUTION_ERROR) rather than reaching the
--     source database as a syntax error.
--   * database_name + table_name give the auto-generated total-record
--     count query (see below) a fully-qualified FROM. There is no
--     separate scope_sql column: the run_params dict that scopes rule_sql
--     already IS the definition of what's in scope for this run, so a
--     second, independently hand-written WHERE clause was pure
--     duplication (and a real drift risk -- the two could silently
--     disagree). Every key present in run_params is applied as an equality
--     filter against database_name.table_name, AND'd together -- see
--     rules_engine/executor.py::_build_total_query(). A project whose
--     table doesn't carry a column for one of its run_params keys should
--     not pass that key for rules on this table.
--   * There is no separate named-connection column. sql_dialect ('teradata'
--     | 'postgres' | 's3' | 'file') selects the one connection this rule
--     runs against -- db/connection_factory.py builds exactly one
--     connection per source_type, so a rule needs nothing more than its
--     dialect to pick its source.
--   * natural_key_columns is this engine's analog of dq_rules'
--     primary_key_columns: a comma-separated list of column names present
--     in the rule's own SELECT output, used to build a deterministic
--     natural_key_value for each violating row so reruns are idempotent
--     (UNIQUE INDEX below), mirroring the dq_metrics_summary_uix pattern:
--     catch the duplicate-key error and skip/update rather than
--     delete-then-insert, which leaves a crash-mid-delete window.
--   * rule_variant adds ONE additional generic level on top of
--     project/table (rule_group) for selecting which rules run: NULL
--     means the rule always applies within its rule_group; a non-NULL
--     value means it only applies when the caller's run explicitly
--     requests that exact value (rules_engine/rules.py::load_rules()).
--     This is deliberately a single freeform column, not separate
--     hardcoded year/run_type columns -- a project needing more than one
--     dimension composes a single string (e.g. "2026|MONTHLY"), the same
--     "SQL/config authors are self-contained" philosophy as rule_sql
--     above.
--   * project_name / process_name are descriptive/reporting dimensions,
--     NOT a second filter key -- rule_group stays the one literal column
--     load_rules() filters on (gre_rules_group_variant_ix is unchanged).
--     They exist so a rule_group's rows can be sliced/joined by project
--     without a round trip back to gre_rules, and so this table finally
--     speaks the same scoping vocabulary sampling/schema.sql's
--     gre_sampling_config already uses (project_name/process_name there
--     too). A rule_group is expected to belong to exactly one
--     (project_name, process_name) pair -- rules_engine/runner.py warns
--     if a group's rows disagree with themselves, the same pattern it
--     already uses for sequencing_mode consistency.
-- ============================================================


-- ── 1. gre_rules -- one row per rule ──────────────────────────────────────
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_rules (
    rule_id              INTEGER NOT NULL,
    rule_name            VARCHAR(500) NOT NULL,
    database_name        VARCHAR(200) NOT NULL,    -- teradata/postgres: schema the table lives in.
                                                    -- file: the directory. s3: the s3:// prefix/bucket.
                                                    -- Combined with table_name for the auto total-record
                                                    -- count query (db/connection_factory.py's
                                                    -- SourceAdapter.qualified_name()).
    table_name           VARCHAR(200) NOT NULL,    -- teradata/postgres: table name. file: filename.
                                                    -- s3: object key/glob. The metadata table IS the
                                                    -- source path for file/s3 rules -- no separate setup.
    sql_dialect          VARCHAR(20)  NOT NULL,   -- 'teradata' | 'postgres' | 's3' | 'file' -- ALSO
                                                    -- selects the one connection this rule runs
                                                    -- against (see db/connection_factory.py -- exactly
                                                    -- one connection per value, no separate named-
                                                    -- connection column).
    rule_sql             CLOB NOT NULL,            -- the negative SELECT; never mutates data. For a
                                                    -- file/s3 rule, FROM the view name
                                                    -- db/connection_factory.py::_view_name(table_name)
                                                    -- derives from table_name.
    project_name         VARCHAR(100) NOT NULL,    -- e.g. HEALTHSPRING_UM -- reporting/scoping dimension,
                                                    -- NOT the filter key load_rules() uses (see design
                                                    -- notes above); mirrors gre_sampling_config
    process_name         VARCHAR(100) NOT NULL,    -- e.g. UNIVERSE_VALIDATION -- same as project_name
    rule_group           VARCHAR(100) NOT NULL,    -- groups rules for one use case / table pipeline
    rule_variant         VARCHAR(100),             -- optional extra selection level within rule_group;
                                                    -- NULL = always applies, see design notes above
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

-- rules_engine/rules.py::load_rules() always filters on exactly these
-- three columns -- covers that lookup without a full-table scan.
CREATE INDEX gre_rules_group_variant_ix (rule_group, active_flag, rule_variant)
ON CMSUNIV_FILELAND_DEV_T.gre_rules;

-- Reporting/orchestration lookup by project/process (e.g. "which
-- rule_groups exist for project X" -- see
-- rules_engine/runner.py::discover_rule_groups()), never load_rules()'s
-- own lookup -- that one stays on gre_rules_group_variant_ix above.
CREATE INDEX gre_rules_project_process_ix (project_name, process_name, active_flag)
ON CMSUNIV_FILELAND_DEV_T.gre_rules;


-- ── 2. gre_log -- one row per rule execution attempt ──────────────────────
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_log (
    log_id           BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id           VARCHAR(200) NOT NULL,
    rule_id          INTEGER NOT NULL,
    rule_group       VARCHAR(100),
    project_name     VARCHAR(100),         -- copied from gre_rules.project_name (reporting dimension)
    process_name     VARCHAR(100),         -- copied from gre_rules.process_name
    run_key          VARCHAR(100) NOT NULL,
    seq_no           INTEGER,
    start_time       TIMESTAMP,
    end_time         TIMESTAMP,
    status           VARCHAR(20),          -- 'SUCCESS' | 'ERROR' -- attempt-level, not the verdict
    rowcount         BIGINT,               -- violating rows written to gre_exceptions this attempt
    error_message    VARCHAR(2000),
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (run_id, rule_id);

-- Checkpoint/resume reads this per (rule_group, run_key) to find the first
-- rule with no SUCCESS row yet -- see rules_engine/runner.py::_resume_point().
CREATE INDEX gre_log_group_run_key_ix (rule_group, run_key, status)
ON CMSUNIV_FILELAND_DEV_T.gre_log;


-- ── 3. gre_exceptions -- data findings, engine-populated only ─────────────
-- Column shape is the legacy INSERT list verbatim (record_id, rule_id,
-- table_name, element_name, source_name, issue_desc, exception_flag,
-- exception_approver, run_key, etl_is_curr_ind, etl_load_dt,
-- etl_last_updt_dt) plus run_id, database_name, and natural_key_value,
-- which the legacy shape didn't need but this engine's idempotency and
-- source tie-back do.
--
-- Deliberately does NOT store the violating row's own data -- only enough
-- to re-identify it (database_name/table_name/source_name +
-- natural_key_value). A row that fails every rule in a 10-rule group
-- would otherwise get its full column set duplicated 10 times, once per
-- rule, for no benefit; instead, rules_engine/reporting.py::
-- get_source_records_for_rule() re-joins back to the LIVE source table at
-- report/analysis time using this natural key, which costs nothing at
-- write time and reflects the record as it stands right now (see that
-- function's docstring for the trade-off this makes vs. a point-in-time
-- snapshot).
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_exceptions (
    record_id            BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id                VARCHAR(200),
    rule_id               INTEGER NOT NULL,
    database_name         VARCHAR(200),                 -- copied from gre_rules.database_name
    table_name            VARCHAR(200),
    project_name          VARCHAR(200),                 -- copied from gre_rules.project_name
    process_name          VARCHAR(200),                 -- copied from gre_rules.process_name
    element_name          VARCHAR(200),
    source_name           VARCHAR(100),
    issue_desc            VARCHAR(2000),
    exception_flag        VARCHAR(20) DEFAULT 'OPEN',   -- compliance disposition
    exception_approver     VARCHAR(100),
    run_key               VARCHAR(100) NOT NULL,
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
CREATE UNIQUE INDEX gre_exceptions_uix (rule_id, run_key, natural_key_value)
ON CMSUNIV_FILELAND_DEV_T.gre_exceptions;

-- v1: fast lookup by rule_id/run_key -- this is exactly how gre_results
-- rows are joined back to their underlying records (see
-- rules_engine/reporting.py), mirroring dq_exceptions_run_rule_ix.
CREATE INDEX gre_exceptions_rule_run_key_ix (rule_id, run_key)
ON CMSUNIV_FILELAND_DEV_T.gre_exceptions;


-- ── 4. gre_results -- one row per (rule_id, run_key): rule-LEVEL verdict ──
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_results (
    result_id                 BIGINT GENERATED ALWAYS AS IDENTITY,
    rule_id                    INTEGER NOT NULL,
    run_key                    VARCHAR(100) NOT NULL,
    run_id                     VARCHAR(200) NOT NULL,
    project_name                VARCHAR(100),         -- copied from gre_rules.project_name
    process_name                VARCHAR(100),         -- copied from gre_rules.process_name
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
PRIMARY INDEX (rule_id, run_key);

-- v1: UNIQUE INDEX so a rerun of the same run_key UPDATEs this summary row
-- in place (upsert) instead of expire-and-insert-new -- modeled directly
-- on dq_metrics_summary_uix.
CREATE UNIQUE INDEX gre_results_uix (rule_id, run_key)
ON CMSUNIV_FILELAND_DEV_T.gre_results;
