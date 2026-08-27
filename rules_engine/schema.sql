-- ============================================================
-- Generic Rules Engine (GRE) DDL
-- Schema : {{META_DB}}  -- a PLACEHOLDER, not a literal schema name. This
--          file is a template: rules_engine/deploy_schema.py substitutes
--          {{META_DB}} with rules_engine/config.py's get_meta_db()
--          resolution (GRE_META_DB env var, defaulting to
--          {{META_DB}} for local/dev) before ever sending this
--          to Teradata -- the SAME database name every query this engine
--          runs already resolves at runtime, so the deployed DDL and the
--          queries against it can never point at two different schemas.
--          Promoting this file DEV -> QA -> INT -> UAT -> PROD is then
--          just re-running deploy_schema.py with GRE_META_DB set to that
--          environment's value -- never a hand-edit of this file. See
--          README.md's "Environments" section.
-- DB     : Teradata  (metadata store)
-- ============================================================
-- This package is fully standalone -- no other schema.sql needs to run
-- first (see README.md's "Package separation": rules_engine/ and
-- sampling/ no longer share ANY code or tables; each has its own
-- db_ops.py/config.py and its own run-tracking (gre_rule_audit) and
-- error-log (gre_rule_errors) tables, both created below alongside
-- gre_rules/gre_exceptions/gre_results).
--
-- Standalone from ddl.sql on purpose too: this file creates ONLY
-- gre_*-prefixed objects specific to rule evaluation. It never touches,
-- renames, or alters a single dq_* table, index, or column, and it does
-- not create any gre_sampling_*/gre_sample_* table (see
-- sampling/schema.sql -- that package's own, equally standalone DDL).
--
-- Column naming: this file's column names follow the vocabulary of an
-- existing rule-catalog/exception-tracking table pair
-- (HSABC_DEV_V.CMS_UNIVERSE_RULESENGINE / REPORTING_DEV_V.CMS_RULESENGINE)
-- rather than a from-scratch convention -- rule_syntax/src_tbl_nm/rule_nm/
-- act_ind/load_datetime/last_updated_datetime/src_key_cols/src_key_value
-- below are this engine's own required columns wearing that vocabulary's
-- names; universe_version/universe_year/dgr_nbr/issue_category_name/
-- business_rule/rule_description/created_by/last_updated_by are additional
-- descriptive columns added on top of it (see the gre_rules/gre_exceptions
-- comments below for the full list). run_key stays run_key, NOT batch_id --
-- see the run_key design note below for why.
--
-- Design notes (see rules_engine/config.py's usage,
-- rules_engine/executor.py docstrings for the code that relies on these
-- shapes):
--   * rule_syntax may embed any number of "{key}" OR "$key" tokens (e.g.
--     "{run_date}"/"$run_date", "{year}"/"$year", "{run_type}"/"$run_type"
--     -- freely mixed). The engine string-substitutes each one (quoted,
--     escaped) from the run_params dict passed to this run --
--     see rules_engine/db_ops.py::_substitute_params(). run_params has NO
--     reserved/required key -- entirely up to the rule author what it
--     contains. The one value the tracking/idempotency schema
--     (gre_exceptions_uix, gre_results, gre_rule_audit) keys off is
--     `run_key`, a SEPARATE explicit parameter passed alongside
--     run_params to rules_engine/runner.py's entry points -- see
--     rules_engine/db_ops.py::build_run_key() for a convenience way to build one
--     out of a batch id, a year+month pair, a specific date, or any other
--     column/combination. There is no filter_column/filter_sql system like
--     dq_rules has; SQL-authoring rules are expected to be fully
--     self-contained -- an unresolved "{token}" fails the rule attempt
--     immediately (PARAM_SUBSTITUTION_ERROR) rather than reaching the
--     source database as a syntax error.
--   * rule_syntax may ALSO embed the single reserved literal marker
--     "{extra_filters}" OR "$extra_filters" -- a SEPARATE, opt-in
--     mechanism from run_params above, for ad-hoc "AND col = 'value'"
--     equality condition(s) a caller wants applied at RUN TIME, on a
--     column the rule was never authored to anticipate (e.g. adding
--     run_ty='MNT' without pre-authoring a {run_ty} token on every rule
--     that might ever need it). A caller passes one or more filters as an
--     extra_filters dict (e.g. {"run_ty": "MNT"}) to
--     rules_engine/runner.py's entry points (or --filter KEY=VALUE on
--     run_by_process.py's CLI); the engine splices in
--     "AND col1 = 'v1' AND col2 = 'v2' ..." wherever the marker appears,
--     BEFORE run_params substitution runs. A rule that doesn't embed the
--     marker is completely unaffected even when a caller passes
--     extra_filters -- same "extra values are silently unused"
--     philosophy as an unused run_params key. Column names (the dict's
--     KEYS) are validated as plain SQL identifiers and rejected
--     (PARAM_SUBSTITUTION_ERROR) otherwise, since -- unlike run_params'
--     values, which are always escaped as literal data -- these become
--     column names spliced directly into the SQL text and can't be
--     escaped the same way. See rules_engine/db_ops.py::
--     build_extra_filters_clause()'s docstring for the full mechanics.
--   * database_name + src_tbl_nm give the auto-generated total-record
--     count query (see below) a fully-qualified FROM. There is no
--     separate scope_sql column: the run_params dict that scopes
--     rule_syntax already IS the definition of what's in scope for this
--     run, so a second, independently hand-written WHERE clause was pure
--     duplication (and a real drift risk -- the two could silently
--     disagree). Every key present in run_params is applied as an equality
--     filter against database_name.src_tbl_nm, AND'd together -- see
--     rules_engine/executor.py::_build_total_query(). extra_filters (see
--     above) is ALSO merged into this equality-filter set, but ONLY for a
--     rule that actually embeds the "{extra_filters}"/"$extra_filters"
--     marker -- keeping the total-record-count denominator honoring the
--     same narrowed scope as the rule_syntax scan itself, without
--     affecting a rule that never opted in. database_name is
--     stored here AS AUTHORED (almost always in DEV) and resolved to
--     whichever physical database this process's environment actually
--     has that data in at LOAD time, not stored per-environment -- see
--     rules_engine/config.py::resolve_database_name() and
--     rules_engine/rules.py::load_rules()'s call site. A project whose
--     table doesn't carry a column for one of its run_params keys should
--     not pass that key for rules on this table.
--   * There is no separate named-connection column. sql_dialect ('teradata'
--     | 'postgres' | 's3' | 'file') selects the one connection this rule
--     runs against -- db/connection_factory.py builds exactly one
--     connection per source_type, so a rule needs nothing more than its
--     dialect to pick its source.
--   * src_key_cols is this engine's analog of dq_rules' primary_key_columns:
--     a comma-separated list of column names present in the rule's own
--     SELECT output, used to build a deterministic src_key_value for each
--     violating row so reruns are idempotent (UNIQUE INDEX below),
--     mirroring the dq_metrics_summary_uix pattern: catch the
--     duplicate-key error and skip/update rather than delete-then-insert,
--     which leaves a crash-mid-delete window.
--   * rule_variant adds ONE additional generic level on top of
--     project/table (rule_group) for selecting which rules run: NULL
--     means the rule always applies within its rule_group; a non-NULL
--     value means it only applies when the caller's run explicitly
--     requests that exact value (rules_engine/rules.py::load_rules()).
--     This is deliberately a single freeform column, not separate
--     hardcoded year/run_type columns -- a project needing more than one
--     dimension composes a single string (e.g. "2026|MONTHLY"), the same
--     "SQL/config authors are self-contained" philosophy as rule_syntax
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
CREATE MULTISET TABLE {{META_DB}}.gre_rules (
    rule_id              INTEGER NOT NULL,
    rule_nm              VARCHAR(500) NOT NULL,
    act_ind              BYTEINT DEFAULT 1,
    -- ── Grouping/orchestration columns below -- which rules run, in what
    -- order, and what happens on failure.
    rule_group           VARCHAR(100) NOT NULL,    -- groups rules for one use case / table pipeline
    rule_variant         VARCHAR(100),             -- optional extra selection level within rule_group;
                                                    -- NULL = always applies, see design notes above
    project_name         VARCHAR(100) NOT NULL,    -- e.g. HEALTHSPRING_UM -- reporting/scoping dimension,
                                                    -- NOT the filter key load_rules() uses (see design
                                                    -- notes above); mirrors gre_sampling_config
    process_name         VARCHAR(100) NOT NULL,    -- e.g. UNIVERSE_VALIDATION -- same as project_name
    seq_no               INTEGER DEFAULT 100,      -- run order within a group (sequential mode only)
    sequencing_mode      VARCHAR(20) DEFAULT 'independent',  -- 'independent' | 'sequential'
    on_failure           VARCHAR(20) DEFAULT 'skip_and_continue',  -- 'halt_group' | 'skip_and_continue'
                                                    -- meaningful only when sequencing_mode='sequential'
    -- ── Source/query definition columns below -- what this rule actually
    -- runs and against what.
    database_name        VARCHAR(200) NOT NULL,    -- teradata/postgres: schema the table lives in.
                                                    -- file: the directory. s3: the s3:// prefix/bucket.
                                                    -- Combined with src_tbl_nm for the auto total-record
                                                    -- count query (db/connection_factory.py's
                                                    -- SourceAdapter.qualified_name()).
    src_tbl_nm           VARCHAR(200) NOT NULL,    -- teradata/postgres: table name. file: filename.
                                                    -- s3: object key/glob. The metadata table IS the
                                                    -- source path for file/s3 rules -- no separate setup.
    sql_dialect          VARCHAR(20)  NOT NULL,   -- 'teradata' | 'postgres' | 's3' | 'file' -- ALSO
                                                    -- selects the one connection this rule runs
                                                    -- against (see db/connection_factory.py -- exactly
                                                    -- one connection per value, no separate named-
                                                    -- connection column).
    rule_syntax          CLOB NOT NULL,            -- the negative SELECT; never mutates data. For a
                                                    -- file/s3 rule, FROM the view name
                                                    -- db/connection_factory.py::_view_name(src_tbl_nm)
                                                    -- derives from src_tbl_nm.
    src_key_cols         VARCHAR(500) NOT NULL,    -- comma-separated cols from the rule's own SELECT
    element_name         VARCHAR(200),             -- optional; copied straight into gre_exceptions
    -- ── Breach-behavior columns below -- how a failed record count turns
    -- into PASS/FAIL/WARN.
    threshold_pct        FLOAT,                    -- % of in-scope records that must fail to breach
    threshold_count      INTEGER,                  -- raw count of failed records that must be exceeded
                                                    -- BOTH NULL -- no tolerance was ever configured for
                                                    -- this rule, treated as an effective threshold_count=0:
                                                    -- ANY failed record breaches the rule, not only a
                                                    -- 100%-failed universe. See rules_engine/executor.py::
                                                    -- evaluate_threshold()'s docstring for the full
                                                    -- rationale.
    threshold_operator   CHAR(3) DEFAULT 'OR',      -- 'OR' | 'AND' -- only relevant if both are set
    severity             VARCHAR(50) DEFAULT 'Data Validation Error',  -- free string, project-defined
    -- ── Descriptive/reporting columns below -- purely informational, never
    -- read by engine logic (load_rules()/execute_rule() ignore them
    -- entirely). Added to carry a project's own rule-catalog vocabulary
    -- (e.g. a CMS Universe rule catalog) alongside the engine's own
    -- required columns above; every one is nullable so existing rows and
    -- projects that don't use this vocabulary are unaffected.
    universe_version     VARCHAR(50),              -- e.g. "V22" -- the universe/rule-catalog approval
                                                    -- version this rule belongs to
    universe_year        INTEGER,                  -- reporting year this rule's universe cycle covers
    dgr_nbr              VARCHAR(50),              -- external rule/version identifier, e.g.
                                                    -- "CDAG1V22R4" (encodes universe version + rule
                                                    -- number within it) -- copied onto gre_exceptions
                                                    -- at write time, see below
    issue_category_name  VARCHAR(200),             -- descriptive issue category (e.g. "Missing Data")
    business_rule        VARCHAR(2000),            -- business-friendly statement of what this rule
                                                    -- checks -- distinct from rule_syntax (the actual SQL)
    rule_description     VARCHAR(2000),            -- longer-form description
    -- ── Audit columns below.
    created_by           VARCHAR(100),             -- audit: who/what created this row
    last_updated_by      VARCHAR(100),             -- audit: who/what last modified this row
    load_datetime            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_datetime    TIMESTAMP
)
PRIMARY INDEX (rule_id);

-- rules_engine/rules.py::load_rules() always filters on exactly these
-- three columns -- covers that lookup without a full-table scan.
CREATE INDEX gre_rules_group_variant_ix (rule_group, act_ind, rule_variant)
ON {{META_DB}}.gre_rules;

-- Reporting/orchestration lookup by project/process (e.g. "which
-- rule_groups exist for project X" -- see
-- rules_engine/runner.py::discover_rule_groups()), never load_rules()'s
-- own lookup -- that one stays on gre_rules_group_variant_ix above.
CREATE INDEX gre_rules_project_process_ix (project_name, process_name, act_ind)
ON {{META_DB}}.gre_rules;


-- ── 2. gre_exceptions -- data findings, engine-populated only ─────────────
-- Column shape is the legacy INSERT list verbatim (record_id, rule_id,
-- src_tbl_nm, element_name, source_name, issue_desc, exception_flag,
-- exception_approver, run_key, etl_is_curr_ind, etl_load_dt,
-- etl_last_updt_dt) plus run_id, database_name, and src_key_value, which
-- the legacy shape didn't need but this engine's idempotency and source
-- tie-back do. rule_nm/dgr_nbr/universe_version/run_type/batch_schedule/
-- last_updated_by/last_updated_datetime below extend this further to
-- match a project's own rule-catalog/audit vocabulary (see gre_rules'
-- design notes) -- purely descriptive, never read by engine logic.
--
-- Deliberately does NOT store the violating row's own data -- only enough
-- to re-identify it (database_name/src_tbl_nm/source_name +
-- src_key_value). A row that fails every rule in a 10-rule group would
-- otherwise get its full column set duplicated 10 times, once per rule,
-- for no benefit; instead, rules_engine/reporting.py::
-- get_source_records_for_rule() re-joins back to the LIVE source table at
-- report/analysis time using this natural key, which costs nothing at
-- write time and reflects the record as it stands right now (see that
-- function's docstring for the trade-off this makes vs. a point-in-time
-- snapshot).
CREATE MULTISET TABLE {{META_DB}}.gre_exceptions (
    record_id            BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id                VARCHAR(200),
    run_key               VARCHAR(100) NOT NULL,
    rule_id               INTEGER NOT NULL,
    -- ── Descriptive/reporting columns below -- purely informational, never
    -- read by engine logic. rule_nm/dgr_nbr/universe_version are copied
    -- from gre_rules at write time (like element_name/project_name below);
    -- run_type/batch_schedule are copied from run_params IF the caller
    -- supplies those exact keys for this run, else NULL -- see
    -- rules_engine/executor.py::_write_exceptions().
    rule_nm               VARCHAR(500),                 -- copied from gre_rules.rule_nm
    rule_group            VARCHAR(100),                 -- copied from gre_rules.rule_group
    rule_variant          VARCHAR(100),                 -- copied from gre_rules.rule_variant (this
                                                    -- RULE's own value, NOT the run's requested
                                                    -- rule_variant filter -- see rules_engine/
                                                    -- runner.py::run_by_scope()'s docstring)
    -- ── Source/scope columns below -- where this finding came from.
    database_name         VARCHAR(200),                 -- copied from gre_rules.database_name
    src_tbl_nm            VARCHAR(200),
    project_name          VARCHAR(200),                 -- copied from gre_rules.project_name
    process_name          VARCHAR(200),                 -- copied from gre_rules.process_name
    element_name          VARCHAR(200),
    -- ── Finding detail columns below -- what was found and how to re-find it.
    source_name           VARCHAR(100),
    issue_desc            VARCHAR(2000),
    src_key_value         VARCHAR(1000) NOT NULL,       -- built from rule.src_key_cols
    -- ── Rule-catalog descriptive columns below -- see comment above.
    dgr_nbr               VARCHAR(50),                  -- copied from gre_rules.dgr_nbr
    universe_version      VARCHAR(50),                  -- copied from gre_rules.universe_version
    run_type              VARCHAR(50),                  -- from run_params["run_type"], if supplied
    batch_schedule        VARCHAR(100),                 -- from run_params["batch_schedule"], if supplied
    -- ── Compliance disposition columns below.
    exception_flag        VARCHAR(20) DEFAULT 'OPEN',   -- compliance disposition
    exception_approver     VARCHAR(100),
    -- ── ETL/legacy flag columns below.
    etl_is_curr_ind       CHAR(1) DEFAULT 'Y',
    etl_load_dt           DATE,
    etl_last_updt_dt      TIMESTAMP,
    -- ── Audit columns below.
    load_datetime             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_by       VARCHAR(100),                 -- audit: who/what last modified this row
    last_updated_datetime     TIMESTAMP
)
PRIMARY INDEX (record_id);

-- v1: UNIQUE INDEX is the idempotency mechanism -- catch the duplicate-key
-- error on rerun and skip that row rather than delete-then-insert (same
-- pattern as dq_metrics_summary_uix).
CREATE UNIQUE INDEX gre_exceptions_uix (rule_id, run_key, src_key_value)
ON {{META_DB}}.gre_exceptions;

-- v1: fast lookup by rule_id/run_key -- this is exactly how gre_results
-- rows are joined back to their underlying records (see
-- rules_engine/reporting.py), mirroring dq_exceptions_run_rule_ix.
CREATE INDEX gre_exceptions_rule_run_key_ix (rule_id, run_key)
ON {{META_DB}}.gre_exceptions;


-- ── 3. gre_results -- one row per rule PER EXECUTION ATTEMPT ─────────────
-- Consolidates what used to be two separate tables at the exact same
-- grain: gre_log (one row per rule per attempt, tracking execution
-- status/rowcount/start-end timing) and gre_results (one row per
-- rule_id+run_key, upserted in place, tracking the PASS/FAIL/WARN data
-- verdict). In practice they differed only in start_time/end_time and in
-- which status value was authoritative -- and gre_log's status
-- ('SUCCESS'/'ERROR') was routinely misread as the data verdict when it
-- only ever meant "the attempt ran to completion without raising": a
-- rule that legitimately FAILED its threshold still logged
-- status='SUCCESS' in gre_log every time, while the real verdict lived
-- only in gre_results.status. This table uses gre_results' verdict
-- semantics for its ONE status column -- 'PASS' | 'FAIL' | 'WARN' for a
-- completed evaluation, 'ERROR' when the attempt itself couldn't produce
-- a verdict at all (source prepare failed, run_params substitution
-- failed, the rule_syntax scan failed, the scope/count query failed, or
-- the write of this very row failed) -- see rules_engine/executor.py::
-- _write_result()'s docstring for the full history and
-- execute_rule()'s STEP 0/0b/1/2 failure paths for where ERROR comes
-- from.
--
-- run_id is a new value every time rules_engine/runner.py::run_rule_group()
-- is called (see generate_run_id()), even for a REPEATED run_key -- so a
-- deliberate rerun of the same run_key accumulates one gre_results row
-- per rule per run_id, not one row that gets overwritten (this used to
-- be true only of gre_log; now gre_results keeps that same full attempt
-- history too, never upserted-in-place). active_ind is how "which of
-- these is the CURRENT attempt for this rule_id/run_key" is answered
-- without every reader having to re-derive MAX(load_datetime) (or worse,
-- MAX(run_id), which sorts wrong once a run_id's timestamp suffix rolls
-- past a lexicographic boundary) themselves: the newest run_id's row is
-- 'Y', every earlier run_id's row for the same (rule_id, run_key) is
-- deactivated to 'N' -- see rules_engine/executor.py::
-- _deactivate_prior_results(), called from _write_result() immediately
-- before this attempt's own row is inserted. Never deletes -- full
-- attempt history (including superseded ERROR rows) stays on file for
-- audit; only active_ind flips. This is also how the "same kind of run"
-- rerun policy applies here exactly as it already did for
-- gre_exceptions/gre_rule_errors: nothing from a prior attempt at the
-- same run_key is ever deleted, only marked inactive, with the new
-- attempt's rows added alongside it.
CREATE MULTISET TABLE {{META_DB}}.gre_results (
    result_id                 BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id                     VARCHAR(200) NOT NULL,
    rule_id                    INTEGER NOT NULL,
    rule_group                 VARCHAR(100),
    rule_variant                VARCHAR(100),         -- this RULE's own gre_rules.rule_variant,
                                                        -- NOT the run's requested rule_variant filter
                                                        -- -- see rules_engine/runner.py::
                                                        -- run_by_scope()'s docstring
    project_name                VARCHAR(100),         -- copied from gre_rules.project_name
    process_name                VARCHAR(100),         -- copied from gre_rules.process_name
    run_key                    VARCHAR(100) NOT NULL,
    seq_no                      INTEGER,
    start_time                  TIMESTAMP,
    end_time                    TIMESTAMP,
    total_records               BIGINT,
    failed_records              BIGINT,               -- TRUE count of violating rows from this
                                                        -- attempt's own scan -- see
                                                        -- rules_engine/executor.py::execute_rule()'s
                                                        -- comment above its _write_result() calls for
                                                        -- why this is a fresh COUNT every attempt, not
                                                        -- an accumulator, and always agrees with a
                                                        -- COUNT(*) against gre_exceptions itself.
    failure_pct                 FLOAT,
    threshold_pct_used          FLOAT,      -- effective value actually applied (even if defaulted)
    threshold_count_used        INTEGER,
    threshold_operator_used     CHAR(3),
    severity                    VARCHAR(50),
    status                      VARCHAR(10),  -- 'PASS' | 'FAIL' | 'WARN' | 'ERROR' -- see this
                                               -- section's header comment for why ERROR lives in
                                               -- the SAME column as the data verdict now, not a
                                               -- separate gre_log.status.
    error_message                VARCHAR(2000),  -- populated only when status = 'ERROR'
    executed_sql                 CLOB,         -- the ACTUAL rule_syntax text that ran this attempt,
                                               -- AFTER run_params substitution ({key}/$key already
                                               -- resolved to their literal values) -- see
                                               -- rules_engine/executor.py::execute_rule()'s "query"
                                               -- variable. Lets a reviewer see EXACTLY what SQL
                                               -- produced this attempt's verdict without having to
                                               -- reconstruct it from gre_rules.rule_syntax + whatever
                                               -- run_params happened to be passed that day. Populated
                                               -- for every attempt, including ERROR ones where
                                               -- substitution itself succeeded (SQL_RUNTIME,
                                               -- CONNECTION_UNAVAILABLE, ...); for the two failure
                                               -- points BEFORE substitution runs at all
                                               -- (SOURCE_PREPARE_ERROR, PARAM_SUBSTITUTION_ERROR)
                                               -- this instead holds the RAW, unsubstituted
                                               -- rule_syntax -- still useful there precisely because
                                               -- it shows any unresolved {key}/$key tokens.
    source_tieback_sql          CLOB,         -- generated (never executed) SQL TEXT that joins
                                               -- this rule's source table straight to its
                                               -- gre_exceptions rows for run_key, parsing
                                               -- src_key_value back into its original column(s)
                                               -- in-database via STRTOK (teradata) or split_part
                                               -- (postgres) -- see rules_engine/executor.py::
                                               -- build_source_tieback_sql()'s docstring. NULL for
                                               -- a 'file'/'s3' rule (no durable table for a stored
                                               -- SQL string to reference), a rule with no
                                               -- src_key_cols, or an attempt with zero failed_records
                                               -- (nothing to tie back to). Pull this straight out of
                                               -- gre_results and run it in Toad/whatever SQL
                                               -- client instead of hand-deriving the join.
    active_ind                  CHAR(1) DEFAULT 'Y',  -- 'Y' = this run_id is the CURRENT attempt for
                                               -- this (rule_id, run_key); 'N' = superseded by a
                                               -- later run_id's rerun of the same run_key. See this
                                               -- section's header comment.
    load_datetime                TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_datetime        TIMESTAMP    -- set only when active_ind flips to 'N'
)
PRIMARY INDEX (run_id, rule_id);

-- Reporting/dashboard lookup: "what's the current status of every rule
-- for this run_key" -- filters straight to active_ind='Y' instead of
-- scanning every historical attempt across every past run_id.
CREATE INDEX gre_results_rule_run_key_active_ix (rule_id, run_key, active_ind)
ON {{META_DB}}.gre_results;

-- Historical/ops lookup by (rule_group, run_key, status) -- e.g. "how many
-- ERROR attempts has this run_key ever had, across every rerun".
CREATE INDEX gre_results_group_run_key_ix (rule_group, run_key, status)
ON {{META_DB}}.gre_results;


-- ── 4. gre_rule_audit -- durable, one row per rules_engine run ────────────
-- Written by rules_engine/runner.py::_start_audit()/_finish_audit() ONLY.
-- Used to live in shared/schema.sql, alongside sampling/'s equivalent
-- gre_sampling_audit table (both were kept together there because they
-- once shared a combined gre_audit table -- see the git history around
-- the 2026-08 gre_audit split). Now that rules_engine/ and sampling/
-- share no code or tables at all (see README.md's "Package separation"),
-- this table lives here, where it's actually used.
CREATE MULTISET TABLE {{META_DB}}.gre_rule_audit (
    run_id                 VARCHAR(200) NOT NULL,
    rule_group              VARCHAR(100),
    rule_variant               VARCHAR(100),      -- NULL = no variant requested
    project_name              VARCHAR(100),      -- copied from gre_rules.project_name
    process_name              VARCHAR(100),      -- copied from gre_rules.process_name
    run_key                   VARCHAR(100),        -- caller-supplied tracking/idempotency key
    run_params                 CLOB,             -- the run_params dict this run was actually called
                                                  -- with, JSON-encoded (e.g. '{"year": 2026, "month": 8}'),
                                                  -- NULL when none was passed. This is the RUN-LEVEL
                                                  -- record of what was passed in -- one row per
                                                  -- run_rule_group() call, vs. gre_results.executed_sql
                                                  -- which is the PER-RULE fully-resolved SQL text. Lets a
                                                  -- reviewer answer "what params/filters was run_id X
                                                  -- actually invoked with" directly off this table,
                                                  -- without reading every gre_results row for the run
                                                  -- and reverse-engineering it from executed_sql.
    extra_filters               CLOB,            -- the extra_filters dict this run was actually called
                                                  -- with, JSON-encoded, NULL when none was passed. See
                                                  -- run_params above and rules_engine/db_ops.py::
                                                  -- build_extra_filters_clause()'s docstring -- a rule
                                                  -- without the "{extra_filters}"/"$extra_filters" marker
                                                  -- is unaffected by this even when it's non-NULL here;
                                                  -- this column only records what the CALLER passed for
                                                  -- the whole run, not which individual rules used it.
    started_at                TIMESTAMP,
    ended_at                   TIMESTAMP,
    status                      VARCHAR(20),      -- 'RUNNING' | 'COMPLETED' | 'HALTED'
    total_rules                  INTEGER,
    rules_succeeded                INTEGER,
    rules_errored                    INTEGER,
    triggered_by                      VARCHAR(100),
    load_datetime                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (run_id);

-- count_prior_attempts() (rules_engine/db_ops.py) looks up "how many runs
-- of this rule_group+run_key already exist" every call, to label a new
-- run_id's attempt-N segment -- see rules_engine/README.md's "Identifying
-- an attempt: run_id".
CREATE INDEX gre_rule_audit_group_run_key_ix (rule_group, run_key)
ON {{META_DB}}.gre_rule_audit;

CREATE INDEX gre_rule_audit_status_ix (status)
ON {{META_DB}}.gre_rule_audit;


-- ── 5. gre_rule_errors -- this package's own SQL/execution failure log ───
-- Used to be gre_errors, one table shared with sampling/ (rule_id NULL
-- for a sampling-run error row). Now this package's own -- rule_id is
-- always populated here (every row is tied to a specific rule), and
-- sampling/'s equivalent errors live in sampling/schema.sql's
-- gre_sampling_errors instead, with its own honest process_name column
-- rather than repurposing this table's rule_group column. See
-- README.md's "Package separation".
--
-- Append-only across reruns of the same run_key under a NEW run_id --
-- active_ind marks which error(s) belong to the CURRENT run_id for a
-- given (rule_id, run_key) -- see rules_engine/db_ops.py::
-- _deactivate_prior_errors(), called from log_error() immediately before
-- each new error row is inserted, mirroring gre_results' active_ind
-- reconciliation exactly. Never deletes -- full error history stays on
-- file; only active_ind flips.
CREATE MULTISET TABLE {{META_DB}}.gre_rule_errors (
    error_id         BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id           VARCHAR(200),
    rule_id          INTEGER NOT NULL,
    rule_group       VARCHAR(100),
    rule_variant     VARCHAR(100),         -- the erroring rule's own gre_rules.rule_variant --
                                            -- see rules_engine/runner.py::run_by_scope()'s docstring
    run_key          VARCHAR(100),
    error_type       VARCHAR(50),          -- e.g. SQL_SYNTAX | CONNECTION | RUNTIME | PULL_FAILURE
    error_message    VARCHAR(2000),
    error_detail     CLOB,
    active_ind           CHAR(1) DEFAULT 'Y',  -- 'Y' = belongs to the current run_id for this
                                                -- (rule_id, run_key); 'N' = superseded by a later
                                                -- rerun of the same run_key. See comment above.
    occurred_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_datetime TIMESTAMP             -- set only when active_ind flips to 'N'
)
PRIMARY INDEX (run_id, rule_id);

-- Reporting/dashboard lookup: "what errors are current right now for this
-- run_key" -- filters straight to active_ind='Y' instead of every
-- historical error across every past run_id.
CREATE INDEX gre_rule_errors_rule_run_key_active_ix (rule_id, run_key, active_ind)
ON {{META_DB}}.gre_rule_errors;
