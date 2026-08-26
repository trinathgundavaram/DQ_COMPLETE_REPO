-- ============================================================
-- GENERIC STRATIFIED SAMPLING -- 5 tables, a separate concern from rule
-- evaluation (see rules_engine/schema.sql). Generalizes
-- core/stratified_sampling.py's proven shape (filter -> bucket -> quota ->
-- select -> persist), which hardcodes exactly two stratification levels
-- and one selection method (ranked top-N), to: any number of levels and
-- three selection methods. See sampling/sampling.py's module docstring for
-- the algorithm; see sampling/seed/um_sample.sql for the existing dq_*
-- dq_sampling_config config_id=1 (COMO weekly UM sample) re-expressed in
-- this shape, proving the redesign reproduces it.
--
-- Schema : {{META_DB}}  -- a PLACEHOLDER, not a literal schema name. This
--          file is a template: sampling/deploy_schema.py substitutes
--          {{META_DB}} with sampling/config.py's get_meta_db() resolution
--          (GRE_META_DB env var, defaulting to {{META_DB}} for
--          local/dev) before ever sending this to Teradata. Promoting
--          this file DEV -> QA -> INT -> UAT -> PROD is just re-running
--          deploy_schema.py with GRE_META_DB set to that environment's
--          value -- never a hand-edit of this file. See README.md's
--          "Environments" section.
-- DB     : Teradata  (metadata store)
-- ============================================================
-- This package is fully standalone -- no other schema.sql needs to run
-- first (see README.md's "Package separation": rules_engine/ and
-- sampling/ no longer share ANY code or tables; each has its own
-- db_ops.py/config.py and its own run-tracking (gre_sampling_audit) and
-- error-log (gre_sampling_errors) tables, both created below alongside
-- the 5 gre_sampling_*/gre_sample_* tables).
--
-- Standalone from ddl.sql and rules_engine/schema.sql on purpose too:
-- this file creates ONLY gre_sampling_*/gre_sample_*/gre_errors-successor
-- objects. It never touches, renames, or alters a single dq_* or
-- rules_engine table, index, or column.
-- ============================================================

-- ── 1. gre_sampling_config -- one row per sampling definition ─────────────
CREATE MULTISET TABLE {{META_DB}}.gre_sampling_config (
    config_id            INTEGER NOT NULL,
    project_name         VARCHAR(100) NOT NULL,
    process_name         VARCHAR(100) NOT NULL,
    sample_name          VARCHAR(100) NOT NULL,
    source_type          VARCHAR(20)  NOT NULL,   -- 'teradata' | 'postgres' | 's3' | 'file' -- picks
                                                    -- the one connection this config runs against; see
                                                    -- db/connection_factory.py (one connection per type,
                                                    -- no separate named-connection column)
    universe_table       VARCHAR(200) NOT NULL,   -- fully-qualified FROM target, e.g. "db.schema.table"
    key_columns          VARCHAR(500) NOT NULL,   -- entity key column(s), CSV
    scope_sql            CLOB,                    -- WHERE-fragment; may embed any number of
                                                    -- "{key}" OR "$key" tokens (e.g. {run_date}/
                                                    -- $run_date, {year}/$year, freely mixed),
                                                    -- substituted from run_params the same way
                                                    -- rules_engine's rule_syntax is -- see
                                                    -- sampling/db_ops.py::_substitute_params().
                                                    -- Defaults to '1=1' (whole table) if unset.
                                                    -- Kept as an explicit, hand-authored WHERE-
                                                    -- fragment (unlike rules_engine/schema.sql's
                                                    -- gre_rules, which has no scope_sql at all --
                                                    -- its total-record count is auto-derived from
                                                    -- run_params + database_name.src_tbl_nm)
                                                    -- because "which rows are this cycle's
                                                    -- candidates" isn't always a plain equality
                                                    -- filter (exclusions, date ranges, ...).
    exclusion_sql        CLOB,                    -- WHERE-fragment; matching rows are EXCLUDED.
                                                    -- Same "{key}"/"$key" run_params substitution as
                                                    -- scope_sql above (both go through
                                                    -- _substitute_params with the same dict).
    target_volume        INTEGER NOT NULL DEFAULT 150,
    sampling_method       VARCHAR(20) DEFAULT 'RANKED',  -- 'RANKED'|'RANDOM'|'SYSTEMATIC'
    priority_rank_sql     CLOB,                    -- required for RANKED/SYSTEMATIC; ORDER BY
                                                    -- expression, lowest = highest priority
    rounding_mode          VARCHAR(10) DEFAULT 'FLOOR',  -- 'FLOOR'|'ROUND'|'CEIL'
    schedule_cron            VARCHAR(50),
    act_ind                    BYTEINT DEFAULT 1,
    created_by                   VARCHAR(100),      -- purely descriptive/audit; mirrors
                                                    -- gre_rules.created_by for the same
                                                    -- reason -- see rules_engine/schema.sql
    last_updated_by                VARCHAR(100),    -- purely descriptive/audit; mirrors
                                                    -- gre_rules.last_updated_by
    load_datetime                TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (config_id);


-- ── 2. gre_sampling_strata -- one row per stratification LEVEL, in order ──
-- Replaces dq_sampling_config's two hardcoded columns (determination_column,
-- functional_area_column) with however many levels a project needs. Zero
-- rows for a config = no stratification -- straight to select() on the
-- whole candidate pool.
CREATE MULTISET TABLE {{META_DB}}.gre_sampling_strata (
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
ON {{META_DB}}.gre_sampling_strata;


-- ── 3. gre_sampling_mix -- one row per named bucket value, per level ─────
-- Replaces dq_sampling_config's JSON mix columns. A bucket_value present in
-- the data but NOT listed here for that level absorbs the remainder
-- fraction (1 - sum(named fractions)) -- same rule as the proven dq_*
-- pattern (core/stratified_sampling.py::_target_for_bucket), just as rows.
CREATE MULTISET TABLE {{META_DB}}.gre_sampling_mix (
    mix_id               INTEGER NOT NULL,
    strata_id            INTEGER NOT NULL,
    bucket_value          VARCHAR(200) NOT NULL,
    target_fraction         FLOAT NOT NULL
)
PRIMARY INDEX (mix_id);

CREATE INDEX gre_sampling_mix_strata_ix (strata_id)
ON {{META_DB}}.gre_sampling_mix;


-- ── 4. gre_sample_selections -- one row per candidate CONSIDERED ─────────
-- Every candidate, selected or not (audit defensibility). The row itself is
-- never updated after the run -- a rerun always uses a fresh sample_run_id
-- (unlike rules_engine's gre_exceptions/gre_results, which DO get updated
-- in place on rerun) -- but etl_is_curr_ind IS flipped after the fact: when
-- a later run reuses the same (config_id, run_key) -- e.g. today's cycle
-- gets re-executed -- the PRIOR sample_run_id's rows here are set to
-- etl_is_curr_ind='N' so exactly one run's selections read as "current" per
-- (config_id, run_key) at a time, while every prior attempt's rows stay in
-- the table for history/audit (soft-deactivate, never deleted -- same
-- convention as rules_engine/schema.sql's gre_exceptions.etl_is_curr_ind).
-- See sampling/sampling.py::_deactivate_prior_sampling_runs() -- prior
-- sample_run_id's are found via gre_sampling_audit (sample_config_id,
-- run_key), since run_key isn't stored directly here.
CREATE MULTISET TABLE {{META_DB}}.gre_sample_selections (
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
    etl_is_curr_ind                       CHAR(1) DEFAULT 'Y',  -- 'Y' = this run is the
                                                        -- current/active one for its
                                                        -- (config_id, run_key); 'N' = superseded
                                                        -- by a later rerun of the same run_key.
    load_datetime                          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_datetime                     TIMESTAMP  -- bumped only by the
                                                        -- etl_is_curr_ind deactivate UPDATE (NULL
                                                        -- until then) -- same purpose as
                                                        -- gre_exceptions.last_updated_datetime:
                                                        -- lets metadata_sync's incremental watermark
                                                        -- (COALESCE(last_updated_datetime,
                                                        -- load_datetime)) pick up a flip that
                                                        -- doesn't touch load_datetime.
)
PRIMARY INDEX (sample_run_id);

CREATE INDEX gre_sample_selections_lookup_ix (project_name, process_name, sample_cycle, selected_flag)
ON {{META_DB}}.gre_sample_selections;


-- ── 5. gre_sample_selection_attrs -- which bucket, at each level ─────────
-- One row per (sample_run_id, case_key, strata_id) -- deliberately keyed by
-- this natural composite rather than a fetched-back gre_sample_selections
-- surrogate id: Teradata has no RETURNING clause, so getting an
-- identity value back from the gre_sample_selections insert to use as a
-- foreign key here would need an extra round-trip query per row. case_key
-- is already unique within one sample_run_id, so this composite is just as
-- precise a join key. Replaces a JSON snapshot column with plain rows,
-- same no-JSON rule as everywhere else in this schema.
CREATE MULTISET TABLE {{META_DB}}.gre_sample_selection_attrs (
    sample_run_id         VARCHAR(200) NOT NULL,
    case_key              VARCHAR(500) NOT NULL,
    strata_id             INTEGER NOT NULL,
    level_order            INTEGER,          -- denormalized for ordered reads without a join
    bucket_value              VARCHAR(200),
    etl_is_curr_ind               CHAR(1) DEFAULT 'Y',  -- mirrors gre_sample_selections.
                                                    -- etl_is_curr_ind for the same sample_run_id
                                                    -- -- kept in lockstep by
                                                    -- _deactivate_prior_sampling_runs() so a
                                                    -- consumer can filter either table
                                                    -- independently.
    load_datetime                TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_datetime           TIMESTAMP  -- mirrors gre_sample_selections.
                                                    -- last_updated_datetime -- see that column's
                                                    -- comment.
)
PRIMARY INDEX (sample_run_id, case_key);


-- ── 6. gre_sampling_audit -- durable, one row per sampling run ────────────
-- Written by sampling/sampling.py::_write_audit() ONLY. Used to live in
-- shared/schema.sql, alongside rules_engine/'s equivalent gre_rule_audit
-- table (both were kept together there because they once shared a
-- combined gre_audit table -- see the git history around the 2026-08
-- gre_audit split). Now that rules_engine/ and sampling/ share no code or
-- tables at all (see README.md's "Package separation"), this table lives
-- here, where it's actually used.
CREATE MULTISET TABLE {{META_DB}}.gre_sampling_audit (
    run_id                 VARCHAR(200) NOT NULL,
    run_key                   VARCHAR(100),        -- caller-supplied tracking/idempotency key
    sample_config_id          INTEGER,             -- -> gre_sampling_config
    sampling_method            VARCHAR(20),        -- 'RANKED'|'RANDOM'|'SYSTEMATIC'
    random_seed                 BIGINT,             -- RANDOM/SYSTEMATIC only: the ONE seed used for
                                                     -- this whole run -- see sampling/sampling.py's
                                                     -- module docstring for how a single seed plus
                                                     -- deterministic (sorted) bucket processing order
                                                     -- reproduces every per-bucket offset/draw without
                                                     -- storing them separately
    target_volume                INTEGER,
    total_candidates              INTEGER,
    total_selected                 INTEGER,
    started_at                TIMESTAMP,
    ended_at                   TIMESTAMP,
    status                      VARCHAR(20),        -- 'RUNNING' | 'COMPLETED' | 'ERROR'
    triggered_by                 VARCHAR(100),
    load_datetime                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (run_id);

-- count_prior_attempts() and _deactivate_prior_sampling_runs() (both in
-- sampling/db_ops.py / sampling/sampling.py) both look this pair up every call.
CREATE INDEX gre_sampling_audit_config_run_key_ix (sample_config_id, run_key)
ON {{META_DB}}.gre_sampling_audit;

CREATE INDEX gre_sampling_audit_status_ix (status)
ON {{META_DB}}.gre_sampling_audit;


-- ── 7. gre_sampling_errors -- this package's own SQL/execution failure log
-- Used to be gre_errors, one table shared with rules_engine/ (a sampling
-- error had rule_id=NULL and its process_name overloaded into that
-- table's rule_group column, purely for triage). Now this package's own
-- -- an honest process_name column, no rule_id/rule_group at all (a
-- sampling run never had either). See README.md's "Package separation".
--
-- Append-only across reruns of the same run_key under a NEW run_id --
-- active_ind marks which error(s) belong to the CURRENT run_id for a
-- given run_key -- see sampling/db_ops.py::_deactivate_prior_errors(),
-- called from log_error() immediately before each new error row is
-- inserted, mirroring gre_sample_selections' etl_is_curr_ind
-- reconciliation. Never deletes -- full error history stays on file;
-- only active_ind flips.
CREATE MULTISET TABLE {{META_DB}}.gre_sampling_errors (
    error_id         BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id           VARCHAR(200),
    process_name     VARCHAR(100),         -- config's process_name, for triage -- see
                                            -- sampling/sampling.py::_log_sampling_error()
    run_key          VARCHAR(100),
    error_type       VARCHAR(50),          -- e.g. PULL_FAILURE | SELECT_PERSIST_FAILURE | STRATIFY_FAILURE
    error_message    VARCHAR(2000),
    error_detail     CLOB,
    active_ind           CHAR(1) DEFAULT 'Y',  -- 'Y' = belongs to the current run_id for this
                                                -- run_key; 'N' = superseded by a later rerun of
                                                -- the same run_key. See comment above.
    occurred_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_datetime TIMESTAMP             -- set only when active_ind flips to 'N'
)
PRIMARY INDEX (run_id);

-- Reporting/dashboard lookup: "what errors are current right now for this
-- run_key" -- filters straight to active_ind='Y' instead of every
-- historical error across every past run_id.
CREATE INDEX gre_sampling_errors_run_key_active_ix (run_key, active_ind)
ON {{META_DB}}.gre_sampling_errors;
