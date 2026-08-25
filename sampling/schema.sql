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
-- Schema : CMSUNIV_FILELAND_DEV_T  (DEV)  -- same metadata store as dq_*
-- DB     : Teradata  (metadata store)
-- ============================================================
-- Deploy AFTER shared/schema.sql -- gre_audit (written to via
-- sampling/sampling.py::_write_audit) and gre_errors (written to via
-- shared/db_ops.py::log_error) are both defined there, not here.
--
-- Standalone from ddl.sql and rules_engine/schema.sql on purpose: this
-- file creates ONLY gre_sampling_*/gre_sample_*-prefixed objects. It
-- never touches, renames, or alters a single dq_* or rules_engine table,
-- index, or column.
-- ============================================================

-- ── 1. gre_sampling_config -- one row per sampling definition ─────────────
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_sampling_config (
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
                                                    -- "{key}" tokens (e.g. {run_date}, {year}),
                                                    -- substituted from run_params the same way
                                                    -- rules_engine's rule_syntax is -- see
                                                    -- shared/db_ops.py::_substitute_params().
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
                                                    -- Same "{key}" run_params substitution as
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


-- ── 3. gre_sampling_mix -- one row per named bucket value, per level ─────
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
-- sample_run_id's are found via gre_audit (run_type='SAMPLING',
-- sample_config_id, run_key), since run_key isn't stored directly here.
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
    etl_is_curr_ind                       CHAR(1) DEFAULT 'Y',  -- 'Y' = this run is the
                                                        -- current/active one for its
                                                        -- (config_id, run_key); 'N' = superseded
                                                        -- by a later rerun of the same run_key.
                                                        -- Existing deploy with real data: see
                                                        -- migrate_gre_sampling_reconciliation.sql at
                                                        -- the repo root (drop + recreate, not ALTER
                                                        -- TABLE).
    load_datetime                          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_datetime                     TIMESTAMP  -- bumped only by the
                                                        -- etl_is_curr_ind deactivate UPDATE (NULL
                                                        -- until then) -- same purpose as
                                                        -- gre_exceptions.last_updated_datetime:
                                                        -- lets metadata_sync's incremental watermark
                                                        -- (COALESCE(last_updated_datetime,
                                                        -- load_datetime)) pick up a flip that
                                                        -- doesn't touch load_datetime. Existing
                                                        -- deploy with real data: see
                                                        -- migrate_gre_sampling_reconciliation.sql at
                                                        -- the repo root (drop + recreate, not ALTER
                                                        -- TABLE).
)
PRIMARY INDEX (sample_run_id);

CREATE INDEX gre_sample_selections_lookup_ix (project_name, process_name, sample_cycle, selected_flag)
ON CMSUNIV_FILELAND_DEV_T.gre_sample_selections;


-- ── 5. gre_sample_selection_attrs -- which bucket, at each level ─────────
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
    etl_is_curr_ind               CHAR(1) DEFAULT 'Y',  -- mirrors gre_sample_selections.
                                                    -- etl_is_curr_ind for the same sample_run_id
                                                    -- -- kept in lockstep by
                                                    -- _deactivate_prior_sampling_runs() so a
                                                    -- consumer can filter either table
                                                    -- independently. Existing deploy with real
                                                    -- data: see migrate_gre_sampling_reconciliation.sql
                                                    -- at the repo root (drop + recreate, not ALTER
                                                    -- TABLE).
    load_datetime                TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_datetime           TIMESTAMP  -- mirrors gre_sample_selections.
                                                    -- last_updated_datetime -- see that column's
                                                    -- comment. Existing deploy with real data: see
                                                    -- migrate_gre_sampling_reconciliation.sql at the
                                                    -- repo root (drop + recreate, not ALTER TABLE).
)
PRIMARY INDEX (sample_run_id, case_key);
