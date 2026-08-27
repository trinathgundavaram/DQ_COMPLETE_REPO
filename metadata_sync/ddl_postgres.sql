-- Postgres mirror of the gre_* metadata tables. Read-only from the rest
-- of the app's perspective -- only metadata_sync/ writes here.
-- {{SCHEMA}} is substituted by create_postgres_tables.py from
-- METADATA_SYNC_PG_SCHEMA (default gre_mirror). Types translated from the
-- Teradata source (rules_engine/schema.sql, sampling/schema.sql -- these
-- two packages are fully independent and share no tables, see README.md's
-- "Package separation"): CLOB -> TEXT, BYTEINT -> SMALLINT, FLOAT ->
-- DOUBLE PRECISION. IDENTITY columns (log_id, record_id, result_id,
-- error_id) are plain BIGINT -- values are copied verbatim, never minted
-- here.

CREATE SCHEMA IF NOT EXISTS {{SCHEMA}};

CREATE TABLE IF NOT EXISTS {{SCHEMA}}.metadata_sync_watermark (
    table_name      VARCHAR(100) PRIMARY KEY,
    last_watermark  TIMESTAMP,
    last_synced_at  TIMESTAMP,
    last_row_count  BIGINT
);

CREATE TABLE IF NOT EXISTS {{SCHEMA}}.gre_rules (
    rule_id                INTEGER PRIMARY KEY,
    rule_nm                VARCHAR(500) NOT NULL,
    act_ind                SMALLINT,
    rule_group             VARCHAR(100) NOT NULL,
    rule_variant           VARCHAR(100),
    project_name           VARCHAR(100) NOT NULL,
    process_name           VARCHAR(100) NOT NULL,
    seq_no                 INTEGER,
    sequencing_mode        VARCHAR(20),
    on_failure             VARCHAR(20),
    database_name          VARCHAR(200) NOT NULL,
    src_tbl_nm             VARCHAR(200) NOT NULL,
    sql_dialect            VARCHAR(20)  NOT NULL,
    rule_syntax            TEXT NOT NULL,
    src_key_cols           VARCHAR(500) NOT NULL,
    element_name           VARCHAR(200),
    threshold_pct          DOUBLE PRECISION,
    threshold_count        INTEGER,
    threshold_operator     CHAR(3),
    severity               VARCHAR(50),
    universe_version       VARCHAR(50),
    universe_year          INTEGER,
    dgr_nbr                VARCHAR(50),
    issue_category_name    VARCHAR(200),
    business_rule          VARCHAR(2000),
    rule_description       VARCHAR(2000),
    created_by              VARCHAR(100),
    last_updated_by          VARCHAR(100),
    load_datetime             TIMESTAMP,
    last_updated_datetime      TIMESTAMP
);
CREATE INDEX IF NOT EXISTS gre_rules_group_variant_ix
    ON {{SCHEMA}}.gre_rules (rule_group, act_ind, rule_variant);

CREATE TABLE IF NOT EXISTS {{SCHEMA}}.gre_exceptions (
    record_id             BIGINT PRIMARY KEY,
    run_id                VARCHAR(200),
    run_key               VARCHAR(100) NOT NULL,
    rule_id               INTEGER NOT NULL,
    rule_nm               VARCHAR(500),
    database_name         VARCHAR(200),
    src_tbl_nm            VARCHAR(200),
    project_name          VARCHAR(200),
    process_name          VARCHAR(200),
    element_name          VARCHAR(200),
    source_name           VARCHAR(100),
    issue_desc            VARCHAR(2000),
    src_key_value         VARCHAR(1000) NOT NULL,
    dgr_nbr               VARCHAR(50),
    universe_version      VARCHAR(50),
    run_type              VARCHAR(50),
    batch_schedule        VARCHAR(100),
    exception_flag        VARCHAR(20),
    exception_approver    VARCHAR(100),
    etl_is_curr_ind       CHAR(1),
    etl_load_dt           DATE,
    etl_last_updt_dt      TIMESTAMP,
    load_datetime         TIMESTAMP,
    last_updated_by       VARCHAR(100),
    last_updated_datetime TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS gre_exceptions_uix
    ON {{SCHEMA}}.gre_exceptions (rule_id, run_key, src_key_value);
-- the point of the mirror: fast tie-back join to a Postgres source table
CREATE INDEX IF NOT EXISTS gre_exceptions_src_key_value_ix
    ON {{SCHEMA}}.gre_exceptions (src_key_value);

-- Consolidates the old gre_log (removed) -- one row per rule PER
-- EXECUTION ATTEMPT (run_id), not per rule_id+run_key, with active_ind
-- marking the current attempt for a given (rule_id, run_key). See
-- rules_engine/schema.sql's "gre_results" section for the full
-- rationale.
CREATE TABLE IF NOT EXISTS {{SCHEMA}}.gre_results (
    result_id                BIGINT PRIMARY KEY,
    run_id                   VARCHAR(200) NOT NULL,
    rule_id                  INTEGER NOT NULL,
    rule_group               VARCHAR(100),
    project_name             VARCHAR(100),
    process_name             VARCHAR(100),
    run_key                  VARCHAR(100) NOT NULL,
    seq_no                   INTEGER,
    start_time               TIMESTAMP,
    end_time                 TIMESTAMP,
    total_records            BIGINT,
    failed_records           BIGINT,
    failure_pct              DOUBLE PRECISION,
    threshold_pct_used       DOUBLE PRECISION,
    threshold_count_used     INTEGER,
    threshold_operator_used  CHAR(3),
    severity                 VARCHAR(50),
    status                   VARCHAR(10),
    error_message            VARCHAR(2000),
    executed_sql             TEXT,
    source_tieback_sql       TEXT,
    active_ind               CHAR(1),
    load_datetime            TIMESTAMP,
    last_updated_datetime    TIMESTAMP
);
CREATE INDEX IF NOT EXISTS gre_results_group_run_key_ix
    ON {{SCHEMA}}.gre_results (rule_group, run_key, status);
CREATE INDEX IF NOT EXISTS gre_results_rule_run_key_active_ix
    ON {{SCHEMA}}.gre_results (rule_id, run_key, active_ind);

-- gre_audit split into gre_rule_audit / gre_sampling_audit (2026-08) --
-- mirrors the same split on the Teradata side. rules_engine/ and
-- sampling/ are now fully independent packages that share no tables (see
-- README.md's "Package separation"), so unlike the first cut of this
-- split there is no gre_audit compatibility VIEW here either -- a
-- consumer that needs both run-tracking tables together should query
-- gre_rule_audit and gre_sampling_audit separately, or UNION them itself.
-- metadata_sync/tables.py syncs these two tables directly
-- (GRE_RULE_AUDIT/GRE_SAMPLING_AUDIT specs).
CREATE TABLE IF NOT EXISTS {{SCHEMA}}.gre_rule_audit (
    run_id              VARCHAR(200) PRIMARY KEY,
    rule_group          VARCHAR(100),
    rule_variant        VARCHAR(100),
    project_name        VARCHAR(100),
    process_name        VARCHAR(100),
    run_key             VARCHAR(100),
    run_params          TEXT,       -- JSON-encoded run_params dict for this run, NULL if none
    extra_filters       TEXT,       -- JSON-encoded extra_filters dict for this run, NULL if none
    started_at          TIMESTAMP,
    ended_at            TIMESTAMP,
    status              VARCHAR(20),
    total_rules         INTEGER,
    rules_succeeded     INTEGER,
    rules_errored       INTEGER,
    triggered_by        VARCHAR(100),
    load_datetime       TIMESTAMP
);
CREATE INDEX IF NOT EXISTS gre_rule_audit_status_ix ON {{SCHEMA}}.gre_rule_audit (status);
CREATE INDEX IF NOT EXISTS gre_rule_audit_group_run_key_ix ON {{SCHEMA}}.gre_rule_audit (rule_group, run_key);

CREATE TABLE IF NOT EXISTS {{SCHEMA}}.gre_sampling_audit (
    run_id              VARCHAR(200) PRIMARY KEY,
    run_key             VARCHAR(100),
    sample_config_id    INTEGER,
    sampling_method     VARCHAR(20),
    random_seed         BIGINT,
    target_volume       INTEGER,
    total_candidates    INTEGER,
    total_selected      INTEGER,
    started_at          TIMESTAMP,
    ended_at            TIMESTAMP,
    status              VARCHAR(20),
    triggered_by        VARCHAR(100),
    load_datetime       TIMESTAMP
);
CREATE INDEX IF NOT EXISTS gre_sampling_audit_status_ix ON {{SCHEMA}}.gre_sampling_audit (status);
CREATE INDEX IF NOT EXISTS gre_sampling_audit_config_run_key_ix ON {{SCHEMA}}.gre_sampling_audit (sample_config_id, run_key);

-- Drop any leftover pre-split gre_audit object from an older mirror --
-- there is no replacement TABLE or VIEW under this name any more (see the
-- comment above); this is purely cleanup so a rerun of this script
-- against an old mirror doesn't leave a stale, now-orphaned object
-- sitting around under the retired name.
DROP VIEW IF EXISTS {{SCHEMA}}.gre_audit;
DROP TABLE IF EXISTS {{SCHEMA}}.gre_audit;

-- gre_errors split into gre_rule_errors / gre_sampling_errors (2026-08),
-- same split/rationale as gre_audit above. No compatibility view for the
-- old gre_errors name either -- see the comment above gre_rule_audit.
DROP TABLE IF EXISTS {{SCHEMA}}.gre_errors;

CREATE TABLE IF NOT EXISTS {{SCHEMA}}.gre_rule_errors (
    error_id         BIGINT PRIMARY KEY,
    run_id           VARCHAR(200),
    rule_id          INTEGER,
    rule_group       VARCHAR(100),
    run_key          VARCHAR(100),
    error_type       VARCHAR(50),
    error_message    VARCHAR(2000),
    error_detail     TEXT,
    active_ind       CHAR(1),
    occurred_at      TIMESTAMP,
    last_updated_datetime TIMESTAMP
);
CREATE INDEX IF NOT EXISTS gre_rule_errors_run_rule_ix ON {{SCHEMA}}.gre_rule_errors (run_id, rule_id);
CREATE INDEX IF NOT EXISTS gre_rule_errors_rule_run_key_active_ix
    ON {{SCHEMA}}.gre_rule_errors (rule_id, run_key, active_ind);

CREATE TABLE IF NOT EXISTS {{SCHEMA}}.gre_sampling_errors (
    error_id         BIGINT PRIMARY KEY,
    run_id           VARCHAR(200),
    process_name     VARCHAR(100),
    run_key          VARCHAR(100),
    error_type       VARCHAR(50),
    error_message    VARCHAR(2000),
    error_detail     TEXT,
    active_ind       CHAR(1),
    occurred_at      TIMESTAMP,
    last_updated_datetime TIMESTAMP
);
CREATE INDEX IF NOT EXISTS gre_sampling_errors_run_key_active_ix
    ON {{SCHEMA}}.gre_sampling_errors (run_key, active_ind);

CREATE TABLE IF NOT EXISTS {{SCHEMA}}.gre_sampling_config (
    config_id           INTEGER PRIMARY KEY,
    act_ind             SMALLINT,
    project_name        VARCHAR(100) NOT NULL,
    process_name        VARCHAR(100) NOT NULL,
    sample_name         VARCHAR(100) NOT NULL,
    source_type         VARCHAR(20)  NOT NULL,
    universe_table      VARCHAR(200) NOT NULL,
    key_columns         VARCHAR(500) NOT NULL,
    scope_sql           TEXT,
    exclusion_sql       TEXT,
    target_volume       INTEGER,
    sampling_method     VARCHAR(20),
    priority_rank_sql   TEXT,
    rounding_mode       VARCHAR(10),
    schedule_cron       VARCHAR(50),
    created_by          VARCHAR(100),
    last_updated_by     VARCHAR(100),
    load_datetime       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS {{SCHEMA}}.gre_sampling_strata (
    strata_id        INTEGER PRIMARY KEY,
    config_id        INTEGER NOT NULL,
    level_order      INTEGER NOT NULL,
    level_name       VARCHAR(100),
    stratify_expr    VARCHAR(1000) NOT NULL
);
CREATE INDEX IF NOT EXISTS gre_sampling_strata_config_ix
    ON {{SCHEMA}}.gre_sampling_strata (config_id, level_order);

CREATE TABLE IF NOT EXISTS {{SCHEMA}}.gre_sampling_mix (
    mix_id           INTEGER PRIMARY KEY,
    strata_id        INTEGER NOT NULL,
    bucket_value     VARCHAR(200) NOT NULL,
    target_fraction  DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS gre_sampling_mix_strata_ix
    ON {{SCHEMA}}.gre_sampling_mix (strata_id);

CREATE TABLE IF NOT EXISTS {{SCHEMA}}.gre_sample_selections (
    sample_run_id      VARCHAR(200) NOT NULL,
    config_id          INTEGER,
    project_name       VARCHAR(100),
    process_name       VARCHAR(100),
    sample_cycle       DATE,
    case_key           VARCHAR(500) NOT NULL,
    priority_rank      INTEGER,
    selected_flag      SMALLINT,
    excluded_flag      SMALLINT,
    exclusion_reason   VARCHAR(500),
    etl_is_curr_ind    CHAR(1),
    load_datetime      TIMESTAMP,
    last_updated_datetime TIMESTAMP,
    PRIMARY KEY (sample_run_id, case_key)
);
ALTER TABLE {{SCHEMA}}.gre_sample_selections ADD COLUMN IF NOT EXISTS etl_is_curr_ind CHAR(1);
ALTER TABLE {{SCHEMA}}.gre_sample_selections ADD COLUMN IF NOT EXISTS last_updated_datetime TIMESTAMP;
CREATE INDEX IF NOT EXISTS gre_sample_selections_lookup_ix
    ON {{SCHEMA}}.gre_sample_selections (project_name, process_name, sample_cycle, selected_flag);

CREATE TABLE IF NOT EXISTS {{SCHEMA}}.gre_sample_selection_attrs (
    sample_run_id    VARCHAR(200) NOT NULL,
    case_key         VARCHAR(500) NOT NULL,
    strata_id        INTEGER NOT NULL,
    level_order      INTEGER,
    bucket_value     VARCHAR(200),
    etl_is_curr_ind  CHAR(1),
    load_datetime    TIMESTAMP,
    last_updated_datetime TIMESTAMP,
    PRIMARY KEY (sample_run_id, case_key, strata_id)
);
ALTER TABLE {{SCHEMA}}.gre_sample_selection_attrs ADD COLUMN IF NOT EXISTS etl_is_curr_ind CHAR(1);
ALTER TABLE {{SCHEMA}}.gre_sample_selection_attrs ADD COLUMN IF NOT EXISTS last_updated_datetime TIMESTAMP;
