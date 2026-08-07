-- ============================================================
-- Data Quality Framework DDL — SAMPLING FRAMEWORK (sampling/)  (v7)
-- Schema : CMSUNIV_FILELAND_DEV_T  (DEV)
-- DB     : Teradata  (metadata store)
-- ============================================================
-- Run ddl_shared.sql FIRST — dq_sampling_config references dq_scope
-- defined there. This framework is independently deployable from the
-- rules engine (sampling/ imports rules_engine/ as a plain library for a
-- couple of shared helpers, but the reverse is never true — see
-- DESIGN.md). Its schema is exactly these two tables.
-- ============================================================

-- ============================================================
-- SAMPLING FRAMEWORK (sampling/) — dq_sampling_config and
-- dq_sample_selections are its only two tables.
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
