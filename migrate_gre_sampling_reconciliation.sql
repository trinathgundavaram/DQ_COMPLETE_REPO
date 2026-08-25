-- ============================================================
-- One-time migration: recreate gre_sample_selections /
-- gre_sample_selection_attrs with their etl_is_curr_ind /
-- last_updated_datetime reconciliation columns, via DROP + CREATE --
-- not ALTER TABLE.
-- ============================================================
-- Replaces the old alter_sampling_tables.sql (which used ALTER TABLE ...
-- ADD COLUMN, which this repo's DDL policy no longer allows -- see
-- README.md's "Redeploying / changing the schema": schema changes are
-- made by editing schema.sql's CREATE statements directly and redeploying
-- via drop-then-recreate).
--
-- Use this INSTEAD OF sampling/schema_drop.sql + sampling/schema.sql if
-- gre_sample_selections/gre_sample_selection_attrs already hold real
-- sampling history you don't want to lose (those drop/recreate scripts
-- are for a fresh or disposable-data environment only -- see their own
-- headers). If you're on a fresh environment with nothing worth keeping
-- in these two tables, just run the schema_drop.sql + schema.sql pair
-- instead -- it's simpler and this file is unnecessary.
--
-- Zero data loss: every existing row is copied into the new table before
-- the old one is renamed out of the way. Not zero-downtime -- there's no
-- way to keep the ORIGINAL name resolving to a working table for the
-- couple of seconds between "rename old out of the way" and "create new"
-- (Teradata has no online-DDL swap for this); run during a maintenance
-- window.
--
-- No IDENTITY columns on either table (sample_run_id/case_key/strata_id
-- are caller-supplied, not generated), so unlike
-- migrate_gre_log_errors_results.sql, the backfill below copies every
-- column verbatim, including the natural keys.
--
-- Run this as one script, top to bottom, in order:
--   1. Rename the two existing tables out of the way.
--   2. Create the two new tables (identical to sampling/schema.sql's
--      gre_sample_selections/gre_sample_selection_attrs -- kept in sync
--      manually).
--   3. Backfill each from its renamed-away predecessor.
--   4. (Optional, commented) Sanity-check counts.
--   5. Drop the two renamed-away legacy tables -- see Step 5 below for
--      the exact statements; commented out so nothing is destroyed by
--      just running this script once, uncomment when you're ready.
--
-- After running this, redeploy sampling/'s application code
-- (sampling/sampling.py -- the version that reads/writes
-- etl_is_curr_ind/last_updated_datetime on rerun reconciliation). Nothing
-- else needs to change.
-- ============================================================


-- ── Step 1: rename the existing tables out of the way ───────────────────
RENAME TABLE CMSUNIV_FILELAND_DEV_T.gre_sample_selections
    TO CMSUNIV_FILELAND_DEV_T.gre_sample_selections_legacy;
RENAME TABLE CMSUNIV_FILELAND_DEV_T.gre_sample_selection_attrs
    TO CMSUNIV_FILELAND_DEV_T.gre_sample_selection_attrs_legacy;


-- ── Step 2: create the two new tables (final shape, from schema.sql) ────
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_sample_selections (
    sample_run_id         VARCHAR(200) NOT NULL,
    config_id             INTEGER,
    project_name          VARCHAR(100),
    process_name          VARCHAR(100),
    sample_cycle           DATE,
    case_key                 VARCHAR(500) NOT NULL,
    priority_rank              INTEGER,
    excluded_flag                 BYTEINT DEFAULT 0,
    exclusion_reason                 VARCHAR(500),
    selected_flag                       BYTEINT DEFAULT 0,
    etl_is_curr_ind                       CHAR(1) DEFAULT 'Y',
    load_datetime                          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_datetime                     TIMESTAMP
)
PRIMARY INDEX (sample_run_id);

CREATE INDEX gre_sample_selections_lookup_ix (project_name, process_name, sample_cycle, selected_flag)
ON CMSUNIV_FILELAND_DEV_T.gre_sample_selections;


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_sample_selection_attrs (
    sample_run_id         VARCHAR(200) NOT NULL,
    case_key              VARCHAR(500) NOT NULL,
    strata_id             INTEGER NOT NULL,
    level_order            INTEGER,
    bucket_value              VARCHAR(200),
    etl_is_curr_ind               CHAR(1) DEFAULT 'Y',
    load_datetime                TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_datetime           TIMESTAMP
)
PRIMARY INDEX (sample_run_id, case_key);


-- ── Step 3: backfill from each renamed-away predecessor ─────────────────
-- Every pre-existing row gets etl_is_curr_ind='Y' -- same one-time-backfill
-- trade-off the old alter_sampling_tables.sql already accepted: nothing
-- was "current vs superseded" before this reconciliation feature existed,
-- so every pre-existing row looks current until the next rerun of its
-- (config_id, run_key) correctly deactivates whichever of these rows it
-- supersedes.
INSERT INTO CMSUNIV_FILELAND_DEV_T.gre_sample_selections (
    sample_run_id, config_id, project_name, process_name, sample_cycle,
    case_key, priority_rank, excluded_flag, exclusion_reason, selected_flag,
    etl_is_curr_ind, load_datetime, last_updated_datetime
)
SELECT
    sample_run_id, config_id, project_name, process_name, sample_cycle,
    case_key, priority_rank, excluded_flag, exclusion_reason, selected_flag,
    'Y', load_datetime, CAST(NULL AS TIMESTAMP)
FROM CMSUNIV_FILELAND_DEV_T.gre_sample_selections_legacy;

INSERT INTO CMSUNIV_FILELAND_DEV_T.gre_sample_selection_attrs (
    sample_run_id, case_key, strata_id, level_order, bucket_value,
    etl_is_curr_ind, load_datetime, last_updated_datetime
)
SELECT
    sample_run_id, case_key, strata_id, level_order, bucket_value,
    'Y', load_datetime, CAST(NULL AS TIMESTAMP)
FROM CMSUNIV_FILELAND_DEV_T.gre_sample_selection_attrs_legacy;


-- ── Step 4: sanity check before Step 5 (uncomment and run) ──────────────
-- Row counts on each side should match exactly.
--
-- SELECT
--     (SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.gre_sample_selections_legacy) AS selections_old,
--     (SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.gre_sample_selections) AS selections_new,
--     (SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.gre_sample_selection_attrs_legacy) AS attrs_old,
--     (SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.gre_sample_selection_attrs) AS attrs_new;


-- ── Step 5: drop the legacy tables once you've verified Step 4 ──────────
-- Commented out on purpose -- nothing is destroyed by running this script
-- as-is. Uncomment and run these two once you're satisfied the new
-- tables are correct.
--
-- DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_sample_selections_legacy;
-- DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_sample_selection_attrs_legacy;
