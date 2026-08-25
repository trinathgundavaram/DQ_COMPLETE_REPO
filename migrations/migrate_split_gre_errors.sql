-- ============================================================
-- One-time migration: split the combined gre_errors table into
-- gre_rule_errors / gre_sampling_errors.
-- ============================================================
-- Same split, same rationale, same shape as migrate_split_gre_audit.sql
-- (see that file's header) -- rules_engine/ and sampling/ are fully
-- independent packages that share no tables (see README.md's "Package
-- separation"). gre_rule_errors keeps rule_id/rule_group (always
-- populated for rules_engine); gre_sampling_errors drops rule_id
-- entirely and carries process_name instead (sampling has no rule
-- concept, and the old combined table's rule_group column was being
-- repurposed to hold sampling's process_name -- this migration gives it
-- an honest column name instead of continuing that repurposing).
--
-- error_id is a GENERATED ALWAYS AS IDENTITY column, so it is
-- deliberately omitted from the INSERT...SELECT column lists below --
-- both new tables mint fresh IDs for the backfilled rows rather than
-- copying the old ones. Nothing in the codebase references error_id as a
-- foreign key outside test files, so this is safe (same reasoning as
-- migrate_gre_log_errors_results.sql's backfill).
--
-- Use this INSTEAD OF rules_engine/schema_drop.sql + rules_engine/
-- schema.sql + sampling/schema_drop.sql + sampling/schema.sql if your
-- gre_errors table already holds real error history you don't want to
-- lose (the drop/recreate scripts are for a fresh or disposable-data
-- environment only -- see their own headers).
--
-- Run this as one script, top to bottom, in order:
--   1. Create the two new tables (see rules_engine/schema.sql and
--      sampling/schema.sql for the same DDL -- kept in sync manually).
--   2. Backfill each from the existing gre_errors: rule_id IS NOT NULL
--      rows go to gre_rule_errors, rule_id IS NULL rows go to
--      gre_sampling_errors (process_name has no source column on the old
--      table -- it comes across NULL; backfilled sampling error rows
--      will need process_name populated by hand if that distinction
--      matters for historical rows).
--   3. Rename the old gre_errors table out of the way (kept, not dropped
--      -- see the cleanup note at the bottom).
--
-- No compatibility view is created for the old gre_errors name -- unlike
-- the gre_audit split, this migration does not offer a transitional
-- Step 4, since gre_errors was never intended as a stable read target
-- for anything outside the two packages that write to it (compare
-- gre_audit, which genuinely had external readers). If you do have an
-- external consumer of gre_errors, model a compatibility view yourself
-- off migrate_split_gre_audit.sql's Step 4 -- the same UNION ALL
-- approach applies (rule_id/rule_group NULL on sampling rows,
-- process_name NULL on rule-engine rows).
--
-- After running this, redeploy rules_engine/ and sampling/'s application
-- code (rules_engine/db_ops.py, sampling/db_ops.py -- the versions that
-- target gre_rule_errors/gre_sampling_errors directly, and never
-- gre_errors) -- this script only migrates the DATA/DDL, not the
-- application code.
-- ============================================================


-- ── Step 1: create the two new tables ───────────────────────────────────
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_rule_errors (
    error_id         BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id           VARCHAR(200),
    rule_id          INTEGER NOT NULL,
    rule_group       VARCHAR(100),
    run_key          VARCHAR(100),
    error_type       VARCHAR(50),
    error_message    VARCHAR(2000),
    error_detail     CLOB,
    active_ind           CHAR(1) DEFAULT 'Y',
    occurred_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_datetime TIMESTAMP
)
PRIMARY INDEX (run_id, rule_id);

CREATE INDEX gre_rule_errors_rule_run_key_active_ix (rule_id, run_key, active_ind)
ON CMSUNIV_FILELAND_DEV_T.gre_rule_errors;


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_sampling_errors (
    error_id         BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id           VARCHAR(200),
    process_name     VARCHAR(100),
    run_key          VARCHAR(100),
    error_type       VARCHAR(50),
    error_message    VARCHAR(2000),
    error_detail     CLOB,
    active_ind           CHAR(1) DEFAULT 'Y',
    occurred_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_datetime TIMESTAMP
)
PRIMARY INDEX (run_id);

CREATE INDEX gre_sampling_errors_run_key_active_ix (run_key, active_ind)
ON CMSUNIV_FILELAND_DEV_T.gre_sampling_errors;


-- ── Step 2: backfill from the existing gre_errors ───────────────────────
-- error_id omitted -- fresh IDs are minted (GENERATED ALWAYS AS IDENTITY,
-- see header note above).
INSERT INTO CMSUNIV_FILELAND_DEV_T.gre_rule_errors (
    run_id, rule_id, rule_group, run_key, error_type, error_message,
    error_detail, active_ind, occurred_at, last_updated_datetime
)
SELECT
    run_id, rule_id, rule_group, run_key, error_type, error_message,
    error_detail, active_ind, occurred_at, last_updated_datetime
FROM CMSUNIV_FILELAND_DEV_T.gre_errors
WHERE rule_id IS NOT NULL;

INSERT INTO CMSUNIV_FILELAND_DEV_T.gre_sampling_errors (
    run_id, process_name, run_key, error_type, error_message,
    error_detail, active_ind, occurred_at, last_updated_datetime
)
SELECT
    run_id, CAST(NULL AS VARCHAR(100)) AS process_name, run_key, error_type,
    error_message, error_detail, active_ind, occurred_at, last_updated_datetime
FROM CMSUNIV_FILELAND_DEV_T.gre_errors
WHERE rule_id IS NULL;

-- Sanity check before proceeding -- both counts together should equal
-- gre_errors's total row count.
--
-- SELECT
--     (SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.gre_errors) AS old_total,
--     (SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.gre_rule_errors)
--       + (SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.gre_sampling_errors) AS new_total;


-- ── Step 3: rename the old table out of the way ─────────────────────────
-- Kept, not dropped -- spot-check the new tables against gre_errors_legacy
-- first, then drop it manually whenever you're comfortable:
--   DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_errors_legacy;
RENAME TABLE CMSUNIV_FILELAND_DEV_T.gre_errors TO CMSUNIV_FILELAND_DEV_T.gre_errors_legacy;
