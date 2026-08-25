-- ============================================================
-- One-time migration: split the combined gre_audit table into
-- gre_rule_audit / gre_sampling_audit.
-- ============================================================
-- UPDATE (2026-08): rules_engine/ and sampling/ are now fully independent
-- packages that share no tables at all (see README.md's "Package
-- separation") -- as part of that work, the gre_audit compatibility VIEW
-- this script originally created in Step 4 was retired. The current
-- application code never reads or writes gre_audit in any form. Step 4
-- below is kept for a zero-downtime transition (so external readers of
-- gre_audit don't break the moment this script runs), but treat the view
-- it creates as TEMPORARY: drop it once every consumer has moved to
-- querying gre_rule_audit/gre_sampling_audit directly (see the commented
-- DROP VIEW at the very bottom of this file). Skip Step 4 entirely if you
-- have no external readers of gre_audit to worry about.
--
-- Use this INSTEAD OF rules_engine/schema_drop.sql + rules_engine/
-- schema.sql + sampling/schema_drop.sql + sampling/schema.sql if your
-- gre_audit table already holds real run history you don't want to lose
-- (the drop/recreate scripts are for a fresh or disposable-data
-- environment only -- see their own headers).
--
-- Zero data loss, zero downtime for readers: gre_audit keeps returning
-- results (from the old table, unchanged) right up until the Step 4
-- CREATE VIEW statement, at which point it starts returning the exact
-- same rows/shape, just sourced from the two new tables instead.
--
-- Run this as one script, top to bottom, in order:
--   1. Create the two new tables (see rules_engine/schema.sql and
--      sampling/schema.sql for the same DDL, with matching column
--      comments -- kept in sync manually).
--   2. Backfill each from the existing gre_audit, split by run_type.
--   3. Rename the old gre_audit table out of the way (kept, not dropped
--      -- see the cleanup note at the bottom).
--   4. (Optional, transitional) Create gre_audit as a view reproducing
--      the old shape exactly, for external readers only.
--
-- After running this, redeploy rules_engine/ and sampling/'s code (both
-- packages write to gre_rule_audit/gre_sampling_audit directly, never to
-- gre_audit in any form) -- this script only migrates the DATA/DDL, not
-- the application code.
-- ============================================================


-- ── Step 1: create the two new tables ───────────────────────────────────
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_rule_audit (
    run_id                 VARCHAR(200) NOT NULL,
    rule_group              VARCHAR(100),
    project_name              VARCHAR(100),
    process_name              VARCHAR(100),
    run_key                   VARCHAR(100),
    rule_variant               VARCHAR(100),
    started_at                TIMESTAMP,
    ended_at                   TIMESTAMP,
    status                      VARCHAR(20),
    total_rules                  INTEGER,
    rules_succeeded                INTEGER,
    rules_errored                    INTEGER,
    triggered_by                      VARCHAR(100),
    load_datetime                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (run_id);

CREATE INDEX gre_rule_audit_group_run_key_ix (rule_group, run_key)
ON CMSUNIV_FILELAND_DEV_T.gre_rule_audit;

CREATE INDEX gre_rule_audit_status_ix (status)
ON CMSUNIV_FILELAND_DEV_T.gre_rule_audit;


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_sampling_audit (
    run_id                 VARCHAR(200) NOT NULL,
    run_key                   VARCHAR(100),
    sample_config_id          INTEGER,
    sampling_method            VARCHAR(20),
    random_seed                 BIGINT,
    target_volume                INTEGER,
    total_candidates              INTEGER,
    total_selected                 INTEGER,
    started_at                TIMESTAMP,
    ended_at                   TIMESTAMP,
    status                      VARCHAR(20),
    triggered_by                 VARCHAR(100),
    load_datetime                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (run_id);

CREATE INDEX gre_sampling_audit_config_run_key_ix (sample_config_id, run_key)
ON CMSUNIV_FILELAND_DEV_T.gre_sampling_audit;

CREATE INDEX gre_sampling_audit_status_ix (status)
ON CMSUNIV_FILELAND_DEV_T.gre_sampling_audit;


-- ── Step 2: backfill from the existing gre_audit, split by run_type ────
-- run_type = 'RULE_GROUP' is the DEFAULT on the old table (see its
-- original DDL), so also catch any row where run_type somehow ended up
-- NULL but rule_group is populated -- belt-and-suspenders against a
-- pre-run_type-column-existing row, if this environment is old enough to
-- have one.
INSERT INTO CMSUNIV_FILELAND_DEV_T.gre_rule_audit (
    run_id, rule_group, project_name, process_name, run_key, rule_variant,
    started_at, ended_at, status, total_rules, rules_succeeded, rules_errored,
    triggered_by, load_datetime
)
SELECT
    run_id, rule_group, project_name, process_name, run_key, rule_variant,
    started_at, ended_at, status, total_rules, rules_succeeded, rules_errored,
    triggered_by, load_datetime
FROM CMSUNIV_FILELAND_DEV_T.gre_audit
WHERE run_type = 'RULE_GROUP' OR (run_type IS NULL AND rule_group IS NOT NULL);

INSERT INTO CMSUNIV_FILELAND_DEV_T.gre_sampling_audit (
    run_id, run_key, sample_config_id, sampling_method, random_seed,
    target_volume, total_candidates, total_selected,
    started_at, ended_at, status, triggered_by, load_datetime
)
SELECT
    run_id, run_key, sample_config_id, sampling_method, random_seed,
    target_volume, total_candidates, total_selected,
    started_at, ended_at, status, triggered_by, load_datetime
FROM CMSUNIV_FILELAND_DEV_T.gre_audit
WHERE run_type = 'SAMPLING';

-- Sanity check before proceeding -- both counts together should equal
-- gre_audit's total row count. If they don't, STOP here and investigate
-- (e.g. a row with run_type outside 'RULE_GROUP'/'SAMPLING', or
-- run_type NULL with rule_group also NULL -- neither backfill query above
-- would have caught it) before running Step 3, which is harder to undo.
--
-- SELECT
--     (SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.gre_audit) AS old_total,
--     (SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.gre_rule_audit)
--       + (SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.gre_sampling_audit) AS new_total;


-- ── Step 3: rename the old table out of the way ─────────────────────────
-- Kept, not dropped -- a view and a table can't share a name in Teradata,
-- so gre_audit the table has to move before gre_audit the view can be
-- created. Once you've spot-checked the new tables (and the view below)
-- against gre_audit_legacy, drop it manually whenever you're comfortable:
--   DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_audit_legacy;
RENAME TABLE CMSUNIV_FILELAND_DEV_T.gre_audit TO CMSUNIV_FILELAND_DEV_T.gre_audit_legacy;


-- ── Step 4 (optional, transitional): gre_audit becomes a view, same ────
-- shape as before -- SKIP this step if nothing outside the application
-- reads gre_audit; the app itself never will again.
CREATE VIEW CMSUNIV_FILELAND_DEV_T.gre_audit AS
SELECT
    run_id, 'RULE_GROUP' AS run_type,
    rule_group, project_name, process_name, run_key, rule_variant,
    started_at, ended_at, status,
    total_rules, rules_succeeded, rules_errored,
    CAST(NULL AS INTEGER)      AS sample_config_id,
    CAST(NULL AS VARCHAR(20))  AS sampling_method,
    CAST(NULL AS BIGINT)       AS random_seed,
    CAST(NULL AS INTEGER)      AS target_volume,
    CAST(NULL AS INTEGER)      AS total_candidates,
    CAST(NULL AS INTEGER)      AS total_selected,
    triggered_by, load_datetime
FROM CMSUNIV_FILELAND_DEV_T.gre_rule_audit
UNION ALL
SELECT
    run_id, 'SAMPLING' AS run_type,
    CAST(NULL AS VARCHAR(100)) AS rule_group,
    CAST(NULL AS VARCHAR(100)) AS project_name,
    CAST(NULL AS VARCHAR(100)) AS process_name,
    run_key,
    CAST(NULL AS VARCHAR(100)) AS rule_variant,
    started_at, ended_at, status,
    CAST(NULL AS INTEGER) AS total_rules,
    CAST(NULL AS INTEGER) AS rules_succeeded,
    CAST(NULL AS INTEGER) AS rules_errored,
    sample_config_id, sampling_method, random_seed,
    target_volume, total_candidates, total_selected,
    triggered_by, load_datetime
FROM CMSUNIV_FILELAND_DEV_T.gre_sampling_audit;


-- Once every external consumer of gre_audit (if you ran Step 4 at all)
-- has moved to querying gre_rule_audit/gre_sampling_audit directly,
-- retire the view -- the application code doesn't depend on it either way:
--   DROP VIEW CMSUNIV_FILELAND_DEV_T.gre_audit;
--   DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_audit_legacy;
--
-- ============================================================
-- After this script: redeploy rules_engine/ and sampling/'s application
-- code (rules_engine/db_ops.py, sampling/db_ops.py, rules_engine/runner.py,
-- sampling/sampling.py -- the versions that target
-- gre_rule_audit/gre_sampling_audit directly, and never gre_audit).
-- ============================================================
