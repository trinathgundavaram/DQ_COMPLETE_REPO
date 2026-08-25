-- ============================================================
-- One-time migration: recreate gre_log / gre_errors / gre_results with
-- their active_ind (gre_log/gre_errors/gre_results) and source_tieback_sql
-- (gre_results) columns, via DROP + CREATE -- not ALTER TABLE.
-- ============================================================
-- Replaces the old alter_gre_log_errors_results.sql / alter_gre_results.sql
-- (both used ALTER TABLE ... ADD COLUMN, which this repo's DDL policy no
-- longer allows -- see README.md's "Redeploying / changing the schema":
-- schema changes are made by editing schema.sql's CREATE statements
-- directly and redeploying via drop-then-recreate). Both old scripts
-- targeted gre_results with two DIFFERENT columns at two different times;
-- since a recreate always rebuilds a table to its FULL current shape (not
-- an incremental delta), this one file now recreates gre_results ONCE,
-- with both columns already present, instead of two separate ALTERs.
--
-- Use this INSTEAD OF rules_engine/schema_drop.sql + rules_engine/schema.sql
-- and shared/schema_drop.sql + shared/schema.sql if gre_log/gre_errors/
-- gre_results already hold real run history you don't want to lose (those
-- drop/recreate scripts are for a fresh or disposable-data environment
-- only -- see their own headers). If you're on a fresh environment with
-- nothing worth keeping in these three tables, just run the schema_drop.sql
-- + schema.sql pair instead -- it's simpler and this file is unnecessary.
--
-- Zero data loss: every existing row is copied into the new table before
-- the old one is renamed out of the way. Not zero-downtime -- unlike
-- migrate_split_gre_audit.sql's rename-then-view swap, there's no way to
-- keep the ORIGINAL name resolving to a working table for the couple of
-- seconds between "rename old out of the way" and "create new" here
-- (Teradata has no ALTER TABLE ... RENAME COLUMN or online-DDL swap for
-- this); run during a maintenance window.
--
-- log_id / error_id / result_id are GENERATED ALWAYS AS IDENTITY on all
-- three tables -- Teradata does not allow inserting explicit values into a
-- GENERATED ALWAYS identity column without OVERRIDING SYSTEM VALUE, and
-- nothing in this codebase stores or joins on any of these three as a
-- foreign key elsewhere (grep the repo for `.log_id`/`.error_id`/
-- `.result_id` outside tests/ -- nothing references them), so the backfill
-- below deliberately OMITS these columns and lets fresh identity values
-- generate. Every other column, plus each row's original relative order
-- (via ORDER BY on the old table's own identity column), is preserved
-- exactly.
--
-- Run this as one script, top to bottom, in order:
--   1. Rename the three existing tables out of the way.
--   2. Create the three new tables (identical to rules_engine/schema.sql's
--      gre_log/gre_results and shared/schema.sql's gre_errors -- kept in
--      sync manually).
--   3. Backfill each from its renamed-away predecessor.
--   4. (Optional, commented) Sanity-check counts.
--   5. Drop the three renamed-away legacy tables -- see Step 5 below for
--      the exact statements; commented out so nothing is destroyed by
--      just running this script once, uncomment when you're ready.
--
-- After running this, redeploy rules_engine/ and shared/'s application
-- code (rules_engine/executor.py, shared/db_ops.py -- the versions that
-- read/write active_ind/source_tieback_sql). Nothing else needs to change.
-- ============================================================


-- ── Step 1: rename the existing tables out of the way ───────────────────
RENAME TABLE CMSUNIV_FILELAND_DEV_T.gre_log TO CMSUNIV_FILELAND_DEV_T.gre_log_legacy;
RENAME TABLE CMSUNIV_FILELAND_DEV_T.gre_errors TO CMSUNIV_FILELAND_DEV_T.gre_errors_legacy;
RENAME TABLE CMSUNIV_FILELAND_DEV_T.gre_results TO CMSUNIV_FILELAND_DEV_T.gre_results_legacy;


-- ── Step 2: create the three new tables (final shape, from schema.sql) ──
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_log (
    log_id           BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id           VARCHAR(200) NOT NULL,
    rule_id          INTEGER NOT NULL,
    rule_group       VARCHAR(100),
    project_name     VARCHAR(100),
    process_name     VARCHAR(100),
    run_key          VARCHAR(100) NOT NULL,
    seq_no           INTEGER,
    start_time       TIMESTAMP,
    end_time         TIMESTAMP,
    status           VARCHAR(20),
    rowcount         BIGINT,
    error_message    VARCHAR(2000),
    active_ind           CHAR(1) DEFAULT 'Y',
    load_datetime    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_datetime TIMESTAMP
)
PRIMARY INDEX (run_id, rule_id);

CREATE INDEX gre_log_rule_run_key_active_ix (rule_id, run_key, active_ind)
ON CMSUNIV_FILELAND_DEV_T.gre_log;

CREATE INDEX gre_log_group_run_key_ix (rule_group, run_key, status)
ON CMSUNIV_FILELAND_DEV_T.gre_log;


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_errors (
    error_id         BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id           VARCHAR(200),
    rule_id          INTEGER,
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

CREATE INDEX gre_errors_rule_run_key_active_ix (rule_id, run_key, active_ind)
ON CMSUNIV_FILELAND_DEV_T.gre_errors;


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_results (
    result_id                 BIGINT GENERATED ALWAYS AS IDENTITY,
    rule_id                    INTEGER NOT NULL,
    run_key                    VARCHAR(100) NOT NULL,
    run_id                     VARCHAR(200) NOT NULL,
    project_name                VARCHAR(100),
    process_name                VARCHAR(100),
    total_records               BIGINT,
    failed_records              BIGINT,
    failure_pct                 FLOAT,
    threshold_pct_used          FLOAT,
    threshold_count_used        INTEGER,
    threshold_operator_used     CHAR(3),
    severity                    VARCHAR(50),
    status                      VARCHAR(10),
    source_tieback_sql          CLOB,
    active_ind                  CHAR(1) DEFAULT 'Y',
    evaluated_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (rule_id, run_key);

CREATE UNIQUE INDEX gre_results_uix (rule_id, run_key)
ON CMSUNIV_FILELAND_DEV_T.gre_results;


-- ── Step 3: backfill from each renamed-away predecessor ─────────────────
-- log_id/error_id/result_id omitted -- see the IDENTITY note above. Every
-- pre-existing row gets active_ind='Y' (gre_results already only ever had
-- one row per rule_id/run_key -- see gre_results_uix -- so there's nothing
-- to reconcile there) and, for gre_results, source_tieback_sql=NULL until
-- the next rerun of each rule populates it going forward.
INSERT INTO CMSUNIV_FILELAND_DEV_T.gre_log (
    run_id, rule_id, rule_group, project_name, process_name, run_key,
    seq_no, start_time, end_time, status, rowcount, error_message,
    active_ind, load_datetime, last_updated_datetime
)
SELECT
    run_id, rule_id, rule_group, project_name, process_name, run_key,
    seq_no, start_time, end_time, status, rowcount, error_message,
    'Y', load_datetime, CAST(NULL AS TIMESTAMP)
FROM CMSUNIV_FILELAND_DEV_T.gre_log_legacy
ORDER BY log_id;

INSERT INTO CMSUNIV_FILELAND_DEV_T.gre_errors (
    run_id, rule_id, rule_group, run_key, error_type, error_message,
    error_detail, active_ind, occurred_at, last_updated_datetime
)
SELECT
    run_id, rule_id, rule_group, run_key, error_type, error_message,
    error_detail, 'Y', occurred_at, CAST(NULL AS TIMESTAMP)
FROM CMSUNIV_FILELAND_DEV_T.gre_errors_legacy
ORDER BY error_id;

INSERT INTO CMSUNIV_FILELAND_DEV_T.gre_results (
    rule_id, run_key, run_id, project_name, process_name, total_records,
    failed_records, failure_pct, threshold_pct_used, threshold_count_used,
    threshold_operator_used, severity, status, source_tieback_sql,
    active_ind, evaluated_at
)
SELECT
    rule_id, run_key, run_id, project_name, process_name, total_records,
    failed_records, failure_pct, threshold_pct_used, threshold_count_used,
    threshold_operator_used, severity, status, CAST(NULL AS CLOB),
    'Y', evaluated_at
FROM CMSUNIV_FILELAND_DEV_T.gre_results_legacy
ORDER BY result_id;


-- ── Step 4: sanity check before Step 5 (uncomment and run) ──────────────
-- Row counts on each side should match exactly.
--
-- SELECT
--     (SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.gre_log_legacy) AS log_old,
--     (SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.gre_log) AS log_new,
--     (SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.gre_errors_legacy) AS errors_old,
--     (SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.gre_errors) AS errors_new,
--     (SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.gre_results_legacy) AS results_old,
--     (SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.gre_results) AS results_new;


-- ── Step 5: drop the legacy tables once you've verified Step 4 ──────────
-- Commented out on purpose -- nothing is destroyed by running this script
-- as-is. Uncomment and run these three once you're satisfied the new
-- tables are correct.
--
-- DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_log_legacy;
-- DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_errors_legacy;
-- DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_results_legacy;
