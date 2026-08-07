-- ============================================================
-- Migration: v6 -> v7  (schema normalization: dq_scope + trimmed columns)
-- Target    : Teradata metadata store
-- Source    : v6 schema (SQL-dialect enforcement, dq_case_dispositions,
--             dq_notification_routes, dq_sampling_config/dq_sample_selections)
-- ============================================================
-- WHAT THIS DOES
-- ---------------
-- Introduces dq_scope(scope_id, project_name, process_name) as a single
-- dimension table and re-points every table that used to carry its own
-- raw project_name/process_name pair at it via a scope_id FK, then drops
-- the now-redundant columns from child tables that can derive the same
-- information through an existing FK join (run_id -> dq_run_control,
-- config_id -> dq_sampling_config). See ddl_shared.sql's v7 header comment for
-- the full rationale (in particular: rule_code/severity/table_name on
-- dq_rule_execution/dq_exceptions are DELIBERATELY NOT touched by this
-- migration -- those are frozen audit snapshots, not duplication).
--
-- HOW TO RUN
-- ----------
-- 1. Take a backup first. This migration DROPs columns in Phases 3-5,
--    which is destructive -- Teradata has no "undo" for DROP COLUMN.
--    A cheap safety net per table:
--        CREATE MULTISET TABLE <db>.dq_rules_v6_backup AS <db>.dq_rules
--        WITH DATA;
--    (repeat for every table this script alters or drops columns from --
--    see the phase list below). Drop the *_v6_backup tables once you've
--    verified the application against the new schema.
-- 2. Replace every occurrence of the placeholder database name
--    CMSUNIV_FILELAND_DEV_T below with your actual metadata schema name
--    (matches whatever DQ_META_DB/get_meta_db() resolves to in your
--    environment -- see config/env_config.py).
-- 3. Run phase by phase, in order, verifying row counts between phases
--    (sample verification queries are included after each phase). Do
--    NOT skip Phase 2 (the dq_scope backfill) before Phase 3 (the
--    scope_id UPDATE) -- Phase 3 needs every project/process pair to
--    already have a dq_scope row to join against.
-- 4. Test this against a lower environment (DEV/QA) copy of the schema
--    before running it against PROD -- this script has been validated
--    for internal consistency and against ddl_shared.sql's documented v7 shape,
--    not executed against a live Teradata instance as part of this repo
--    (the test suite uses DuckDB as a stand-in; see tests/test_engine_e2e.py).
-- 5. Deploy the application code that expects v7 (this repo, current
--    HEAD) only AFTER Phase 5 completes -- the running engine assumes
--    scope_id exists and the old project_name/process_name columns on
--    dq_rules/dq_run_control/dq_metrics_summary/dq_sampling_config are
--    gone (utils/db_helpers.py::get_scope_id/find_scope_id, and every
--    scope_id-joined query in rules_engine/engine.py, rules_engine/metrics.py,
--    rules_engine/reporting.py, sampling/engine.py).
-- ============================================================


-- ============================================================
-- PHASE 1 — Create the new dq_scope dimension table
-- ============================================================

CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_scope (
    scope_id      BIGINT GENERATED ALWAYS AS IDENTITY,
    project_name  VARCHAR(100) NOT NULL,
    process_name  VARCHAR(100),
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (scope_id);

CREATE UNIQUE INDEX dq_scope_lookup_uix (project_name, process_name)
ON CMSUNIV_FILELAND_DEV_T.dq_scope;

-- Verify: table exists and is empty.
-- SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.dq_scope;   -- expect 0


-- ============================================================
-- PHASE 2 — Backfill dq_scope: one row per distinct (project_name,
-- process_name) pair that exists anywhere across the four tables that
-- currently carry those columns. INSERT ... WHERE NOT EXISTS makes each
-- of the four statements idempotent and safe to re-run if this phase is
-- interrupted partway through.
-- ============================================================

INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_scope (project_name, process_name)
SELECT DISTINCT r.project_name, r.process_name
FROM CMSUNIV_FILELAND_DEV_T.dq_rules r
WHERE NOT EXISTS (
    SELECT 1 FROM CMSUNIV_FILELAND_DEV_T.dq_scope s
    WHERE s.project_name = r.project_name
      AND (s.process_name = r.process_name
           OR (s.process_name IS NULL AND r.process_name IS NULL))
);

INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_scope (project_name, process_name)
SELECT DISTINCT rc.project_name, rc.process_name
FROM CMSUNIV_FILELAND_DEV_T.dq_run_control rc
WHERE NOT EXISTS (
    SELECT 1 FROM CMSUNIV_FILELAND_DEV_T.dq_scope s
    WHERE s.project_name = rc.project_name
      AND (s.process_name = rc.process_name
           OR (s.process_name IS NULL AND rc.process_name IS NULL))
);

INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_scope (project_name, process_name)
SELECT DISTINCT ms.project_name, ms.process_name
FROM CMSUNIV_FILELAND_DEV_T.dq_metrics_summary ms
WHERE NOT EXISTS (
    SELECT 1 FROM CMSUNIV_FILELAND_DEV_T.dq_scope s
    WHERE s.project_name = ms.project_name
      AND (s.process_name = ms.process_name
           OR (s.process_name IS NULL AND ms.process_name IS NULL))
);

INSERT INTO CMSUNIV_FILELAND_DEV_T.dq_scope (project_name, process_name)
SELECT DISTINCT sc.project_name, sc.process_name
FROM CMSUNIV_FILELAND_DEV_T.dq_sampling_config sc
WHERE NOT EXISTS (
    SELECT 1 FROM CMSUNIV_FILELAND_DEV_T.dq_scope s
    WHERE s.project_name = sc.project_name
      AND (s.process_name = sc.process_name
           OR (s.process_name IS NULL AND sc.process_name IS NULL))
);

-- Verify: every project/process pair from the four source tables now has
-- exactly one dq_scope row, and dq_scope has no unexpected duplicates
-- (the unique index would have already rejected any, but a fast sanity
-- check doesn't hurt):
-- SELECT project_name, process_name, COUNT(*)
-- FROM CMSUNIV_FILELAND_DEV_T.dq_scope
-- GROUP BY project_name, process_name
-- HAVING COUNT(*) > 1;                              -- expect 0 rows


-- ============================================================
-- PHASE 3 — Add scope_id to the four scope-keyed tables, backfill it via
-- a join back to dq_scope, then drop the old project_name/process_name
-- columns. Run one table's ADD/UPDATE/DROP sequence at a time and verify
-- before moving to the next.
-- ============================================================

-- ── dq_rules ──────────────────────────────────────────────────────────
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD scope_id BIGINT;

UPDATE CMSUNIV_FILELAND_DEV_T.dq_rules r
   SET scope_id = (
       SELECT s.scope_id FROM CMSUNIV_FILELAND_DEV_T.dq_scope s
       WHERE s.project_name = r.project_name
         AND (s.process_name = r.process_name
              OR (s.process_name IS NULL AND r.process_name IS NULL))
   );

-- Verify: no unmapped rows before dropping the source columns.
-- SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.dq_rules WHERE scope_id IS NULL;  -- expect 0

ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules DROP COLUMN project_name;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules DROP COLUMN process_name;

CREATE INDEX dq_rules_scope_ix (scope_id, active_flag)
ON CMSUNIV_FILELAND_DEV_T.dq_rules;

-- ── dq_run_control ────────────────────────────────────────────────────
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_run_control ADD scope_id BIGINT;

UPDATE CMSUNIV_FILELAND_DEV_T.dq_run_control rc
   SET scope_id = (
       SELECT s.scope_id FROM CMSUNIV_FILELAND_DEV_T.dq_scope s
       WHERE s.project_name = rc.project_name
         AND (s.process_name = rc.process_name
              OR (s.process_name IS NULL AND rc.process_name IS NULL))
   );

-- SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.dq_run_control WHERE scope_id IS NULL;  -- expect 0

ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_run_control DROP COLUMN project_name;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_run_control DROP COLUMN process_name;

CREATE INDEX dq_run_control_scope_ix (scope_id, run_type, start_time)
ON CMSUNIV_FILELAND_DEV_T.dq_run_control;

-- ── dq_metrics_summary ────────────────────────────────────────────────
-- Note: the pre-v7 UNIQUE INDEX on dq_metrics_summary was keyed on
-- (project_name, process_name, run_type, batch_id, dataset_id, run_month)
-- (see the v1->v2 block above) -- it must be dropped and recreated on
-- (scope_id, run_type, batch_id, dataset_id, run_month), or the MERGE in
-- rules_engine/metrics.py::_upsert_metrics will violate the old index the moment
-- two scope_ids share a run_type/batch_id/dataset_id/run_month combo.
DROP INDEX dq_metrics_summary_uix ON CMSUNIV_FILELAND_DEV_T.dq_metrics_summary;

ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_metrics_summary ADD scope_id BIGINT;

UPDATE CMSUNIV_FILELAND_DEV_T.dq_metrics_summary ms
   SET scope_id = (
       SELECT s.scope_id FROM CMSUNIV_FILELAND_DEV_T.dq_scope s
       WHERE s.project_name = ms.project_name
         AND (s.process_name = ms.process_name
              OR (s.process_name IS NULL AND ms.process_name IS NULL))
   );

-- SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.dq_metrics_summary WHERE scope_id IS NULL;  -- expect 0

ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_metrics_summary DROP COLUMN project_name;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_metrics_summary DROP COLUMN process_name;

CREATE UNIQUE INDEX dq_metrics_summary_uix
    (scope_id, run_type, batch_id, dataset_id, run_month)
ON CMSUNIV_FILELAND_DEV_T.dq_metrics_summary;

-- ── dq_sampling_config ────────────────────────────────────────────────
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_sampling_config ADD scope_id BIGINT;

UPDATE CMSUNIV_FILELAND_DEV_T.dq_sampling_config sc
   SET scope_id = (
       SELECT s.scope_id FROM CMSUNIV_FILELAND_DEV_T.dq_scope s
       WHERE s.project_name = sc.project_name
         AND (s.process_name = sc.process_name
              OR (s.process_name IS NULL AND sc.process_name IS NULL))
   );

-- SELECT COUNT(*) FROM CMSUNIV_FILELAND_DEV_T.dq_sampling_config WHERE scope_id IS NULL;  -- expect 0

ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_sampling_config DROP COLUMN project_name;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_sampling_config DROP COLUMN process_name;

-- Optional, environment-permitting: enforce NOT NULL on the four scope_id
-- columns above to match rules_engine/ddl.sql's and sampling/ddl.sql's
-- final v7 shape (dq_rules,
-- dq_run_control, and dq_sampling_config declare scope_id NOT NULL;
-- dq_metrics_summary leaves it nullable). Teradata's ALTER TABLE ... ADD
-- CONSTRAINT for column nullability varies by platform version -- if
-- your version doesn't support altering nullability in place, recreate
-- the table (CREATE TABLE ... AS ... WITH DATA, swap names) instead;
-- Phase 3's verification queries already confirm zero NULLs, so a NOT
-- NULL constraint is a formality at this point, not a data-safety gate.


-- ============================================================
-- PHASE 4 — Drop the now-redundant columns from the six child tables
-- that derive project/process (and, for the first two, run_type/
-- run_mode/batch_id/dataset_id/dates too) via an existing FK join
-- instead of repeating them: dq_rule_execution/dq_exceptions join
-- through run_id -> dq_run_control; dq_rule_issues/dq_column_profile/
-- dq_anomaly_log join the same way; dq_sample_selections joins through
-- config_id -> dq_sampling_config. Nothing here needs a backfill --
-- these are pure drops, no new column is added to these six tables.
--
-- NOT touched: dq_rule_execution.rule_code/severity/table_name and
-- dq_exceptions.rule_code/table_name -- deliberate frozen audit
-- snapshots of what a mutable dq_rules row said at execution time, see
-- ddl_shared.sql's v7 header comment. Do not drop those.
-- ============================================================

ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_execution DROP COLUMN project_name;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_execution DROP COLUMN process_name;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_execution DROP COLUMN run_type;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_execution DROP COLUMN run_mode;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_execution DROP COLUMN batch_id;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_execution DROP COLUMN dataset_id;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_execution DROP COLUMN start_date;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_execution DROP COLUMN end_date;

ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_exceptions DROP COLUMN project_name;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_exceptions DROP COLUMN process_name;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_exceptions DROP COLUMN run_type;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_exceptions DROP COLUMN run_mode;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_exceptions DROP COLUMN batch_id;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_exceptions DROP COLUMN dataset_id;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_exceptions DROP COLUMN start_date;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_exceptions DROP COLUMN end_date;

ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_issues DROP COLUMN project_name;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_issues DROP COLUMN process_name;

ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_column_profile DROP COLUMN project_name;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_column_profile DROP COLUMN process_name;

ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_anomaly_log DROP COLUMN project_name;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_anomaly_log DROP COLUMN process_name;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_anomaly_log DROP COLUMN run_type;

ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_sample_selections DROP COLUMN project_name;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_sample_selections DROP COLUMN process_name;

-- Note: if any of the ALTER ... DROP COLUMN statements above error
-- because a given column doesn't exist in your specific v6 deployment
-- (e.g. an earlier local variant that never carried run_mode on
-- dq_exceptions), that's expected drift between environments -- just
-- comment out the offending line and continue; it means that column was
-- already absent, not that the migration is broken.


-- ============================================================
-- PHASE 5 — Rename + trim the disposition table
-- ============================================================

RENAME TABLE CMSUNIV_FILELAND_DEV_T.dq_case_dispositions
          TO CMSUNIV_FILELAND_DEV_T.dq_exception_dispositions;

ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_exception_dispositions DROP COLUMN run_id;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_exception_dispositions DROP COLUMN rule_id;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_exception_dispositions DROP COLUMN rule_code;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_exception_dispositions DROP COLUMN project_name;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_exception_dispositions DROP COLUMN process_name;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_exception_dispositions DROP COLUMN primary_key_str;

CREATE INDEX dq_exception_dispositions_lookup_ix (exception_id, effective_flag)
ON CMSUNIV_FILELAND_DEV_T.dq_exception_dispositions;

-- All dispositions are re-derivable via a JOIN to dq_exceptions on
-- exception_id (which is itself immutable) -- see ddl_shared.sql's v7 header
-- comment, point 3, for why nothing here needed a backfill either.


-- ============================================================
-- FINAL VERIFICATION
-- ============================================================
-- Compare the live schema against rules_engine/ddl.sql's and
-- sampling/ddl.sql's v7 CREATE TABLE statements
-- for every table this script touched, e.g.:
--   HELP TABLE CMSUNIV_FILELAND_DEV_T.dq_rules;
--   HELP TABLE CMSUNIV_FILELAND_DEV_T.dq_run_control;
--   HELP TABLE CMSUNIV_FILELAND_DEV_T.dq_metrics_summary;
--   HELP TABLE CMSUNIV_FILELAND_DEV_T.dq_sampling_config;
--   HELP TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_execution;
--   HELP TABLE CMSUNIV_FILELAND_DEV_T.dq_exceptions;
--   HELP TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_issues;
--   HELP TABLE CMSUNIV_FILELAND_DEV_T.dq_column_profile;
--   HELP TABLE CMSUNIV_FILELAND_DEV_T.dq_anomaly_log;
--   HELP TABLE CMSUNIV_FILELAND_DEV_T.dq_sample_selections;
--   HELP TABLE CMSUNIV_FILELAND_DEV_T.dq_exception_dispositions;
-- Then run a real DQ rules-engine run and a stratified-sampling run
-- against this schema (e.g. `python main.py --project ... --process ...`)
-- before removing the *_v6_backup tables from step 0.
