-- ============================================================
-- Data Quality Framework DDL — SHARED FOUNDATION  (v7)
-- Schema : CMSUNIV_FILELAND_DEV_T  (DEV)
-- DB     : Teradata  (metadata store)
-- ============================================================
-- Run this file FIRST, before rules_engine/ddl.sql and/or sampling/ddl.sql.
-- It creates the tables both frameworks depend on: dq_scope (the
-- project/process dimension every other table joins to) and dq_connections
-- (the source-connection catalogue db/connection_factory.py reads
-- alongside DQ_* env vars). Everything else in this repo's schema is
-- framework-specific and lives in its own folder:
--   rules_engine/ddl.sql  — dq_rules through dq_notification_routes
--   sampling/ddl.sql      — dq_sampling_config, dq_sample_selections
--
-- The full v1-v7 change rationale (why scope_id was introduced, which
-- columns were trimmed and why, etc.) follows immediately below, along
-- with the illustrative ALTER-TABLE version history for existing
-- deployments -- kept together here since both span tables in both
-- frameworks.
-- ============================================================
-- ============================================================
-- Data Quality Framework DDL  (v7 — schema normalization)
-- Schema : CMSUNIV_FILELAND_DEV_T  (DEV)
-- DB     : Teradata  (metadata store)
-- ============================================================
-- NOTE: The metadata store is always Teradata.
-- Source systems (PostgreSQL, Databricks, SQL Server, file)
-- are configured via environment variables — see
-- db/connection_factory.py and db/adapters.py for details.
-- The dq_connections table below is a catalogue / reference;
-- runtime credentials are read from env vars, NOT from this table.
-- ============================================================

-- ── New columns added in v2 ────────────────────────────────
-- dq_rules:
--   priority           INTEGER  DEFAULT 100   — lower = runs first
--   depends_on_rule_id INTEGER  DEFAULT NULL  — skip this rule if parent fails
--   require_rows       BYTEINT  DEFAULT 0     — 1 = FAIL when total_records = 0
--   threshold_operator CHAR(3)  DEFAULT 'OR'  — 'OR'|'AND' for pct+count thresholds
--   filter_sql         CLOB     DEFAULT NULL  — verbatim WHERE fragment (highest prio)
-- dq_metrics_summary:
--   UNIQUE INDEX to prevent MERGE race-condition double-insert
-- ── New in v3 ──────────────────────────────────────────────
-- dq_rules:
--   updated_at         TIMESTAMP  DEFAULT NULL  — audit trail for rule edits
-- dq_exceptions:
--   Secondary index on (run_id, rule_id) for fast exception lookups
-- dq_rule_execution:
--   Secondary index on (run_id, status) for dashboard status filtering
-- ── New in v4 ──────────────────────────────────────────────
-- dq_rules:
--   check_type   VARCHAR(50)   — built-in type (NOT_NULL, FRESHNESS, etc.)
--   check_column VARCHAR(500)  — column(s) the check applies to
--   check_params CLOB          — JSON dict of check-type parameters
-- dq_check_catalog:
--   Reference table listing all built-in check types (auto-populated)
-- ── New in v5 ──────────────────────────────────────────────
-- Rule suppression, versioning, column profiling, anomaly detection —
-- see dq_rule_suppressions / dq_rule_versions / dq_column_profile /
-- dq_profile_config / dq_anomaly_config / dq_anomaly_log below.
-- ── New in v6 ──────────────────────────────────────────────
-- SQL-dialect enforcement (dq_rules.sql_dialect), case-level disposition
-- (dq_case_dispositions), config-driven stratified sampling
-- (dq_sampling_config / dq_sample_selections), notification routing
-- (dq_notification_routes / dq_rules.business_correctable).
-- ── New in v7 (this file) ───────────────────────────────────
-- Schema normalization — removes duplicated columns in favor of keys:
--
-- 1. dq_scope(scope_id, project_name, process_name) — a single dimension
--    table. Every table that used to carry its own raw
--    project_name/process_name pair now carries a scope_id FK instead
--    (dq_rules, dq_run_control, dq_metrics_summary, dq_sampling_config).
--    Resolved via utils/db_helpers.py::get_scope_id() (get-or-create) at
--    write time, or find_scope_id() (lookup-only) at read/filter time.
--
-- 2. dq_rule_execution, dq_exceptions, dq_rule_issues, dq_column_profile,
--    dq_anomaly_log all carry run_id, and run_id already maps 1:1 to a
--    dq_run_control row that has scope_id (and, for dq_rule_execution /
--    dq_exceptions, also run_type/run_mode/batch_id/dataset_id/dates).
--    Repeating project_name/process_name/run_type/run_mode/batch_id/
--    dataset_id/start_date/end_date on every one of those child rows was
--    pure duplication with no audit-fidelity purpose (dq_run_control's
--    values are set once at run start and never change) — all of it is
--    now dropped from these five tables and read back via a JOIN to
--    dq_run_control on run_id.
--
--    dq_sample_selections carries config_id, which already maps 1:1 to a
--    dq_sampling_config row with scope_id — its own project_name/
--    process_name columns are dropped the same way.
--
--    NOT touched by this: dq_rule_execution.rule_code/severity/table_name
--    and dq_exceptions.rule_code/table_name. Those are frozen SNAPSHOTS
--    of what a mutable dq_rules row said AT EXECUTION TIME — dq_rules can
--    be edited later (severity reclassified, table renamed), and a CMS
--    audit record must show what was judged at the time, not what a live
--    join to dq_rules says today. That's a deliberate design constraint,
--    not accidental redundancy — see DESIGN.md.
--
-- 3. dq_case_dispositions (renamed dq_exception_dispositions) duplicated
--    run_id, rule_id, rule_code, project_name, process_name, and
--    primary_key_str — all of which already live on dq_exceptions.
--    exception_id, which is ITSELF immutable (never updated), so there is
--    no point-in-time-snapshot argument for repeating them here. Trimmed
--    to just the disposition fields, joined to dq_exceptions by
--    exception_id.
--
-- 4. dq_profile_config, dq_anomaly_config, dq_notification_routes are
--    DELIBERATELY NOT normalized to scope_id. These are low-row-count,
--    NULL-wildcard config tables ("NULL project_name = applies to every
--    project") matched by a most-specific-row-wins rule at read time.
--    They're not high-volume fact tables, so the duplication-avoidance
--    payoff is negligible, and collapsing project_name+process_name into
--    a single scope_id FK would still need to represent three distinct
--    wildcard levels (fully global / project-wide / exact project+
--    process), which only adds indirection to the specificity-matching
--    logic in rules_engine/metrics.py, rules_engine/reporting.py, and rules_engine/profiler.py
--    for no real benefit. Left as plain nullable columns on purpose.
-- ============================================================

-- ============================================================
-- SHARED FOUNDATION — used by both frameworks below.
--
-- This repo hosts TWO SEPARATE FRAMEWORKS against one metadata store:
--   1. The DQ RULES ENGINE       (rules_engine/)     -- dq_rules through
--      dq_anomaly_log below.
--   2. The SAMPLING FRAMEWORK    (sampling/) -- dq_sampling_config and
--      dq_sample_selections below.
-- Neither framework's Python code imports the other (sampling/ uses
-- rules_engine.executor/rules_engine.rule_sql as a plain library — see sampling/engine.py's
-- module docstring — but rules_engine/ has zero awareness sampling/ exists). The
-- two are independently deployable; dq_scope, dq_connections, and the
-- connector layer in db/ are the only things they share.
-- ============================================================

-- ── dq_scope: project/process dimension (v7) — SHARED ─────────────────────
-- Every other table that needs to say "this row belongs to project X,
-- process Y" references this table by scope_id instead of repeating the
-- two VARCHAR columns. process_name may be NULL (a scope can represent
-- "project X, no sub-process concept"). Both frameworks use this same
-- table — a sampling config and a rule set for the same project/process
-- resolve to the same scope_id.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_scope (
    scope_id      BIGINT GENERATED ALWAYS AS IDENTITY,
    project_name  VARCHAR(100) NOT NULL,
    process_name  VARCHAR(100),
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (scope_id);

CREATE UNIQUE INDEX dq_scope_lookup_uix (project_name, process_name)
ON CMSUNIV_FILELAND_DEV_T.dq_scope;



-- ── dq_connections: connection catalogue — SHARED ──────────────────────────
-- Reference only — credentials are read from DQ_<NAME>_* env vars at
-- runtime, NOT from this table (see db/connection_factory.py,
-- db/adapters.py). Both the rules engine and the sampling framework pull
-- their source data through these same connection entries.
-- Connection catalogue (reference only — credentials stored in env vars).
-- SHARED: both frameworks pull their source data through these same
-- connection entries via db/connection_factory.py.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_connections (
    connection_id    INTEGER NOT NULL,
    connection_name  VARCHAR(100) NOT NULL,    -- matches DQ_CONNECTION_NAMES entry
    source_type      VARCHAR(50) NOT NULL,     -- teradata | postgresql | s3
                                               -- (databricks | sqlserver adapters exist
                                               --  in code but are uncatalogued here)
    host             VARCHAR(500),
    port             INTEGER,
    database_name    VARCHAR(200),
    description      VARCHAR(500),
    active_flag      BYTEINT DEFAULT 1,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (connection_name);


-- ============================================================
-- ALTER TABLE scripts for existing deployments
-- Run the block matching your current version once, in order.
-- ============================================================

/*  ── v1 → v2 ──────────────────────────────────────────────
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD filter_sql           CLOB;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD threshold_operator   CHAR(3) DEFAULT 'OR';
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD require_rows         BYTEINT DEFAULT 0;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD priority             INTEGER DEFAULT 100;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD depends_on_rule_id   INTEGER;

CREATE UNIQUE INDEX dq_metrics_summary_uix
    (project_name, process_name, run_type, batch_id, dataset_id, run_month)
ON CMSUNIV_FILELAND_DEV_T.dq_metrics_summary;
*/

/*  ── v2 → v3 ──────────────────────────────────────────────
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD updated_at TIMESTAMP;

CREATE INDEX dq_exceptions_run_rule_ix (run_id, rule_id)
ON CMSUNIV_FILELAND_DEV_T.dq_exceptions;

CREATE INDEX dq_rule_execution_status_ix (run_id, status)
ON CMSUNIV_FILELAND_DEV_T.dq_rule_execution;
*/

/*  ── v3 → v4 (check type system) ─────────────────────────
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD check_type   VARCHAR(50);
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD check_column VARCHAR(500);
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD check_params CLOB;

-- Create and seed the catalog table (run once) — see CREATE + INSERT
-- statements above.
*/

/*  ── v4 → v5 (suppression, versioning, profiling, anomaly) ───
-- New tables (see CREATE statements above):
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_suppressions ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_versions ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_column_profile ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_profile_config ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_anomaly_config ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_anomaly_log ...
-- No changes to existing tables in v5.
*/

/*  ── v5 → v6 (dialect enforcement, disposition, routing, sampling) ──────
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD sql_dialect VARCHAR(10);
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_rules ADD business_correctable BYTEINT DEFAULT 0;
ALTER TABLE CMSUNIV_FILELAND_DEV_T.dq_connections
  ADD CONSTRAINT dq_connections_source_type_ck
  CHECK (source_type IN ('teradata', 'postgresql', 's3'));

-- New tables (see CREATE statements above):
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_case_dispositions ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_notification_routes ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_sampling_config ...
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_sample_selections ...
*/

-- ── v6 → v7 (schema normalization: dq_scope + trimmed columns) ─────────
-- Runnable migration script: migrations/v6_to_v7.sql (not illustrative
-- comments like the blocks above -- this one is meant to be executed,
-- phase by phase, against a real v6 deployment). See that file for the
-- full backfill/ALTER/DROP sequence, verification queries between
-- phases, and pre-migration backup guidance.
