-- ============================================================
-- Shared DDL -- tables written to by BOTH rules_engine/ and sampling/
-- Schema : CMSUNIV_FILELAND_DEV_T  (DEV)  -- same metadata store as dq_*,
--          reached at runtime via the same "teradata" connection name and
--          shared/config.py's get_meta_db() resolution (GRE_META_DB
--          overrides it independently if these tables ever need to move
--          to their own schema without a code change).
-- DB     : Teradata  (metadata store)
-- ============================================================
-- Deploy this FIRST, before rules_engine/schema.sql and sampling/schema.sql
-- -- both of those packages assume gre_rule_audit/gre_sampling_audit/
-- gre_audit and gre_errors already exist.
--
-- gre_audit split into gre_rule_audit / gre_sampling_audit (2026-08)
-- ---------------------------------------------------------------------
-- Used to be ONE table (`gre_audit`) with a `run_type` discriminator --
-- rule-run columns (rule_group, project_name, process_name, total_rules,
-- rules_succeeded, rules_errored, rule_variant) NULL on every sampling
-- row, and sampling-run columns (sample_config_id, sampling_method,
-- random_seed, target_volume, total_candidates, total_selected) NULL on
-- every rule-group row. That's real confusion for anyone using
-- rules_engine/ WITHOUT sampling/ (the common case -- sampling/ is
-- deliberately independent, see sampling/README.md): every gre_audit
-- query drags along six always-NULL sampling columns that mean nothing
-- to a rules-only deployment, and vice versa.
--
-- Now: `gre_rule_audit` carries ONLY rule-engine columns, `gre_sampling_audit`
-- carries ONLY sampling columns -- a rules_engine-only user queries
-- gre_rule_audit and never sees a sampling column, period. `gre_audit`
-- itself still exists, but now as a VIEW (UNION ALL of the two tables,
-- reproducing the exact old shape including `run_type`) purely for
-- backward compatibility -- any existing dashboard/report/ad hoc query
-- already pointed at gre_audit keeps working unchanged. New code in
-- rules_engine/ and sampling/ reads/writes the two real tables directly,
-- never the view.
--
-- Both new tables -- like the old single gre_audit -- still live here in
-- shared/, not split out into rules_engine/schema.sql and
-- sampling/schema.sql: the ORIGINAL reason for keeping gre_audit in
-- shared/ ("reuse one place rather than force-fitting run-tracking DDL
-- under either package, and keep the existing shared/-first deploy order
-- intact") applies just as much to two tables as it did to one -- see
-- "Why these two tables are here and not under rules_engine/ or
-- sampling/" further down, and shared/README.md's "Why these two tables
-- are shared, not split per-package" for the fuller rationale.
--
-- If you have an EXISTING deployment with data already in the old
-- gre_audit table, do NOT just re-run this file over it -- see
-- migrate_split_gre_audit.sql at the repo root, which creates these two
-- new tables, backfills them from the existing gre_audit by run_type,
-- renames the old table out of the way, then creates this same
-- compatibility view -- with zero data loss and zero downtime for
-- anything reading gre_audit.
--
-- Why these two (plus gre_errors) are here and not under rules_engine/ or
-- sampling/:
--   * gre_rule_audit/gre_sampling_audit are each one row per RUN of their
--     respective package. run_key -- the caller-supplied tracking/
--     idempotency identifier (a batch id, a year+month pair, a specific
--     date, or any other column/combination -- see shared/db_ops.py::
--     build_run_key()) -- is populated on both, the one column genuinely
--     common to both tables' purpose.
--   * gre_errors is the ONE execution-failure log for the whole engine --
--     rules_engine/executor.py's rule crashes AND sampling/sampling.py's
--     sampling-run crashes both write here via shared/db_ops.py::log_error().
--     rule_id/rule_group are NULL for a sampling-run error row; run_key is
--     populated for both. Deliberately NOT split the same way gre_audit
--     was -- rule_id being NULL for one row type is one nullable column,
--     not a pile of irrelevant ones; see shared/README.md for the fuller
--     "why gre_errors stays one table" reasoning.
--
-- Standalone from ddl.sql on purpose: this file creates ONLY gre_*-prefixed
-- objects. It never touches, renames, or alters a single dq_* table, index,
-- or column.
-- ============================================================


-- ── gre_rule_audit -- durable, one row per rules_engine run ────────────────
-- Written by rules_engine/runner.py::_start_audit()/_finish_audit() ONLY --
-- sampling/ never touches this table. See the module header above for why
-- this used to be part of a combined gre_audit.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_rule_audit (
    run_id                 VARCHAR(200) NOT NULL,
    rule_group              VARCHAR(100),
    project_name              VARCHAR(100),      -- copied from gre_rules.project_name
    process_name              VARCHAR(100),      -- copied from gre_rules.process_name
    run_key                   VARCHAR(100),        -- caller-supplied tracking/idempotency key
    rule_variant               VARCHAR(100),      -- NULL = no variant requested
    started_at                TIMESTAMP,
    ended_at                   TIMESTAMP,
    status                      VARCHAR(20),      -- 'RUNNING' | 'COMPLETED' | 'HALTED'
    total_rules                  INTEGER,
    rules_succeeded                INTEGER,
    rules_errored                    INTEGER,
    triggered_by                      VARCHAR(100),
    load_datetime                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (run_id);

-- count_prior_attempts() (shared/db_ops.py) looks up "how many runs of
-- this rule_group+run_key already exist" every call, to label a new
-- run_id's attempt-N segment -- see rules_engine/README.md's "Identifying
-- an attempt: run_id".
CREATE INDEX gre_rule_audit_group_run_key_ix (rule_group, run_key)
ON CMSUNIV_FILELAND_DEV_T.gre_rule_audit;

CREATE INDEX gre_rule_audit_status_ix (status)
ON CMSUNIV_FILELAND_DEV_T.gre_rule_audit;


-- ── gre_sampling_audit -- durable, one row per sampling run ────────────────
-- Written by sampling/sampling.py::_write_audit() ONLY -- rules_engine/
-- never touches this table.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_sampling_audit (
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
-- sampling/sampling.py) both look this pair up every call.
CREATE INDEX gre_sampling_audit_config_run_key_ix (sample_config_id, run_key)
ON CMSUNIV_FILELAND_DEV_T.gre_sampling_audit;

CREATE INDEX gre_sampling_audit_status_ix (status)
ON CMSUNIV_FILELAND_DEV_T.gre_sampling_audit;


-- ── gre_audit -- BACKWARD-COMPATIBILITY VIEW, not a table ──────────────────
-- Reproduces the exact old combined gre_audit shape (including run_type)
-- as a UNION ALL of the two tables above, for anything still querying
-- gre_audit directly. New code never reads/writes through this view --
-- see the module header above. Deploy this AFTER both tables (it selects
-- from them), which the ordering in this file already guarantees.
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


-- ── gre_errors -- SQL/execution failures, routed to engineering ────────────
-- Kept fully separate from gre_exceptions (rules_engine/schema.sql): a rule
-- or sampling run crashing is never the same row as a rule finding a data
-- violation.
--
-- Like gre_log, this is append-only across reruns of the same run_key
-- under a NEW run_id (rule_id is NULL instead for a sampling-run error --
-- see sampling/sampling.py::_log_sampling_error()). active_ind marks
-- which error(s) belong to the CURRENT run_id for a given
-- (rule_id, run_key) -- see shared/db_ops.py::_deactivate_prior_errors(),
-- called from log_error() immediately before each new error row is
-- inserted, mirroring gre_log's active_ind reconciliation exactly. Never
-- deletes -- full error history stays on file; only active_ind flips.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_errors (
    error_id         BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id           VARCHAR(200),
    rule_id          INTEGER,       -- NULL for a sampling-run error row
    rule_group       VARCHAR(100),  -- NULL for a sampling-run error row
    run_key          VARCHAR(100),
    error_type       VARCHAR(50),          -- e.g. SQL_SYNTAX | CONNECTION | RUNTIME | PULL_FAILURE
    error_message    VARCHAR(2000),
    error_detail     CLOB,
    active_ind           CHAR(1) DEFAULT 'Y',  -- 'Y' = belongs to the current run_id for this
                                                -- (rule_id, run_key); 'N' = superseded by a later
                                                -- rerun of the same run_key. See comment above.
    occurred_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_datetime TIMESTAMP             -- set only when active_ind flips to 'N'
)
PRIMARY INDEX (run_id, rule_id);

-- Reporting/dashboard lookup: "what errors are current right now for this
-- run_key" -- filters straight to active_ind='Y' instead of every
-- historical error across every past run_id.
CREATE INDEX gre_errors_rule_run_key_active_ix (rule_id, run_key, active_ind)
ON CMSUNIV_FILELAND_DEV_T.gre_errors;
