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
-- -- both of those packages assume gre_audit and gre_errors already exist.
--
-- Why these two tables are here and not under rules_engine/ or sampling/:
--   * gre_audit is one row per RUN, whichever package produced the run.
--     run_type ('RULE_GROUP' | 'SAMPLING') discriminates the two; the
--     rule-group-only columns (rule_group, project_name, process_name,
--     total_rules, ...) are only meaningful when run_type='RULE_GROUP' and
--     are NULL for a sampling row, and vice versa for the sampling-only
--     columns. run_key -- the caller-supplied tracking/idempotency
--     identifier (a batch id, a year+month pair, a specific date, or any
--     other column/combination -- see shared/db_ops.py::build_run_key())
--     -- is populated by BOTH run types. This was a deliberate design
--     choice ("reuse gre_audit rather than inventing a parallel run-log
--     table just for sampling") kept as a single shared table -- not
--     split into two independent audit tables -- when this repo was
--     reorganized into separate rules_engine/ and sampling/ folders, so
--     it lives here in shared/ rather than being duplicated or force-fit
--     under either package.
--   * gre_errors is the ONE execution-failure log for the whole engine --
--     rules_engine/executor.py's rule crashes AND sampling/sampling.py's
--     sampling-run crashes both write here via shared/db_ops.py::log_error().
--     rule_id/rule_group are NULL for a sampling-run error row; run_key is
--     populated for both.
--
-- Standalone from ddl.sql on purpose: this file creates ONLY gre_*-prefixed
-- objects. It never touches, renames, or alters a single dq_* table, index,
-- or column.
-- ============================================================


-- ── gre_audit -- durable, one row per run (rule_group OR sampling) ────────
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_audit (
    run_id                 VARCHAR(200) NOT NULL,
    run_type               VARCHAR(20) DEFAULT 'RULE_GROUP',  -- 'RULE_GROUP' | 'SAMPLING'
    rule_group              VARCHAR(100),        -- RULE_GROUP runs only
    project_name              VARCHAR(100),      -- RULE_GROUP runs only; copied from gre_rules.project_name
    process_name              VARCHAR(100),      -- RULE_GROUP runs only; copied from gre_rules.process_name
    run_key                   VARCHAR(100),        -- caller-supplied tracking/idempotency key
    rule_variant               VARCHAR(100),      -- RULE_GROUP runs only; NULL = no variant requested
    started_at                TIMESTAMP,
    ended_at                   TIMESTAMP,
    status                      VARCHAR(20),      -- 'RUNNING' | 'COMPLETED' | 'HALTED'
    total_rules                  INTEGER,          -- RULE_GROUP runs only
    rules_succeeded                INTEGER,        -- RULE_GROUP runs only
    rules_errored                    INTEGER,      -- RULE_GROUP runs only
    sample_config_id                  INTEGER,     -- SAMPLING runs only (-> gre_sampling_config)
    sampling_method                     VARCHAR(20),  -- SAMPLING runs only: 'RANKED'|'RANDOM'|'SYSTEMATIC'
    random_seed                           BIGINT,   -- SAMPLING runs only, RANDOM/SYSTEMATIC: the ONE
                                                     -- seed used for this whole run -- see
                                                     -- sampling/sampling.py's module docstring for how a
                                                     -- single seed plus deterministic (sorted) bucket
                                                     -- processing order reproduces every per-bucket
                                                     -- offset/draw without storing them separately
    target_volume                          INTEGER, -- SAMPLING runs only
    total_candidates                        INTEGER,-- SAMPLING runs only
    total_selected                           INTEGER,-- SAMPLING runs only
    triggered_by                              VARCHAR(100),
    load_datetime                              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (run_id);


-- ── gre_errors -- SQL/execution failures, routed to engineering ────────────
-- Kept fully separate from gre_exceptions (rules_engine/schema.sql): a rule
-- or sampling run crashing is never the same row as a rule finding a data
-- violation.
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.gre_errors (
    error_id         BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id           VARCHAR(200),
    rule_id          INTEGER,       -- NULL for a sampling-run error row
    rule_group       VARCHAR(100),  -- NULL for a sampling-run error row
    run_key          VARCHAR(100),
    error_type       VARCHAR(50),          -- e.g. SQL_SYNTAX | CONNECTION | RUNTIME | PULL_FAILURE
    error_message    VARCHAR(2000),
    error_detail     CLOB,
    occurred_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (run_id, rule_id);
