-- Run against CMSUNIV_FILELAND_DEV_T before deploying the active_ind
-- rerun-precedence feature. Adds the active_ind columns gre_log,
-- gre_errors, and gre_results now declare (see rules_engine/schema.sql,
-- shared/schema.sql, rules_engine/executor.py::_deactivate_prior_log_attempts(),
-- shared/db_ops.py::_deactivate_prior_errors()), mirroring the
-- etl_is_curr_ind pattern alter_sampling_tables.sql already applied to
-- gre_sample_selections/gre_sample_selection_attrs.

ALTER TABLE CMSUNIV_FILELAND_DEV_T.gre_log
    ADD active_ind CHAR(1) DEFAULT 'Y';
ALTER TABLE CMSUNIV_FILELAND_DEV_T.gre_log
    ADD last_updated_datetime TIMESTAMP;

ALTER TABLE CMSUNIV_FILELAND_DEV_T.gre_errors
    ADD active_ind CHAR(1) DEFAULT 'Y';
ALTER TABLE CMSUNIV_FILELAND_DEV_T.gre_errors
    ADD last_updated_datetime TIMESTAMP;

ALTER TABLE CMSUNIV_FILELAND_DEV_T.gre_results
    ADD active_ind CHAR(1) DEFAULT 'Y';

CREATE INDEX gre_log_rule_run_key_active_ix (rule_id, run_key, active_ind)
ON CMSUNIV_FILELAND_DEV_T.gre_log;

CREATE INDEX gre_errors_rule_run_key_active_ix (rule_id, run_key, active_ind)
ON CMSUNIV_FILELAND_DEV_T.gre_errors;

-- Backfill: every row that predates this change gets active_ind='Y' from
-- the DEFAULT, i.e. every past attempt/error looks "current" until the
-- next time each (rule_id, run_key) actually reruns -- exactly the same
-- one-time-backfill trade-off alter_sampling_tables.sql already accepted
-- for gre_sample_selections/gre_sample_selection_attrs. If a
-- (rule_id, run_key) has multiple pre-existing gre_log/gre_errors rows
-- across different historical run_ids and you want ONLY the latest one
-- marked current immediately (rather than waiting for the next rerun),
-- run this one-time cleanup AFTER the ALTERs above:
--
-- UPDATE CMSUNIV_FILELAND_DEV_T.gre_log l
-- SET active_ind = 'N'
-- WHERE l.run_id <> (
--     SELECT MAX(l2.run_id)
--     FROM CMSUNIV_FILELAND_DEV_T.gre_log l2
--     WHERE l2.rule_id = l.rule_id AND l2.run_key = l.run_key
-- );
--
-- (run_id sorts correctly here because generate_run_id() embeds a
-- YYYYMMDD_HHMMSS timestamp suffix -- lexicographic MAX = most recent.
-- The equivalent UPDATE for gre_errors is the same shape, keyed on
-- (rule_id, run_key) with rule_id IS NULL handled separately for
-- sampling-run error rows.)
