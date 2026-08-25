-- Run against CMSUNIV_FILELAND_DEV_T before deploying feature/rerun-reconciliation.
-- Adds the reconciliation columns sampling/schema.sql now declares for these
-- two tables (see sampling/sampling.py::_deactivate_prior_sampling_runs()).

ALTER TABLE CMSUNIV_FILELAND_DEV_T.gre_sample_selections
    ADD etl_is_curr_ind CHAR(1) DEFAULT 'Y';
ALTER TABLE CMSUNIV_FILELAND_DEV_T.gre_sample_selections
    ADD last_updated_datetime TIMESTAMP;

ALTER TABLE CMSUNIV_FILELAND_DEV_T.gre_sample_selection_attrs
    ADD etl_is_curr_ind CHAR(1) DEFAULT 'Y';
ALTER TABLE CMSUNIV_FILELAND_DEV_T.gre_sample_selection_attrs
    ADD last_updated_datetime TIMESTAMP;

-- Backfill: every row that predates this change is, by definition, from
-- some prior run -- but this ALTER's DEFAULT 'Y' already marks them all
-- active. That's fine as a one-time backfill (nothing was "current vs
-- superseded" before this feature existed); the NEXT rerun of each
-- (config_id, run_key) will correctly deactivate whichever of these
-- pre-existing rows it supersedes.
