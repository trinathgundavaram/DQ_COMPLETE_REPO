-- Run against CMSUNIV_FILELAND_DEV_T before deploying feature/rerun-reconciliation.
-- Adds the new source_tieback_sql column gre_results now declares
-- (see rules_engine/executor.py::build_source_tieback_sql()).

ALTER TABLE CMSUNIV_FILELAND_DEV_T.gre_results
    ADD source_tieback_sql CLOB;

-- No backfill needed: existing rows just get source_tieback_sql=NULL
-- until the next time each rule reruns for its run_key, at which point
-- execute_rule() populates it going forward.
