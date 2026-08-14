-- ============================================================
-- Generic Rules Engine (GRE) -- redeploy helper
-- ============================================================
-- schema.sql is pure CREATE, correct as-is for a first deployment onto an
-- empty schema. Once gre_* objects exist, this script drops all of them
-- so schema.sql can be re-run cleanly -- the engine's policy (given no
-- gre_* table has live/production data yet) is to redeploy schema
-- changes by editing schema.sql's CREATE statements directly and
-- dropping/recreating, rather than writing ALTER TABLE migrations. Once
-- real data exists in these tables, that policy needs revisiting --
-- this script is destructive and will discard everything in gre_*.
--
-- Usage: run this, then re-run schema.sql.
--
-- In Teradata, DROP TABLE also drops every index defined on that table
-- (CREATE INDEX/CREATE UNIQUE INDEX are structurally part of the table,
-- not independent objects) -- so dropping the 12 tables below is
-- sufficient; there's no separate DROP INDEX step needed.
--
-- Plain DROP TABLE (no IF EXISTS -- Teradata has no such clause for
-- this). Errors on any table that doesn't exist yet; that's expected and
-- harmless the first time you run this against a partially-deployed
-- schema -- just re-run schema.sql afterward regardless of which drops
-- errored.
-- ============================================================

DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_sample_selection_attrs;
DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_sample_selections;
DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_sampling_mix;
DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_sampling_strata;
DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_sampling_config;

DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_results;
DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_case;
DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_errors;
DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_exceptions;
DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_audit;
DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_log;
DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_rules;
