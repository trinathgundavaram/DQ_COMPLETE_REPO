-- ============================================================
-- shared/ -- redeploy helper
-- ============================================================
-- Mirrors rules_engine/schema_drop.sql and sampling/schema_drop.sql's
-- policy: no gre_* table holds live/production data yet, so schema
-- changes are made by editing schema.sql's CREATE statements directly
-- and redeploying via drop-then-recreate, not ALTER TABLE migrations.
--
-- (If gre_rule_audit/gre_sampling_audit already hold real history in your
-- environment -- e.g. you deployed the old single gre_audit table before
-- this split existed -- do NOT use this file. Use
-- migrate_split_gre_audit.sql at the repo root instead, which preserves
-- every existing row.)
--
-- Drops the VIEW first (gre_audit depends on the two tables below; in
-- Teradata a DROP TABLE on a table a view depends on succeeds regardless
-- and just leaves the view unusable, but dropping the view first keeps
-- the order unambiguous and avoids a dangling view in between statements
-- if this script is only partially run). Then the two real tables, then
-- gre_errors.
--
-- Usage: run this, then re-run shared/schema.sql. Then rules_engine/
-- and/or sampling/'s own schema.sql (both assume these exist).
--
-- Plain DROP TABLE/DROP VIEW (no IF EXISTS -- Teradata has no such clause
-- for either). Errors on any object that doesn't exist yet; that's
-- expected and harmless the first time you run this against a
-- partially-deployed schema -- just re-run shared/schema.sql afterward
-- regardless of which drops errored.
-- ============================================================

DROP VIEW CMSUNIV_FILELAND_DEV_T.gre_audit;
DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_rule_audit;
DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_sampling_audit;
DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_errors;
