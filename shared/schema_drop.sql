-- ============================================================
-- shared/ -- redeploy helper
-- ============================================================
-- Drop the two tables shared by rules_engine/ and sampling/. Run this
-- LAST when tearing down (after rules_engine/schema_drop.sql and
-- sampling/schema_drop.sql -- neither of those packages' tables reference
-- gre_audit/gre_errors via a foreign key, but dropping in this order keeps
-- the shared/first, drop/last convention consistent everywhere in this
-- repo). Re-run shared/schema.sql before rules_engine/schema.sql or
-- sampling/schema.sql on the way back up -- both assume these two tables
-- already exist.
--
-- Given no gre_* table has live/production data yet, this engine's policy
-- is to redeploy schema changes by editing schema.sql's CREATE statements
-- directly and dropping/recreating, rather than writing ALTER TABLE
-- migrations. Once real data exists in these tables, that policy needs
-- revisiting -- this script is destructive and will discard everything in
-- gre_audit/gre_errors.
--
-- In Teradata, DROP TABLE also drops every index defined on that table --
-- neither table below has one, so there's nothing extra to drop.
--
-- Plain DROP TABLE (no IF EXISTS -- Teradata has no such clause for this).
-- Errors on a table that doesn't exist yet; that's expected and harmless
-- the first time you run this against a partially-deployed schema.
-- ============================================================

DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_errors;
DROP TABLE CMSUNIV_FILELAND_DEV_T.gre_audit;
