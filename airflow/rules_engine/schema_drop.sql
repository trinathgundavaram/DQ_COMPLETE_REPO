-- ============================================================
-- rules_engine/ -- redeploy helper
-- ============================================================
-- schema.sql is pure CREATE, correct as-is for a first deployment -- this
-- package is fully standalone (see its own header: no other schema.sql
-- needs to run first). Once these gre_* objects exist, this script drops
-- all of them so schema.sql can be re-run cleanly -- the engine's policy
-- (given no gre_* table has live/production data yet) is to redeploy
-- schema changes by editing schema.sql's CREATE statements directly and
-- dropping/recreating, rather than writing ALTER TABLE migrations. Once
-- real data exists in these tables, that policy needs revisiting -- this
-- script is destructive and will discard everything in these 5 tables.
--
-- Usage: run this, then re-run schema.sql. Does NOT touch sampling/'s
-- gre_sampling_*/gre_sample_* tables -- see that package's own
-- schema_drop.sql for those (the two packages share nothing -- see
-- README.md's "Package separation").
--
-- {{META_DB}} is a placeholder, not a literal schema name -- see
-- schema.sql's header for why (environment-parameterized metadata
-- store) and rules_engine/deploy_schema.py for the tool that substitutes
-- it from GRE_META_DB before this ever reaches Teradata.
--
-- In Teradata, DROP TABLE also drops every index defined on that table
-- (CREATE INDEX/CREATE UNIQUE INDEX are structurally part of the table,
-- not independent objects) -- so dropping the 5 tables below is
-- sufficient; there's no separate DROP INDEX step needed.
--
-- Plain DROP TABLE (no IF EXISTS -- Teradata has no such clause for
-- this). Errors on any table that doesn't exist yet; that's expected and
-- harmless the first time you run this against a partially-deployed
-- schema -- just re-run schema.sql afterward regardless of which drops
-- errored.
-- ============================================================

DROP TABLE {{META_DB}}.gre_results;
DROP TABLE {{META_DB}}.gre_exceptions;
DROP TABLE {{META_DB}}.gre_rules;
DROP TABLE {{META_DB}}.gre_rule_audit;
DROP TABLE {{META_DB}}.gre_rule_errors;
