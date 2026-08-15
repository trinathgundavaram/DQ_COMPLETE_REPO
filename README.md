# GRE + Sampling

Two independent, config-driven engines that share a small common
infrastructure layer:

- **[`rules_engine/`](rules_engine/README.md)** -- the Generic Rules
  Engine (GRE). `gre_rules` rows define a negative SQL SELECT per rule;
  the engine runs each active rule for a `rule_group`/`batch_id`, writes
  every violating row to `gre_exceptions`, evaluates a threshold, and
  upserts a `gre_results` verdict.
- **[`sampling/`](sampling/README.md)** -- generic stratified sampling.
  Given a candidate universe, an exclusion filter, any number of
  stratification levels, and a selection method (RANKED/RANDOM/
  SYSTEMATIC), picks a target-volume sample and persists every candidate
  considered.
- **[`shared/`](shared/README.md)** -- what both of the above actually
  share: low-level DB helpers (`db_ops.py`), local `.env` credential
  loading + metadata-store resolution (`config.py`), and the two tables
  written to by both (`gre_audit`, `gre_errors`).
- **`db/`** -- `adapters.py` + `connection_factory.py`, reused as-is from
  the dq_* engine this repo originally sat alongside. This is the ONLY
  code either package depends on outside its own folder (plus `shared/`)
  -- neither `rules_engine/` nor `sampling/` modifies these two files.

Each package is independently testable and has its own DDL -- see
`tests/` and each package's `schema.sql`/`schema_drop.sql`.

## Setup

1. **First deployment** (no `gre_*` table exists yet, or you're OK
   discarding what's in them): run `shared/schema.sql`, then
   `rules_engine/schema.sql`, then `sampling/schema.sql`, in that order
   -- `rules_engine/`'s and `sampling/`'s tables are written to via
   `gre_audit`/`gre_errors`, which `shared/schema.sql` creates first. See
   "Redeploying / changing the schema" below for how to make DDL changes
   later.

2. **Local credentials**: copy `dev.env.example` to `dev.env` (repo
   root) and fill in real values. `shared/config.py` loads it
   automatically the first time either package is imported -- see that
   file's docstring, and `dev.env.example`'s header, for exactly how.
   `dev.env` is gitignored; never commit it. This is an interim, local-
   dev-only credential path -- see "Credentials" below for the planned
   pivot to a secrets manager.

3. **Connection names**: `DQ_CONNECTION_NAMES` (comma-separated) lists
   every named connection `db/connection_factory.py` should build; each
   needs its own `DQ_<NAME>_TYPE`/`DQ_<NAME>_HOST`/... block. See
   `dev.env.example` for the shape.

4. **Metadata connection/db**: both packages default to the connection
   named `"teradata"` and schema `CMSUNIV_FILELAND_DEV_T`. Override with
   `GRE_META_CONNECTION`/`GRE_META_DB` if the `gre_*` tables live
   somewhere else.

5. **Tunables** (all optional, env-driven): `GRE_EXCEPTION_CHUNK`
   (bulk-write chunk size, default 500), `GRE_MAX_EXCEPTIONS` (detail-row
   capture cap per rule attempt, default 10000), `GRE_QUERY_MAX_RETRIES`
   (source-query retry attempts, default 3).

## Redeploying / changing the schema

Since no `gre_*` table holds live/production data yet, this repo's DDL
policy is: make schema changes by editing the relevant `schema.sql`'s
`CREATE` statements directly, then redeploy by dropping and recreating
rather than writing `ALTER TABLE` migrations. Each package has its own
`schema_drop.sql` for this:

```
shared/schema_drop.sql          -- drop gre_audit, gre_errors
rules_engine/schema_drop.sql    -- drop gre_rules, gre_log, gre_exceptions, gre_case, gre_results
sampling/schema_drop.sql        -- drop the 5 gre_sampling_*/gre_sample_* tables

-- then redeploy, shared first (rules_engine/ and sampling/ both assume
-- gre_audit/gre_errors already exist):
shared/schema.sql
rules_engine/schema.sql
sampling/schema.sql
```

Teradata has no `DROP TABLE IF EXISTS` -- each `schema_drop.sql` uses
plain `DROP TABLE`, which errors harmlessly on a table that doesn't exist
yet (e.g. a fresh environment); just re-run the matching `schema.sql`
regardless of which drops errored. Dropping a table in Teradata also
drops every index defined on it, so no separate `DROP INDEX` step is
needed anywhere.

**This policy needs revisiting once real data lands in any `gre_*`
table** -- at that point, schema changes should become real migrations
(`ALTER TABLE`), not drop-and-recreate.

## Credentials

Today: local `.env` file (`dev.env`), loaded by `shared/config.py` via
`python-dotenv`, into the exact env var names `db/adapters.py` and
`db/connection_factory.py` already expect -- see `shared/config.py`'s
docstring for the full mechanics. Neither of those two reused files was
touched to support this.

Planned pivot: AWS Secrets Manager (or similar), for non-local
deployments. This only requires changing `shared/config.py`'s
`_load_env_file()` to populate the same env var names from Secrets
Manager instead of a file -- `db/adapters.py`, `db/connection_factory.py`,
`rules_engine/`, and `sampling/` all only ever see already-populated env
vars either way, so none of them need to change.

## Running the tests

```
pip install -r requirements.txt
python3 -m pytest tests/ -q
```

Every test runs against an in-memory DuckDB connection -- no live
Teradata/Postgres/etc. connection is needed.

| Test file | Covers |
|---|---|
| `test_shared_config.py` | `shared/config.py` -- `.env` loading, metadata connection/db resolution, batch-readiness checks. |
| `test_shared_db_ops.py` | `shared/db_ops.py` -- bulk writes with duplicate-key tolerance, the dialect guard, `{key}` run_params substitution (`_substitute_params`/`build_run_params`). |
| `test_rules_engine_rules.py` | `rules_engine/rules.py` -- `load_rules()`'s group/active_flag filtering, ordering, and `rule_variant` selection (universal vs. exact-match). |
| `test_rules_engine_executor.py` | `rules_engine/executor.py` -- threshold evaluation, natural-key building, `execute_rule()` end-to-end, the single-scan/memoized-total big-dataset path, run_params substitution and its fail-fast `PARAM_SUBSTITUTION_ERROR`. |
| `test_rules_engine_runner.py` | `rules_engine/runner.py` -- checkpoint/resume, `sequencing_mode` (`halt_group` vs. `skip_and_continue`), the shared `total_cache`, `rule_variant` end-to-end, `run_params` threading. |
| `test_sampling.py` | `sampling/sampling.py` -- rounding modes, selection methods, recursive stratification, candidate pull (including the `exclusion_sql` run_params fix), `run_sampling()` end-to-end, and a frozen regression fixture proving output still matches the originally-validated UM sample. |
