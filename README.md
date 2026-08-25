# GRE + Sampling

Two independent, config-driven engines that share a small common
infrastructure layer:

- **[`rules_engine/`](rules_engine/README.md)** -- the Generic Rules
  Engine (GRE). `gre_rules` rows define a negative SQL SELECT per rule;
  the engine runs each active rule for a `rule_group`/`run_key`, writes
  every violating row to `gre_exceptions`, evaluates a threshold, and
  upserts a `gre_results` verdict.
- **[`sampling/`](sampling/README.md)** -- generic stratified sampling.
  Given a candidate universe, an exclusion filter, any number of
  stratification levels, and a selection method (RANKED/RANDOM/
  SYSTEMATIC), picks a target-volume sample and persists every candidate
  considered.
- **[`shared/`](shared/README.md)** -- what both of the above actually
  share: low-level DB helpers (`db_ops.py`), local `.env` credential
  loading + metadata-store resolution (`config.py`), the shared error log
  (`gre_errors`), and each package's own run-tracking table
  (`gre_rule_audit`, `gre_sampling_audit`) -- plus a `gre_audit`
  backward-compatibility VIEW over the latter two, for anything still
  querying the old combined shape. See `shared/README.md`'s "Why
  `gre_audit` is now two tables plus a view".
- **`db/connection_factory.py`** -- every source adapter (Teradata,
  Postgres, S3, file) plus `ConnectionFactory`, in one file. Exactly ONE
  connection per source_type -- no named/multi-connection setup. This is
  the ONLY code either package depends on outside its own folder (plus
  `shared/`).

Each package is independently testable and has its own DDL -- see
`tests/` and each package's `schema.sql`/`schema_drop.sql`.

## Setup

1. **First deployment** (no `gre_*` table exists yet, or you're OK
   discarding what's in them): run `shared/schema.sql`, then
   `rules_engine/schema.sql`, then `sampling/schema.sql`, in that order
   -- `rules_engine/` writes to `gre_rule_audit`/`gre_errors`, `sampling/`
   writes to `gre_sampling_audit`/`gre_errors`, all of which
   `shared/schema.sql` creates first (along with the `gre_audit`
   backward-compatibility view). See "Redeploying / changing the schema"
   below for how to make DDL changes later, and
   `migrate_split_gre_audit.sql` at the repo root instead if you already
   have real history in an existing combined `gre_audit` table.

2. **Local credentials**: copy `dev.env.example` to `dev.env` (repo
   root) and fill in real values. `shared/config.py` loads it
   automatically the first time either package is imported -- see that
   file's docstring, and `dev.env.example`'s header, for exactly how.
   `dev.env` is gitignored; never commit it. This is an interim, local-
   dev-only credential path -- see "Credentials" below for the planned
   pivot to a secrets manager.

3. **Connections**: exactly one connection per source_type -- `teradata`,
   `postgres` (AWS RDS/Aurora-compatible), `s3`, `file`. Fill in only the
   type(s) this deployment needs (`TERADATA_HOST`/`POSTGRES_HOST`/`S3_*`/
   `FILE_BASE_PATH`, see `dev.env.example`); `ConnectionFactory.load()`
   skips and logs any type that isn't configured rather than failing.
   A rule or sampling config selects its source_type directly
   (`gre_rules.sql_dialect` / `gre_sampling_config.source_type`) -- there
   is no separate named-connection column or setup step.

4. **Metadata connection/db**: both packages default to source_type
   `"teradata"` and schema `CMSUNIV_FILELAND_DEV_T`. Override with
   `GRE_META_CONNECTION`/`GRE_META_DB` if the `gre_*` tables live
   somewhere else.

5. **Tunables** (all optional, env-driven): `GRE_EXCEPTION_CHUNK`
   (bulk-write chunk size, default 500), `GRE_QUERY_MAX_RETRIES`
   (source-query retry attempts, default 3). There is no cap on
   gre_exceptions detail-row capture -- every violating row is captured,
   every attempt, regardless of count (see rules_engine/executor.py::
   _scan_violations()'s docstring); a rule matching a very large number of
   rows needs correspondingly more memory for that one attempt's scan.

## Running the engines end to end

Both packages are libraries, not CLIs -- there's no `main.py`/entrypoint in
this repo to run. A caller (your own script, scheduler job, Airflow task,
etc.) builds one `ConnectionFactory` and calls into `rules_engine/` and/or
`sampling/` directly:

```python
from db.connection_factory import ConnectionFactory
from shared import config as gre_config
from rules_engine.runner import run_rule_group, run_all_active_groups
from sampling.sampling import run_sampling

cf = ConnectionFactory()
cf.load()                                    # brings up every configured source_type

# run_key is an opaque tracking/idempotency identifier YOU construct --
# there's no fixed shape to it. shared/db_ops.py::build_run_key() is a
# convenience formatter for joining parts together, but a plain string you
# already have works just as well.
from shared.db_ops import build_run_key

run_key = build_run_key("BATCH_2026_08_14")     # a plain batch id, or...
run_key = build_run_key(2026, 8)                # ...a year+month scope ("2026_8"), or...
run_key = "2026-08-14"                          # ...just use an existing date/id string directly.

# Rules: one specific rule_group ---------------------------------------
summary = run_rule_group("claims_dq", run_key, cf)
print(summary["status"], summary["succeeded"], summary["errored"])

# Rules: every active rule_group for one project, in one call -----------
meta_conn = cf.get(gre_config.get_meta_connection_name())
outcome = run_all_active_groups(
    meta_conn, gre_config.get_meta_db(), run_key, cf,
    project_name="HEALTHSPRING_UM",
)
for rule_group, group_summary in outcome["rule_groups"].items():
    print(rule_group, group_summary["status"])

# Sampling: one sampling config -----------------------------------------
result = run_sampling(config_id=1, run_key="2026-08-14", cf=cf)
print(result["status"], result["selected"], "/", result["target_volume"])
```

`run_key` is deliberately NOT merged into `run_params` -- `run_params` is
a completely free-form dict used only for `{key}` substitution in
`rule_syntax`/`scope_sql`/`exclusion_sql`, and doubles as the equality
filters for the auto-generated total-record count. If a rule's SQL needs
to reference the run's tracking value as a literal column filter, pass it
explicitly via `run_params` under whatever key matches an actual column
(e.g. `run_params={"batch_id": "BATCH_2026_08_14"}`).

Nothing here needs the two engines to run together or in a particular
order -- `rules_engine/` and `sampling/` are independent (see each
package's own README) and neither imports the other. A caller driving both
against the same run just calls into each in whatever order its own
schedule requires. See [`rules_engine/README.md`](rules_engine/README.md)
for `rule_variant`/`project_name`/`process_name` scoping and the
multi-group fan-out (`run_all_active_groups()`), and
[`sampling/README.md`](sampling/README.md) for `run_params`-based scoping
of a sampling pull.

## Running everything one process owns: `run_by_process_name()`

The common case -- "run every active rule_group/sampling config a given
process owns, without looking up which ones those are first" -- has a
dedicated convenience wrapper in each package, resolving `meta_conn`/
`meta_db` from `cf` automatically:

```python
from rules_engine.runner import run_by_process_name
from sampling.sampling import run_sampling_for_process_name

outcome = run_by_process_name("UNIVERSE_VALIDATION", run_key, cf)
for rule_group, summary in outcome["rule_groups"].items():
    print(rule_group, summary["status"])

outcome = run_sampling_for_process_name("WEEKLY_REVIEW_SAMPLE", run_key, cf)
for config_id, summary in outcome["sampling_configs"].items():
    print(config_id, summary["status"])
```

Both accept an optional `project_name=` to narrow further, and raise
`ValueError` if nothing active matches the `process_name` given (almost
always a typo, so this fails loudly instead of silently doing nothing).

**`run_by_process.py`** at the repo root is a thin CLI on top of both, for
a quick local or scheduled run without writing a script:

```bash
python run_by_process.py rules --process-name UNIVERSE_VALIDATION
python run_by_process.py rules --process-name UNIVERSE_VALIDATION --project-name HEALTHSPRING_UM --run-key BATCH_2026_08_19
python run_by_process.py sampling --process-name WEEKLY_REVIEW_SAMPLE
```

`--run-key` defaults to today's date (`YYYY-MM-DD`) if omitted. Exit code
is `0` if everything completed, `1` if any group/config errored or the
`process_name` didn't match anything.

## Redeploying / changing the schema

Since no `gre_*` table holds live/production data yet, this repo's DDL
policy is: make schema changes by editing the relevant `schema.sql`'s
`CREATE` statements directly, then redeploy by dropping and recreating
rather than writing `ALTER TABLE` migrations. Each package has its own
`schema_drop.sql` for this:

```
shared/schema_drop.sql          -- drop the gre_audit view, gre_rule_audit, gre_sampling_audit, gre_errors
rules_engine/schema_drop.sql    -- drop gre_rules, gre_log, gre_exceptions, gre_results
sampling/schema_drop.sql        -- drop the 5 gre_sampling_*/gre_sample_* tables

-- then redeploy, shared first (rules_engine/ and sampling/ both assume
-- gre_rule_audit/gre_sampling_audit/gre_errors already exist):
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
`python-dotenv`, into the exact env var names `db/connection_factory.py`
already expects -- see `shared/config.py`'s docstring for the full
mechanics.

Planned pivot: AWS Secrets Manager (or similar), for non-local
deployments. This only requires changing `shared/config.py`'s
`_load_env_file()` to populate the same env var names from Secrets
Manager instead of a file -- `db/connection_factory.py`, `rules_engine/`,
and `sampling/` all only ever see already-populated env vars either way,
so none of them need to change.

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
| `test_shared_db_ops.py` | `shared/db_ops.py` -- bulk writes with duplicate-key tolerance, `{key}` run_params substitution (`_substitute_params`/`build_run_key`). |
| `test_rules_engine_rules.py` | `rules_engine/rules.py` -- `load_rules()`'s group/act_ind filtering, ordering, and `rule_variant` selection (universal vs. exact-match). |
| `test_rules_engine_executor.py` | `rules_engine/executor.py` -- threshold evaluation, natural-key building, `execute_rule()` end-to-end, the single-scan/memoized-total big-dataset path, run_params substitution and its fail-fast `PARAM_SUBSTITUTION_ERROR`. |
| `test_rules_engine_runner.py` | `rules_engine/runner.py` -- checkpoint/resume, `sequencing_mode` (`halt_group` vs. `skip_and_continue`), the shared `total_cache`, `rule_variant` end-to-end, `run_params` threading. |
| `test_sampling.py` | `sampling/sampling.py` -- rounding modes, selection methods, recursive stratification, candidate pull (including the `exclusion_sql` run_params fix), `run_sampling()` end-to-end, and a frozen regression fixture proving output still matches the originally-validated UM sample. |
