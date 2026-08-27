# GRE + Sampling

Two fully independent, config-driven engines:

- **[`rules_engine/`](rules_engine/README.md)** -- the Generic Rules
  Engine (GRE). `gre_rules` rows define a negative SQL SELECT per rule;
  the engine runs each active rule for a `rule_group`/`run_key`, writes
  every violating row to `gre_exceptions`, evaluates a threshold, and
  upserts a `gre_results` verdict. Owns its own `db_ops.py`, `config.py`,
  `gre_rule_audit` (run-tracking), and `gre_rule_errors` (error log).
- **[`sampling/`](sampling/README.md)** -- generic stratified sampling.
  Given a candidate universe, an exclusion filter, any number of
  stratification levels, and a selection method (RANKED/RANDOM/
  SYSTEMATIC), picks a target-volume sample and persists every candidate
  considered. Owns its own `db_ops.py`, `config.py`, `gre_sampling_audit`
  (run-tracking), and `gre_sampling_errors` (error log).
- **`db/connection_factory.py`** -- every source adapter (Teradata,
  Postgres, S3, file) plus `ConnectionFactory`, in one file. Exactly ONE
  connection per source_type -- no named/multi-connection setup. This is
  the ONLY code either package depends on outside its own folder.

## Package separation

`rules_engine/` and `sampling/` share **no code and no tables**. Each
package carries its own copy of the low-level DB helpers (`db_ops.py`:
bulk writes, `{key}` run_params substitution, `generate_run_id()`,
`build_run_key()`, `count_prior_attempts()`, `log_error()`) and its own
copy of the config/credentials layer (`config.py`: local `.env` loading,
metadata connection/db resolution, parallel-execution tunables,
readiness-check registry). The two files are byte-for-byte identical
between packages except for each package's `count_prior_attempts()`
signature (`rule_group` vs. `sample_config_id`) and `log_error()`'s
target table.

There used to be a `shared/` package holding one copy of `db_ops.py`/
`config.py` plus a combined `gre_audit` run-tracking table and a combined
`gre_errors` error table. It has been removed. `gre_audit` was split into
`gre_rule_audit`/`gre_sampling_audit`, and `gre_errors` was split into
`gre_rule_errors`/`gre_sampling_errors` -- see each package's own
`schema.sql` for the DDL, which each already defines the split shape
directly (there's no migration path from the old combined tables --
no `gre_*` table holds live/production data yet, so this is treated as a
fresh deployment; see "Redeploying / changing the schema" below).
Neither split table has a compatibility view standing in for the old
combined name -- anything that queried `gre_audit`/`gre_errors` under the
old combined shape needs to move to the package-specific tables instead,
or build its own UNION ALL across them. `db/connection_factory.py` is the
one deliberate exception to "no shared code" -- it's pure
source-connection infrastructure with no rules/sampling-specific logic,
and duplicating it would double the number of real live connections
opened per source_type whenever both packages run in the same process.

Each package is independently testable and has its own DDL -- see
`tests/` and each package's `schema.sql`/`schema_drop.sql`. Neither
`schema.sql` needs the other to run first.

## Setup

1. **First deployment** (no `gre_*` table exists yet, or you're OK
   discarding what's in them -- no `gre_*` table holds live/production
   data yet, so every deployment today is treated this way): run
   `rules_engine/schema.sql` and `sampling/schema.sql` -- in either order,
   or just the one package you need; neither depends on the other. See
   "Redeploying / changing the schema" below for how to make DDL changes
   later.

2. **Local credentials**: copy `dev.env.example` to `dev.env` (repo
   root) and fill in real values. Each package's own `config.py` loads it
   automatically the first time that package is imported -- see either
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
from rules_engine import config as gre_config
from rules_engine.runner import run_rule_group, run_all_active_groups
from sampling.sampling import run_sampling

cf = ConnectionFactory()
cf.load()                                    # brings up every configured source_type

# run_key is an opaque tracking/idempotency identifier YOU construct --
# there's no fixed shape to it. Either package's db_ops.py::build_run_key()
# is a convenience formatter for joining parts together (identical in both
# -- pick whichever package you're already importing), but a plain string
# you already have works just as well.
from rules_engine.db_ops import build_run_key

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

For `rules_engine/` specifically, a `run_params`/`--param` value whose
key does NOT match a real column (used only for `rule_syntax` text
substitution, e.g. `RUNTYPE`) belongs in the separate `text_params`/
`--text-param` instead -- passing it as `run_params` breaks the
auto-generated total-record count with an "unresolved/unknown column"
error, since every `run_params` key is assumed to name a real column.
See [`rules_engine/README.md`](rules_engine/README.md)'s "Text-only
substitution values: `text_params`" section.

Also for `rules_engine/`: rerunning the same `run_key` blanket-
deactivates every currently-active `gre_results`/`gre_rule_errors`/
`gre_exceptions` row for that `(rule_group, run_key)` in one pass before
the new attempt's rules execute -- nothing from a prior attempt is left
active, even for a rule no longer part of the current attempt's rule
set. See [`rules_engine/README.md`](rules_engine/README.md)'s "Rerunning
a `run_key`" section.

Nothing here needs the two engines to run together or in a particular
order -- `rules_engine/` and `sampling/` are independent (see each
package's own README) and neither imports the other. A caller driving both
against the same run just calls into each in whatever order its own
schedule requires. See [`rules_engine/README.md`](rules_engine/README.md)
for `rule_variant`/`project_name`/`process_name` scoping and the
multi-group fan-out (`run_all_active_groups()`), and
[`sampling/README.md`](sampling/README.md) for `run_params`-based scoping
of a sampling pull.

## Running at whatever level you scope it to: `run_by_scope()`

For `rules_engine/`, the general-purpose entry point is `run_by_scope()`:
pass whichever of `project_name`/`process_name`/`rule_group` you have, and
it runs at exactly that level -- project (every process under it),
process, process+rule_group, or one rule_group directly, each optionally
narrowed further by `rule_variant`. Whatever you leave out means "every
value of it," never "nothing":

```python
from rules_engine.runner import run_by_scope
from sampling.sampling import run_sampling_for_process_name

# Project level, process level, rule_group level, or any combination --
# see rules_engine/README.md's "One entry point for every scoping level"
# section for the full set of examples:
outcome = run_by_scope(run_key=run_key, cf=cf, process_name="UNIVERSE_VALIDATION")
for rule_group, summary in outcome["rule_groups"].items():
    print(rule_group, summary["status"])

outcome = run_sampling_for_process_name("WEEKLY_REVIEW_SAMPLE", run_key, cf)
for config_id, summary in outcome["sampling_configs"].items():
    print(config_id, summary["status"])
```

`run_by_scope()` raises `ValueError` if none of `project_name`/
`process_name`/`rule_group` are given, and also if `project_name`/
`process_name` are given but match nothing active (almost always a typo,
so this fails loudly instead of silently doing nothing). `sampling/`'s
`run_sampling_for_process_name()` accepts an optional `project_name=` to
narrow further, with the same fail-loudly-on-no-match behavior.

(`rules_engine/runner.py` also still has the older, narrower
`run_by_process_name()` -- process level only -- kept for backward
compatibility; new code should use `run_by_scope()`.)

**`run_by_process.py`** at the repo root is a thin CLI on top of both, for
a quick local or scheduled run without writing a script:

```bash
python run_by_process.py rules --project-name HEALTHSPRING_UM
python run_by_process.py rules --process-name UNIVERSE_VALIDATION
python run_by_process.py rules --process-name UNIVERSE_VALIDATION --project-name HEALTHSPRING_UM --run-key BATCH_2026_08_19
python run_by_process.py rules --rule-group ODAG3 --rule-variant EAST
python run_by_process.py sampling --process-name WEEKLY_REVIEW_SAMPLE
```

`--run-key` defaults to today's date (`YYYY-MM-DD`) if omitted. Exit code
is `0` if everything completed, `1` if any group/config errored or the
scope didn't match anything.

## Redeploying / changing the schema

Since no `gre_*` table holds live/production data yet, this repo's DDL
policy is: make schema changes by editing the relevant `schema.sql`'s
`CREATE` statements directly, then redeploy by dropping and recreating
rather than writing `ALTER TABLE` migrations. Each package has its own
`schema_drop.sql` for this:

```
rules_engine/schema_drop.sql    -- drop gre_rules, gre_exceptions, gre_results, gre_rule_audit, gre_rule_errors
sampling/schema_drop.sql        -- drop the 7 gre_sampling_*/gre_sample_* tables (incl. gre_sampling_audit, gre_sampling_errors)

-- then redeploy -- independently; neither package needs the other's
-- schema.sql to run first:
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

Today: local `.env` file (`dev.env`), loaded by each package's own
`config.py` via `python-dotenv`, into the exact env var names
`db/connection_factory.py` already expects -- see either `config.py`'s
docstring for the full mechanics (identical between the two).

Planned pivot: AWS Secrets Manager (or similar), for non-local
deployments. This only requires changing each package's `config.py`'s
`_load_env_file()` to populate the same env var names from Secrets
Manager instead of a file -- `db/connection_factory.py`, `rules_engine/`,
and `sampling/` all only ever see already-populated env vars either way,
so none of them need to change.

## Debug logging

Both packages log through the standard `logging` module (`logger =
logging.getLogger(__name__)` in every file) -- neither ever calls
`logging.basicConfig()` itself, so importing them never clobbers a host
application's own logging setup.

Detailed (DEBUG) logging is ON BY DEFAULT -- `configure_logging()`'s
default level, and `run_by_process.py`'s `--log-level` flag, both resolve
to `DEBUG` unless told otherwise, so nothing extra needs to be passed to
see it.

To use the library functions directly (not through `run_by_process.py`),
call the opt-in helper once before running anything -- it's still opt-in
at the "does it run at all" level (nothing calls `logging.basicConfig()`
automatically at import time, so importing these packages never clobbers
a host application's own logging setup), just not opt-in at the "what
level" question anymore:

```python
from rules_engine.config import configure_logging   # or: from sampling.config import configure_logging
configure_logging()          # DEBUG by default now
configure_logging("INFO")    # or set GRE_LOG_LEVEL=INFO to quiet it down
```

`run_by_process.py` (the CLI wrapper) already calls this for you on every
run -- no flag needed:

```
python run_by_process.py rules --process-name UNIVERSE_VALIDATION
python run_by_process.py rules --process-name UNIVERSE_VALIDATION --log-level INFO   # quieter
```

What gets logged, by level:

- **INFO** -- which Teradata/Postgres host/db/user a connection actually
  resolved to (`db/connection_factory.py`, password never included), and
  one "run starting" line per attempt (`rules_engine/runner.py`'s
  `run_rule_group starting: ... meta_host=...`, `sampling/sampling.py`'s
  `run_sampling starting: ... meta_host=...`). This is aimed squarely at
  catching a run silently pointed at the wrong environment -- the same
  schema NAME can exist on two different hosts, one missing a column the
  other has, which otherwise surfaces as a confusing "column not present"
  error against a schema that looks identical by name.
- **DEBUG** -- every SQL statement's TEXT (collapsed to one line,
  truncated at 500 characters) and row COUNTS (rows returned by a SELECT,
  rows affected by a DML, chunk sizes on bulk writes), from
  `rules_engine/db_ops.py` / `sampling/db_ops.py`'s `execute_query()`,
  `execute_dml()`, `_chunked_executemany()`, `bulk_insert_or_skip()`, and
  `_run_source_query()`. At every metadata-store call site (rule
  discovery, rule loading, `gre_rule_audit` start/finish, `gre_results`
  writes/deactivations, `gre_rule_errors` writes/deactivations) bind
  parameter VALUES are logged too (`rules_engine/db_ops.py`'s opt-in
  `log_params=True`; see `rules_engine/README.md`'s "Metadata-query
  logging" section). For a rule's own `rule_syntax` scan and the
  auto-generated total-record `COUNT(*)` query, there's no separate
  "params" step at all -- `run_params`/`extra_filters` are TEXT-substituted
  into the SQL string before it ever runs, so the logged SQL TEXT already
  shows every substituted value inline.

What never gets logged, at any level: bind parameter values from a query
run against a SOURCE connection, or any fetched/written row DATA. Only
param/row COUNTS are logged for those. `rule_syntax`/`scope_sql`/
`exclusion_sql` run against source tables that can carry real case,
member, or claim identifiers -- logging shape (what ran, how many rows)
instead of content keeps a debug log safe to share for troubleshooting
without it becoming its own PHI-adjacent data spill. `log_params=True` is
only ever turned on for metadata-store (`gre_*`) queries, never for a
source-connection query -- `_run_source_query()` never passes bind params
at all, so this guarantee holds regardless of the flag. `gre_exceptions`'
bulk writes are the one metadata-store table excluded from this -- a
violating record's own key/detail VALUES are real source data, not
engine bookkeeping, so those bulk writers keep logging counts only,
unconditionally.

**Log rotation**: each package's log file (`rules_engine.log` /
`sampling.log`, under `GRE_LOG_DIR`) rotates at MIDNIGHT, not by file
size -- one file per calendar day, named with a date suffix (e.g.
`rules_engine.log.2026-08-27`) once that day is over; today's activity
always lives at the plain, unchanging `GRE_LOG_FILE` path. Restarting the
process never truncates or overwrites what's already there -- every
handler opens in append mode. `GRE_LOG_RETENTION_DAYS` (default `30`)
caps how many past days are kept before the oldest dated file is deleted;
set it to `0` to keep every day's log forever.

## Running the tests

```
pip install -r requirements.txt
python3 -m pytest tests/ -q
```

Every test runs against an in-memory DuckDB connection -- no live
Teradata/Postgres/etc. connection is needed.

| Test file | Covers |
|---|---|
| `test_rules_engine_config.py` | `rules_engine/config.py` -- `.env` loading, metadata connection/db resolution, batch-readiness checks. |
| `test_rules_engine_db_ops.py` | `rules_engine/db_ops.py` -- bulk writes with duplicate-key tolerance, `{key}` run_params substitution (`_substitute_params`/`build_run_key`), `count_prior_attempts()` keyed by `rule_group`. |
| `test_sampling_config.py` | `sampling/config.py` -- identical coverage to `test_rules_engine_config.py`, against sampling's own copy. |
| `test_sampling_db_ops.py` | `sampling/db_ops.py` -- identical coverage to `test_rules_engine_db_ops.py`, against sampling's own copy, with `count_prior_attempts()` keyed by `sample_config_id` instead. |
| `test_rules_engine_rules.py` | `rules_engine/rules.py` -- `load_rules()`'s group/act_ind filtering, ordering, and `rule_variant` selection (no filter when omitted, universal-plus-exact-match when passed). |
| `test_rules_engine_executor.py` | `rules_engine/executor.py` -- threshold evaluation, natural-key building, `execute_rule()` end-to-end, the single-scan/memoized-total big-dataset path, run_params substitution and its fail-fast `PARAM_SUBSTITUTION_ERROR`, and `text_params` substitution without total-count scoping. |
| `test_rules_engine_runner.py` | `rules_engine/runner.py` -- checkpoint/resume, `sequencing_mode` (`halt_group` vs. `skip_and_continue`), the shared `total_cache`, `rule_variant` end-to-end, `run_params`/`text_params` threading, `run_by_scope()`'s project/process/rule_group/rule_variant dispatch, and the blanket `active_ind`/`etl_is_curr_ind` rerun deactivation. |
| `test_sampling.py` | `sampling/sampling.py` -- rounding modes, selection methods, recursive stratification, candidate pull (including the `exclusion_sql` run_params fix), `run_sampling()` end-to-end, and a frozen regression fixture proving output still matches the originally-validated UM sample. |
