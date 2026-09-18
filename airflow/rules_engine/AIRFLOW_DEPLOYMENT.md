# GRE Rules Framework -- Airflow Module

`airflow/` is a **separate, self-contained copy** of just the rules
framework from `DQ_COMPLETE_REPO`, packaged to be called from an Airflow
DAG. Nothing outside `airflow/` was touched, and every one of
`rules_engine/`'s own original files (`config.py`, `runner.py`,
`executor.py`, ...) is an exact, unmodified copy of the original repo's
`rules_engine/`.

**Connectivity is limited to Teradata + Postgres, and every run uses
exactly ONE of them -- never both.** `rules_engine/db/` here is a
trimmed copy of the original repo's `db/connection_factory.py` with the
flat-file (CSV/Excel/TSV/Parquet) and S3 adapters removed --
`TeradataAdapter` and `PostgresAdapter` are the only two source types
this package can build a connection for, and `gre_rules_task.py` (below)
loads only whichever one a given run's Airflow Variable names.

Everything this integration needs -- the framework itself, its Airflow
bridge, its trimmed connection factory, and its CLI wrapper -- lives
inside THIS `rules_engine/` folder. The one and only exception is
`gre_rules_dag.py`, one level up in `airflow/`, since Airflow needs an
actual DAG file to discover and that has to sit outside any package:

```
airflow/
├── gre_rules_dag.py            # the ONLY file outside rules_engine/ -- example DAG
└── rules_engine/
    ├── (original files, unchanged: __init__.py, config.py, db_ops.py,
    │    deploy_schema.py, executor.py, parallel.py, reporting.py,
    │    rules.py, runner.py, schema.sql, schema_drop.sql, README.md)
    ├── db/                      # trimmed copy of the original db/connection_factory.py --
    │   │                        #   Teradata + Postgres adapters only
    │   ├── __init__.py
    │   └── connection_factory.py
    ├── run_rules.py             # rules-only copy of ../run_by_process.py, trimmed to
    │                            #   drop the sampling subcommand, and rewritten to
    │                            #   expose a run_rules() function (not just a CLI) so
    │                            #   it can be called in-process from Airflow
    ├── gre_rules_task.py        # the ONLY Airflow-aware file in this folder -- the
    │                            #   bridge described below
    ├── requirements.txt         # trimmed to what rules_engine/ + db/ need (no
    │                            #   sampling-only or metadata_sync-only packages)
    └── AIRFLOW_DEPLOYMENT.md    # this file
```

`db/` stays its own folder only because Python's import system requires
it (`rules_engine/parallel.py`, unmodified, does
`from db.connection_factory import ...`, and that path has to resolve to
an actual package on disk); `run_rules.py` and `gre_rules_task.py`,
which don't need to be packages themselves, sit flat inside
`rules_engine/` right alongside it. See "How the imports resolve" below
for exactly how `run_rules.py`/`gre_rules_task.py`/`gre_rules_dag.py`
find each other and `rules_engine`/`db` despite this nesting.

What was intentionally left out entirely: `sampling/`, `metadata_sync/`,
`report_source_records.py`, `tests/`, `dev.env.example` -- none of
those are part of the rules framework itself, and none are needed for
this Airflow integration (see "Why no `.env` file here" below).

## How it fits together

1. **`rules_engine/`'s own original files** -- the engine, unmodified.
   **`rules_engine/db/`** -- a trimmed copy of the original
   `connection_factory.py`, Teradata + Postgres only. Both read
   configuration and credentials from environment variables only
   (neither has any idea Airflow exists).
2. **`rules_engine/run_rules.py`** -- a rules-only wrapper (no
   sampling). Same behavior as the original repo's
   `run_by_process.py rules ...` subcommand, plus a plain
   `run_rules(...)` function so a caller can invoke it directly instead
   of shelling out to a CLI.
3. **`rules_engine/gre_rules_task.py`** -- the bridge. Its
   `run_gre_rules()` function:
   1. Reads the Airflow Variable FIRST (step 2 below) far enough to know
      which single connection to load, then loads credentials for
      exactly that one connection type (`teradata` or `postgres`, never
      both) from ONE Airflow Connection.
   2. Reads every other runtime input -- environment, metadata db, log
      level, and the run's scope (project/process/rule_group/
      rule_variant/run_key/run_params/extra_filters/text_params) -- from
      that SAME Airflow Variable.
   3. Sets the environment variables `rules_engine/`/`db/` already
      expect (`TERADATA_HOST`, `TERADATA_USER`, `TERADATA_PASSWORD`,
      `TERADATA_LOGMECH`, or `POSTGRES_HOST`, `POSTGRES_PORT`,
      `POSTGRES_DATABASE`, `POSTGRES_USER`, `POSTGRES_PASSWORD`,
      `POSTGRES_SSLMODE` -- whichever one type is in use -- plus
      `GRE_ENVIRONMENT`, `GRE_META_DB`, `GRE_META_CONNECTION`, ...) --
      **before** importing/calling `run_rules.py`, since some of those
      env vars are read at import time, not just at call time.
   4. Calls `run_rules.run_rules(...)` -- the existing engine, as-is.
   5. Raises `RuntimeError` on any non-`COMPLETED` rule_group (or a
      bad/empty scope), so the Airflow task fails normally; otherwise
      returns the outcome dict.
4. **`gre_rules_dag.py`** (one level up, in `airflow/`) -- an example DAG
   that calls `run_gre_rules()` from a `PythonOperator` (and, as an
   alternative, a TaskFlow `@task`). The DAG itself hardcodes nothing
   about the run -- see "Everything is Variable-driven" below.

No business logic was moved into Airflow -- the DAG and the bridge
module only ever read Airflow config and set environment variables;
`rules_engine/` runs exactly as it does outside Airflow.

## How the imports resolve, given the nesting

Two different "roots" need to be on `sys.path` because `db/` sits inside
`rules_engine/` while `rules_engine` itself has to resolve as a
top-level package:

- `rules_engine/` itself -- so `from db.connection_factory import ...`
  (in `run_rules.py`, and, unmodified, in `rules_engine/parallel.py`/
  `rules_engine/deploy_schema.py`) finds `db` as a direct child of it,
  and so `gre_rules_task.py`'s `from run_rules import run_rules` finds
  its sibling.
- `rules_engine/`'s PARENT (`airflow/`) -- so `from rules_engine.config
  import ...` / `from rules_engine.runner import ...` (used inside
  `run_rules.py`'s own `run_rules()` function) resolve `rules_engine` as
  a top-level package.

Every file bootstraps whichever of these it needs itself (see the
`sys.path` block near the top of `run_rules.py`, `gre_rules_task.py`,
and `gre_rules_dag.py`) -- nothing relies on the Airflow worker's DAG
processor having already set `sys.path` up a particular way.

## Everything is Variable-driven -- nothing hardcoded in the DAG

Every input this integration needs lives in **one Airflow Variable**
(JSON) -- which connection to use, which Connection id, environment,
metadata db, log level, and the run's scope at whatever level you need
(project / process / rule_group / rule_variant). `gre_rules_dag.py`
itself hardcodes nothing but which Variable to read
(`variable_key`, itself overridable per trigger -- see below) -- to run
at a different level, against a different scope, or switch from
Teradata to Postgres, edit the Variable, never the DAG file.

### Variable shape (all keys optional except `connection_type`)

```json
{
  "connection_type": "teradata",
  "connection_id": "gre_teradata_prod",

  "environment": "PROD",
  "meta_db": "GRE_META_PROD",
  "meta_connection": "teradata",
  "log_level": "INFO",
  "log_dir": "/opt/airflow/logs/gre",
  "max_parallel_rules": "4",

  "project_name": "HEALTHSPRING_UM",
  "process_name": "UNIVERSE_VALIDATION",
  "rule_group": null,
  "rule_variant": null,
  "run_key": null,
  "run_params": {"year": "2026", "month": "9"},
  "extra_filters": {},
  "text_params": {}
}
```

- `connection_type` -- **required** (or pass `connection_type=` directly
  to `run_gre_rules()`). Exactly `"teradata"` or `"postgres"` -- only
  that one Connection is read; the other source type's env vars are
  never set, so a run never ends up with both loaded at once.
- `connection_id` -- optional. Defaults to `"gre_teradata"` or
  `"gre_postgres"` (matching `connection_type`) if omitted.
- `meta_connection` -- optional. Defaults to `connection_type` if
  omitted, since only one connection is ever loaded and the metadata
  store has to be the one that's actually there.
- `environment` / `meta_db` / `log_level` / `log_dir` /
  `max_parallel_rules` become `GRE_ENVIRONMENT` / `GRE_META_DB` /
  `GRE_LOG_LEVEL` / `GRE_LOG_DIR` / `GRE_MAX_PARALLEL_RULES`
  respectively. Omitting `log_level` gets this package's default of
  `"ERROR"` (errors only).
- `project_name` / `process_name` / `rule_group` / `rule_variant` /
  `run_key` / `run_params` / `extra_filters` / `text_params` are passed
  straight through to `run_rules()` as the matching scoping argument --
  set however many/few of these to run at whatever level you need. See
  `rules_engine/runner.py::run_by_scope()`'s docstring for exactly how
  project/process/rule_group/rule_variant combine to scope a run.

### Calling this at any level, or overriding one-off, without editing the Variable

Trigger the DAG with a run configuration (Airflow UI "Trigger DAG w/
config", the CLI's `-c`, or the REST API) to run a one-off scope, switch
connections, or even read a different Variable entirely:

```json
{"process_name": "UNIVERSE_VALIDATION", "run_key": "BACKFILL_2026_08"}
```
```json
{"variable_key": "gre_rules_config_qa"}
```
```json
{"connection_type": "postgres", "rule_group": "ODAG3"}
```

Every key in that JSON is merged over the configured Variable's config
(`overrides=` in `run_gre_rules()`) -- anything you don't pass keeps the
Variable's own value. `variable_key` in the run config swaps which
Variable is read entirely. `gre_rules_dag.py`'s `variable_key` DAG param
(shown in the Airflow UI's Trigger form) covers the same thing without
needing raw JSON.

## Why no `.env` file here

`dev.env` (loaded by `rules_engine/config.py` via `python-dotenv`) is
for local development / developer machines. In Airflow:

- **One Airflow Connection** provides credentials for whichever single
  source (Teradata or Postgres) that run uses.
- **One Airflow Variable** provides everything else -- which connection,
  environment, metadata db, log level, and scope.
- `gre_rules_task.py` turns those into the same environment variables a
  `.env` file would have populated, before the engine starts.
  `rules_engine/` and `db/connection_factory.py` never know the
  difference.

If you also want to run `run_rules.py`'s CLI by hand from this folder
(outside Airflow, for local testing), you can still create your own
`.env`/exported env vars for that -- it's just never used by the DAG
path.

## Setup

### 1. Airflow Connection -- credentials for whichever ONE source you use

Teradata:

| Field | Value |
|---|---|
| Connection Id | `gre_teradata` (default -- or your own, named via `"connection_id"` in the Variable) |
| Connection Type | `Teradata` (or `Generic` if the Teradata provider package isn't installed -- only host/login/password/extra are read) |
| Host / Login / Password | as usual |
| Extra | `{"logmech": "LDAP"}` (optional -- defaults to `LDAP` if omitted) |

Postgres/Aurora:

| Field | Value |
|---|---|
| Connection Id | `gre_postgres` (default -- or your own, named via `"connection_id"` in the Variable) |
| Connection Type | `Postgres` |
| Host / Schema / Login / Password / Port | as usual |
| Extra | `{"sslmode": "prefer"}` (optional -- defaults to `prefer` if omitted) |

Only ONE of these is ever read per run -- whichever `connection_type`
the Variable (or a trigger-time override) names. File/S3 source
connectivity is not supported by this package at all -- see
`rules_engine/db/connection_factory.py`'s module docstring.

### 2. Airflow Variable -- everything else

See "Variable shape" above. Default key: `gre_rules_config` (override
via the DAG's `variable_key` param, or `variable_key=` if calling
`run_gre_rules()` directly).

### 3. Deploy this folder

Copy (or mount) the entire `airflow/` folder (both `gre_rules_dag.py`
and the `rules_engine/` folder beneath it) into your Airflow deployment.
`gre_rules_dag.py` bootstraps its own `sys.path` entry for
`rules_engine/` before importing `gre_rules_task` (see "How the imports
resolve" above), so no extra `PYTHONPATH` setup is required as long as
this folder's relative layout -- `gre_rules_dag.py` next to
`rules_engine/` -- stays intact; e.g. drop `airflow/`'s contents
straight into your DAGs folder, or symlink/mount it there. Install
`rules_engine/requirements.txt` into the Airflow worker's environment.

## Local/manual use (outside Airflow)

```bash
cd airflow/rules_engine/
export TERADATA_HOST=... TERADATA_USER=... TERADATA_PASSWORD=... TERADATA_LOGMECH=LDAP
export GRE_ENVIRONMENT=DEV GRE_META_DB=GRE_META_DEV

python run_rules.py --process-name UNIVERSE_VALIDATION \
    --project-name HEALTHSPRING_UM \
    --run-key 2026-09-18 \
    --param year=2026 --param month=9
```

Same flags/behavior as the original repo's
`run_by_process.py rules ...` subcommand -- `run_rules.py` is that
subcommand's logic, copied out on its own.
