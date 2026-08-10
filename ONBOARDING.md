# Onboarding a New Project

A step-by-step walkthrough for standing up the DQ Rules Engine (`rules_engine/`)
and, optionally, the Sampling Framework (`sampling/`) against a brand-new
project's data -- from an empty metadata schema to a first successful run.
`HEALTHSPRING_UM_RUNBOOK.md` is a worked example of everything below for
one real project; this document is the generic version for yours.
Nothing here is project-specific -- every name (`ACME_CLAIMS`, table
names, column names) is a placeholder for whatever your project actually
calls things.

## 0. Prerequisites

- Python >= 3.8 (see `requirements.txt`'s header comment).
- A Teradata database to use as the metadata store -- this is the one
  hard requirement; `rules_engine/` and `sampling/` both write their metadata
  tables there regardless of what source system your actual DATA lives
  in (see `db/connection_factory.py`'s docstring: "the metadata store is
  always Teradata").
- Whatever driver your project's SOURCE data needs: `psycopg2-binary`
  (Postgres/Aurora), `duckdb` + `pandas` (flat files or S3), or one of
  the uncatalogued-but-supported adapter (SQL Server) --
  install only what you actually need, per `requirements.txt`'s comments.

```
pip install -r requirements.txt
# or, minimally, for a Teradata-metadata + Postgres-source setup:
pip install teradatasql psycopg2-binary tenacity
```

## 1. Apply the schema

Fresh metadata schema -- run these three files, in this order, against
your Teradata metadata database:

```
ddl_shared.sql        -- FIRST: dq_scope (both frameworks depend on this)
rules_engine/ddl.sql   -- rules-engine tables (skip if you only need sampling)
sampling/ddl.sql       -- sampling tables (skip if you only need the rules engine)
```

## 2. Configure connections

Connection **metadata** (what connections exist, source_type, host, port,
database, ...) lives in `config/connections.yaml` -- it's plain, non-secret
config, safe to commit. Edit it (or point `DQ_CONNECTIONS_FILE` at your own
copy) and add an entry per source system your rules will query.

At minimum, add a `teradata` entry (the metadata store). See that file's
comments for the field shape per `source_type` (teradata, postgresql, s3,
sqlserver, file) -- `config/connections.py::load_connections()` validates
it at startup and fails fast with a specific message on anything malformed
(missing required field, unknown source_type, duplicate name).

Connection **credentials** (user, password, token, access keys) are never
put in that file -- they stay in environment variables:

```
cp .env.example .env
```

Fill in one `DQ_<NAME>_*` block per connection, matching the `name` you
gave it in `config/connections.yaml` (see `.env.example`'s commented-out
Postgres/S3/SQL Server examples -- copy whichever matches your source
system and uncomment). Load `.env` however your process-manager expects
(`python-dotenv`, a systemd `EnvironmentFile`, your container platform's
secrets mechanism, etc.) -- this repo doesn't load `.env` itself; it reads
`os.environ` directly (see `config/env_config.py`, `db/adapters.py`).

## 3. Create a scope for your project/process

Optional -- `utils/db_helpers.py::get_scope_id()` auto-creates a
`dq_scope` row the first time `run_engine()` sees a new
(project, process) pair, so you can skip this and let the first run
create it. Seeding it explicitly up front (as HealthSpring UM's
`config/seed/01_setup.sql` does) is just for documentation/clarity if
you're hand-maintaining SQL seed files:

```sql
INSERT INTO <your_meta_db>.dq_scope (project_name, process_name)
VALUES ('ACME_CLAIMS', 'MONTHLY_AUDIT');
```

## 4. Author your first rules

Every rule lands in `dq_rules` as a complete negative-SQL SELECT --
it returns the rows that VIOLATE the rule; zero rows = PASS. See
`rules_engine/rule_sql.py`'s module docstring for the full authoring
model, and its `check_no_dml_ddl()` guard, which rejects anything that
isn't a read-only SELECT. Start with a couple of simple ones to prove
the pipeline end to end before writing your full rule set.

```sql
INSERT INTO <your_meta_db>.dq_rules (
    rule_id, rule_code, scope_id, src_tbl_nm, source_system,
    rule_name, rule_syntax, primary_key_columns, severity,
    sql_dialect, priority, active_flag
) VALUES (
    1, 'ACME-001',
    (SELECT scope_id FROM <your_meta_db>.dq_scope
     WHERE project_name = 'ACME_CLAIMS' AND process_name = 'MONTHLY_AUDIT'),
    'claims', 'teradata',
    'Claim amount must be non-negative',
    'SELECT claim_id, claim_amount FROM claims WHERE claim_amount < 0',
    'claim_id', 'HIGH',
    'teradata', 20, 1
);
```

`sql_dialect` must match a dialect your `source_system` connection's
`source_type` is compatible with -- see `rules_engine/rule_sql.py`'s
`DIALECT_COMPATIBILITY` table. Mismatches are caught before any rule
runs, not as a confusing mid-run SQL error.

`check_type` is an optional free-text classification tag (e.g.
`'NOT_NULL'`, `'RANGE_CHECK'`) -- purely for grouping/filtering findings
on the dashboard, it never affects how the rule's SQL runs.

## 5. Dry-run before writing anything

```
python rules_engine/main.py --project ACME_CLAIMS --process MONTHLY_AUDIT \
    --run-type TEST --run-mode FULL --dry-run
```

Validates every rule's SQL and dialect against your real connections
without writing a single row to any `dq_*` table. Fix anything this
flags before proceeding.

## 6. Run it for real

```
python rules_engine/main.py --project ACME_CLAIMS --process MONTHLY_AUDIT \
    --run-type MONTHLY --run-mode FULL
```

Check the outcome:

```sql
SELECT * FROM <your_meta_db>.dq_run_control ORDER BY start_time DESC;
SELECT * FROM <your_meta_db>.dq_rule_execution WHERE run_id = '<the run_id above>';
SELECT * FROM <your_meta_db>.dq_exceptions WHERE run_id = '<the run_id above>';
```

Or launch the dashboard for a live view:

```
streamlit run dashboard/streamlit_app.py
```

## 7. (Optional) Stratified sampling

If your process also needs "pick N highest-value cases for human
review" on top of pass/fail rule findings, that's the Sampling Framework
-- a separate framework, not a rules-engine feature (see
`sampling/engine.py`'s module docstring). Seed a `dq_sampling_config`
row (see `config/seed/01_setup.sql`'s `COMO_WEEKLY_SAMPLE` row for a
fully worked example of every field), then run it directly:

```python
from sampling.engine import run_stratified_sampling
from rules_engine.executor import execute_query
from db.connection_factory import ConnectionFactory
from config.env_config import get_meta_db

meta_db = get_meta_db()
cf = ConnectionFactory(); cf.load()
td = cf.get("teradata")
config = execute_query(td, f"""
    SELECT * FROM {meta_db}.dq_sampling_config
    WHERE sample_name = 'MY_SAMPLE' AND active_flag = 1
""")[0]
run_stratified_sampling(cf, td, config, {"run_id": "MANUAL_RUN"}, meta_db)
```

`sampling/anomaly.py` automatically checks the resulting candidate pool
size against this config's own history and flags/alerts on a sharp
drop or spike -- no extra setup needed once a few runs' worth of history
exists (see the `volume_drift` key on the returned summary dict).

## 8. (Optional) Schedule it

```
cp schedule.json.example schedule.json
```

Edit the job list to your project/process/cron cadence (see
`entrypoints.py::cron_run_scheduler`'s docstring for the exact field
shape), then:

```
pip install apscheduler
python entrypoints.py --schedule
```

Or wire `entrypoints.py`'s `lambda_handler` / `glue_main` /
`DataQualityEngineOperator` into Lambda / Glue / Airflow instead -- all
of them are thin wrappers around the same `rules_engine.engine.run_engine()`
call `rules_engine/main.py` and the scheduler use.

## 9. Where to go next

- **DESIGN.md** -- full architecture: the two-framework split, every
  extension point (`db/adapters.py` for a new source system -- a new
  rule is just a new `dq_rules` row, no code change), dialect
  enforcement, the write-statement guard, `dq_scope` normalization.
- **RETENTION.md** -- once you're running in production, plan
  partitioning/archival for the high-growth tables before they become a
  performance problem, not after.
- **METADATA_TABLE_SAMPLE_INSERTS.md** -- a sample INSERT for every one of
  the 18 metadata tables, grouped by who writes them (you, the engine, or
  ops), all tracing one example rule through to a finding.
- **HEALTHSPRING_UM_RUNBOOK.md** -- a complete real-world example of
  everything in this document, for one actual project.
