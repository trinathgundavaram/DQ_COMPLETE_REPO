# Onboarding a New Project

A step-by-step walkthrough for standing up the DQ Rules Engine (`core/`)
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
  hard requirement; `core/` and `sampling/` both write their metadata
  tables there regardless of what source system your actual DATA lives
  in (see `db/connection_factory.py`'s docstring: "the metadata store is
  always Teradata").
- Whatever driver your project's SOURCE data needs: `psycopg2-binary`
  (Postgres/Aurora), `duckdb` + `pandas` (flat files or S3), or one of
  the uncatalogued-but-supported adapters (Databricks, SQL Server) --
  install only what you actually need, per `requirements.txt`'s comments.

```
pip install -r requirements.txt
# or, minimally, for a Teradata-metadata + Postgres-source setup:
pip install teradatasql psycopg2-binary tenacity
```

## 1. Apply the schema

Fresh metadata schema:

```
-- run top to bottom against your Teradata metadata database
ddl.sql
```

Upgrading an existing pre-v7 deployment instead: run
`migrations/v6_to_v7.sql` (phase by phase, with the backups it
recommends -- see that file's header) rather than `ddl.sql` directly.

## 2. Configure connections

```
cp .env.example .env
```

Fill in, at minimum:
- `DQ_CONNECTION_NAMES` -- include `teradata` (the metadata store) plus
  a name for each source system your rules will query.
- `DQ_TERADATA_*` -- your metadata store credentials.
- One `DQ_<NAME>_*` block per additional source connection (see
  `.env.example`'s commented-out Postgres/S3/file/Databricks/SQL Server
  examples -- copy whichever matches your source system and uncomment).

Load `.env` however your process-manager expects (`python-dotenv`,
a systemd `EnvironmentFile`, your container platform's secrets
mechanism, etc.) -- this repo doesn't load `.env` itself; it reads
`os.environ` directly (see `config/env_config.py`,
`db/connection_factory.py`).

Register each source connection's metadata (optional but recommended --
`dq_connections` is a reference/documentation table, not read at
runtime; credentials always come from env vars, never this table):

```sql
INSERT INTO <your_meta_db>.dq_connections
    (connection_id, connection_name, source_type, database_name, description, active_flag)
VALUES
    (1, 'teradata', 'teradata', '<your_meta_db>', 'Metadata store + primary source', 1);
```

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

Two authoring paths, both land in `dq_rules`. Start with a couple of
simple ones to prove the pipeline end to end before writing your full
rule set.

**Path 2 -- built-in check_type** (declarative, no SQL to write; see
`core/check_types.py` for all 24 generators and `tests/test_check_types.py`
for a worked example of each):

```sql
INSERT INTO <your_meta_db>.dq_rules (
    rule_id, rule_code, scope_id, src_tbl_nm, source_system,
    rule_name, primary_key_columns, severity,
    check_type, check_column, priority, active_flag
) VALUES (
    1, 'ACME-001',
    (SELECT scope_id FROM <your_meta_db>.dq_scope
     WHERE project_name = 'ACME_CLAIMS' AND process_name = 'MONTHLY_AUDIT'),
    'claims', 'teradata',
    'Claim amount must not be null', 'claim_id', 'HIGH',
    'NOT_NULL', 'claim_amount', 10, 1
);
```

**Path 1 -- raw SQL** (a complete negative-SQL SELECT -- returns the
rows that VIOLATE the rule; zero rows = PASS; see `core/rule_sql.py`'s
module docstring for the full authoring model, and `core/rule_sql.py`'s
`check_no_dml_ddl()` guard, which rejects anything that isn't a
read-only SELECT):

```sql
INSERT INTO <your_meta_db>.dq_rules (
    rule_id, rule_code, scope_id, src_tbl_nm, source_system,
    rule_name, rule_syntax, primary_key_columns, severity,
    sql_dialect, priority, active_flag
) VALUES (
    2, 'ACME-002',
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
`source_type` is compatible with -- see `core/rule_sql.py`'s
`DIALECT_COMPATIBILITY` table. Mismatches are caught before any rule
runs, not as a confusing mid-run SQL error.

## 5. Dry-run before writing anything

```
python main.py --project ACME_CLAIMS --process MONTHLY_AUDIT \
    --run-type TEST --run-mode FULL --dry-run
```

Validates every rule's SQL and dialect against your real connections
without writing a single row to any `dq_*` table. Fix anything this
flags before proceeding.

## 6. Run it for real

```
python main.py --project ACME_CLAIMS --process MONTHLY_AUDIT \
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
from core.executor import execute_query
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
of them are thin wrappers around the same `core.engine.run_engine()`
call `main.py` and the scheduler use.

## 9. Where to go next

- **DESIGN.md** -- full architecture: the two-framework split, every
  extension point (`core/check_types.py` for a new check type,
  `db/adapters.py` for a new source system), dialect enforcement, the
  write-statement guard, `dq_scope` normalization.
- **RETENTION.md** -- once you're running in production, plan
  partitioning/archival for the high-growth tables before they become a
  performance problem, not after.
- **HEALTHSPRING_UM_RUNBOOK.md** -- a complete real-world example of
  everything in this document, for one actual project.
