# HealthSpring UM — Deployment Runbook

## 1. Apply schema changes

Run `ddl.sql` top to bottom on a fresh environment, or apply the `v5 → v6`
ALTER block (bottom of the file) on an existing one. Then load, in order:

```
config/seed/01_setup.sql
config/seed/02_rules.sql
```

Before loading `02_rules.sql`, replace the assumed `um_universe` column
names with the real MHK ODAG1 field names — see
`config/seed/00_healthspring_um_README.md` for the assumed schema and the
SHRPA/provider-contract co-location dependency.

## 2. Configure connections (env vars — never in `dq_connections`)

```
DQ_CONNECTION_NAMES=teradata,um_refdata,um_archive
DQ_META_CONNECTION=teradata

DQ_TERADATA_TYPE=teradata
DQ_TERADATA_HOST=...
DQ_TERADATA_USER=...
DQ_TERADATA_PASSWORD=...

DQ_UM_REFDATA_TYPE=postgresql
DQ_UM_REFDATA_HOST=...
DQ_UM_REFDATA_DATABASE=um_reference
DQ_UM_REFDATA_USER=...
DQ_UM_REFDATA_PASSWORD=...

DQ_UM_ARCHIVE_TYPE=s3
DQ_UM_ARCHIVE_REGION=us-east-1
# Access key/secret optional — omit to use the instance/task IAM role
```

## 3. Notification channels

Update `destination` values in `config/seed/01_setup.sql`
(routes 1-4) to the real ROAR/business-correction/engineering Teams
webhooks and distribution emails before loading.

## 4. Run it

Weekly universe validation (matches the Friday pull cadence):

```
python main.py --project HEALTHSPRING_UM --process UNIVERSE_VALIDATION \
    --run-type WEEKLY --run-mode DATE --start 2026-08-01 --end 2026-08-07
```

Daily/monthly ROAR views reuse the SAME rule set with a different
`--run-type` / date window — no rule changes needed; the dashboard's
Daily/Weekly/Monthly selector reads whatever window you ask for.

COMO weekly sample (run after universe validation completes, same week).
This is the **Sampling Framework** (`sampling/`) — a separate framework
from the rules engine above, run as its own step; it just happens to
follow a rules-engine run in this cadence:

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
    WHERE sample_name = 'COMO_WEEKLY_SAMPLE' AND active_flag = 1
""")[0]
run_stratified_sampling(cf, td, config, {
    "run_id": "COMO_MANUAL_RUN", "start_date": "2026-08-01", "end_date": "2026-08-07",
}, meta_db)
```
(`sampling/engine.py` is generic — `COMO_WEEKLY_SAMPLE` here is just the
`sample_name` value HealthSpring UM's own seed config uses. It reuses
`core.executor`/`db.connection_factory` as a plain library, the same way
this snippet does directly above — it isn't part of the rules engine.)

Or schedule both via `python entrypoints.py --schedule` using a
`schedule.json` with `"sampling_config_name": "COMO_WEEKLY_SAMPLE"` on the
universe-validation job (see `entrypoints.py::cron_run_scheduler` for the
job-file shape).

## 5. Dashboard

```
streamlit run dashboard/streamlit_app.py
```

Needs the same `DQ_*` env vars as the engine (read-only metadata access is
sufficient).

## 6. Static audit report

Automatic per run when `DQ_AUTO_AUDIT_REPORT=true` is set on the engine
process, or on demand:

```
python -m core.reporting --run-id <run_id> --out-dir ./dq_audit_reports
```

Copy `./dq_audit_reports/*` into the 10-year retention archive (S3, same
bucket family as `um_archive`) as part of your existing backup/retention
tooling — this repo does not manage retention lifecycle itself.

## 7. Verify before relying on this for CMS-facing reporting

```
pytest tests/ -q                                                 # unit/integration tests
python main.py --project HEALTHSPRING_UM --process UNIVERSE_VALIDATION \
    --run-type TEST --run-mode FULL --dry-run                    # validates every rule's SQL
                                                                    # + dialect against real connections
                                                                    # without writing any results
```
