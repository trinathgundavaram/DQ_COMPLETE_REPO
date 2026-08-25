# metadata_sync

Read-only mirror of the 11 `gre_*` metadata tables, Teradata -> Postgres.
Lets a Postgres-side query join `gre_exceptions`/`gre_rules`/etc. straight
to a Postgres source table on `src_key_value`, instead of splitting the
join across two engines.

Standalone: no file here imports `rules_engine/`, `sampling/`, or
`shared/`, and nothing there imports this folder. It only reuses
`db/connection_factory.py` for connections (same `TERADATA_*`/`POSTGRES_*`
env vars as the rest of the repo). Teradata is only ever `SELECT`ed from;
Postgres is only ever written to by this tool.

## Setup

```bash
export TERADATA_HOST=... TERADATA_USER=... TERADATA_PASSWORD=...
export POSTGRES_HOST=... POSTGRES_DATABASE=... POSTGRES_USER=... POSTGRES_PASSWORD=...
export METADATA_SYNC_PG_SCHEMA=gre_mirror   # optional, this is the default

python -m metadata_sync.create_postgres_tables   # idempotent, rerun any time
```

## Run the sync

```bash
python -m metadata_sync.sync_from_teradata                       # all 11 tables
python -m metadata_sync.sync_from_teradata --tables gre_rules,gre_exceptions
python -m metadata_sync.sync_from_teradata --full-refresh        # force full refresh
python -m metadata_sync.sync_from_teradata --dry-run             # log only, write nothing
```

Put it on a schedule (see `crontab.example`).

## How each table syncs (see `tables.py`)

- **full_refresh** -- `gre_rules`, `gre_sampling_config`, `gre_sampling_strata`,
  `gre_sampling_mix`. Small config tables: TRUNCATE + reload every run, so
  Teradata-side deletes are also picked up.
- **incremental** -- everything else. Pull rows where the watermark column
  is >= last-synced watermark minus a lookback window
  (`METADATA_SYNC_LOOKBACK_MINUTES`, default 60), upsert on the primary key.
  `gre_rule_audit`/`gre_sampling_audit` also always re-pull any
  `status = 'RUNNING'` row, since their `ended_at`/`status` UPDATE never
  bumps `load_datetime`. (`gre_audit` itself -- the pre-split combined
  table -- is now a VIEW on both the Teradata and Postgres sides, built
  from these two; it's not in `TABLE_SPECS` and isn't synced directly,
  since a view has no watermark of its own. See `shared/schema.sql`'s
  module header for the full split rationale.)

Add a 13th table by adding one entry to `TABLE_SPECS` in `tables.py` and one
`CREATE TABLE` block to `ddl_postgres.sql`.
