# Retention & Partitioning Strategy

This document is operational guidance for DBAs/platform owners, not code
this repo runs automatically -- consistent with the runbook's existing
stance on the static audit report ("this repo does not manage retention
lifecycle itself," HEALTHSPRING_UM_RUNBOOK.md §6). It extends that same
philosophy to the metadata tables in `ddl_shared.sql`, `rules_engine/ddl.sql`,
and `sampling/ddl.sql`: which ones grow without
bound, how long each one needs to be kept, and how to partition/archive
them in Teradata without breaking the audit trail CMS-facing reporting
depends on.

## 1. Table growth classification

Every table across `ddl_shared.sql`, `rules_engine/ddl.sql`, and
`sampling/ddl.sql` falls into one of three buckets. This matters
because the retention policy and the partitioning approach are both
driven by *why* a table grows, not just how fast.

### High-growth fact tables (one or more rows per rule/run/candidate)

| Table                        | Grows by                          | Framework |
|-------------------------------|------------------------------------|-----------|
| `dq_rule_execution`           | 1 row per rule per run             | rules_engine/     |
| `dq_exceptions`                | up to `MAX_EXCEPTIONS` rows per failing rule per run (capped -- see `rules_engine/executor.py`) | rules_engine/ |
| `dq_run_logs`                  | several rows per run (INFO/WARN/ERROR messages) | rules_engine/ |
| `dq_rule_issues`                | 0-N rows per run (only on pre-validation/engine failures) | rules_engine/ |
| `dq_column_profile`             | 1 row per profiled column per run (opt-in via `dq_profile_config`) | rules_engine/ |
| `dq_anomaly_log`                 | 0-3 rows per run (one per metric that has enough history to check) | rules_engine/ |
| `dq_sample_selections`           | 1 row per candidate considered per sampling run (**not** just the N selected) | sampling/ |
| `dq_exception_dispositions`       | 1 row per disposition event (waive/resolve/reopen) -- sparse, event-driven | rules_engine/ |

These are the tables this document is actually about. Left unmanaged,
`dq_rule_execution` and `dq_exceptions` in particular grow linearly with
run frequency x rule count x universe size forever, and every SELECT
against them (the dashboard, `rules_engine/reporting.py`, `rules_engine/metrics.py`'s
history queries) does a full-table or near-full-table scan without
partition elimination once they're large.

**Worked example** (HealthSpring UM, per the runbook's weekly cadence,
~40 rules): a weekly run writes ~40 `dq_rule_execution` rows and, at a
generous 5% average failure rate against a universe of a few thousand
cases, on the order of a few hundred `dq_exceptions` rows. That's
modest weekly, but compounds: 5 years of weekly runs is ~10,400
`dq_rule_execution` rows and, depending on the exception rate, anywhere
from tens of thousands to low hundreds of thousands of `dq_exceptions`
rows for ONE project/process. A deployment running this daily, or across
many projects/processes, reaches that same order of magnitude in months,
not years.

### Low-growth dimension / summary tables

| Table                     | Grows by                                   |
|-----------------------------|---------------------------------------------|
| `dq_scope`                   | 1 row per distinct (project, process) ever seen -- effectively static |
| `dq_rules`                    | 1 row per active rule -- edited in place, not appended |
| `dq_rule_versions`             | 1 row per tracked-field change to a rule -- sparse, edit-driven |
| `dq_run_control`                | 1 row per run -- same cardinality as `dq_rule_execution`'s run_id, much smaller |
| `dq_metrics_summary`             | 1 row per (scope_id, run_type, batch_id, dataset_id, run_month) -- upserted, not appended; bounded by month count x scope count |
| `dq_rule_suppressions`            | 1 row per suppression event -- sparse |

These never need partitioning for volume reasons. `dq_run_control` is the
one worth watching if run frequency is very high (sub-hourly), since it
grows at the same rate as `dq_rule_execution`'s run_id cardinality.

### Config / reference tables (essentially static)

`dq_connections`, `dq_check_catalog`, `dq_profile_config`,
`dq_anomaly_config`, `dq_notification_routes`, `dq_sampling_config` --
human-edited, low row count (tens to low hundreds of rows), never need a
retention policy.

## 2. Retention windows

Two different retention regimes apply, and conflating them is the
mistake to avoid:

**Audit-of-record data** -- anything that could be asked for in a CMS
audit or compliance review must follow the same 10-year retention this
repo already applies to `dq_sample_selections` (see `sampling/ddl.sql`'s
comment on that table) and the static audit report (runbook §6):
- `dq_rule_execution`, `dq_exceptions`, `dq_exception_dispositions` --
  these ARE the finding and its disposition history; CMS-facing reporting
  depends on being able to reconstruct "what did the engine find, and
  what happened to it" for any historical run.
- `dq_sample_selections` -- already documented as 10y in `sampling/ddl.sql`.
- `dq_run_control` -- the run-level record (status, timing, scope) that
  every audit-of-record row above joins back to via run_id/scope_id
  (v7's normalization made this join load-bearing -- see §6.5 of
  DESIGN.md -- so `dq_run_control` inherits the same retention floor as
  the tables that depend on it).

**Operational/diagnostic data** -- useful for debugging the ENGINE, not
for proving what a rule found:
- `dq_run_logs`, `dq_rule_issues` -- INFO/WARN/ERROR trail and
  pre-validation/engine-failure detail. 90 days is generally enough to
  debug a recent incident; nothing here is a compliance finding.
- `dq_anomaly_log` -- statistical drift detections. 1 year keeps enough
  history for `rules_engine/metrics.py`'s own z-score/IQR baseline calculations
  (`BASELINE_LOOKBACK = 10` runs) many times over; beyond that it's just
  noise for a dashboard trend chart.
- `dq_column_profile` -- profiling snapshots. 1 year, same reasoning as
  `dq_anomaly_log`.
- `dq_rule_versions`, `dq_rule_suppressions` -- low-volume audit trail of
  rule *definition* changes (not findings). Keep indefinitely; the volume
  is trivial and the forensic value ("what did this rule check for when
  it produced that finding") is high -- `rules_engine/rule_lifecycle.py`'s
  `get_version_at_run()` is built specifically to answer that question,
  and it stops working for any run older than however far back
  `dq_rule_versions` has been purged.

If your organization's compliance requirement differs from 10 years,
change the number -- the point of this table is the *classification*
(audit-of-record vs. operational), which shouldn't change even if the
specific retention window does.

## 3. Partitioning strategy (Teradata)

None of the tables in `ddl_shared.sql`, `rules_engine/ddl.sql`, or
`sampling/ddl.sql` are partitioned today -- every one uses a
plain `PRIMARY INDEX` for row distribution, with no `PARTITION BY`
clause. That's fine at low volume; it stops being fine once the
high-growth tables in §1 reach the millions-of-rows range, for two
compounding reasons: (a) queries filtered by a date range (the dashboard's
Daily/Weekly/Monthly selector, `rules_engine/metrics.py`'s history lookback, any
"last N days" report) do a full-table scan instead of touching only the
relevant partitions, and (b) purging aged-out data means a `DELETE`
statement that has to find and remove matching rows one at a time, which
on a large MULTISET table is slow and generates a lot of transient
journal/spool overhead -- versus dropping a partition, which is close to
instantaneous regardless of how many rows are in it.

**Recommended approach: `PARTITION BY RANGE_N` on a date column, added at
table-creation time** (Teradata does not support adding a `PARTITION BY`
clause to an existing table in place -- it requires a rebuild: create the
new partitioned table under a temporary name, `INSERT ... SELECT` the
existing data across, rename, drop the old one, matching the same
"backup, then rebuild" caution `migrations/v6_to_v7.sql` already
documents for the column drops in that migration). This is why the
recommendation below is phrased as DDL to apply on the *next* schema
migration or a planned maintenance window, not a live ALTER.

| Table                | Partition column | Suggested grain |
|------------------------|--------------------|--------------------|
| `dq_rule_execution`      | `run_date`           | Monthly (matches `run_month`, already computed at insert time) |
| `dq_exceptions`           | `created_at`          | Monthly |
| `dq_run_logs`              | `created_at`            | Monthly |
| `dq_rule_issues`            | `created_at`             | Monthly |
| `dq_column_profile`          | `profile_date`            | Monthly |
| `dq_anomaly_log`               | `created_at`               | Monthly |
| `dq_sample_selections`           | `sample_cycle`               | Monthly (or weekly, if sampling runs weekly and the archive/purge cadence should match) |
| `dq_exception_dispositions`       | `disposed_at`                  | Monthly (event-driven -- low volume, but keeping it on the same grain as `dq_exceptions` makes cross-table date-range queries consistent) |

Example DDL for the highest-volume table, `dq_rule_execution` (Teradata
`RANGE_N` partitioning by month, open-ended upper bound so new months
don't require a DDL change every month):

```sql
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_execution (
    run_id          VARCHAR(200),
    rule_id         INTEGER,
    rule_code       VARCHAR(200),
    table_name      VARCHAR(200),
    total_records   BIGINT,
    failed_records  BIGINT,
    passed_records  BIGINT,
    failure_pct     FLOAT,
    pass_pct        FLOAT,
    severity        VARCHAR(20),
    status          VARCHAR(20),
    execution_time  FLOAT,
    run_timestamp   TIMESTAMP,
    run_date        DATE,
    run_month       DATE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (run_id, rule_id)
PARTITION BY RANGE_N(
    run_date BETWEEN DATE '2024-01-01' AND DATE '2034-12-31'
    EACH INTERVAL '1' MONTH
);
```

Notes on this pattern:
- The partition range's upper bound (`2034-12-31` above) needs to be
  revisited before it's reached -- `ALTER TABLE ... MODIFY ... ADD RANGE`
  extends it without a rebuild, so this is a much cheaper maintenance
  task than the initial partitioning migration.
- `PRIMARY INDEX` is unchanged (still `run_id, rule_id`) -- partitioning
  is a physical storage/pruning mechanism layered on top of the primary
  index, not a replacement for it. Existing queries keyed on `run_id`
  keep working exactly as they do today; queries that ALSO filter on
  `run_date`/`created_at` (which most dashboard and reporting queries do)
  get partition elimination for free.
- Apply the same `RANGE_N(... EACH INTERVAL '1' MONTH)` pattern to the
  other seven tables in the table above, substituting each table's own
  partition column.

## 4. Archival workflow

For audit-of-record tables (§2), "retention" does not mean "delete after
N years" -- it means "move to cheaper storage after N years, keep it
retrievable." The pattern to follow is the one the runbook already
establishes for the static audit report: land aged-out data in the same
S3 archive bucket family referenced by the `um_archive`/`*_ARCHIVE`
connection convention (`DQ_CONNECTION_NAMES` in
HEALTHSPRING_UM_RUNBOOK.md §2), as Parquet, queryable later through the
same `S3Adapter`/DuckDB-over-S3 path this repo already uses for source
data (`db/adapters.py::S3Adapter`) -- so an old finding is never
permanently unreachable, just no longer sitting in the hot Teradata
metadata store.

Recommended cycle, run as a periodic DBA/ops job (not by this repo's
Python code):

1. **Export** the partition(s) about to age out of the operational window
   (e.g. every month, export the partition that just crossed the
   operational-tables' retention line -- 90 days/1 year per §2) to
   Parquet in the archive bucket. `TPT`/`BTEQ EXPORT` or a scheduled
   `SELECT ... INTO` via any of the existing source adapters both work;
   this repo doesn't prescribe the export tool.
2. **Verify** the export landed correctly (row count match between the
   partition and the exported file) before touching the source partition.
3. **Drop the partition** (`DELETE` a specific `RANGE_N` partition, or
   `ALTER TABLE ... DROP RANGE` once all rows in it are gone) rather than
   row-by-row `DELETE` -- this is the entire reason §3's partitioning
   scheme exists.
4. For the 10-year audit-of-record tables, skip step 3 entirely until the
   10-year line is actually reached -- the partitioning in §3 is there
   for query performance long before it's needed for space reclamation
   on those tables specifically. Operational tables (90 days/1 year) are
   where step 3 matters in practice.

## 5. What this repo does NOT do

To be explicit about the boundary: no code in `rules_engine/`, `sampling/`,
`utils/`, or `entrypoints.py` purges, archives, or partitions anything.
`rules_engine/engine.py::_cleanup_stale_runs()` is the one piece of built-in
lifecycle management that exists today, and it only reclassifies phantom
`RUNNING` rows as `ABORTED` after `DQ_STALE_RUN_HOURS` -- it does not
delete rows. Implementing §3/§4 above is a DBA/platform decision (schema
migration timing, archive tooling, compliance sign-off on the exact
retention numbers), which is why this document is guidance rather than a
script this repo runs for you.
