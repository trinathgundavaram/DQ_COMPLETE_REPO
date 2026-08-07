# Generic Data-Quality / Compliance Rules Engine — Design Document

This document covers the v6 extension of the existing DQ framework (this
repo) to satisfy two things at once, per the brief: (1) the Clinical Audit
Sample Optimization / HealthSpring UM requirements, and (2) genericness — a
different team pointing this engine at a different dataset tomorrow.

## 1. Starting point

The repo was not built from scratch here. It already implemented a
thread-safe, priority/dependency-ordered rule engine (`core/engine.py`);
accurate failed-record counting even when exceptions are capped; SKIP
recording when a rule can't run; empty-table handling via `require_rows`;
AND/OR threshold logic; retry on transient source errors; lazily-read
credentials; a CLI (`main.py`); rule versioning; suppression; statistical
anomaly detection (z-score/IQR); and a pluggable connector layer
(Teradata, Postgres/Aurora, Databricks, SQL Server, flat files via DuckDB).

Verifying this took a full read of `ddl.sql`, `main.py`, and every file
under `core/`, `db/`, `utils/`, `config/`, plus a DuckDB-backed test pass,
before writing anything (§2 below has the item-by-item gap mapping). What
follows is what genuinely needed to be added, not a rewrite.

## 2. File layout

```
core/
  engine.py            orchestrator: run scheduling, thread pool, pre-validation
  executor.py           query execution + rule evaluation (merged former evaluator.py)
  rule_sql.py            SQL construction, raw-SQL rules, dialect enforcement
  check_types.py        built-in declarative check_type -> SQL generators
  rule_lifecycle.py       rule versioning + suppression
  metrics.py             run metrics + anomaly (z-score/IQR) detection
  reporting.py            notification routing + static audit report
  profiler.py             source-table profiling
  stratified_sampling.py  config-driven ranked sampling (e.g. COMO weekly sample)
  rule_tester.py           single-rule dry-run harness
db/
  adapters.py            SourceAdapter ABC + Teradata/Postgres/Databricks/SqlServer/File/S3
  connection_factory.py  builds + caches adapters from DQ_CONNECTION_NAMES env
utils/
  db_helpers.py          table/db name resolution
  metadata_writers.py    dq_run_logs / dq_rule_issues writers
  ids.py                 run-id + primary-key JSON helpers
  validation.py          rule-definition validation
  alert.py               low-level Teams/email senders
entrypoints.py           Lambda handler, Glue main, Airflow operator, cron runner, CLI - all call core.engine.run_engine()
dashboard/streamlit_app.py   findings / sample / engine-health views
config/seed/             HealthSpring UM onboarded purely via config (see §5)
ddl.sql                  full schema, v1 -> v6
main.py                  CLI entrypoint
```

Each module has one job and a new capability slots into an existing file
rather than requiring a new one: a new source is a new adapter class in
`db/adapters.py`; a new check type is a new generator function in
`core/check_types.py`; a new execution context is a new function in
`entrypoints.py`.

## 3. What was added (v6) and why

### 3.1 SQL-dialect enforcement

New: dialect-checking functions in `core/rule_sql.py`
(`check_dialect`, `DialectMismatchError`, `DIALECT_COMPATIBILITY`),
`dq_rules.sql_dialect` column.

Every rule authored via the raw-SQL path declares `sql_dialect`
(`teradata` | `postgres` | `ansi`). `DIALECT_COMPATIBILITY` maps each
`source_type` (from the adapter, not a second lookup) to the dialects
that are safe against it -- `ansi` is accepted everywhere by definition.
The check runs in two places, both **before** the rule's query ever
reaches the source database:

1. `core/engine.py::_pre_validate_rules` -- load-time, before the run starts.
2. `core/executor.py::execute_rule` -- a second, defense-in-depth check
   immediately before any query is built, so a rule added *after*
   pre-validation ran (or invoked via the rule-tester single-rule harness,
   which bypasses pre-validation) is still protected.

A mismatch raises `DialectMismatchError` with the exact message shape from
the brief ("RULE-014 is written for postgres, cannot run against a
teradata connection") and is logged to `dq_rule_issues` with
`issue_type='DIALECT_MISMATCH'` -- never to `dq_exceptions`. The rule is
recorded as `status='ERROR'`, never `PASS`.

Legacy check_type-generated rules (no `sql_dialect`) are exempt -- the
`check_types.py` generators already branch on `source_type` internally and
can't have a dialect mismatch by construction.

This was the one genuinely new piece of correctness logic (see §4,
Section 8 item 8) -- no dialect concept existed in the reference repo.

### 3.2 Raw-SQL rule authoring

New: `_build_raw_sql()` in `core/rule_sql.py`, and a documented 3-path
priority order (raw SQL -> check_type generation -> legacy fragment).

The reference repo's existing `rule_syntax` fallback treats it as a
WHERE-clause **fragment** that the engine wraps as
`SELECT * FROM {table} t WHERE ({fragment}) AND ({filter})`. That can't
express the SHRPA / provider-contract cross-table JOIN rules the brief
requires ("no separate join-configuration mechanism") -- a WHERE fragment
has no FROM/JOIN of its own.

So: when `sql_dialect` is set, `rule_syntax` is treated as a **complete,
self-contained negative-SQL SELECT** -- the rule owns its own FROM/JOIN/
WHERE. The engine does not parse or rewrite it. If the rule also declares
`filter_column`/`filter_sql` (run-scoping, e.g. `pull_date`), the engine
wraps the *entire* query as an outer subquery: `SELECT * FROM (<rule_syntax>)
dq_raw_sql WHERE (<filter>)` -- this reuses the exact same
`build_filter()` machinery every other rule type already uses, so a raw-SQL
rule gets BATCH/DATE/FULL run-mode scoping for free, without a new
templating language. If no filter is configured, the rule's SQL runs
completely unmodified.

Total-record count for percentage thresholds still comes from
`build_count_query()` against the rule's declared `src_tbl_nm` -- i.e. "how
big is the universe in scope," independent of how complex the rule's own
join logic is. `key_columns` maps onto the existing `primary_key_columns`
column (no new column needed) -- every raw-SQL rule's `validate_rule_params`
now requires it, since it's what lets a two-table join rule and a
single-table rule both produce comparable, storable exception rows.

`check_type` is retained on raw-SQL rules purely as the classification
tag -- it does not affect SQL generation for this path. That's the point:
a taxonomy, not a code-generator, once SQL is the primary authoring
surface.

Tested end-to-end against DuckDB in `tests/test_core.py`, including a
cross-table join mirroring the SHRPA pattern.

### 3.3 S3 connector

New: `S3Adapter` class in `db/adapters.py`, registered in
`connection_factory.py`.

Every rule is SQL, so a source that only hands back file bytes doesn't
work -- S3 needed a SQL-queryable interface, not a write target. Modeled
directly on the existing `FileAdapter` (per-thread DuckDB connections via
`threading.local()`, shared-safe, `prepare(rule)` hook), but:

- Uses DuckDB's `httpfs` extension to query Parquet/CSV **directly on S3**
  (`read_parquet(...)` / `read_csv_auto(...)`), no local staging.
- Supports globs / Hive-partitioned prefixes -- this is how a "series of
  dated snapshots" source is queried: a rule points at
  `s3://bucket/pulls/pull_date=*/*.parquet` and scopes to one week via its
  own `filter_column`, exactly like any DB-backed rule.
- Credentials (`SET s3_access_key_id=...` etc.) are configured **inside
  `_get_thread_conn()`**, i.e. read from env vars at connection-build time,
  never cached at import -- satisfies the same credential-rotation
  requirement the other adapters already met.
- Falls back cleanly to the instance/task IAM role when no explicit
  access key is set.

`Databricks`/`SQL Server` adapters live in the same `db/adapters.py` file
(pluggable architecture intact) but are not in `dq_connections`' sanctioned
`source_type` set for this instance -- adding a 4th source later is one new
adapter class in that file + one factory entry, not an engine change.

### 3.4 Case-level disposition

New: `dq_case_dispositions` table.

The existing `dq_rule_suppressions` table only silences a whole *rule*
(with a reason/expiry) -- it has no concept of "this one case was reviewed
and waived." Given the CMS-audit-defensibility requirement, a disposition
is now its own append-only table, keyed to `exception_id`, with
`effective_flag` marking the current state. **`dq_exceptions` itself is
never updated or deleted** -- a waiver/correction is always a new row in
`dq_case_dispositions`, joined at read time by both the dashboard and the
static report.

### 3.5 Notification audience-splitting

New: notification functions in `core/reporting.py`
(`notify_run_completion`, `_dispatch`), `dq_notification_routes` table,
`utils/alert.send_alert_to()`, `dq_rules.business_correctable`.

This was a real gap, not just missing config: `core/engine.py`'s previous
`_send_completion_alert()` lumped `FAIL/WARN/ERROR/SKIP/SUPPRESSED` into one
`failed_rules` count and sent ONE alert on ONE channel -- exactly the
failure mode the brief calls out ("a rule that errors must never look
identical to a rule that ran clean"). That function was removed entirely
rather than kept alongside the fix, so there is exactly one
completion-notification code path.

The engine splits the post-run count into `data_issue_rules` (FAIL/WARN --
real data findings) vs. `engine_issue_rules` (ERROR/SKIP -- the engine
couldn't evaluate the rule at all) vs. `suppressed_rules`, and
`reporting.notify_run_completion()` sends these as two independent
notification calls, looked up against `dq_notification_routes` by
`(project, process, finding_class)`.

**The code has no concept of audience names.** `dq_notification_routes.
audience` is a free-text label carried through purely for humans managing
the routing table -- the engine never branches on its value. The only two
decisions `core/reporting.py` makes in code are:

- `finding_class = 'DATA_VIOLATION'` -> every active route for that finding
  class gets the full message; routes with `business_correctable_only = 1`
  ALSO get a second, filtered message containing only rows whose rule has
  `dq_rules.business_correctable = 1`.
- `finding_class = 'ENGINE_FAILURE'` -> every active route registered under
  that finding class gets the message. There is no code path from an
  engine failure to a `DATA_VIOLATION` route, because routes are looked up
  by `finding_class` and a route only ever has one.

HealthSpring UM's own routing config (`config/seed/01_setup.sql`) happens
to name its audiences ROAR / BUSINESS / ENGINEERING / QA -- that is project
config, not engine behavior. A different project inserts
`dq_notification_routes` rows with whatever audience labels make sense to
it (`"on-call"`, `"data-eng"`, anything) and `reporting.py` does not change.

No routes configured -> falls back to the existing global `send_alert()`
channel; the message body always states which finding class it is, so
even the degraded/fallback case is unambiguous.

### 3.6 Config-driven stratified sampling

New: `core/stratified_sampling.py`, `dq_sampling_config`,
`dq_sample_selections`.

(HealthSpring UM's own use of this -- the "COMO weekly sample" -- is
configured entirely in `config/seed/01_setup.sql`. The module itself has
no knowledge of COMO, ROAR, or any other project-specific name;
`tests/test_core.py` asserts this directly by scanning the module source
for project-specific vocabulary.)

Deliberately a **separate module and separate output table** from rule
evaluation, because it's a different question: rules ask "is this row
valid" (pass/fail against the whole universe); sampling asks "which ~150
cases, out of a clean universe, are highest-value for a human reviewer this
week" -- a ranking/quota problem. Forcing that into `dq_exceptions` would
conflate "flagged" with "selected for review," which are not the same
thing.

Algorithm (all config-driven via `dq_sampling_config`, nothing hardcoded):
1. Pull candidates from the universe table, `WHERE NOT (exclusion_sql)`
   (auto-approvals, SHRPA-no-PA, diabetic supplies) and the run's date scope.
2. Rank by `priority_rank_sql` (a plain `ORDER BY` expression: revision=1 ->
   inpatient/outpatient balance -> expedited/standard balance -> code variety
   -> clinical-review-required).
3. Compute a per-stratum target count from `determination_mix_json`
   (80/10/2/8 split) and, within each determination bucket,
   `functional_area_mix_json` (13% Part B / 8% Behavioral Health /
   remainder Pre-Cert-OP -- unnamed categories absorb the leftover
   percentage rather than being silently dropped).
4. Take the top-N per stratum by priority rank; top up from the overall
   remaining pool by priority if a thin week under-fills a stratum, so the
   sample doesn't fall short of the target purely from bucket imbalance.
5. Persist **every candidate considered**, selected or not, with its
   stratum and priority rank, to `dq_sample_selections` -- audit
   defensibility means "why wasn't case X selected" has to be answerable a
   year later, not just "here are the 150."

Out-of-network denial capping ("only a subset of denials being
out-of-network") is flagged as a follow-up in the seed config rather than
guessed at -- the exact target % wasn't in the source document excerpt
provided, and a wrong guess baked into code is worse than an explicit
TODO in config.

Tested end-to-end against a 1,000-row synthetic universe in
`tests/test_core.py` (mix ratios, exclusion enforcement, target volume,
and a check that the module contains no project-specific vocabulary).

### 3.7 Execution-context wrappers

New: `entrypoints.py` -- `lambda_handler`, `glue_main`,
`cron_run_once`/`cron_run_scheduler`, `DataQualityEngineOperator`.

All are thin: they parse a platform-specific trigger shape (Lambda event /
Glue job args / cron tick / Airflow context) and call
`core.engine.run_engine()` -- the same function `main.py`'s existing CLI
calls. Nothing in `core/`, `db/`, or `utils/` imports from
`entrypoints.py`, so the dependency only goes one way -- remove any one
wrapper function and the engine still runs everywhere else.
`DataQualityEngineOperator` degrades gracefully (wrapped in
`try/except ImportError`) if `apache-airflow` isn't installed, rather than
hard-failing at import time for non-Airflow deployments.

### 3.8 Severity vocabulary

`dq_rules.severity` is free text -- a project stores its own vocabulary
**exactly as its source document defines it** rather than being forced
into a generic enum. `core/executor.py::evaluate_rule()` decides FAIL vs
WARN by INVERSION rather than by hardcoding any project's labels: a small
fixed set of "soft" severities (`WARN`, `WARNING`, `INFO`, `NOTICE`)
resolve to WARN, and everything else -- whatever a project calls it
(`'Compliance Flag'`, `'Timeliness'`, `'P1'`, anything) -- resolves to FAIL.
No project's severity vocabulary is ever written into `core/executor.py`;
HealthSpring UM's three severities work purely because they aren't in the
soft set, the same as any other project's would.

## 4. Section 8 -- known correctness issues

| # | Issue | Status | Evidence |
|---|---|---|---|
| 1 | Failed-record count wrong when capped | Already correct | `core/executor.py::_count_failed()` runs a separate `COUNT(*)` subquery; `_fetch_failed_rows()` is only used for exception capture, never for the recorded count. |
| 2 | Rule that fails to run must be SKIPPED, not omitted | Already correct | `execute_rule()` calls `record_rule_execution(..., status="SKIP", ...)` before returning when `validate_table_exists` fails. |
| 3 | Empty source table must not silently PASS | Already correct | `core/executor.py::evaluate_rule()` -- `require_rows` param, `dq_rules.require_rows` column. |
| 4 | Threshold logic needs AND and OR | Already correct | `evaluate_rule(..., threshold_operator=...)`, wired from `execute_rule()` -> `rule.get("threshold_operator", "OR")`; covered by `tests/test_core.py`. |
| 5 | No silent double-counting on concurrent runs | Already correct | `dq_metrics_summary_uix` UNIQUE INDEX + `core/metrics.py::_upsert_metrics()` catches the duplicate-key error and falls back to a pure `UPDATE`. |
| 6 | Credentials read at call time, not import | Already correct | Every adapter's `_require()`/env read happens inside `build()`, called from `ConnectionFactory._build()` at `load()`/reconnect/`new_connection()` time -- never at module import. `utils/alert.py::_load_config()` is called inside `send_alert()`, not at import. |
| 7 | Retry transient source errors | Already correct | `core/executor.py` wraps `_count_total`, `_count_failed`, `_run_table_check`, `_fetch_failed_rows` in a `tenacity` retry decorator (`DQ_QUERY_MAX_RETRIES`, exponential backoff). |
| 8 | Dialect mismatch must fail before execution | Fixed here | Did not exist -- no dialect concept in the reference repo at all. Added dialect enforcement in `core/rule_sql.py` + `dq_rules.sql_dialect`, wired into both `engine.py::_pre_validate_rules` (load-time) and `executor.py::execute_rule` (defense-in-depth). See §3.1 and `tests/test_core.py`. |

Net new work from Section 8: one item (dialect enforcement). Everything
else was already correct in the repo; re-verified here rather than
re-implemented, since re-implementing already-correct code has no value
and risks regressions.

## 5. What was deliberately NOT changed

- The connector interface (`SourceAdapter` ABC) -- S3 slots in without
  touching `cursor()`/`commit()`/`close()`/`ping()`/`prepare()`.
- The thread-pool / dependency-graph / priority-ordering engine core.
- The rule-versioning, suppression, profiling, and anomaly-detection
  modules -- reused rather than reinvented; "what did the rule say a year
  ago" is already served by `dq_rule_versions`.
- `dq_exceptions`'s schema and immutability contract -- extended via a
  joined table, not mutated.

## 6. Data model deltas (v6) -- see `ddl.sql` for full DDL

| Change | Table | Why |
|---|---|---|
| `sql_dialect` column | `dq_rules` | fail-fast dialect enforcement |
| `business_correctable` column | `dq_rules` | audience routing |
| `source_type` CHECK constraint | `dq_connections` | scope to 3 sanctioned adapters |
| New table | `dq_case_dispositions` | additive-only disposition, immutable exceptions |
| New table | `dq_notification_routes` | audience routing by finding_class |
| New table | `dq_sampling_config` | stratified-sampling config |
| New table | `dq_sample_selections` | stratified-sampling immutable output |

## 7. Genericness test

To onboard an unrelated second use case: insert rows into `dq_connections`
(new `source_type` from the sanctioned 3, or a 4th adapter class in
`db/adapters.py`), insert rows into `dq_rules` (raw SQL + `sql_dialect` +
`check_type` tag, or check_type-generated), optionally a
`dq_sampling_config` row if it needs ranked sampling, and
`dq_notification_routes` rows for its own audiences. Zero files under
`core/` or `utils/` need to change, and `db/adapters.py` only changes if a
4th connector type is needed. This is the same mechanism HealthSpring UM
itself is configured through (`config/seed/`) -- it isn't a special case.

## 8. Documented follow-ups (not guessed at)

- **Out-of-network denial cap** (COMO sampling: "only a subset of denials
  being out-of-network") -- the exact target percentage wasn't specified in
  the source material available. Flagged as a TODO in
  `config/seed/01_setup.sql` rather than inventing a number.
- **Real ODAG1 column names** -- the actual MHK extract layout wasn't
  available; `config/seed/02_rules.sql` uses descriptive column names and
  `config/seed/00_healthspring_um_README.md` documents the assumed schema
  and the one-time remapping step required before go-live. Rule *logic*
  (SLA day counts, allowed values, conditional structure) is correct
  regardless of the final column names.
- **SHRPA / provider-contract co-location** -- cross-table join rules
  require SHRPA and provider-contract data to be queryable from the same
  connection as `um_universe`. Documented as an ETL dependency (replicate
  into the Teradata schema) in the README rather than silently assuming a
  working cross-database federation that isn't described anywhere in the
  source material.
