# Generic Data-Quality / Compliance Rules Engine — Design Document

This document covers the v6 extension of the existing DQ framework (this
repo) to satisfy two things at once, per the brief: (1) the Clinical Audit
Sample Optimization / HealthSpring UM requirements, and (2) genericness — a
different team pointing this engine at a different dataset tomorrow.

**This repo hosts two separate frameworks, not one.** The DQ Rules Engine
(`rules_engine/`) answers "is this row valid?" The Sampling Framework
(`sampling/`) answers "which N cases are highest-value for a human
reviewer this cycle?" — a different question, with its own config table,
its own output table, and its own package. `sampling/` uses a small set of
functions from `rules_engine/` and the shared connector layer as a plain library
dependency (see §3.6/§9), but `rules_engine/` has no awareness `sampling/` exists, and
neither is required to deploy or run the other. Wherever this document
says "the engine," it means the rules engine specifically, unless a
section is explicitly about sampling.

## 1. Starting point

The repo was not built from scratch here. It already implemented a
thread-safe, priority/dependency-ordered rule engine (`rules_engine/engine.py`);
accurate failed-record counting even when exceptions are capped; SKIP
recording when a rule can't run; empty-table handling via `require_rows`;
AND/OR threshold logic; retry on transient source errors; lazily-read
credentials; a CLI (`rules_engine/main.py`); rule versioning; suppression; statistical
anomaly detection (z-score/IQR); and a pluggable connector layer
(Teradata, Postgres/Aurora, Databricks, SQL Server, flat files via DuckDB).

Verifying this took a full read of the schema DDL, the CLI, and every file
under `rules_engine/`, `db/`, `utils/`, `config/`, plus a DuckDB-backed
verification pass, before writing anything (§2 below has the item-by-item
gap mapping). What follows is what genuinely needed to be added, not a
rewrite.

## 2. File layout

This repo is organized as three top-level, purpose-specific folders --
`rules_engine/`, `dashboard/`, `sampling/` -- plus the infrastructure all
three genuinely share (`db/`, `utils/`, `config/`, and the top-level
orchestration/deployment glue). Nothing in this repo bundles an automated
test suite -- it's a reusable framework, not an application with its own
release gate; verification during development was done with DuckDB-backed
manual runs.

```
rules_engine/                    -- FRAMEWORK 1: the DQ rules engine
  main.py                CLI entrypoint: `python rules_engine/main.py ...`
  ddl.sql                 rules-engine tables only (dq_rules through
                          dq_notification_routes) -- run ddl_shared.sql first
  engine.py               orchestrator: run scheduling, thread pool, pre-validation
  executor.py             query execution + rule evaluation (merged former evaluator.py)
  rule_sql.py             SQL construction, raw-SQL rules, dialect enforcement
  check_types.py          built-in declarative check_type -> SQL generators
  rule_lifecycle.py       rule versioning + suppression
  metrics.py              run metrics + anomaly (z-score/IQR) detection
  reporting.py             notification routing + static audit report
  profiler.py              source-table profiling
  rule_tester.py            single-rule dry-run harness
dashboard/                -- FRAMEWORK VIEWER: read-only, its own folder
  streamlit_app.py         findings / sample / engine-health views over both
                          frameworks' output tables -- no schema of its own
sampling/                -- FRAMEWORK 2: the sampling framework (separate; see §3.6)
  engine.py                config-driven ranked sampling (e.g. COMO weekly sample)
  anomaly.py               candidate-pool volume drift check (reuses rules_engine.metrics's
                          z-score/IQR math as a library — see §3.6)
  ddl.sql                  sampling-only tables: dq_sampling_config,
                          dq_sample_selections -- run ddl_shared.sql first
db/                       -- shared: connector layer, used by all three folders above
  adapters.py             SourceAdapter ABC + Teradata/Postgres/Databricks/SqlServer/File/S3
  connection_factory.py   builds + caches adapters from DQ_CONNECTION_NAMES env
utils/                    -- shared: cross-cutting helpers, used by all three folders above
  db_helpers.py           table/db name resolution + dq_scope resolver
  metadata_writers.py     dq_run_logs / dq_rule_issues writers
  ids.py                  run-id + primary-key JSON helpers
  validation.py           rule-definition validation
  alert.py                low-level Teams/email senders
config/seed/              HealthSpring UM onboarded purely via config (see §5)
entrypoints.py            shared: Lambda handler, Glue main, Airflow operator, cron
                          runner -- all call rules_engine.engine.run_engine() (the
                          same function rules_engine/main.py's CLI calls); the cron
                          runner can optionally chain sampling.engine as a follow-on
                          step (see §3.6) -- the only place both frameworks meet
ddl_shared.sql            the two tables both frameworks depend on -- dq_scope
                          (project/process dimension) and dq_connections (source
                          catalogue) -- plus the full v1->v7 change rationale and
                          illustrative version-history ALTER blocks. Run this file
                          FIRST, before rules_engine/ddl.sql and/or sampling/ddl.sql.
migrations/v6_to_v7.sql   runnable schema-normalization migration (see §6.5) --
                          a real ALTER/backfill/DROP script, not illustrative
                          comments like ddl_shared.sql's earlier v1->v2 .. v5->v6 blocks
RETENTION.md              partitioning/archival strategy for high-growth tables --
                          operational guidance, not code this repo runs itself
ONBOARDING.md             generic step-by-step walkthrough: empty schema -> first
                          successful run, for a brand-new project (not HealthSpring UM)
.env.example              every DQ_* env var this repo reads, with inline docs
schedule.json.example     entrypoints.py --schedule job-file shape, worked examples
```

Each module has one job and a new capability slots into an existing file
rather than requiring a new one: a new source is a new adapter class in
`db/adapters.py`; a new check type is a new generator function in
`rules_engine/check_types.py`; a new execution context is a new function in
`entrypoints.py`. The one-file-per-concern rule applies across the
framework boundary too: `sampling/` is two small, single-purpose files --
`engine.py` for the sample-selection algorithm, `anomaly.py` for the
candidate-pool drift check -- not one file doing two jobs. See §3.6.

`db/`, `utils/`, and `config/` stay outside the three framework folders on
purpose: they're genuinely shared low-level infrastructure (connection
pooling, env-var parsing, ID generation, metadata writers), not
domain logic belonging to any one framework. Duplicating them into
`rules_engine/`, `dashboard/`, and `sampling/` separately would mean three
copies of connection-handling code drifting out of sync -- the same
one-directional-dependency principle that keeps `rules_engine/` from
importing `sampling/` argues against that duplication too. `entrypoints.py`
stays at the repo root for the same reason: it is explicitly the one file
where both frameworks' orchestration meets (see above), so it cannot live
inside either folder without implying a dependency that doesn't exist.

## 3. What was added (v6) and why

### 3.1 SQL-dialect enforcement

New: dialect-checking functions in `rules_engine/rule_sql.py`
(`check_dialect`, `DialectMismatchError`, `DIALECT_COMPATIBILITY`),
`dq_rules.sql_dialect` column.

Every rule authored via the raw-SQL path declares `sql_dialect`
(`teradata` | `postgres` | `ansi`). `DIALECT_COMPATIBILITY` maps each
`source_type` (from the adapter, not a second lookup) to the dialects
that are safe against it -- `ansi` is accepted everywhere by definition.
The check runs in two places, both **before** the rule's query ever
reaches the source database:

1. `rules_engine/engine.py::_pre_validate_rules` -- load-time, before the run starts.
2. `rules_engine/executor.py::execute_rule` -- a second, defense-in-depth check
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

New: `_build_raw_sql()` in `rules_engine/rule_sql.py`, and a documented 3-path
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

Verified end-to-end against DuckDB during development, including a
cross-table join mirroring the SHRPA pattern. This repo doesn't bundle
an automated test suite (see §2) -- verification was manual, DuckDB-backed.

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

New: `dq_exception_dispositions` table (named `dq_case_dispositions`
through v6; renamed in v7 -- see §6.5).

The existing `dq_rule_suppressions` table only silences a whole *rule*
(with a reason/expiry) -- it has no concept of "this one case was reviewed
and waived." Given the CMS-audit-defensibility requirement, a disposition
is now its own append-only table, keyed to `exception_id`, with
`effective_flag` marking the current state. **`dq_exceptions` itself is
never updated or deleted** -- a waiver/correction is always a new row in
`dq_exception_dispositions`, joined at read time by both the dashboard and
the static report.

### 3.5 Notification audience-splitting

New: notification functions in `rules_engine/reporting.py`
(`notify_run_completion`, `_dispatch`), `dq_notification_routes` table,
`utils/alert.send_alert_to()`, `dq_rules.business_correctable`.

This was a real gap, not just missing config: `rules_engine/engine.py`'s previous
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
decisions `rules_engine/reporting.py` makes in code are:

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

### 3.6 The Sampling Framework -- a separate framework, not a rules-engine feature

New: `sampling/` package (`sampling/engine.py`), `dq_sampling_config`,
`dq_sample_selections`.

This is not a module inside the rules engine -- it's a second, independent
framework in this repo, because it answers a fundamentally different
question than `dq_rules` does: rules ask "is this row valid" (pass/fail
against the whole universe); sampling asks "which ~150 cases, out of a
clean universe, are highest-value for a human reviewer this week" -- a
ranking/quota problem, not a validation problem. Forcing that into
`dq_exceptions` would conflate "flagged" with "selected for review," which
are not the same thing, so it gets its own package, its own config table
(`dq_sampling_config`), and its own output table (`dq_sample_selections`)
that share nothing with `dq_rules`/`dq_rule_execution`/`dq_exceptions`.

**The dependency only ever goes one way.** `sampling/` imports a small,
enumerable set of things from outside its own package, all as a plain
library, the same way any external caller would use them:
- `rules_engine.executor.execute_query` / `bulk_insert` (`sampling/engine.py`) --
  the same thin parameterised-query wrappers every source adapter already uses.
- `rules_engine.rule_sql.build_filter` (`sampling/engine.py`) -- the same
  BATCH/DATE/FULL run-mode date-scoping every rule uses, so a sampling
  config's `scope_column` gets identical window handling for free,
  without a second templating language.
- `db.connection_factory.ConnectionFactory` (`sampling/engine.py`) -- the
  shared connector layer.
- `rules_engine.metrics.evaluate_metric_drift` (`sampling/anomaly.py` only --
  never `engine.py` directly) -- the same z-score/IQR statistics `rules_engine/`
  uses for DQ-score anomaly detection, reused for candidate-pool volume
  drift (see below). This is a single pure, DB-free function with no
  side effects; `sampling/` never touches `dq_metrics_summary`,
  `dq_anomaly_config`, or any other rules-engine metrics table, and never
  calls `calculate_metrics()`/`detect_and_log()` themselves.

`rules_engine/` has zero imports from `sampling/` and zero knowledge it exists.
`.github/workflows/ci.yml`'s framework-boundary check enforces the
`rules_engine/`-never-imports-`sampling/` half of this directly on every
push, via a static AST scan of every file under `rules_engine/`. (The
reverse direction -- `sampling/` may only reuse `rules_engine.metrics`'s
one sanctioned function, and must never query `dq_rule_execution`/
`dq_exceptions`/`dq_rules` directly -- is a design invariant documented
here rather than a CI-checked one.) A deployment can run the sampling framework against
a metadata store that has never evaluated a single `dq_rules` row, and
vice versa. The only place the two frameworks meet at all is
`entrypoints.py::_run_cron_sampling` -- deployment glue that optionally
chains a sampling run after a rules-engine cron job, for projects that
want both on the same schedule (see §3.7) -- and
`dashboard/streamlit_app.py`, which is a read-only viewer over both
frameworks' tables, not a dependency either framework has on the other.

(HealthSpring UM's own use of this -- the "COMO weekly sample" -- is
configured entirely in `config/seed/01_setup.sql`. The module itself has
no knowledge of COMO, ROAR, or any other project-specific name -- verified
by inspection during development, not by an automated scan.)

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

**Candidate-pool volume drift detection (`sampling/anomaly.py`).** After
step 1 pulls the candidate pool, `run_stratified_sampling()` compares its
size against that same `dq_sampling_config` row's own history --
config_id-scoped, so a different sample definition has its own
independent baseline -- using `rules_engine.metrics.evaluate_metric_drift()`
(z-score and/or IQR, same math and same severity tiers `rules_engine/`'s DQ-score
anomaly detection uses). No new metadata table was needed: every prior
run's candidate count is already fully recoverable from
`dq_sample_selections` (one row per candidate per `sample_run_id`,
written by step 5 below), so this is pure read + pure math on top of data
the framework was already persisting. A sharp drop usually means an
upstream feed broke and fewer rows landed than normal; a sharp jump can
mean a `universe_table`/`exclusion_sql` config change let unintended rows
through -- both are worth a human's attention before N cases get pulled
from a pool that's quietly the wrong size. Detection is non-fatal (same
principle as `rules_engine/`'s post-run metrics/anomaly steps) and its result is
always present under the `volume_drift` key of `run_stratified_sampling()`'s
return dict (`{}` when nothing looks anomalous or there isn't history yet).

Out-of-network denial capping ("only a subset of denials being
out-of-network") is flagged as a follow-up in the seed config rather than
guessed at -- the exact target % wasn't in the source document excerpt
provided, and a wrong guess baked into code is worse than an explicit
TODO in config.

Verified end-to-end against a 1,000-row synthetic universe during
development (mix ratios, exclusion enforcement, target volume, the
no-project-specific-vocabulary check, the framework-boundary guard
described above, and -- separately, against a synthetic
`dq_sample_selections` history -- the volume drift detector: flags a
sharp drop, stays silent on a stable pool, and returns `{}` cleanly on a
config's very first-ever run with no history to compare against).

### 3.7 Execution-context wrappers

New: `entrypoints.py` -- `lambda_handler`, `glue_main`,
`cron_run_once`/`cron_run_scheduler`, `DataQualityEngineOperator`.

All are thin: they parse a platform-specific trigger shape (Lambda event /
Glue job args / cron tick / Airflow context) and call
`rules_engine.engine.run_engine()` -- the same function `rules_engine/main.py`'s existing CLI
calls. Nothing in `rules_engine/`, `db/`, `sampling/`, or `utils/` imports from
`entrypoints.py`, so the dependency only goes one way -- remove any one
wrapper function and the engine still runs everywhere else.
`DataQualityEngineOperator` degrades gracefully (wrapped in
`try/except ImportError`) if `apache-airflow` isn't installed, rather than
hard-failing at import time for non-Airflow deployments.

`cron_run_scheduler`/`_run_cron_job` are also the one spot where this file
optionally chains into the Sampling Framework: a job entry's
`sampling_config_name` field, if set, fires
`sampling.engine.run_stratified_sampling()` after the rules-engine run
completes (`_run_cron_sampling`). That's deployment-schedule
convenience, not a code dependency between the two frameworks -- see §3.6
for why `sampling/` never imports back into `rules_engine/`.

### 3.8 Severity vocabulary

`dq_rules.severity` is free text -- a project stores its own vocabulary
**exactly as its source document defines it** rather than being forced
into a generic enum. `rules_engine/executor.py::evaluate_rule()` decides FAIL vs
WARN by INVERSION rather than by hardcoding any project's labels: a small
fixed set of "soft" severities (`WARN`, `WARNING`, `INFO`, `NOTICE`)
resolve to WARN, and everything else -- whatever a project calls it
(`'Compliance Flag'`, `'Timeliness'`, `'P1'`, anything) -- resolves to FAIL.
No project's severity vocabulary is ever written into `rules_engine/executor.py`;
HealthSpring UM's three severities work purely because they aren't in the
soft set, the same as any other project's would.

## 4. Section 8 -- known correctness issues

| # | Issue | Status | Evidence |
|---|---|---|---|
| 1 | Failed-record count wrong when capped | Already correct | `rules_engine/executor.py::_count_failed()` runs a separate `COUNT(*)` subquery; `_fetch_failed_rows()` is only used for exception capture, never for the recorded count. |
| 2 | Rule that fails to run must be SKIPPED, not omitted | Already correct | `execute_rule()` calls `record_rule_execution(..., status="SKIP", ...)` before returning when `validate_table_exists` fails. |
| 3 | Empty source table must not silently PASS | Already correct | `rules_engine/executor.py::evaluate_rule()` -- `require_rows` param, `dq_rules.require_rows` column. |
| 4 | Threshold logic needs AND and OR | Already correct | `evaluate_rule(..., threshold_operator=...)`, wired from `execute_rule()` -> `rule.get("threshold_operator", "OR")`; verified manually against DuckDB. |
| 5 | No silent double-counting on concurrent runs | Already correct | `dq_metrics_summary_uix` UNIQUE INDEX + `rules_engine/metrics.py::_upsert_metrics()` catches the duplicate-key error and falls back to a pure `UPDATE`. |
| 6 | Credentials read at call time, not import | Already correct | Every adapter's `_require()`/env read happens inside `build()`, called from `ConnectionFactory._build()` at `load()`/reconnect/`new_connection()` time -- never at module import. `utils/alert.py::_load_config()` is called inside `send_alert()`, not at import. |
| 7 | Retry transient source errors | Already correct | `rules_engine/executor.py` wraps `_count_total`, `_count_failed`, `_run_table_check`, `_fetch_failed_rows` in a `tenacity` retry decorator (`DQ_QUERY_MAX_RETRIES`, exponential backoff). |
| 8 | Dialect mismatch must fail before execution | Fixed here | Did not exist -- no dialect concept in the reference repo at all. Added dialect enforcement in `rules_engine/rule_sql.py` + `dq_rules.sql_dialect`, wired into both `engine.py::_pre_validate_rules` (load-time) and `executor.py::execute_rule` (defense-in-depth). See §3.1. |

Net new work from Section 8: one item (dialect enforcement). Everything
else was already correct in the repo; re-verified here rather than
re-implemented, since re-implementing already-correct code has no value
and risks regressions.

## 5. What was deliberately NOT changed

- The connector interface (`SourceAdapter` ABC) -- S3 slots in without
  touching `cursor()`/`commit()`/`close()`/`ping()`/`prepare()`.
- The thread-pool / dependency-graph / priority-ordering engine rules_engine.
- The rule-versioning, suppression, profiling, and anomaly-detection
  modules -- reused rather than reinvented; "what did the rule say a year
  ago" is already served by `dq_rule_versions`.
- `dq_exceptions`'s schema and immutability contract -- extended via a
  joined table, not mutated.

## 6. Data model deltas (v6) -- see `ddl_shared.sql`, `rules_engine/ddl.sql`,
and `sampling/ddl.sql` for full DDL

| Change | Table | Why |
|---|---|---|
| `sql_dialect` column | `dq_rules` | fail-fast dialect enforcement |
| `business_correctable` column | `dq_rules` | audience routing |
| `source_type` CHECK constraint | `dq_connections` | scope to 3 sanctioned adapters |
| New table | `dq_exception_dispositions` (v6 name: `dq_case_dispositions`) | additive-only disposition, immutable exceptions |
| New table | `dq_notification_routes` | audience routing by finding_class |
| New table | `dq_sampling_config` | stratified-sampling config |
| New table | `dq_sample_selections` | stratified-sampling immutable output |

## 6.5 Schema normalization (v7)

The v1-v6 schema had every project-scoped table carry its own raw
`project_name`/`process_name` VARCHAR(100) pair, and `dq_rule_execution`/
`dq_exceptions` additionally repeated `run_type`, `run_mode`, `batch_id`,
`dataset_id`, and the run's dates on every single row -- a run with 200
rules meant 200 copies of the same eight values. v7 removes that
duplication in favor of keys, with one explicit exception for
audit-fidelity reasons:

**New: `dq_scope(scope_id, project_name, process_name)`.** Every table
that used to carry its own project/process pair now carries a `scope_id`
FK instead: `dq_rules`, `dq_run_control`, `dq_metrics_summary`,
`dq_sampling_config`. Resolved via `utils/db_helpers.py::get_scope_id()`
(get-or-create, race-safe the same way `rules_engine/metrics.py::_upsert_metrics`
already handled concurrent-run MERGE races) at write time, or
`find_scope_id()` (read-only, returns `None` for an unseen project/process
rather than creating one) at read/filter time. `rules_engine/engine.py::run_engine`
resolves this once per run and stores it on the in-memory `run` dict as
`run["scope_id"]`, so every rule execution in that run reuses the same
resolved value instead of re-resolving per rule.

**Dropped entirely (not even a `scope_id`) from `dq_rule_execution`,
`dq_exceptions`, `dq_rule_issues`, `dq_column_profile`, and
`dq_anomaly_log`:** all five already carry `run_id`, and `run_id` maps 1:1
to a `dq_run_control` row that has `scope_id` (plus, for
`dq_rule_execution`/`dq_exceptions`, `run_type`/`run_mode`/`batch_id`/
`dataset_id`/dates too). Read code joins to `dq_run_control` (and
`dq_scope`, for project/process names) on `run_id` instead. Similarly,
`dq_sample_selections` carries `config_id`, which maps 1:1 to a
`dq_sampling_config` row with `scope_id` -- its project/process columns
are dropped the same way.

**What did NOT get normalized away, on purpose:**
`dq_rule_execution.rule_code`/`severity`/`table_name` and
`dq_exceptions.rule_code`/`table_name` stay exactly as they were. These
are frozen snapshots of what a *mutable* `dq_rules` row said **at
execution time** -- `dq_rules` can be edited later (a severity
reclassified from WARN to ERROR, a table renamed), and a CMS-audit-facing
execution record has to show what was judged back then, not what a live
join to `dq_rules` says today. Collapsing these into a join would quietly
rewrite history, which is the opposite of the audit-defensibility
requirement this whole v6/v7 effort exists to satisfy. This is a different
situation from `dq_exception_dispositions` below, which denormalized from
an *already-immutable* row -- there the join-vs-repeat trade-off has no
audit argument on the "repeat it" side, so it went the other way.

**`dq_exception_dispositions` trimmed of `run_id`, `rule_id`, `rule_code`,
`project_name`, `process_name`, `primary_key_str`** -- all six already
live on `dq_exceptions.exception_id`, and `dq_exceptions` is itself
immutable (never updated), so unlike the point-in-time-snapshot case
above, there's no fidelity reason to repeat them. Joined to `dq_exceptions`
by `exception_id` for everything else.

**Deliberately left un-normalized: `dq_profile_config`,
`dq_anomaly_config`, `dq_notification_routes`.** These are low-row-count,
NULL-wildcard config tables ("`project_name IS NULL` = applies to every
project") matched by a most-specific-row-wins rule at read time
(`rules_engine/metrics.py::_load_config`, `rules_engine/reporting.py::_load_routes`,
`rules_engine/profiler.py::_load_profile_configs`). They're not high-volume fact
tables, so the duplication-avoidance payoff is negligible, and collapsing
project_name+process_name into one `scope_id` FK would still need to
represent three distinct wildcard levels (fully global / project-wide /
exact project+process) -- that only adds indirection to the
specificity-matching logic for no real benefit. Left as plain nullable
columns.

## 7. Genericness test

To onboard an unrelated second use case: insert rows into `dq_connections`
(new `source_type` from the sanctioned 3, or a 4th adapter class in
`db/adapters.py`), insert rows into `dq_rules` (raw SQL + `sql_dialect` +
`check_type` tag, or check_type-generated), optionally a
`dq_sampling_config` row if it needs ranked sampling, and
`dq_notification_routes` rows for its own audiences. Zero files under
`rules_engine/` or `utils/` need to change, and `db/adapters.py` only changes if a
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
