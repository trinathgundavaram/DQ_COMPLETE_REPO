# Generic Rules Engine (GRE)

A standalone SQL-authoring DQ/compliance rules engine, separate from this
repo's `dq_*` engine (`core/`, `ddl.sql`). Nothing here touches, imports,
or renames any `dq_*` object or any file under `core/`, `utils/`, or
`config/` — the only reuse is `db/adapters.py` and
`db/connection_factory.py`, imported directly.

## Files

| File | Responsibility |
|---|---|
| `schema.sql` | DDL for the 12 `gre_*` tables + idempotency unique indexes |
| `config.py` | Metadata connection/schema resolution; batch-readiness extension point |
| `rules.py` | Loads active rules for a `rule_group` from `gre_rules` |
| `executor.py` | Runs one rule, writes findings (batched), evaluates threshold, upserts `gre_results`. Also the ONE place in `gre/` with `execute_query`/`execute_dml`/`bulk_insert`/`bulk_insert_or_skip`/`log_error` — `rules.py`, `runner.py`, `reporting.py`, and `sampling.py` all import these rather than re-implementing them |
| `runner.py` | Orchestrates a rule_group run: readiness gate, checkpoint/resume, sequencing |
| `reporting.py` | `get_breaches()` / `get_records_for_result()` — a report is just a query |
| `sampling.py` | Config-driven stratified sampling — a separate concern, N-level generalization of `core/stratified_sampling.py`; reuses `executor.py`'s `bulk_insert`/`log_error`/`_run_source_query`/`_substitute_batch_id` |
| `seed/um_sample.sql` | The real `dq_sampling_config.config_id=1` UM sample re-expressed as `gre_sampling_config`/`_strata`/`_mix` rows |

## Setup

1. Run `schema.sql` against your Teradata metadata schema (defaults to
   `CMSUNIV_FILELAND_DEV_T`, same schema `dq_*` uses — set `GRE_META_DB` to
   point elsewhere without a code change).
2. Make sure `DQ_CONNECTION_NAMES` (and the matching `DQ_<NAME>_*` env
   vars) already includes every connection your rules will query, per
   `db/connection_factory.py`'s existing convention. No new connector
   plumbing is needed.
3. `GRE_META_CONNECTION` (default `"teradata"`) picks which of those
   connections holds the `gre_*` tables.
4. Optional tuning for large rule/candidate volumes (see "Running big
   datasets" below): `GRE_EXCEPTION_CHUNK` (default `500`) and
   `GRE_MAX_EXCEPTIONS` (default `10000`).

## Authoring a rule

Insert one row into `gre_rules`. Key fields:

- `rule_sql` — a complete, self-contained negative SELECT that returns
  violating rows. It may embed a literal `{batch_id}` token anywhere; the
  engine substitutes it (quoted/escaped) before running. There's no
  filter_column/filter_sql system — rules fully self-scope.
- `natural_key_columns` — comma-separated column names present in
  `rule_sql`'s own SELECT output (e.g. `"claim_id"` or
  `"member_id,claim_id"`). This is what makes a rerun of the same batch
  idempotent: `gre_exceptions` has a `UNIQUE(rule_id, batch_id,
  natural_key_value)` index, and a duplicate insert is caught and skipped,
  never deleted-and-reinserted.
- `scope_sql` — optional; if unset, the denominator for threshold %s
  defaults to `SELECT COUNT(*) FROM {table_name} WHERE {batch_id_column} =
  '{batch_id}'` (`batch_id_column` defaults to `'batch_id'`).
- `threshold_pct` / `threshold_count` / `threshold_operator` / `severity` —
  same pct-vs-count-vs-both decision logic as `core/executor.py`'s
  `evaluate_rule()`. No threshold configured at all falls back to "breach
  only if every in-scope record failed" (implemented as its own explicit
  `failed == total` check, not `threshold_pct=100` with `>`).
- `sequencing_mode` / `on_failure` — only `sequential` groups honor
  `seq_no` order and `on_failure` (`halt_group` stops starting further
  rules in the group for this run; already-committed findings are never
  rolled back). `independent` (default) runs every rule regardless of
  another rule's outcome.
- `sql_dialect` — `'teradata'` | `'postgres'` | `'ansi'` (NOT NULL). Checked
  against the target connection's `source_type` (see
  `db/adapters.py::SourceAdapter.source_type`) before `rule_sql` ever runs
  — `gre/executor.py::check_dialect()`. `'ansi'` is accepted everywhere;
  a genuine mismatch (e.g. a `'teradata'`-dialect rule pointed at a
  `postgresql` connection) fails fast with a clear `DIALECT_MISMATCH`
  `gre_errors` row instead of a confusing mid-run SQL syntax error. A
  connection whose `source_type` isn't in `DIALECT_COMPATIBILITY`
  (currently `databricks`/`sqlserver` — see the comment there) skips the
  check with a warning rather than guessing.

Zero engine code changes are needed to onboard a new rule or a whole new
use case — see `tests/test_gre_executor.py` / `test_gre_runner.py` for
worked examples.

## Running big datasets

Two things in `gre/executor.py` are specifically shaped for a rule that
matches a very large number of rows, mirroring `core/executor.py`'s
proven fix #1 pattern rather than inventing a new one:

- **`rule_sql` is scanned ONCE per rule, not twice.** `_scan_violations`
  streams the rule's own query via `fetchmany()` a single time, producing
  BOTH the true failed count (every row is counted, even past any cap) AND
  a capped row list for detail capture from that same pass — replacing the
  earlier two-query design (a separate `COUNT(*)`-wrapped query, then
  `rule_sql` run again in full to fetch rows), which scanned the identical
  predicate against the identical rows twice per rule. This roughly halves
  read load against the source table for every rule, with no change to how
  rules are written. One trade-off: since count and capture now share one
  query, a failure during that scan fails the whole rule (there's no
  longer an independently-obtained count to fall back on if only the
  detail-fetch half had failed) — see `_scan_violations`'s docstring.
- **`gre_exceptions` detail-row capture is capped**, not one unbounded
  `fetchall()`. `_scan_violations` pulls rows in `GRE_EXCEPTION_CHUNK`-
  sized batches and stops *storing* (though not counting) once
  `GRE_MAX_EXCEPTIONS` rows have been collected (`0`/negative = unlimited).
  A rule matching 10 million rows still gets an exact `failed_records`, and
  `gre_exceptions` gets a bounded, configurable number of detail rows
  instead of trying to hold everything in memory. As with the dq_* engine,
  **a capped rule still reports SUCCESS/PASS-FAIL-WARN correctly** — only
  the *detail* rows are truncated, and a later rerun of the same
  `batch_id` will NOT retroactively backfill the rows that got dropped by
  the cap (there's nothing to trigger that rerun automatically).
- **Writes are batched, not one row at a time.** `_write_exceptions` first
  de-duplicates violating rows by natural key within the current pull (so
  a rule that legitimately returns the same natural key twice in one
  result set — e.g. a join fan-out — doesn't cost a wasted duplicate-key
  round trip per repeat), then writes via `bulk_insert_or_skip`: one
  `executemany()` per `GRE_EXCEPTION_CHUNK`-sized chunk instead of one
  `INSERT` + commit per row, falling back to row-by-row only for a chunk
  that actually collides with a natural key committed by an earlier
  attempt on this `batch_id` (i.e. a genuine rerun). `gre_log.rowcount`
  reflects rows actually written *this* attempt, not the cumulative total
  on file. The plain (no duplicate-handling) counterpart, `bulk_insert`,
  is reused by `gre/sampling.py` for `gre_sample_selections` /
  `gre_sample_selection_attrs`, which are append-only with no unique index
  and so need no duplicate-key fallback.
- **`_compute_total`'s COUNT(*) is memoized across a whole `run_rule_group()`
  call.** Several rules in a group very often ask the same "how many rows
  are in this batch" question — same `table_name`/`batch_id_column`, or an
  intentionally shared `scope_sql` — so `runner.py` threads one dict
  (keyed by `source_connection` + the resolved query text) through every
  `execute_rule()` call in the run, and a repeat query is served from that
  cache instead of re-scanning the same rows once per rule. Scoped to a
  single run (a fresh cache every call) since the source isn't expected to
  change mid-run — the same assumption `gre_exceptions`' idempotency
  already relies on. Direct `execute_rule()` calls that don't pass a
  `total_cache` (e.g. in tests) get the old always-fresh-query behavior.
- **`gre/sampling.py`'s `_priority_rank` is computed by the database**, via
  `ROW_NUMBER() OVER (ORDER BY priority_rank_sql)` selected alongside the
  rest of the candidate pull, instead of a Python `enumerate()` loop over
  the fetched rows in `_pull_candidates`. For a candidate pool in the
  millions this moves that O(n) pass into the source engine, which is
  already doing the `ORDER BY` for the query itself.

Both env vars default to and mean the same thing as their `dq_*` engine
counterparts (`DQ_EXCEPTION_CHUNK` / `DQ_MAX_EXCEPTIONS`), just
independently configurable per engine.

## Running it

```python
from db.connection_factory import ConnectionFactory
from gre.runner import run_rule_group

cf = ConnectionFactory()
cf.load()
summary = run_rule_group(rule_group="claims_dq", batch_id="2026-08-13", cf=cf)
```

## Sampling

Stratified sampling is a fully separate concern from rule evaluation — it
can run with zero `gre_rules` rows defined. Three separable questions map
to three separable config pieces:

- **What gets filtered out** — `gre_sampling_config.exclusion_sql`, a plain
  WHERE-fragment.
- **What gets sampled** — `gre_sampling_strata` (one row per stratification
  level, in `level_order`) + `gre_sampling_mix` (one row per named bucket
  value within a level; any bucket value present in the data but not
  listed absorbs the remainder fraction). Zero `gre_sampling_strata` rows
  for a config means no stratification — straight to selection on the
  whole candidate pool. This is what makes the level count configurable:
  `gre/sampling.py::_stratify()` recurses one level at a time and doesn't
  know or care how many levels deep it is, so a third (or tenth) level is
  one more `gre_sampling_strata` row, never a code change.
- **How it gets sampled** — `gre_sampling_config.sampling_method`:
  `RANKED` (top-N by `priority_rank_sql`, deterministic), `RANDOM`
  (uniform draw), or `SYSTEMATIC` (fixed interval with a random start
  offset). `priority_rank_sql` is required for `RANKED`/`SYSTEMATIC`,
  ignored for `RANDOM`.

```python
from db.connection_factory import ConnectionFactory
from gre.sampling import run_sampling

cf = ConnectionFactory()
cf.load()
result = run_sampling(config_id=1, batch_id="2026-08-14", cf=cf)
```

**Reproducibility for `RANDOM`/`SYSTEMATIC`**: one seed is generated (or
passed explicitly) per `sample_run_id` and persisted on the `gre_audit`
row (`random_seed`). Buckets are always processed in a fixed, deterministic
order (sorted bucket values) through a single seeded `Random()` instance,
so re-running `_stratify()` with the same seed reproduces every bucket's
draw exactly — `SYSTEMATIC`'s interval is arithmetic on the data itself
(`floor(bucket_size / n)`), not random, so it doesn't need separate
persistence. This was a deliberate choice over persisting interval/offset
per bucket in a companion table: simpler schema, at the cost of needing a
replay (not a single-row lookup) to answer "what were bucket B's exact
draw parameters."

Every candidate considered — selected or not — is persisted to
`gre_sample_selections`, with one `gre_sample_selection_attrs` row per
candidate per stratification level, keyed by `(sample_run_id, case_key)`
rather than a fetched-back surrogate id (Teradata has no `RETURNING`
clause, and `case_key` is already unique within one `sample_run_id`).
Never updated after the run — a rerun uses a fresh `sample_run_id`, unlike
`gre_exceptions`/`gre_results`.

## Tests

```
pytest tests/test_gre_executor.py tests/test_gre_runner.py tests/test_gre_sampling.py -v
```

DuckDB stands in for every connection (same convention as
`tests/test_core.py`) — no live DB needed. `test_gre_sampling.py` includes
a direct regression check: the same fixture universe run through both
`core/stratified_sampling.py` and `gre/sampling.py` with equivalent config,
asserting matching per-bucket selected counts.

## Deliberately deferred / documented assumptions

- **Batch-readiness precondition**: ships as a no-op extension point
  (`gre_config.register_readiness_check`). Wire up a real check per
  `rule_group` when you know what "batch complete" means for your source
  systems — see `config.py`'s docstring.
- **`gre_exceptions` is append-only, never deleted.** A rerun of the *same*
  `batch_id` against unchanged source data won't duplicate rows (that's
  the whole point of the unique index), but if you rerun a batch_id after
  *fixing* the underlying data, previously-written exception rows for
  violations that no longer reproduce are **not** retracted — the table
  assumes `batch_id` denotes an immutable snapshot/load, which is the
  common pattern for these pipelines. `etl_is_curr_ind` is in the schema
  as the extension point for a future soft-retraction/versioning pass if
  your actual usage reruns a batch_id after correcting data; it isn't
  wired into engine logic in this v1.
- **Glue/Secrets Manager credentials**: the legacy `maa-compliance-rules-engine`
  code that pattern would be mirrored from isn't in this repo, so it
  wasn't copied. `db/adapters.py`'s existing env-var-at-call-time pattern
  is used as-is; add a Secrets-Manager-backed env var loader ahead of
  `db/adapters.py`'s calls if/when this runs on Glue.
- **`gre_case`** is pure reference data (rules join out to it to correlate
  findings across tables); the engine itself never reads or writes it.
- **`gre_audit` was extended, not left alone**, to add the sampling
  module: `rule_group`/`batch_id` are now nullable, and a `run_type`
  discriminator (`'RULE_GROUP'`/`'SAMPLING'`) plus nullable
  sampling-specific columns were added, per the prompt's explicit "reuse
  gre_audit rather than inventing a parallel run-log table." Nothing has
  been deployed against this schema yet, so this is a straight edit of the
  v3 DDL, not a live migration — if `schema.sql` has already been run
  against a real schema by the time you read this, you'll need an `ALTER
  TABLE` instead.
- **`gre_sampling_config.scope_sql` is a WHERE-fragment**, not a COUNT
  query — same field name as `gre_rules.scope_sql`, different shape,
  because it answers a different question here ("which rows are this
  cycle's candidates" vs. "what's the denominator for a threshold %").
  Flagging this explicitly since the name reuse could otherwise read as
  the same mechanism.
