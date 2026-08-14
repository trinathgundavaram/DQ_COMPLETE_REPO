# Generic Rules Engine (GRE)

A standalone SQL-authoring DQ/compliance rules engine, separate from this
repo's `dq_*` engine (`core/`, `ddl.sql`). Nothing here touches, imports,
or renames any `dq_*` object or any file under `core/`, `utils/`, or
`config/` — the only reuse is `db/adapters.py` and
`db/connection_factory.py`, imported directly.

## Files

| File | Responsibility |
|---|---|
| `schema.sql` | DDL for the 7 `gre_*` tables + the two idempotency unique indexes |
| `config.py` | Metadata connection/schema resolution; batch-readiness extension point |
| `rules.py` | Loads active rules for a `rule_group` from `gre_rules` |
| `executor.py` | Runs one rule, writes findings, evaluates threshold, upserts `gre_results` |
| `runner.py` | Orchestrates a rule_group run: readiness gate, checkpoint/resume, sequencing |
| `reporting.py` | `get_breaches()` / `get_records_for_result()` — a report is just a query |

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

Zero engine code changes are needed to onboard a new rule or a whole new
use case — see `tests/test_gre_executor.py` / `test_gre_runner.py` for
worked examples.

## Running it

```python
from db.connection_factory import ConnectionFactory
from gre.runner import run_rule_group

cf = ConnectionFactory()
cf.load()
summary = run_rule_group(rule_group="claims_dq", batch_id="2026-08-13", cf=cf)
```

## Tests

```
pytest tests/test_gre_executor.py tests/test_gre_runner.py -v
```

DuckDB stands in for every connection (same convention as
`tests/test_core.py`) — no live DB needed.

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
