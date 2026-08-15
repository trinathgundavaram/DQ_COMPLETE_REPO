# rules_engine/ -- Generic Rules Engine (GRE)

A generic, config-driven rule evaluation engine: `gre_rules` rows define a
negative SQL SELECT per rule (the query returns the rows that VIOLATE the
rule); the engine runs each active rule for a `rule_group` (optionally
narrowed further by `rule_variant` -- see below) against a `batch_id`,
writes every violating row to `gre_exceptions`, evaluates a threshold, and
upserts a `gre_results` verdict row.

## Scoping a rule's data: `run_params`

`rule_sql` may embed any number of `"{key}"` tokens (not just
`{batch_id}`) -- each run passes a `run_params` dict, and every matching
token is substituted (quoted, escaped) before the query runs. A project
scopes its data however it needs: month/year, `batch_id` + `run_type`, a
date range, a region/contract column, or nothing at all (whole table).
`batch_id` is always present in `run_params` -- it's still the
tracking/idempotency key (`gre_exceptions_uix`, `gre_log`, `gre_results`,
`gre_audit`) -- but a rule can reference any other key the caller
supplies too:

```python
summary = run_rule_group(
    "claims_dq", "BATCH_2026_08_14", cf,
    run_params={"year": 2026, "run_type": "MONTHLY"},
)
```

An unresolved `"{token}"` (a rule references a key the run didn't supply)
fails that rule attempt immediately with a `PARAM_SUBSTITUTION_ERROR`,
before any query reaches the source database -- the same
fail-fast-before-any-query philosophy as the dialect guard.

There is no separate `scope_sql` column to author for the total-record
(denominator) count. Each `gre_rules` row names its `database_name` +
`table_name`, and the engine builds `SELECT COUNT(*) FROM
{database_name}.{table_name} WHERE ...` automatically, applying every key
in `run_params` as an equality filter (AND'd together) -- the SAME dict
that scopes `rule_sql` already says what's in scope for the total, so
there's nothing left to hand-write. A rule's table needs a real column
for every key its run passes (e.g. if a table has no `batch_id` column,
don't include one when scoping runs against it).

## Selecting which rules run: `rule_variant`

`rule_variant` is one additional, generic level of selection on top of
`rule_group`/table: within one `rule_group`, a `gre_rules` row with
`rule_variant IS NULL` always applies; a row with an explicit value only
applies when the caller's run requests that exact value:

```python
# Only universal (rule_variant IS NULL) rules for this group run:
run_rule_group("claims_dq", "BATCH_2026_08_14", cf)

# Universal rules PLUS any rule whose rule_variant == "2026":
run_rule_group("claims_dq", "BATCH_2026_08_14", cf, rule_variant="2026")
```

This is a single freeform column, not separate hardcoded `year_column`/
`run_type_column` fields -- a project needing more than one dimension at
once composes a single string (e.g. `"2026|MONTHLY"`), the same
"SQL/config authors are self-contained" philosophy `rule_sql` already
follows.

This package is deliberately independent of [`sampling/`](../sampling/README.md)
-- the two share only what's in [`shared/`](../shared/README.md) (DB
helpers, credential/config loading, and the `gre_audit`/`gre_errors`
tables). Nothing in `rules_engine/` imports from `sampling/`, or vice
versa.

## Files

| File | What |
|---|---|
| `rules.py` | `load_rules()` -- loads active `gre_rules` rows for a rule_group (+ optional `rule_variant` filter), ordered by `seq_no`. |
| `executor.py` | `execute_rule()` -- runs one rule end-to-end: dialect guard, `run_params` substitution, single-scan evaluation (`_scan_violations`), threshold evaluation, `gre_exceptions`/`gre_results`/`gre_log` writes. |
| `runner.py` | `run_rule_group()` -- orchestration entry point: readiness gate, checkpoint/resume, sequencing_mode-aware loop over `execute_rule()`, `gre_audit` start/finish. |
| `reporting.py` | `get_breaches()` / `get_records_for_result()` -- thin read-only queries against `gre_results`/`gre_exceptions`. |
| `schema.sql` | `gre_rules`, `gre_log`, `gre_exceptions`, `gre_case`, `gre_results`. Deploy after `shared/schema.sql`. |
| `schema_drop.sql` | Drops the 5 tables above, for the drop-and-recreate redeploy policy (see the repo root README). |

## Big-dataset path

Two optimizations that matter once a rule matches millions of rows (see
`executor.py`'s module docstring for the full write-up):

- **Single-scan evaluation** (`_scan_violations`): `rule_sql` is scanned
  ONCE via streamed `fetchmany()`, producing both the true failed-record
  count (uncapped) and a `GRE_MAX_EXCEPTIONS`-capped row list for detail
  capture -- instead of a separate `COUNT(*)` query plus a full detail
  fetch.
- **Memoized total counts** (`_compute_total`'s `total_cache`): rules in
  the same group that ask the identical "how many rows are in this batch"
  question share one `COUNT(*)` result for the whole `run_rule_group()`
  call, threaded through via `total_cache`.

Both `_scan_violations`'s and `_write_exceptions`'s row writes go through
`shared/db_ops.py`'s chunked `bulk_insert_or_skip()` rather than one
INSERT+commit per row.

## Quick start

```python
from db.connection_factory import ConnectionFactory
from rules_engine.runner import run_rule_group

cf = ConnectionFactory()
cf.load()

summary = run_rule_group("claims_dq", "BATCH_2026_08_14", cf)
print(summary["status"], summary["succeeded"], summary["errored"])
```

See the repo root README for environment setup (`dev.env`, `DQ_CONNECTION_NAMES`, etc.).
