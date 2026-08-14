# rules_engine/ -- Generic Rules Engine (GRE)

A generic, config-driven rule evaluation engine: `gre_rules` rows define a
negative SQL SELECT per rule (the query returns the rows that VIOLATE the
rule); the engine runs each active rule for a `rule_group`/`batch_id`,
writes every violating row to `gre_exceptions`, evaluates a threshold, and
upserts a `gre_results` verdict row.

This package is deliberately independent of [`sampling/`](../sampling/README.md)
-- the two share only what's in [`shared/`](../shared/README.md) (DB
helpers, credential/config loading, and the `gre_audit`/`gre_errors`
tables). Nothing in `rules_engine/` imports from `sampling/`, or vice
versa.

## Files

| File | What |
|---|---|
| `rules.py` | `load_rules()` -- loads active `gre_rules` rows for a rule_group, ordered by `seq_no`. |
| `executor.py` | `execute_rule()` -- runs one rule end-to-end: dialect guard, single-scan evaluation (`_scan_violations`), threshold evaluation, `gre_exceptions`/`gre_results`/`gre_log` writes. |
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
