# shared/

Infrastructure used by BOTH [`rules_engine/`](../rules_engine/README.md) and
[`sampling/`](../sampling/README.md) -- kept here instead of duplicated in
each, or force-fit under one of them.

| File | What |
|---|---|
| `config.py` | Local `.env` credential loading (see `dev.env.example` at the repo root) + `GRE_META_CONNECTION`/`GRE_META_DB` resolution + the batch-readiness extension point. Both `rules_engine/runner.py` and `sampling/sampling.py` import this. |
| `db_ops.py` | Low-level DB helpers: `execute_query`/`execute_dml`, chunked `bulk_insert`/`bulk_insert_or_skip`, the dialect guard (`check_dialect`/`DialectMismatchError`), `{batch_id}` token substitution, the retry-wrapped `_run_source_query`, and `log_error()` (the one shared `gre_errors` write path). |
| `schema.sql` | `gre_audit` and `gre_errors` -- the two tables both packages write to. |
| `schema_drop.sql` | Drops both tables (see the file for redeploy ordering). |

## Why these two tables are shared, not split per-package

`gre_audit` is one row per run, whichever package produced it --
`run_type` (`'RULE_GROUP'` | `'SAMPLING'`) discriminates the two, with the
rule-run columns NULL for a sampling row and vice versa. `gre_errors` is
the one execution-failure log for the whole engine: a rule crash and a
sampling-run crash both land here (`rule_id`/`rule_group` NULL for the
latter).

Splitting the *code and DDL* for rule evaluation and sampling into
separate `rules_engine/`/`sampling/` folders didn't require splitting
these two tables too -- they were already a deliberate single-table design
("reuse `gre_audit` rather than inventing a parallel run-log table just
for sampling"), and duplicating that design into two independent audit/
error tables would trade a real, working piece of shared infrastructure
for symmetry alone. So they live here instead.

## Deploy order

```
shared/schema.sql          -- first: rules_engine/ and sampling/ both assume gre_audit/gre_errors exist
rules_engine/schema.sql
sampling/schema.sql
```

Tear-down is the reverse (see each `schema_drop.sql`).
