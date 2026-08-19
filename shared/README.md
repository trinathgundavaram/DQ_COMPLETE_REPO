# shared/

Infrastructure used by BOTH [`rules_engine/`](../rules_engine/README.md) and
[`sampling/`](../sampling/README.md) -- kept here instead of duplicated in
each, or force-fit under one of them.

| File | What |
|---|---|
| `config.py` | Local `.env` credential loading (see `dev.env.example` at the repo root) + `GRE_META_CONNECTION`/`GRE_META_DB` resolution + the run-readiness extension point. Both `rules_engine/runner.py` and `sampling/sampling.py` import this. |
| `db_ops.py` | Low-level DB helpers: `execute_query`/`execute_dml`, chunked `bulk_insert`/`bulk_insert_or_skip`, `{key}` run_params token substitution (`_substitute_params`), the `build_run_key()` tracking-key formatter, the retry-wrapped `_run_source_query`, and `log_error()` (the one shared `gre_errors` write path). |
| `schema.sql` | `gre_audit` and `gre_errors` -- the two tables both packages write to. |
| `schema_drop.sql` | Drops both tables (see the file for redeploy ordering). |

## `run_key`: tracking a run, independent of `run_params`

`run_key` is an opaque, caller-supplied string -- the ONE tracking/
idempotency identifier every metadata table (`gre_log`,
`gre_exceptions_uix`, `gre_results_uix`, `gre_audit`, `gre_errors`) keys
off, in both `rules_engine/` and `sampling/`. There's no fixed shape to
it: a plain batch id, a year+month combo, a specific date, a region, or
any combination all work equally well.

- `build_run_key(*parts, delimiter="_")` joins one or more values into a
  single string (`build_run_key(2026, 8) -> "2026_8"`) -- purely a
  convenience formatter; callers are free to build their own string
  directly instead (e.g. reuse an existing date/batch value).

`run_key` is deliberately NOT merged into `run_params` -- see below.

## `run_params`: how each project scopes its own data

Different projects scope data differently -- a month/year pair, a
business batch id + run type combination, a region/contract column, or no
filter at all. Rather than the engine hardcoding filter columns, a
rule's/config's SQL embeds whichever `"{key}"` tokens it needs, and the
caller supplies a matching dict at run time:

- `_substitute_params(sql, params)` replaces every `"{key}"` token present
  in `params` (quoted, escaped) and raises `ValueError` if any
  `"{token}"`-shaped text remains afterward -- a project forgot to pass a
  param, or a rule/config has a typo -- so the failure is caught and
  logged before it ever reaches the source database, not as a confusing
  SQL syntax error.

`run_params` is completely free-form -- there is no reserved/required
key. `run_key` is passed alongside `run_params` as its own explicit
argument, never auto-merged in: `run_params` doubles as the equality
filters for the rules engine's auto-generated total-record count, and
`run_key` is often NOT a real column on a rule's/config's table (e.g. a
composite like `"2026_8"`), so auto-injecting it would silently break
that query for most tables. If a rule's/config's SQL needs to reference
the run's tracking value as a literal column filter, pass it explicitly
via `run_params` under whatever key matches an actual column.

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
