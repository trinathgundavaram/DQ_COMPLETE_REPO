# shared/

Infrastructure used by BOTH [`rules_engine/`](../rules_engine/README.md) and
[`sampling/`](../sampling/README.md) -- kept here instead of duplicated in
each, or force-fit under one of them.

| File | What |
|---|---|
| `config.py` | Local `.env` credential loading (see `dev.env.example` at the repo root) + `GRE_META_CONNECTION`/`GRE_META_DB` resolution + the run-readiness extension point. Both `rules_engine/runner.py` and `sampling/sampling.py` import this. |
| `db_ops.py` | Low-level DB helpers: `execute_query`/`execute_dml`, chunked `bulk_insert`/`bulk_insert_or_skip`, `{key}` run_params token substitution (`_substitute_params`), the `build_run_key()` tracking-key formatter, the retry-wrapped `_run_source_query`, and `log_error()` (the one shared `gre_errors` write path). |
| `schema.sql` | `gre_rule_audit`, `gre_sampling_audit`, `gre_errors` -- the run-tracking and error-log tables the two packages write to -- plus the `gre_audit` backward-compatibility VIEW (a UNION of the first two, reproducing the pre-split combined shape for anything still querying it directly). See "Why `gre_audit` is now two tables plus a view" below. |
| `schema_drop.sql` | Drops the view and all three tables (see the file for redeploy ordering). |

## `run_key`: tracking a run, independent of `run_params`

`run_key` is an opaque, caller-supplied string -- the ONE tracking/
idempotency identifier every metadata table (`gre_log`,
`gre_exceptions_uix`, `gre_results_uix`, `gre_rule_audit`/
`gre_sampling_audit`, `gre_errors`) keys off, in both `rules_engine/` and
`sampling/`. There's no fixed shape to it: a plain batch id, a year+month
combo, a specific date, a region, or any combination all work equally
well.

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

## Why `gre_audit` is now two tables plus a view

Used to be ONE table, one row per run whichever package produced it --
`run_type` (`'RULE_GROUP'` | `'SAMPLING'`) discriminated the two, with
rule-run columns (`rule_group`, `project_name`, `total_rules`, ...) NULL
for a sampling row and sampling-run columns (`sample_config_id`,
`sampling_method`, `random_seed`, ...) NULL for a rule-group row. That's
real, avoidable confusion for the common case of using `rules_engine/`
WITHOUT `sampling/` (it's deliberately independent -- see
[`sampling/README.md`](../sampling/README.md)): every query against
`gre_audit` dragged along six always-NULL sampling columns that mean
nothing to a rules-only deployment, and vice versa.

As of 2026-08, `gre_rule_audit` carries ONLY rule-engine columns and
`gre_sampling_audit` carries ONLY sampling columns -- a rules-only user
queries `gre_rule_audit` and never sees a sampling column, period.
`gre_audit` still exists as a name, but now as a VIEW (`UNION ALL` of the
two tables, reproducing the exact old combined shape including
`run_type`) purely so anything already pointed at it -- a dashboard, an ad
hoc report, the Postgres mirror in [`metadata_sync/`](../metadata_sync/README.md)
-- keeps working unchanged. New code in `rules_engine/` and `sampling/`
reads/writes the two real tables directly, never the view. If you have an
existing deployment with real history already in the old combined
`gre_audit` table, use `migrate_split_gre_audit.sql` at the repo root
instead of the drop/recreate scripts below -- it backfills both new
tables from the existing data with zero loss.

`gre_errors` was deliberately left as ONE table, not split the same way:
it's the one execution-failure log for the whole engine (a rule crash and
a sampling-run crash both land here, `rule_id`/`rule_group` simply NULL
for the latter) -- one nullable column on a genuinely shared log is not
the same problem as six always-irrelevant columns on a run-tracking table,
and splitting it would duplicate `log_error()`'s machinery for no real
clarity gain.

Both `gre_rule_audit`/`gre_sampling_audit` still live here in `shared/`,
not moved out into `rules_engine/schema.sql`/`sampling/schema.sql` --
splitting the *code and DDL* for rule evaluation and sampling into
separate `rules_engine/`/`sampling/` folders doesn't require splitting
*where their table definitions live* too. Keeping them in one place (this
file) preserves the existing `shared/` first deploy order below without
restructuring it, the same practical reason the original single
`gre_audit` table lived here.

## Deploy order

```
shared/schema.sql          -- first: rules_engine/ and sampling/ both assume gre_rule_audit/gre_sampling_audit/gre_audit/gre_errors exist
rules_engine/schema.sql
sampling/schema.sql
```

Tear-down is the reverse (see each `schema_drop.sql`).
