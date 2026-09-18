# rules_engine/ -- Generic Rules Engine (GRE)

A generic, config-driven rule evaluation engine: `gre_rules` rows define a
negative SQL SELECT per rule (the query returns the rows that VIOLATE the
rule); the engine runs each active rule for a `rule_group` (every active
rule_variant by default, optionally narrowed to one -- see below) against
a `run_key`, writes every violating row to `gre_exceptions`, evaluates a
threshold, and upserts a `gre_results` verdict row.

## Tracking a run: `run_key`

`run_key` is an opaque, caller-supplied string -- the ONE tracking/
idempotency identifier `gre_exceptions` (`gre_exceptions_uix`),
`gre_results`, and `gre_rule_audit` all key off. There's no
fixed shape to it: a plain batch id, a year+month combo, a specific date,
a region, or any combination all work equally well. `rules_engine/db_ops.py`'s
`build_run_key(*parts, delimiter="_")` is a convenience formatter
(`build_run_key(2026, 8) -> "2026_8"`), or just pass your own string.

## Identifying an attempt: `run_id`

`run_key` (above) says WHICH logical run this is -- a batch, a period, a
date. `run_id` says WHICH SPECIFIC ATTEMPT at it this is: `run_rule_group()`
mints a brand new `run_id` every time it's called, even when called again
with the exact same `run_key` (a deliberate rerun, or a resumed run after a
crash). Every row `gre_exceptions`, `gre_results`, `gre_rule_errors`,
and `gre_rule_audit` write for one call all carry that same `run_id`.

`run_id` is built by `rules_engine/runner.py::_build_group_run_id()` on top
of `rules_engine/db_ops.py::generate_run_id()` (the same underlying helper
`sampling/sampling.py` uses for `sample_run_id`), shaped as:

```
{project_name}.{rule_group}::{run_key}::attempt-{N}::{triggered_by}::{YYYYMMDDTHHMMSS.ffffff}::{6 hex chars}

e.g. HEALTHSPRING_UM.claims_dq::BATCH_2026_08_19::attempt-2::jsmith::20260819T151500.500000::f9e8d7
```

(`project_name` is omitted, along with its trailing `.`, when a rule_group's
rules have no `project_name` set -- the id then starts straight from
`rule_group`.)

- `{project_name}.{rule_group}` says which business process this run
  belongs to, not just which rule_group -- readable at a glance in
  `gre_results`/`gre_exceptions`/a log line without a join back to `gre_rules`.
- `run_key` right after it means a human can tell what ran and for which
  batch/period together, in one look.
- `attempt-{N}` (`N` = `count_prior_attempts()` + 1, counted from
  `gre_rule_audit` for this exact `(rule_group, run_key)` pair) makes a rerun
  visibly a rerun -- "this is the 2nd attempt at BATCH_2026_08_19" reads
  directly off the id, instead of requiring a human to compare two run_ids'
  timestamps to work out which came first. This is a convenience LABEL,
  not the uniqueness mechanism (see the hex suffix below) -- a rare race
  between two callers starting the same run_key at the same instant could
  in principle produce the same attempt number on both, which is harmless
  cosmetically since nothing's correctness depends on it.
- `triggered_by` -- who/what kicked this off (a login, a scheduler name,
  or `"SYSTEM"` for the default/unattended case) -- is already collected
  as a `run_rule_group()` parameter and recorded on `gre_rule_audit`; folding it
  into the id too makes it visible on `gre_exceptions`/`gre_results`
  rows as well, which don't otherwise carry it.
- `::` separates the parts because `rule_group`/`run_key`/`triggered_by`
  often already contain `_` or `-` themselves; nothing in this codebase
  parses a run_id back apart, so this is purely for readability, not
  machine parsing.
- The timestamp is microsecond-precision and sortable
  (`ORDER BY run_id` already sorts oldest-to-newest), so the exact moment
  an attempt started is readable directly off the id.
- The trailing 6 hex characters guarantee uniqueness even if two calls
  land in the same microsecond -- a real risk under the old
  second-precision-only format this replaced, since a run_id collision
  corrupts the `active_ind`/`etl_is_curr_ind` "which attempt is current"
  reconciliation every write path here relies on (each filters on
  `run_id <> this_run_id` to tell "this attempt" from "an earlier one").

Like `run_key`, `run_id` is opaque as far as the engine is concerned --
nothing re-parses it. `gre_rule_audit` is still the place to look up the FULL
context of one `run_id` (its `run_key`, `rule_group`, `status`,
`started_at`, `triggered_by`, etc.) rather than trying to decode it from
the string -- the label parts above exist purely so the common questions
(what ran, for which batch, which attempt, triggered by whom) don't
*require* that lookup for a quick glance.

`rules_engine.runner.generate_run_id(rule_group, run_key)` -- the plain
2-part shape (no project/attempt/triggered_by) -- is kept as a public
function for any external caller still relying on it, but `run_rule_group()`
itself no longer uses it internally.

## Rerunning a `run_key`: `active_ind` / `etl_is_curr_ind`

Every rule always re-executes for its `run_key` -- there's no checkpoint/
resume skip of an already-succeeded rule. Rerunning the same `run_key`
(deliberately, or resuming after a crash) re-runs the whole group under a
brand new `run_id`, and `run_rule_group()` blanket-deactivates every
currently-active row for `(rule_group, run_key)` across `gre_results`,
`gre_rule_errors`, and `gre_exceptions` -- flipping `active_ind`/
`etl_is_curr_ind` to `'N'` -- in ONE pass, BEFORE any rule in the new
attempt executes. Nothing from a prior attempt at the same `run_key` is
left active by default, for any of the three tables.

This is a single, up-front, orchestration-level pass
(`rules_engine/runner.py::_deactivate_all_active_for_run()`), not a
per-rule check made right before each rule writes its own new row -- that
older design only ever deactivated a rule_id's OWN prior rows at the
moment that same rule_id executed again, so a rule_id no longer part of
the current attempt's active set (deactivated in `gre_rules`, removed
from the group, or narrowed out by a `rule_variant` filter) kept its
stale `active_ind='Y'`/`etl_is_curr_ind='Y'` rows forever. The blanket
pass closes that gap: it touches every active row for the `(rule_group,
run_key)` pair regardless of which rule_id produced it, so a rule that
simply isn't run again this attempt still gets cleaned up.

Known accepted limitation: the blanket pass scopes by each rule's
CURRENT `gre_rules.rule_group` value (read off this attempt's loaded
rules), so a `rule_id` whose `rule_group` was reassigned BETWEEN attempts
leaves its old rows (filed under the OLD `rule_group`) untouched -- an
edge case rare enough (`rule_group` reassignment mid-flight) not to
warrant tracking every `rule_id` that ever touched a `run_key` across
every historical `rule_group` it was ever tagged with.

Each of the three `UPDATE`s is independently try/excepted and never
raises -- a failure to deactivate one table's stale rows is logged, but
never blocks the run itself from proceeding (same "best-effort,
non-fatal housekeeping" philosophy as the rest of this engine's
logging/audit writes).

## Scoping a rule's data: `run_params`

`rule_syntax` may embed any number of `"{key}"` or `"$key"` tokens (freely mixed) -- each run passes a
`run_params` dict, and every matching token is substituted (quoted,
escaped) before the query runs. A project scopes its data however it
needs: month/year, a business batch id, a date range, a region/contract
column, or nothing at all (whole table). `run_params` is completely
free-form -- there is no reserved/required key, and `run_key` is
deliberately NOT auto-merged into it (see below):

```python
summary = run_rule_group(
    "claims_dq", "BATCH_2026_08_14", cf,
    run_params={"year": 2026, "run_type": "MONTHLY"},
)
```

An unresolved `"{token}"` (a rule references a key the run didn't supply)
fails that rule attempt immediately with a `PARAM_SUBSTITUTION_ERROR`,
before any query reaches the source database.

There is no separate `scope_sql` column to author for the total-record
(denominator) count. Each `gre_rules` row names its `database_name` +
`src_tbl_nm`, and the engine builds `SELECT COUNT(*) FROM {table_ref}
WHERE ...` automatically (`table_ref` is `database_name.src_tbl_nm` for a
real database, or the prepared view name for a file/S3 source -- see
"One connection per source: `sql_dialect`" below), applying every key in
`run_params` as an equality filter (AND'd together) -- the SAME dict that
scopes `rule_syntax` already says what's in scope for the total, so there's
nothing left to hand-write. A rule's table needs a real column for every
key its run passes (e.g. if a table has no matching column, don't include
that key when scoping runs against it). This is also why `run_key` isn't
auto-injected into `run_params`: `run_key` is often NOT a real column
(e.g. a composite like `"2026_8"`), so auto-adding it would silently
break the total-record query for most tables. If `rule_syntax` needs to
reference the run's tracking value as a literal column filter, pass it
explicitly via `run_params` under whatever key matches an actual column
(e.g. `run_params={"batch_id": "BATCH_2026_08_14"}`).

**A `run_params` key that turns out NOT to be a real column doesn't fail
the rule.** If the total-count query above fails (e.g. "column not
found"), it's retried ONCE with `run_params` dropped from the
denominator entirely (`extra_filters` only, if the rule opted into
those) -- the rule still produces a real verdict, just against a
coarser (never narrower, never wrong-in-a-way-that-hides-a-breach)
denominator, instead of erroring out over an unrelated column-name
mismatch. A `WARNING` is logged when this fallback kicks in, naming the
`run_params` key that didn't resolve to a real column. Prefer
`text_params` (below) for a value you already know isn't a column --
it skips the failed first attempt and its extra query round trip, and
keeps the denominator precisely scoped by whichever `run_params` keys
ARE real columns.

## Text-only substitution values: `text_params`

Every `run_params` key is assumed to name a real column, because it
doubles as a total-record-count filter (see above). A value that's only
ever needed for `rule_syntax` text substitution -- and doesn't correspond
to any actual column -- breaks that auto-generated count with an
"unresolved/unknown column" error from the source database the moment
it's passed as `run_params`.

`text_params` is the escape hatch: identical `"{key}"`/`"$key"`
substitution into `rule_syntax`, but a `text_params` key is NEVER folded
into the total-count `WHERE` clause.

```python
summary = run_rule_group(
    "claims_dq", "BATCH_2026_08_14", cf,
    run_params={"batch_id": "BATCH_2026_08_14"},      # batch_id IS a real column -- scopes the count too
    text_params={"RUNTYPE": "MNT"},                    # RUNTYPE is not a column -- substitution only
    extra_filters={"run_ty": "MNT"},                    # run_ty IS the real column -- scope via a filter instead
)
```

or from the CLI:

```
python run_by_process.py rules --process-name UNIVERSE_VALIDATION     --param year=2026 --text-param RUNTYPE=MNT --filter run_ty=MNT
```

Rule of thumb: if a `run_params`/`--param` key's NAME doesn't match a
real column on the rule's table, it belongs in `text_params`/
`--text-param` instead, and the actual scoping column (if any) goes
through `extra_filters`/`--filter`. `text_params` merges with
`run_params` for substitution purposes only -- on a key collision,
`text_params` wins -- and is merged into `gre_rule_audit.run_params`'s
JSON for audit visibility (there is no separate `gre_rule_audit`
column for it). It has no reserved/required key, same as `run_params`.

## Ad-hoc runtime filters: `extra_filters`

`run_params` above is for tokens a rule's author explicitly wrote into
`rule_syntax`. `extra_filters` is for a column the rule *wasn't*
authored to anticipate -- e.g. narrowing a long-standing rule to
`run_ty = 'MNT'` at run time without touching `gre_rules` at all.

**`extra_filters` now applies to EVERY rule run under the process by
default** -- a rule's `rule_syntax` does not need to embed anything for
it to take effect. This is a deliberate default-on design: a caller
passing `extra_filters` wants it applied across the whole run, not only
to rules an author remembered to opt in. There are two cases, depending
on whether a rule's `rule_syntax` happens to embed the literal marker
`"{extra_filters}"`/`"$extra_filters"`:

1. **Marker present** -- spliced in at that exact position, same as
   before (the precise, author-controlled placement):

   ```sql
   SELECT claim_id, denial_reason FROM claims
   WHERE denial_reason IS NULL AND claim_year = {year} AND claim_month = {month} {extra_filters}
   ```

2. **No marker** -- `rule_syntax` is wrapped as a **derived table** and
   the filter applied on the OUTER query instead:
   `"SELECT * FROM (<rule_syntax>) w WHERE col1 = 'v1' AND col2 = 'v2' ..."`.
   An earlier version of this textually appended `"AND col = 'val'"`
   directly onto the end of `rule_syntax` -- that is WRONG the moment
   `rule_syntax`'s `WHERE` clause has a top-level `OR`: SQL's "`AND`
   binds tighter than `OR`" precedence means an appended `AND` only
   attaches to the LAST `OR` branch, silently leaving every other branch
   completely unfiltered (confirmed in production: a rule with
   `"... WHERE A OR B"` combined with a no-marker filter left every row
   matching branch `A` unfiltered by the extra_filters column). The
   derived-table wrap sidesteps operator precedence entirely: the outer
   `WHERE` always applies to `rule_syntax`'s FULL result set, regardless
   of how many top-level `AND`/`OR` branches it has, or whether it has a
   `WHERE` clause (or `GROUP BY`/`HAVING`/`ORDER BY`) at all.

   **The one remaining failure mode**: if `rule_syntax`'s own `SELECT`
   list doesn't project the filtered column at all (an explicit column
   list that omits it), the outer query can't see it, and the source
   database raises an ordinary "column not found" error -- caught and
   logged the same as any other broken `rule_syntax`
   (`SQL_RUNTIME`/`SCOPE_QUERY_FAILURE`). **This is intentional: if
   applying `extra_filters` breaks a rule whose `SELECT` list genuinely
   can't see the filtered column, that failure is expected and gets
   logged, not prevented.** Fix a rule that breaks this way by either
   widening its `SELECT` list to include the column, or adding the
   `{extra_filters}`/`$extra_filters` marker at the correct position in
   its `rule_syntax` for precise, author-controlled placement instead.

A caller passes one or more filters as a plain dict:

```python
summary = run_rule_group(
    "claims_dq", "BATCH_2026_08_14", cf,
    run_params={"year": 2026, "month": 8},
    extra_filters={"run_ty": "MNT"},
)
```

or from the CLI:

```
python run_by_process.py rules --process-name UNIVERSE_VALIDATION     --param year=2026 --param month=8 --filter run_ty=MNT
```

`--param`/`--filter`/`--text-param` (and every other `run_by_process.py`
flag) are named options, not positional arguments -- pass them in any
order on the command line, and repeat any of them once per key; the
same key passed twice to the same flag has the last occurrence win.

`extra_filters` supports more than one filter at once (`{"run_ty": "MNT",
"region": "EAST"}` -> `AND region = 'EAST' AND run_ty = 'MNT'`, sorted for
deterministic SQL text). It is also merged into the auto-generated
total-record (denominator) count UNCONDITIONALLY now (no marker-presence
gate), since it structurally applies to every rule's scan by default --
`failure_pct` always reflects the same narrowed scope the scan itself
used. `gre_results.source_tieback_sql` (the generated re-join back to the
live source table) reuses that SAME resolved `run_params`/`extra_filters`
scope as extra `AND s.col = 'val'` conditions, on top of its natural-key
(`src_key_cols`) join -- not the natural key alone. Without this, a
`src_key_cols` value that's only unique WITHIN one run's scope (e.g. a
`claim_id` that repeats across `batch_id` values, unique only per batch)
could tie back to the WRONG row once other batches/scopes exist in the
live source table; reproducing the same scope guarantees the generated
SQL pulls exactly the input rows the rule evaluated. See
`executor.py::build_source_tieback_sql()`'s `scope_params` parameter.

Unlike `run_params`' values (always escaped as literal data),
`extra_filters`' KEYS become literal column names spliced directly into
the SQL text -- each key is validated as a plain SQL identifier
(letters/digits/underscore, not starting with a digit) and the whole
attempt fails fast with `PARAM_SUBSTITUTION_ERROR` on anything else,
rather than risking an unescapable column name reaching the source
database. See `rules_engine/db_ops.py::build_extra_filters_clause()`'s
docstring for the full mechanics. This validation now runs ONCE per
`run_rule_group()` call (not once per rule) -- see "Performance: one
`extra_filters` validation per run, not per rule" below -- so an invalid
`extra_filters` dict fails the WHOLE run immediately, before any rule
executes, with `status="INVALID_EXTRA_FILTERS"`, instead of failing
redundantly once per rule.

Whatever actually ran -- `run_params` and `extra_filters` both already
resolved to literal values -- is captured verbatim on
`gre_results.executed_sql` for every attempt (or the raw/partially
templated `rule_syntax` when substitution itself failed), so a reviewer
never has to reconstruct it by hand from `gre_rules.rule_syntax` plus
whatever parameters happened to be passed that run.

At the RUN level (one row per `run_rule_group()` call, not per rule),
the exact `run_params`/`extra_filters` dicts the caller passed in are
also recorded, JSON-encoded, on `gre_rule_audit.run_params` /
`gre_rule_audit.extra_filters` -- `NULL` on either column when that run
didn't pass one. This is the fastest way to answer "what was `run_id X`
actually invoked with" without reading every `gre_results` row for that
run and reverse-engineering it out of `executed_sql`:

```sql
SELECT run_params, extra_filters FROM gre_rule_audit WHERE run_id = '...';
```

## Performance: one `extra_filters` validation per run, not per rule

`run_rule_group()` validates and builds the `extra_filters` SQL clause
exactly ONCE per run (immediately after loading the rule group's active
rules, before any rule executes), rather than once per rule inside
`execute_rule()`. Since `extra_filters` now applies to every rule by
default, rebuilding/re-validating the identical dict once per rule was
pure redundant work for a group of any size (e.g. 68 rebuilds for a
68-rule group) -- and meant an invalid `extra_filters` dict only
surfaced after N identical `PARAM_SUBSTITUTION_ERROR` log entries, one
per rule, instead of failing fast. The precomputed clause is threaded
down through the sequential loop and the parallel (`sequencing_mode=
'independent'`) path alike. `execute_rule()` still validates internally
when called directly (bypassing `run_rule_group()`, e.g. from a test or
a custom caller) -- the hoist is purely a fast path for the normal
`run_rule_group()` entry point and changes no behavior, only how many
times the identical validation runs.

## One connection per source: `sql_dialect`

There is no separate named-connection column. `sql_dialect` -- `'teradata'
| 'postgres' | 's3' | 'file'` -- both tells the engine how `rule_syntax` is
written AND selects the one connection this rule runs against:
`db/connection_factory.py`'s `ConnectionFactory` builds exactly ONE
connection per source_type, so a rule needs nothing more than its dialect
to pick its source (`cf.get(rule["sql_dialect"])`).

For `teradata`/`postgres`, `database_name`/`src_tbl_nm` are the schema and
table exactly as they'd appear in `database_name.src_tbl_nm`. For `file`/
`s3`, they drive the source path directly -- **the metadata table IS the
per-rule configuration, nothing else to set up**:

- `file`: `database_name` = the directory the file lives in (absolute, or
  relative to `FILE_BASE_PATH`), `src_tbl_nm` = the filename (e.g.
  `"claims.csv"`).
- `s3`: `database_name` = the `s3://` prefix/bucket, `src_tbl_nm` = the
  object key or glob under it (e.g. `"pull_date=*/*.parquet"`).

Before `rule_syntax` runs, `execute_rule()` calls `db_conn.prepare(rule)`,
which for `file`/`s3` registers a DuckDB view from those two columns --
the view name is derived from `src_tbl_nm` (see
`db/connection_factory.py::_view_name()`), and `rule_syntax` references that
same name in its `FROM` clause:

```sql
-- gre_rules row: database_name='/data/inbound', src_tbl_nm='claims.csv',
-- sql_dialect='file'
SELECT claim_id FROM claims WHERE denial_reason IS NULL AND claim_batch = '{claim_batch}'
--                   ^^^^^^ view name = _view_name('claims.csv') = 'claims'
```

## Environment-portable rule_syntax: `$env` / `$ENV`

`gre_rules.database_name`'s `$env`/`$ENV` token (see `rules_engine/config.py::resolve_database_name()`) covers the common case: one rule, one source table, named in the dedicated `database_name` column. Some rules can't fit that shape -- `rule_syntax` itself joins across more than one source database, or otherwise embeds a literal `database.table` reference beyond the one `database_name`/`src_tbl_nm` already describe. For that case, `rules_engine/config.py::resolve_env_tokens()` applies the SAME `$env`/`$ENV` token substitution directly to `rule_syntax` text -- `load_rules()` runs every loaded rule's `rule_syntax` through it, right alongside `database_name`'s own resolution, so it's automatic and unconditional (a no-op for any rule_syntax with no token at all):

```sql
-- Authored once; database_name = 'QNXT_core_$env_T' (resolved separately,
-- same token, for the engine's own auto-generated total-count/tieback SQL):
SELECT c.claim_id
FROM QNXT_core_$env_T.claims c
JOIN QNXT_ref_$env_T.codes r ON c.code_id = r.code_id
WHERE c.denial_reason IS NULL
```

resolves, in QA, to:

```sql
SELECT c.claim_id
FROM QNXT_core_qa_T.claims c
JOIN QNXT_ref_qa_T.codes r ON c.code_id = r.code_id
WHERE c.denial_reason IS NULL
```

Same case-insensitive matching, case-preserving substitution, and double-underscore collapse as `database_name`'s token (see `resolve_database_name()`'s docstring for the full mechanics) -- `resolve_env_tokens()` is the identical substitution, just applied to free-form text that can embed several database names at once instead of to one column.

**This runs BEFORE `run_params`/`extra_filters` substitution** (`rules_engine/executor.py::execute_rule()`), at rule-LOAD time rather than at execute time -- so `$env`/`$ENV` never reaches `_substitute_params()`/`build_extra_filters_clause()` at all, and can never be mistaken for a missing `run_params` key. Putting `$env`/`$ENV` in `rule_syntax` used to fail with `Unresolved parameter token(s) ['ENV'] in SQL -- no matching key in the run_params passed to this run` -- that mechanism has no idea what `$env` means, since it only fills in tokens a caller explicitly supplies via `run_params`. `resolve_env_tokens()` closes that gap by resolving `$env`/`$ENV` the same way `database_name` already does, before `rule_syntax` ever reaches `run_params` substitution.

One deliberate limitation: `resolve_env_tokens()` does **not** apply the `GRE_DB_MAP_<AUTHORED_NAME>` legacy override (`resolve_database_name()`'s mechanism 1) -- that override is keyed to one exact, whole authored `database_name` value, which has no equivalent meaning against free-form SQL text embedding several database names. A database needing that legacy, non-token exception mapping keeps using it for its own `database_name` column; `rule_syntax` only ever gets the `$env`/`$ENV` TOKEN mechanism, since that's the one that generalizes to arbitrary embedded text.

## Metadata-query logging: `log_params`

`db_ops.py::execute_query()`/`execute_dml()` take an opt-in `log_params`
flag. When `True`, the actual bound parameter values are logged (not just
a count) -- turned on at every metadata-store call site in this package:
rule discovery (`discover_rule_groups()`), rule loading (`load_rules()`),
`gre_rule_audit` start/finish, `gre_results`/`gre_rule_errors` writes
(`_write_result()`/`log_error()`), and the blanket rerun deactivation of
`gre_results`/`gre_rule_errors`/`gre_exceptions`
(`_deactivate_all_active_for_run()` -- see "Rerunning a `run_key`" above),
attempt-history lookups (`_write_exceptions()`'s existing-row query in
`rules_engine/executor.py`), and every reporting/drill-down query in
`rules_engine/reporting.py` (`get_breaches()`, `get_records_for_result()`,
`get_source_records_for_rule()`, `get_source_records_for_process()`).
This is
safe specifically because those call sites only ever bind metadata-store
values (rule_group, run_key, project_name, executed_sql text, error
messages, and the like) -- never a raw fetched source/business row value.
The one place a source connection reaches `execute_query()`/
`execute_dml()` (`_run_source_query()`, evaluating `rule_syntax` against a
source table) never passes bind params at all, so `log_params` defaults
to `False` and is simply never turned on there -- source/business data is
never logged, regardless of this flag.

**For `rule_syntax` itself, there's no separate "params" step to enable at
all.** `run_params`/`extra_filters` substitution (`_substitute_params()`/
`build_extra_filters_clause()`) is TEXT substitution, not a DB-API bind
parameter -- every `{key}`/`$key` value is already baked into the literal
SQL string BEFORE it's ever executed. So the DEBUG-level SQL text logged
by `_run_source_query()`/`execute_query()` for a rule's own scan, and for
the auto-generated total-record `COUNT(*)` query (`_build_total_query()`),
is ALREADY the fully-resolved query with every substituted value visible
inline -- there's nothing more to turn on. (That log line truncates at
500 characters, same as any other SQL text this package logs -- for the
complete, untruncated resolved SQL of one specific attempt, `gre_results.
executed_sql` has no length cap; see "Outcomes" in `GRE_Execution_Guide.md`
for that column's full behavior, including its fallback to the
raw/partially-templated `rule_syntax` when substitution itself failed.)

**Note**: `gre_exceptions`' bulk writes (`bulk_insert_or_skip()`/
`bulk_execute()`) are deliberately excluded from `log_params` -- unlike
every other write above, a `gre_exceptions` row's own values (`src_key_
value`, `element_name`, etc.) ARE real source/business data copied off a
violating record, not metadata-store bookkeeping, so those bulk writers
keep their existing "row counts only, never values" logging unconditionally.

### One log file per run

Every call to `configure_logging()` -- i.e. every invocation of this
normally short-lived, invoked-fresh-per-run CLI package -- writes to its
own, uniquely-named log file:
`<base>_<YYYYMMDD_HHMMSS_ffffff>_<pid>.log` under `GRE_LOG_DIR`. No two
runs ever share or append into the same file, so a single run's
activity is never interleaved with any other run's the way a shared,
daily-rotated file used to require timestamp-filtering to untangle --
the file for a specific run is simply the one whose name matches that
run's start time.

`<base>` defaults to `rules_engine` (`sampling` for `sampling/config.py`)
and is overridable via `GRE_LOG_FILE` -- now used as a filename PREFIX,
not a literal filename. `_base_log_name()` strips a trailing `.log`
automatically, so an old-style `GRE_LOG_FILE=rules_engine.log` setting
(the exact literal filename the previous shared-file design used) still
produces a clean `rules_engine_<timestamp>_<pid>.log` name rather than a
confusing double extension.

`GRE_LOG_RETENTION_DAYS` (default `30`) still caps how many PAST DAYS of
these per-run files are kept: `_prune_old_run_logs()` runs at the top of
every `configure_logging()` call, deleting any of this package's own
past run-log files whose OWN embedded timestamp (not filesystem mtime,
which a backup/AV scan can bump) is older than the cutoff, before this
run's fresh file is created. Set it to `0` to keep every run's log
forever -- no automatic deletion at all. This replaced the earlier
`TimedRotatingFileHandler`-based daily-rotation design, which required a
startup catch-up (`_rotate_stale_log_at_startup()`) to work around
`TimedRotatingFileHandler` computing its next rollover from "now" at
construction time -- a fresh, short-lived process starting today always
computed a rollover later that same day, which its own brief run never
lived long enough to reach, so the rollover never actually fired for
this package's normal usage pattern. Giving every run its own file from
the start removes the need for any of that: there is no shared file to
roll over, and no live-rollover-vs-startup-catch-up distinction to
reason about.

## Selecting which rules run: `rule_variant`

`rule_variant` is one additional, generic, OPT-IN level of selection on
top of `rule_group`/table. Not passing it means "don't filter on
rule_variant at all" -- every active rule in the group runs, regardless
of what each row's own `rule_variant` is set to. Passing a value narrows
to rows where `rule_variant` is NULL (universal) OR matches that value
exactly:

```python
# Every active rule in the group runs, whatever rule_variant each one has:
run_rule_group("claims_dq", "BATCH_2026_08_14", cf)

# Narrowed to universal rules PLUS any rule whose rule_variant == "2026":
run_rule_group("claims_dq", "BATCH_2026_08_14", cf, rule_variant="2026")
```

This is deliberately NOT "rule_variant not passed means only the
universal/NULL rows run" -- that stricter default used to be the
behavior, and it's a trap: a project that tags its rules with
`rule_variant` for its own bookkeeping but never intends to filter by it
would see those rules silently excluded from every ordinary run. Treat
`rule_variant` as a narrowing a caller reaches for on purpose, not an
implicit filter that can exclude rules nobody asked to exclude.

This is a single freeform column, not separate hardcoded `year_column`/
`run_type_column` fields -- a project needing more than one dimension at
once composes a single string (e.g. `"2026|MONTHLY"`), the same
"SQL/config authors are self-contained" philosophy `rule_syntax` already
follows.

## Project/process scoping: `project_name` / `process_name`

Every `gre_rules` row also carries `project_name`/`process_name` (e.g.
`HEALTHSPRING_UM` / `UNIVERSE_VALIDATION`) -- descriptive/reporting
dimensions, mirroring what `sampling/`'s `gre_sampling_config` already
carries, so both halves of this engine speak the same scoping vocabulary.
They are **not** a second filter key: `rule_group` remains the one literal
column `load_rules()` filters on. `run_rule_group()` reads them off the
loaded rules (lowest `seq_no` wins if a group's rows disagree with
themselves -- logged as a warning, same pattern as its `sequencing_mode`
consistency check) and stamps them onto `gre_rule_audit` for the run; every
`gre_exceptions`/`gre_results`/`gre_rule_errors` row carries its own rule's
`project_name`/`process_name` too (and, likewise, its own `rule_group`/
`rule_variant` -- see "Descriptive/reporting columns" below), so any of
those tables can be sliced or joined by project, process, rule_group, or
rule_variant without a round trip back to `gre_rules`.

Note the distinction: `gre_exceptions.rule_variant` etc. is the RULE's own
`rule_variant` value, not the run's requested `rule_variant` filter (which
may be `None`, meaning "no filter was applied" -- see `run_by_scope()`
below for running at any of these levels).

## Descriptive/reporting columns (rule-catalog vocabulary, audit)

Beyond the columns the engine actually reads, `gre_rules` and
`gre_exceptions` also carry a set of purely descriptive columns -- never
read by `load_rules()`/`execute_rule()`, safe to leave NULL, added so a
project's own rule-catalog and audit vocabulary (e.g. a CMS Universe rule
catalog) can live directly on these tables instead of a separate mapping
table:

**`gre_rules`**: `universe_version` (e.g. `"V22"` -- the catalog/approval
version this rule belongs to), `universe_year`, `dgr_nbr` (an external
rule/version identifier, e.g. `"CDAG1V22R4"` -- encodes the universe
version and the rule's number within it), `issue_category_name`,
`business_rule` (a business-friendly statement of what the rule checks,
distinct from `rule_syntax` itself), `rule_description`, `created_by`,
`last_updated_by`.

**`gre_exceptions`** gets three of those copied straight from the rule
row that produced each exception, the same way `element_name`/
`project_name`/`process_name` already are: `rule_nm`, `dgr_nbr`,
`universe_version`. It also gets two run-level values, copied from
`run_params` *only if the caller supplies those exact keys* for this run
(they are not reserved -- see `run_params` above -- just a courtesy
landing spot if you use them): `run_type`, `batch_schedule`. Plus
`last_updated_by`/`last_updated_datetime` for audit symmetry with `load_datetime`.

```python
summary = run_rule_group(
    "claims_dq", run_key, cf,
    run_params={"run_type": "MONTHLY", "batch_schedule": "WEEKDAYS_0600"},
)
```

A rule that never sets `dgr_nbr`/`universe_version`/etc. -- which is every
rule written before this existed -- just gets NULL there; nothing about
this is required.

## Running multiple projects/processes: `run_all_active_groups()`

`run_rule_group()` is deliberately single-`rule_group`: one call, one
checkpoint/resume scope, one `gre_rule_audit` row. Driving several
projects/processes through the engine in one operation (e.g. a nightly job
covering more than one use case) is a thin orchestration layer on top,
not a change to that contract:

```python
from rules_engine.runner import run_all_active_groups

# Every active rule_group across every project/process:
outcome = run_all_active_groups(meta_conn, meta_db, "BATCH_2026_08_14", cf)

# Narrowed to one project (or project + process):
outcome = run_all_active_groups(
    meta_conn, meta_db, "BATCH_2026_08_14", cf,
    project_name="HEALTHSPRING_UM",
)

for rule_group, summary in outcome["rule_groups"].items():
    print(rule_group, summary["status"], summary["succeeded"], summary["errored"])
```

`discover_rule_groups(meta_conn, meta_db, project_name=None, process_name=None)`
does the lookup alone (distinct, active `rule_group` values, via
`gre_rules_project_process_ix` when either filter is supplied) if you want
to inspect or reorder the group list yourself before running anything.
Each discovered group still gets its own `run_id`, `gre_rule_audit` row, and
checkpoint/resume via an unmodified `run_rule_group()` call -- this is a
fan-out, not a merged run, and one group erroring doesn't stop the rest.

`run_by_process_name(process_name, run_key, cf, meta_conn=None, meta_db=None,
project_name=None, ...)` is a thin convenience layer on top of
`run_all_active_groups()` for the "run everything this process owns" case:
it resolves `meta_conn`/`meta_db` from `cf`/`rules_engine.config` itself if
you don't pass them, and raises `ValueError` if no active `rule_group`
matches the `process_name` (and `project_name`, if given) -- almost always
a typo, so it fails loudly instead of silently doing nothing. It's kept
for backward compatibility; new code should reach for `run_by_scope()`
below, which covers this same case plus every other level of scoping.

### One entry point for every scoping level: `run_by_scope()`

`run_by_scope()` is the general-purpose dispatcher: pass whichever of
`project_name`/`process_name`/`rule_group` you have, and it runs at
exactly that level. Whatever you leave out means "every value of it," not
"nothing" -- so you get project level, process level, process+rule_group,
or one rule_group directly, all through one call, each optionally
narrowed further by `rule_variant`:

```python
from rules_engine.runner import run_by_scope

# Project level -- every active process (and rule_group) under it:
outcome = run_by_scope(run_key="BATCH_2026_08_14", cf=cf,
                        project_name="HEALTHSPRING_UM")

# Process level -- every active rule_group for this process, any project:
outcome = run_by_scope(run_key="BATCH_2026_08_14", cf=cf,
                        process_name="UNIVERSE_VALIDATION")

# Process + project together -- narrows to their intersection:
outcome = run_by_scope(run_key="BATCH_2026_08_14", cf=cf,
                        project_name="HEALTHSPRING_UM",
                        process_name="UNIVERSE_VALIDATION")

# rule_group level -- runs exactly that group, skipping discovery entirely
# (project_name/process_name, if also passed, are then only used for
# gre_rule_audit bookkeeping, since a rule_group already uniquely
# identifies which rules run):
outcome = run_by_scope(run_key="BATCH_2026_08_14", cf=cf, rule_group="ODAG3")

# Any of the above, narrowed further by rule_variant:
outcome = run_by_scope(run_key="BATCH_2026_08_14", cf=cf,
                        rule_group="ODAG3", rule_variant="EAST")

for rule_group, summary in outcome["rule_groups"].items():
    print(rule_group, summary["status"], summary["succeeded"], summary["errored"])
```

`run_by_scope()` raises `ValueError` if NONE of `project_name`/
`process_name`/`rule_group` are given (call `run_all_active_groups()`
directly for a genuinely unscoped, engine-wide run), and also raises
`ValueError` when `project_name`/`process_name` are given but match
nothing -- most likely a typo, not a legitimately empty scope.

The repo root's `run_by_process.py` wraps this in a CLI:

```
python run_by_process.py rules --project-name HEALTHSPRING_UM
python run_by_process.py rules --process-name UNIVERSE_VALIDATION
python run_by_process.py rules --rule-group ODAG3
python run_by_process.py rules --rule-group ODAG3 --rule-variant EAST
```

This package is deliberately independent of [`sampling/`](../sampling/README.md)
-- the two share no code or tables at all (see the repo root README's
"Package separation"). `rules_engine/` owns its own `db_ops.py`,
`config.py`, and the `gre_rule_audit`/`gre_rule_errors` tables. Nothing
in `rules_engine/` imports from `sampling/`, or vice versa.

## Files

| File | What |
|---|---|
| `rules.py` | `load_rules()` -- loads active `gre_rules` rows for a rule_group (every rule_variant by default; optionally narrowed to one), ordered by `seq_no`. |
| `executor.py` | `execute_rule()` -- runs one rule end-to-end: source prepare (`db_conn.prepare(rule)`), `run_params` substitution, single-scan evaluation (`_scan_violations`), threshold evaluation, `gre_exceptions`/`gre_results` writes. |
| `runner.py` | `run_rule_group()` -- orchestration entry point: readiness gate, checkpoint/resume, sequencing_mode-aware loop over `execute_rule()`, `gre_rule_audit` start/finish. `discover_rule_groups()`/`run_all_active_groups()`/`run_by_scope()` -- multi-group fan-out and scoped dispatch by project/process/rule_group (see "Running multiple projects/processes" above). Also the opt-in parallel path (`_run_pending_parallel()`) -- see "Parallel rule execution" below. |
| `parallel.py` | `ConnectionPool`/`build_pools()`/`close_pools()` -- the bounded per-connection connection pooling the parallel path uses instead of the single shared `cf.get()` connection. |
| `reporting.py` | `get_breaches()` / `get_records_for_result()` -- thin read-only queries against `gre_results`/`gre_exceptions`. `get_source_records_for_rule()` -- ties `gre_exceptions` back to the live source record (see "Tying exceptions back to source records" below). |
| `schema.sql` | `gre_rules` (incl. `project_name`/`process_name`), `gre_exceptions`, `gre_results` (one row per rule per execution attempt), `gre_rule_audit`, `gre_rule_errors`. `gre_exceptions`/`gre_results`/`gre_rule_errors` each also carry `rule_group`/`rule_variant`, copied from the rule that produced the row (see "Project/process scoping" above). Fully standalone -- no other package's `schema.sql` needs to run first. |
| `schema_drop.sql` | Drops the 5 tables above, for the drop-and-recreate redeploy policy (see the repo root README). |

## Parallel rule execution (opt-in)

`run_rule_group()` is single-threaded by default and requires zero config
-- one rule at a time, in the order `load_rules()` returns them. Setting
`GRE_MAX_PARALLEL_RULES` above its default of `1` turns on a second path,
used ONLY for `sequencing_mode='independent'` groups:

```
GRE_MAX_PARALLEL_RULES=4          # up to 4 rules executing concurrently
GRE_POSTGRES_MAX_PARALLEL=2       # but never more than 2 of those hitting postgres at once
GRE_TERADATA_MAX_PARALLEL=8       # the warehouse can take more concurrent load
```

The two settings compose: `GRE_MAX_PARALLEL_RULES` caps how many rules in
a group may run at the same time at all; `GRE_<TYPE>_MAX_PARALLEL` (same
`GRE_<TYPE>_*` naming `db/connection_factory.py` already uses for that
source_type's other settings) further caps how many of those
concurrently-running rules may simultaneously hold a session against one
*specific* source_type. Both default to `1`, so raising the group-wide
cap alone changes nothing for a source until that source's own cap is
raised too -- a source system that wasn't sized for concurrent load never
gets hit harder just because a `rule_group`'s worker count went up.

`sequencing_mode='sequential'` groups never take this path, regardless of
either setting -- their entire purpose (a guaranteed run order, plus
`on_failure=halt_group` support) is incompatible with rules finishing out
of order.

**How it stays safe to run concurrently.** The single shared connection
`cf.get(source_type)` normally hands back (reused for the whole run in
the sequential path -- see the earlier discussion in this README, or ask
the engine to explain its own connection lifecycle) is NOT safe for two
threads to query at once. So the parallel path never uses `cf.get()` for
a connection a worker will run a query on; `rules_engine/parallel.py`'s
`ConnectionPool` instead builds up to `GRE_<TYPE>_MAX_PARALLEL` genuinely
independent connections per source_type via `cf.new_connection()` --
capped by `GRE_MAX_PARALLEL_RULES` too, so a pool is never sized larger
than the group could ever use concurrently -- and hands them out through
a blocking queue, whose `get()` is what actually enforces the
per-connection cap. This applies to the metadata connection too: every worker gets its
own pooled connection for writing to `gre_exceptions`/`gre_results`,
not the single connection the top-level run uses for
`gre_rule_audit` bookkeeping. Every pooled connection is closed once the group
finishes, win or lose.

A connection that can't be pooled at all (unconfigured, or every build
attempt fails) fails every rule that needs it closed -- `ERROR` /
`CONNECTION_UNAVAILABLE`, logged the same way the sequential path already
handles a missing connection -- rather than blocking forever waiting for a
connection that will never arrive.

## Tying exceptions back to source records

A row can fail every rule in a `rule_group`. `gre_exceptions` deliberately
does **not** store the violating row's own data -- if it did, a row
failing 10 rules would get its full column set captured 10 times, once
per rule, purely because the same source data is already sitting right
there in the source table. Instead each `gre_exceptions` row keeps only
enough to re-identify it: `database_name`/`src_tbl_nm`/`source_name`
(copied from the rule at write time) plus a `src_key_value` built
from `rule['src_key_cols']` (`"col1=val1|col2=val2"`, via
`executor.py`'s `build_src_key()`/`_format_src_key()`).

`reporting.py`'s `get_source_records_for_rule(cf, meta_conn, meta_db,
rule_id, run_key)` does the tie-back the other way, lazily, at
report/analysis time -- "pull the 50 records that failed rule 1" for a
dashboard, an analyst review, or a downstream share-out:

```python
from rules_engine.reporting import get_source_records_for_rule

records = get_source_records_for_rule(cf, meta_conn, "CMSUNIV_FILELAND_DEV_T",
                                       rule_id=1, run_key="BATCH_2026_08_14")
```

It queries the current-version (`etl_is_curr_ind = 'Y'`) `gre_exceptions`
rows for that `(rule_id, run_key)`, parses each `src_key_value` back
into column/value pairs (`executor.py`'s `parse_src_key()`), and
re-joins to the LIVE source table in `EXCEPTION_CHUNK`-sized batches (the
same chunk size `bulk_insert_or_skip()` uses) via the same
`ConnectionFactory` every rule run already uses -- a single-column key
becomes `col IN (...)`, a composite key becomes an `OR` of per-record
`AND`s (no portable cross-dialect multi-column `IN`). Each returned dict
is the source row's own columns, plus this finding's context under
underscore-prefixed keys that can't collide with a real source column:
`_record_id`, `_rule_id`, `_src_key_value`, `_exception_flag`.

This is a **live** re-join, not a point-in-time snapshot: it reflects the
source table's current state, so a record corrected or deleted upstream
since the rule ran may come back with updated data, or not come back at
all (logged at `INFO`, not raised, since that's expected drift rather
than an error). That trade-off is accepted in exchange for never
duplicating source data into `gre_exceptions` in the first place. A
one-row-fails-N-rules scenario still produces N independent
`gre_exceptions` rows (one per rule, required for per-rule reporting) but
each is a single small row, and each ties back independently to the
*same* underlying source record rather than N copies of it.

## Big-dataset path / memory footprint at scale

Optimizations that matter once a rule matches millions of rows (see
`executor.py`'s module docstring for the full write-up):

- **Single-scan evaluation** (`_scan_violations`): `rule_syntax` is scanned
  ONCE via streamed `fetchmany()` (`GRE_EXCEPTION_CHUNK`-sized batches,
  default 500), producing both the true failed-record count and the
  complete violating-row list for detail capture -- instead of a separate
  `COUNT(*)` query plus a full detail fetch. This roughly halves read load
  against the source for every rule.
- **Natural-key projection** (`_scan_violations`'s `src_key_cols`
  parameter): a violating row is retained ONLY as its `src_key_cols`
  columns, not every column `rule_syntax` happened to `SELECT` -- safe
  because `_write_exceptions()` (the only consumer of these rows) never
  reads anything else off a row. For a rule that selects many/wide columns
  (or `SELECT *`) but keys on just one or two of them, this is the single
  biggest lever on retained memory for a rule with a large violation
  count: it turns memory use from O(violations &times; every selected
  column) into O(violations &times; key columns only). It does NOT reduce
  what's fetched *over the wire* per batch -- the driver still returns
  every selected column for the `GRE_EXCEPTION_CHUNK` rows in flight at
  any one moment -- only what's kept afterward.
- **Memoized total counts, failures included** (`_compute_total`'s
  `total_cache`): rules in the same group that ask the identical "how many
  rows are in this batch" question share one `COUNT(*)` result for the
  whole `run_rule_group()` call, threaded through via `total_cache`. This
  is memoized whether the query SUCCEEDS or FAILS -- e.g. a `run_params`
  key that doesn't name a real column (see "text_params" above) makes
  this query fail identically for every rule sharing that table +
  run_params. Without caching the failure, each of those rules would
  independently re-issue the identical, already-known-to-fail query
  against the source database -- one full network round trip per rule,
  all doomed to the same outcome -- before falling back to a
  run_params-less denominator. The first failure is cached (as a
  `_CachedTotalFailure` sentinel wrapping the exception); every
  subsequent rule with the identical `(sql_dialect, query text)` key
  re-raises the SAME cached exception immediately, with no additional
  round trip, and the existing run_params-less fallback proceeds exactly
  as before. Purely a within-run performance fix -- it changes nothing
  about WHICH rules fall back or what they fall back to, only how many
  times the source database is asked the same already-answered question.
- **Chunked bulk writes**: both `_scan_violations`'s reads and
  `_write_exceptions`'s writes go through `GRE_EXCEPTION_CHUNK`-sized
  batches (`bulk_insert_or_skip`/`bulk_execute`), never one row per
  round trip.

There is deliberately still NO cap on how many rows get captured: every
violating row gets a `gre_exceptions` row, every attempt, no matter how
many rows `rule_syntax` matches -- compliance/audit review needs the
complete record set, not a sample. The trade-off this makes explicit:
**memory for one rule's one attempt scales with that rule's violation
count** (at the reduced, key-columns-only width above) -- a rule matching
an extremely large number of rows needs correspondingly more memory to
hold all of them before the bulk write. In practice this is now bounded by
`violations &times; (a handful of key column values)`, not
`violations &times; (every column the rule selects)`.

**What this means for planning capacity, concretely:**

- **A single rule, however large the source table, uses roughly constant
  memory** as long as its *violation count* (not table size) stays
  bounded -- a well-behaved rule on a healthy dataset should rarely
  violate on more than a small fraction of rows. A rule matching, say, 5
  million rows on a 2-column key needs on the order of a few hundred MB,
  not the width of the whole table.
- **`GRE_MAX_PARALLEL_RULES` multiplies this.** Raising it above 1 runs
  that many rules concurrently (`sequencing_mode='independent'` groups
  only), each with its own independent `_scan_violations()` call in
  flight at the same time -- worst-case peak memory is roughly
  `GRE_MAX_PARALLEL_RULES &times; (the single largest concurrently-running
  rule's projected violation set)`, not just one rule's. Size this against
  the server's available RAM, not just against source-connection
  concurrency caps (`GRE_<SOURCE_TYPE>_MAX_PARALLEL`) -- those bound load
  on the *source*, not memory on *this* process.
- **`GRE_EXCEPTION_CHUNK`** (default 500) bounds the transient per-batch
  full-row buffer during the scan and the batch size of every bulk write.
  Raising it trades a few more rows in flight for fewer round trips;
  lowering it trades the reverse. It rarely needs to change -- the
  natural-key projection above already does the heavy lifting for wide
  tables.
- **Narrow `rule_syntax`'s `SELECT` list.** Even with projection, the
  driver still buffers every selected column for each in-flight
  `GRE_EXCEPTION_CHUNK`-sized batch -- a `SELECT *` on a wide table still
  costs more per batch than `SELECT <just the columns the rule and
  src_key_cols actually need>`, even though the retained memory afterward
  is identical either way.
- **Index what `rule_syntax`'s `WHERE` filters on and what `src_key_cols`
  names**, same as any large-table query -- this engine pushes all
  filtering/counting down to the source database; it never scans in
  Python. The total-record count (`_build_total_query`) and the violation
  scan both benefit from the same indexes a hand-written query against
  that table would.

## Quick start

```python
from db.connection_factory import ConnectionFactory
from rules_engine.runner import run_rule_group

cf = ConnectionFactory()
cf.load()

# One rule_group:
summary = run_rule_group("claims_dq", "BATCH_2026_08_14", cf)
print(summary["status"], summary["succeeded"], summary["errored"])

# Every active rule_group for one project (see "Running multiple
# projects/processes" above):
# from rules_engine.runner import run_all_active_groups
# meta_conn = cf.get("teradata")  # or gre_config.get_meta_connection_name()
# outcome = run_all_active_groups(meta_conn, "CMSUNIV_FILELAND_DEV_T",
#                                  "BATCH_2026_08_14", cf, project_name="HEALTHSPRING_UM")
```

See the repo root README for environment setup (`dev.env`, one connection per source_type, etc.), and its "Running the engines end to end" section for a full worked example wiring up `ConnectionFactory` and calling into both packages.
