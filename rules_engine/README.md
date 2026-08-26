# rules_engine/ -- Generic Rules Engine (GRE)

A generic, config-driven rule evaluation engine: `gre_rules` rows define a
negative SQL SELECT per rule (the query returns the rows that VIOLATE the
rule); the engine runs each active rule for a `rule_group` (optionally
narrowed further by `rule_variant` -- see below) against a `run_key`,
writes every violating row to `gre_exceptions`, evaluates a threshold, and
upserts a `gre_results` verdict row.

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

## Ad-hoc runtime filters: `extra_filters`

`run_params` above is for tokens a rule's author explicitly wrote into
`rule_syntax`. `extra_filters` is a separate, opt-in mechanism for a
column the rule *wasn't* authored to anticipate -- e.g. narrowing a
long-standing rule to `run_ty = 'MNT'` at run time without adding a
`{run_ty}` token to every rule that might ever need it, and without
touching `gre_rules` at all.

A rule opts in by embedding the literal marker `"{extra_filters}"` or
`"$extra_filters"` anywhere in its `rule_syntax` -- typically right
before the end of the `WHERE` clause:

```sql
SELECT claim_id, denial_reason FROM claims
WHERE denial_reason IS NULL AND claim_year = {year} AND claim_month = {month} {extra_filters}
```

A caller then passes one or more filters as a plain dict, and the engine
splices in `AND col1 = 'v1' AND col2 = 'v2' ...` wherever the marker
appears, BEFORE `run_params` substitution runs:

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

`extra_filters` supports more than one filter at once (`{"run_ty": "MNT",
"region": "EAST"}` -> `AND region = 'EAST' AND run_ty = 'MNT'`, sorted for
deterministic SQL text). A rule that never embeds the marker is
completely unaffected even when a caller passes `extra_filters` -- same
"extra values are silently unused" philosophy as an unused `run_params`
key. `extra_filters` is also merged into the auto-generated total-record
(denominator) count above, but ONLY for a rule that actually embeds the
marker, so `failure_pct` still reflects the same narrowed scope the scan
itself used.

Unlike `run_params`' values (always escaped as literal data),
`extra_filters`' KEYS become literal column names spliced directly into
the SQL text -- each key is validated as a plain SQL identifier
(letters/digits/underscore, not starting with a digit) and the whole
attempt fails fast with `PARAM_SUBSTITUTION_ERROR` on anything else,
rather than risking an unescapable column name reaching the source
database. See `rules_engine/db_ops.py::build_extra_filters_clause()`'s
docstring for the full mechanics.

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
`gre_exceptions`/`gre_results` row carries its own rule's
`project_name`/`process_name` too, so any of those tables can be sliced or
joined by project without a round trip back to `gre_rules`.

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
`run_all_active_groups()` for the common "run everything this process
owns" case: it resolves `meta_conn`/`meta_db` from `cf`/`rules_engine.config`
itself if you don't pass them, and raises `ValueError` if no active
`rule_group` matches the `process_name` (and `project_name`, if given) --
almost always a typo, so it fails loudly instead of silently doing
nothing:

```python
from rules_engine.runner import run_by_process_name

outcome = run_by_process_name("UNIVERSE_VALIDATION", "BATCH_2026_08_14", cf)
for rule_group, summary in outcome["rule_groups"].items():
    print(rule_group, summary["status"])
```

The repo root's `run_by_process.py` wraps this in a CLI: `python
run_by_process.py rules --process-name UNIVERSE_VALIDATION`.

This package is deliberately independent of [`sampling/`](../sampling/README.md)
-- the two share no code or tables at all (see the repo root README's
"Package separation"). `rules_engine/` owns its own `db_ops.py`,
`config.py`, and the `gre_rule_audit`/`gre_rule_errors` tables. Nothing
in `rules_engine/` imports from `sampling/`, or vice versa.

## Files

| File | What |
|---|---|
| `rules.py` | `load_rules()` -- loads active `gre_rules` rows for a rule_group (+ optional `rule_variant` filter), ordered by `seq_no`. |
| `executor.py` | `execute_rule()` -- runs one rule end-to-end: source prepare (`db_conn.prepare(rule)`), `run_params` substitution, single-scan evaluation (`_scan_violations`), threshold evaluation, `gre_exceptions`/`gre_results` writes. |
| `runner.py` | `run_rule_group()` -- orchestration entry point: readiness gate, checkpoint/resume, sequencing_mode-aware loop over `execute_rule()`, `gre_rule_audit` start/finish. `discover_rule_groups()`/`run_all_active_groups()` -- multi-group fan-out by project/process (see "Running multiple projects/processes" above). Also the opt-in parallel path (`_run_pending_parallel()`) -- see "Parallel rule execution" below. |
| `parallel.py` | `ConnectionPool`/`build_pools()`/`close_pools()` -- the bounded per-connection connection pooling the parallel path uses instead of the single shared `cf.get()` connection. |
| `reporting.py` | `get_breaches()` / `get_records_for_result()` -- thin read-only queries against `gre_results`/`gre_exceptions`. `get_source_records_for_rule()` -- ties `gre_exceptions` back to the live source record (see "Tying exceptions back to source records" below). |
| `schema.sql` | `gre_rules` (incl. `project_name`/`process_name`), `gre_exceptions`, `gre_results` (one row per rule per execution attempt), `gre_rule_audit`, `gre_rule_errors`. Fully standalone -- no other package's `schema.sql` needs to run first. |
| `schema_drop.sql` | Drops the 6 tables above, for the drop-and-recreate redeploy policy (see the repo root README). |

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
`_record_id`, `_rule_id`, `_src_key_value`, `_issue_desc`,
`_exception_flag`.

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
- **Memoized total counts** (`_compute_total`'s `total_cache`): rules in
  the same group that ask the identical "how many rows are in this batch"
  question share one `COUNT(*)` result for the whole `run_rule_group()`
  call, threaded through via `total_cache`.
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
