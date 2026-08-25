# sampling/ -- Generic Stratified Sampling

A generic, config-driven stratified sampling engine: given a candidate
universe (`universe_table`), an exclusion filter, any number of
stratification levels (`gre_sampling_strata`/`gre_sampling_mix`), and a
selection method (RANKED / RANDOM / SYSTEMATIC), pick a `target_volume`
sample and persist every candidate considered -- selected or not -- for
audit defensibility.

This generalizes the older `core/stratified_sampling.py` shape (filter ->
bucket -> quota -> select -> persist), which hardcoded exactly two
stratification levels and one selection method, to: any number of levels
and three methods, entirely from config.

This package is deliberately independent of
[`rules_engine/`](../rules_engine/README.md) -- rules ask "is this row
valid?" (pass/fail against a whole universe); sampling asks "which N
cases are the highest-value ones for a reviewer this cycle?" (a
ranking/quota problem). It can run standalone with zero `gre_rules` rows
defined. The two share no code or tables at all (see the repo root
README's "Package separation"). `sampling/` owns its own `db_ops.py`,
`config.py`, and the `gre_sampling_audit`/`gre_sampling_errors` tables.

## Files

| File | What |
|---|---|
| `sampling.py` | `run_sampling()` -- the whole pipeline: load config, pull candidates (DB-side `ROW_NUMBER()` for `_priority_rank`), recursively stratify (`_stratify`), select per bucket (`_select`), shortfall top-up, persist, audit. |
| `schema.sql` | `gre_sampling_config`, `gre_sampling_strata`, `gre_sampling_mix`, `gre_sample_selections`, `gre_sample_selection_attrs`, `gre_sampling_audit`, `gre_sampling_errors`. Fully standalone -- no other package's `schema.sql` needs to run first. |
| `schema_drop.sql` | Drops the 7 tables above, for the drop-and-recreate redeploy policy (see the repo root README). |
| `seed/um_sample.sql` | The original COMO weekly UM sample (`dq_sampling_config.config_id=1`) re-expressed in this schema -- also the regression fixture `tests/test_sampling.py` checks against. |

## Tracking a run: `run_key`

`run_key` is an opaque, caller-supplied string -- the ONE tracking
identifier `gre_sampling_audit` and `gre_sampling_errors` key off, and it's one of the parts
`sample_run_id` is built from (see "Identifying an attempt: `sample_run_id`"
below). There's no fixed shape to it: a plain batch id, a year+month combo,
a specific date, a region, or any combination all work equally well.
`sampling/db_ops.py`'s `build_run_key(*parts, delimiter="_")` is a
convenience formatter (`build_run_key(2026, 8) -> "2026_8"`), or just pass
your own string.

## Identifying an attempt: `sample_run_id`

`run_key` (above) says WHICH logical run this is; `sample_run_id` says
WHICH SPECIFIC ATTEMPT at it this is -- `run_sampling()` mints a brand new
`sample_run_id` every call, even for a repeated `run_key`. It's built on
`sampling/db_ops.py::generate_run_id()` (an identical copy of the helper
`rules_engine/runner.py` uses for its own `run_id` -- see that package's
README, "Identifying an attempt: `run_id`", for the full shape/rationale,
which this mirrors exactly), from the config's `project_name` and
`sample_name`, the `run_key`, an attempt count, and `triggered_by`:

```
{project_name}.{sample_name}::{run_key}::attempt-{N}::{triggered_by}::{YYYYMMDDTHHMMSS.ffffff}::{6 hex chars}

e.g. HEALTHSPRING_UM.weekly_review_sample::2026-08-01::attempt-2::jsmith::20260819T143022.183045::f9e8d7
```

(`project_name` is omitted, along with its trailing `.`, when the config has
no `project_name` set.) `N` = `sampling/db_ops.py::count_prior_attempts()` + 1,
counted from `gre_sampling_audit` for this exact `(config_id, run_key)` pair -- makes
a rerun visibly "attempt 2" without comparing two ids' timestamps.
`triggered_by` defaults to `"SYSTEM"` and is also recorded on `gre_sampling_audit`;
pass your own value the same way `rules_engine`'s `triggered_by` works:

```python
result = run_sampling(config_id=1, run_key="2026-08-14", cf=cf, triggered_by="jsmith")
```

Microsecond precision plus a short random suffix means two calls can never
collide onto the same id -- important here specifically, since
`_deactivate_prior_sampling_runs()` finds a config's PRIOR `sample_run_id`s
via `gre_sampling_audit` by excluding the current one (`run_id <> current`); a
collision would make an attempt indistinguishable from its own predecessor.
(The attempt-N label above is a readability convenience only -- this
collision-safety guarantee comes entirely from the trailing hex suffix, not
from N.)

## Scoping the candidate pull: `run_params`

`scope_sql` and `exclusion_sql` may both embed any number of `"{key}"`
tokens -- each run passes a `run_params` dict, and every matching token is
substituted (quoted, escaped) before the pull query runs, the SAME
mechanism and dict shape `rules_engine/` uses for `rule_syntax`/`scope_sql`.
`run_params` is completely free-form -- there is no reserved/required
key, and `run_key` is deliberately NOT auto-merged into it, since it's
often not a real column on the universe table (e.g. a composite like
`"2026_8"`). If `scope_sql`/`exclusion_sql` needs the run's tracking value
as a literal column filter, pass it explicitly via `run_params` under
whatever key matches an actual column:

```python
result = run_sampling(
    config_id=1, run_key="2026-08-14", cf=cf,
    run_params={"pull_date": "2026-08-14", "region": "NORTHEAST"},
)
```

`scope_sql` defaults to `'1=1'` (whole table) if unset. An unresolved
`"{token}"` in either `scope_sql` or `exclusion_sql` fails the pull with a
clear `ValueError` before it reaches the source database, logged as
`PULL_FAILURE`.

## Algorithm

```
pull candidates (DB-side ROW_NUMBER() for RANKED/SYSTEMATIC priority)
  -> _stratify(candidates, levels, target_volume)
       bucket by level's stratify_expr
       compute each bucket's target via gre_sampling_mix
         (a bucket_value absent from the mix absorbs the remainder fraction)
       recurse into the next level with that smaller target
     -> at the bottom (or zero levels configured): _select()
          RANKED     : first N of the priority-ordered pull
          RANDOM     : seeded shuffle, first N
          SYSTEMATIC : seeded random start offset, then every Kth candidate
  -> shortfall top-up from the remaining pool if quotas under-filled
  -> persist every candidate considered (gre_sample_selections / _attrs)
  -> gre_sampling_audit row
```

Adding a third (or tenth) stratification level is one more
`gre_sampling_strata` row plus its `gre_sampling_mix` rows -- zero code
changes, since `_stratify()` recurses without knowing how many levels
deep it is.

## Reproducibility

For RANDOM/SYSTEMATIC, ONE seed is generated (or supplied) per
`sample_run_id` and persisted on the `gre_sampling_audit` row. Buckets at every
level are always processed in deterministic (sorted) order using one
seeded `random.Random` instance threaded through the whole recursion, so
re-running with the same seed reproduces every bucket's draw exactly.

## Quick start

```python
from db.connection_factory import ConnectionFactory
from sampling.sampling import run_sampling

cf = ConnectionFactory()
cf.load()

result = run_sampling(config_id=1, run_key="2026-08-14", cf=cf)
print(result["status"], result["selected"], "/", result["target_volume"])
```

## Running everything one process owns: `run_sampling_for_process_name()`

Mirrors `rules_engine/runner.py`'s `run_by_process_name()`: discovers
every active `gre_sampling_config` scoped to one `process_name`
(optionally narrowed further by `project_name`) and runs each against the
same `run_key`/`run_params`/`seed`, resolving `meta_conn`/`meta_db` from
`cf`/`sampling.config` itself if you don't pass them:

```python
from sampling.sampling import run_sampling_for_process_name

outcome = run_sampling_for_process_name("WEEKLY_REVIEW_SAMPLE", "2026-08-14", cf)
for config_id, summary in outcome["sampling_configs"].items():
    print(config_id, summary["status"], summary["selected"], "/", summary["target_volume"])
```

Raises `ValueError` if no active config matches the `process_name` (and
`project_name`, if given) -- almost always a typo, so it fails loudly
instead of silently doing nothing. `discover_sampling_configs(meta_conn,
meta_db, project_name=None, process_name=None)` does the lookup alone if
you want the matching `config_id`s without running them. The repo root's
`run_by_process.py` wraps this in a CLI: `python run_by_process.py
sampling --process-name WEEKLY_REVIEW_SAMPLE`.

See the repo root README for environment setup (`dev.env`, one connection per source_type, etc.).
