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
defined. The two share only what's in
[`shared/`](../shared/README.md) (DB helpers, credential/config loading,
and the `gre_audit`/`gre_errors` tables).

## Files

| File | What |
|---|---|
| `sampling.py` | `run_sampling()` -- the whole pipeline: load config, pull candidates (DB-side `ROW_NUMBER()` for `_priority_rank`), recursively stratify (`_stratify`), select per bucket (`_select`), shortfall top-up, persist, audit. |
| `schema.sql` | `gre_sampling_config`, `gre_sampling_strata`, `gre_sampling_mix`, `gre_sample_selections`, `gre_sample_selection_attrs`. Deploy after `shared/schema.sql`. |
| `schema_drop.sql` | Drops the 5 tables above, for the drop-and-recreate redeploy policy (see the repo root README). |
| `seed/um_sample.sql` | The original COMO weekly UM sample (`dq_sampling_config.config_id=1`) re-expressed in this schema -- also the regression fixture `tests/test_sampling.py` checks against. |

## Scoping the candidate pull: `run_params`

`scope_sql` and `exclusion_sql` may both embed any number of `"{key}"`
tokens (not just `{batch_id}`) -- each run passes a `run_params` dict, and
every matching token is substituted (quoted, escaped) before the pull
query runs, the SAME mechanism and dict shape `rules_engine/` uses for
`rule_sql`/`scope_sql`. `batch_id` is always present in `run_params`:

```python
result = run_sampling(
    config_id=1, batch_id="2026-08-14", cf=cf,
    run_params={"region": "NORTHEAST"},
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
  -> gre_audit row (run_type='SAMPLING')
```

Adding a third (or tenth) stratification level is one more
`gre_sampling_strata` row plus its `gre_sampling_mix` rows -- zero code
changes, since `_stratify()` recurses without knowing how many levels
deep it is.

## Reproducibility

For RANDOM/SYSTEMATIC, ONE seed is generated (or supplied) per
`sample_run_id` and persisted on the `gre_audit` row. Buckets at every
level are always processed in deterministic (sorted) order using one
seeded `random.Random` instance threaded through the whole recursion, so
re-running with the same seed reproduces every bucket's draw exactly.

## Quick start

```python
from db.connection_factory import ConnectionFactory
from sampling.sampling import run_sampling

cf = ConnectionFactory()
cf.load()

result = run_sampling(config_id=1, batch_id="2026-08-14", cf=cf)
print(result["status"], result["selected"], "/", result["target_volume"])
```

See the repo root README for environment setup (`dev.env`, one connection per source_type, etc.).
