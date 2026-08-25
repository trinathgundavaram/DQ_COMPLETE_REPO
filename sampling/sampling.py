"""
sampling/sampling.py
----------------------
Generic, config-driven stratified sampling -- a SEPARATE concern from rule
evaluation (see rules_engine/). Rules ask "is this row valid?" (pass/fail
against a whole universe); sampling asks "which N cases, out of a clean
universe, are the highest-value ones for a human reviewer this cycle?" A
ranking/quota problem, not a pass/fail problem -- its own package, own
tables, and it can run standalone with zero gre_rules rows defined for a
project.

This generalizes core/stratified_sampling.py's proven shape (filter ->
bucket-by-config -> quota -> select -> persist) rather than copying it:
that module hardcodes exactly two stratification levels
(determination_column, functional_area_column) and one selection method
(ranked top-N). Here, both are config:

  1. WHAT GETS FILTERED OUT -> config['exclusion_sql']    (unchanged shape)
  2. WHAT GETS SAMPLED      -> gre_sampling_strata / gre_sampling_mix
                                (any number of levels, not a fixed two)
  3. HOW IT GETS SAMPLED    -> config['sampling_method']
                                (RANKED / RANDOM / SYSTEMATIC, applied
                                uniformly at every leaf bucket)

Built on this package's own db_ops.py + config.py
-----------------------------------------------------
The low-level DB helpers (execute_query/execute_dml/bulk_insert,
_run_source_query, _substitute_params, build_run_key) and the
gre_sampling_errors writer (log_error) come from sampling/db_ops.py --
this package's own copy of what used to be shared/db_ops.py, before
rules_engine/ and sampling/ were split into fully independent packages
with zero shared code (see README.md's "Package separation"; the two
copies -- sampling/db_ops.py and rules_engine/db_ops.py -- are identical
apart from their gre_sampling_errors/gre_rule_errors split). Metadata
connection/db resolution comes from sampling/config.py, this package's
own copy of what used to be shared/config.py.

scope_sql AND exclusion_sql both go through the same {key} run_params
substitution as rules_engine's rule_syntax -- see
sampling/db_ops.py::_substitute_params()'s docstring for why this is a
free-form dict rather than a single fixed value. Unlike rules_engine (which auto-
derives its total-record count straight from run_params against
database_name.src_tbl_nm -- see rules_engine/executor.py::
_build_total_query()), sampling's candidate universe isn't always a plain
equality filter (exclusions, priority ordering, ...), so scope_sql/
exclusion_sql stay as explicit, hand-authored WHERE-fragments here.

Algorithm
---------
pull candidates -> _stratify(candidates, levels, target_volume) recurses
one level at a time: bucket by that level's stratify_expr, compute each
bucket's target via gre_sampling_mix (a bucket_value absent from the mix
absorbs the "remainder" fraction -- see _target_for_bucket, identical rule
to the proven dq_* pattern), recurse into the next level with that smaller
target. Zero levels (or the bottom of the recursion) -> _select(), which
implements RANKED/RANDOM/SYSTEMATIC. Two levels reproduces the existing
dq_* UM behavior exactly (see tests/test_sampling.py's frozen regression
fixture, and sampling/seed/um_sample.sql for the config_id=1 sample
re-expressed in this shape).

Reproducibility for RANDOM/SYSTEMATIC
--------------------------------------
ONE random seed is generated (or supplied) per sample_run_id and stored on
the gre_sampling_audit summary row. Buckets at every level are always processed in
a fixed, deterministic order (bucket values sorted ascending) using a
single random.Random(seed) instance threaded through the whole recursion,
so re-running _stratify with the same seed reproduces every bucket's
offset/draw exactly, in the same order they were originally consumed.
SYSTEMATIC's interval (floor(bucket_size / n)) is NOT random -- it's
arithmetic on the data itself -- so it doesn't need separate persistence;
only the seed does. This was an explicit design choice (see the project's
decision log) over persisting interval/offset per bucket in a companion
table -- simpler schema, at the cost of "what were bucket B's exact draw
parameters" requiring a replay rather than a single row lookup.
"""

import logging
import math
import random
from datetime import datetime

from sampling.db_ops import (
    execute_query, execute_dml, bulk_insert, log_error,
    _run_source_query, _substitute_params, generate_run_id, count_prior_attempts,
)

# gre_sampling_audit -- sampling's OWN run-tracking table (sampling/
# schema.sql). Written to via _write_audit() below; rules_engine/ never
# reads or writes it (its equivalent is rules_engine/runner.py::
# _start_audit()/_finish_audit() against gre_rule_audit, defined in
# rules_engine/schema.sql) -- the two packages share no tables at all, see
# README.md's "Package separation".

logger = logging.getLogger(__name__)

VALID_METHODS = {"RANKED", "RANDOM", "SYSTEMATIC"}
_METHODS_REQUIRING_PRIORITY = {"RANKED", "SYSTEMATIC"}


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_sampling_config(meta_conn, meta_db: str, config_id) -> tuple:
    """
    Load one gre_sampling_config row plus its ordered gre_sampling_strata
    levels, each with its gre_sampling_mix rows folded into a plain dict.

    Returns (config: dict, levels: list[dict]). Each level dict:
        {strata_id, level_order, level_name, stratify_expr,
         mix: {bucket_value: target_fraction}}

    Zero strata rows for this config -> levels == [] (no stratification;
    _stratify() falls straight to _select() on the whole pool).
    """
    rows = execute_query(
        meta_conn,
        f"SELECT * FROM {meta_db}.gre_sampling_config WHERE config_id = ? AND act_ind = 1",
        [config_id],
    )
    if not rows:
        raise ValueError(f"No active gre_sampling_config row for config_id={config_id}.")
    config = rows[0]

    strata_rows = execute_query(
        meta_conn,
        f"SELECT * FROM {meta_db}.gre_sampling_strata WHERE config_id = ? ORDER BY level_order ASC",
        [config_id],
    )

    levels = []
    for s in strata_rows:
        mix_rows = execute_query(
            meta_conn,
            f"SELECT bucket_value, target_fraction FROM {meta_db}.gre_sampling_mix WHERE strata_id = ?",
            [s["strata_id"]],
        )
        levels.append({
            "strata_id": s["strata_id"],
            "level_order": s["level_order"],
            "level_name": s.get("level_name"),
            "stratify_expr": s["stratify_expr"],
            "mix": {m["bucket_value"]: float(m["target_fraction"]) for m in mix_rows},
        })

    return config, levels


# ---------------------------------------------------------------------------
# Rounding
# ---------------------------------------------------------------------------

def _round_target(value: float, rounding_mode: str) -> int:
    mode = (rounding_mode or "FLOOR").upper()
    if mode == "CEIL":
        return max(0, math.ceil(value))
    if mode == "ROUND":
        return max(0, round(value))
    return max(0, math.floor(value))  # FLOOR (default)


def _target_for_bucket(bucket_value: str, mix: dict, total: int, rounding_mode: str,
                        unmixed_count: int = 1) -> int:
    """
    Same rule as core/stratified_sampling.py::_target_for_bucket, generalized
    to a config-driven rounding_mode instead of hardcoded floor(): a
    bucket_value present in `mix` gets that fraction of `total`; any
    bucket_value NOT in `mix` SHARES the remainder fraction
    (1 - sum(named fractions)) among however many distinct unmixed values
    are actually present at this level (`unmixed_count`), bounded in
    practice by the final target_volume truncation in run_sampling().

    `unmixed_count` defaults to 1 -- the single-unnamed-bucket case this
    was originally written for, and what a direct call (e.g. from a unit
    test) gets if it doesn't know how many sibling unmixed buckets exist.
    _stratify() below is the one caller that knows the real count (from
    the level's actual bucketed data) and passes it explicitly -- giving
    the FULL remainder to every unmixed bucket independently, instead of
    splitting it, over-allocates quota whenever a level has more than one
    bucket_value absent from gre_sampling_mix (e.g. an "Approved"/"Denied"
    mix with three OTHER statuses present in the data would previously
    give each of those three the whole remainder instead of a third of
    it).
    """
    if not mix:
        return total
    if bucket_value in mix:
        return _round_target(total * mix[bucket_value], rounding_mode)
    named_fraction = sum(mix.values())
    remainder_fraction = max(0.0, 1.0 - named_fraction) / max(1, unmixed_count)
    return _round_target(total * remainder_fraction, rounding_mode)


# ---------------------------------------------------------------------------
# Selection methods (question 3 -- HOW it gets sampled)
# ---------------------------------------------------------------------------

def _select(candidates: list, n: int, method: str, rng: random.Random) -> list:
    """
    Apply `method` to pick up to n candidates from this bucket.

    RANKED     : candidates already arrive priority-ordered (the pull query
                 applies ORDER BY priority_rank_sql) -- take the first n.
    RANDOM     : uniform shuffle (using the run's seeded rng, so it's
                 reproducible), take the first n of the shuffled order.
    SYSTEMATIC : candidates already priority- (or key-) ordered; interval
                 k = floor(bucket_size / n), random start offset in [1, k]
                 drawn from the run's seeded rng, take every kth candidate
                 from there.
    """
    if n <= 0 or not candidates:
        return []

    method = (method or "RANKED").upper()

    if method == "RANKED":
        return candidates[:n]

    if method == "RANDOM":
        pool = list(candidates)
        rng.shuffle(pool)
        return pool[:n]

    if method == "SYSTEMATIC":
        pool = list(candidates)
        size = len(pool)
        if n >= size:
            return pool
        k = max(1, size // n)
        offset = rng.randint(1, k)   # 1-indexed, matches the prompt's [1, k]
        return pool[offset - 1::k][:n]

    raise ValueError(f"Unknown sampling_method '{method}'. Must be one of {sorted(VALID_METHODS)}.")


# ---------------------------------------------------------------------------
# Recursive stratification (question 2 -- WHAT gets sampled)
# ---------------------------------------------------------------------------

def _bucket_key(strata_id) -> str:
    return f"_strata_{strata_id}"


def _stratify(candidates: list, levels: list, target: int, method: str, rounding_mode: str,
              rng: random.Random, level_index: int = 0, by_stratum: dict = None, path: str = "") -> list:
    """
    Recurse one stratification level at a time. This ONE function is what
    makes the level count configurable instead of fixed at two -- adding a
    third (or tenth) level is one more gre_sampling_strata row, never a
    code change, because this function doesn't know or care how many
    levels deep it is.

    Bottom of recursion (level_index >= len(levels), i.e. no more levels
    configured, OR no candidates left in this bucket) -> _select() on
    whatever's here. Zero levels configured for the whole config means
    level_index starts at 0 >= len([]) == 0, so it falls straight to
    _select() on the entire candidate pool -- no special-casing needed.
    """
    if by_stratum is None:
        by_stratum = {}

    if level_index >= len(levels) or not candidates:
        chosen = _select(candidates, target, method, rng)
        by_stratum[path or "ALL"] = {"candidates": len(candidates), "selected": len(chosen)}
        return chosen

    level = levels[level_index]
    key = _bucket_key(level["strata_id"])
    mix = level["mix"]

    buckets: dict = {}
    for row in candidates:
        bval = str(row.get(key))
        buckets.setdefault(bval, []).append(row)

    # How many of this level's ACTUAL bucket values are absent from
    # gre_sampling_mix -- computed once per level (not per bucket) so
    # _target_for_bucket can split the remainder fraction evenly across
    # all of them, instead of giving each one the full remainder
    # independently. See _target_for_bucket()'s docstring.
    unmixed_count = sum(1 for bval in buckets if bval not in mix)

    selected = []
    # Deterministic bucket order (sorted) -- required so a seeded rng
    # produces the exact same sequence of draws on every replay.
    for bval in sorted(buckets.keys()):
        brows = buckets[bval]
        btarget = _target_for_bucket(bval, mix, target, rounding_mode, unmixed_count)
        sub_path = f"{path}/{bval}" if path else bval
        selected.extend(_stratify(brows, levels, btarget, method, rounding_mode,
                                  rng, level_index + 1, by_stratum, sub_path))

    return selected


# ---------------------------------------------------------------------------
# Candidate pull
# ---------------------------------------------------------------------------

def _key_columns(config: dict) -> list:
    """gre_sampling_config.key_columns is a comma-separated string; one place to split/trim it."""
    return [c.strip() for c in config["key_columns"].split(",") if c.strip()]


def _pull_candidates(db_conn, config: dict, levels: list, run_params: dict) -> list:
    """
    Pull the candidate universe, with _priority_rank computed by the
    DATABASE (ROW_NUMBER() OVER (ORDER BY priority_rank_sql)) rather than a
    Python loop over the fetched rows. For a candidate pool in the
    millions this pushes an O(n) pass down to the source engine -- which
    is already doing the ORDER BY for the SELECT itself -- instead of a
    second O(n) pass in this process after the full fetch.

    Both scope_sql and exclusion_sql go through _substitute_params() with
    the SAME run_params dict -- previously exclusion_sql got no
    substitution at all, silently unable to reference {run_key} or any
    other run param even though scope_sql could.
    """
    key_cols = _key_columns(config)
    strata_select = [f"{lvl['stratify_expr']} AS {_bucket_key(lvl['strata_id'])}" for lvl in levels]

    scope = (config.get("scope_sql") or "").strip() or "1=1"
    scope = _substitute_params(scope, run_params)
    exclusion = (config.get("exclusion_sql") or "").strip()
    exclusion = _substitute_params(exclusion, run_params)

    where_parts = [f"({scope})"]
    if exclusion:
        where_parts.append(f"NOT ({exclusion})")
    where_clause = " AND ".join(where_parts)

    method = (config.get("sampling_method") or "RANKED").upper()
    priority_sql = (config.get("priority_rank_sql") or "").strip()

    if method in _METHODS_REQUIRING_PRIORITY and not priority_sql:
        raise ValueError(
            f"sampling_method='{method}' requires priority_rank_sql "
            f"(config_id={config.get('config_id')})."
        )

    # RANKED/SYSTEMATIC always have priority_sql here (validated above);
    # RANDOM may or may not -- if it does, the pull is still ordered by it
    # (unchanged from the prior behavior), but the rank itself stays NULL
    # below since RANDOM's own selection is a shuffle, not a rank read.
    #
    # A RANDOM pull with no priority_rank_sql still needs SOME
    # deterministic ORDER BY, or this whole feature's core promise --
    # "the same seed reproduces every bucket's draw exactly" (see this
    # module's docstring) -- silently breaks: rng.shuffle() is only
    # reproducible given the SAME starting row order every run, and most
    # source engines make no ordering guarantee at all for a query with no
    # ORDER BY. Falling back to key_cols keeps the pull's row order
    # (and therefore the shuffle) stable across reruns, without requiring
    # every RANDOM config to also author a priority_rank_sql it has no
    # other use for.
    order_clause = f"ORDER BY {priority_sql}" if priority_sql else f"ORDER BY {', '.join(key_cols)}"

    if method in _METHODS_REQUIRING_PRIORITY:
        rank_select = f"ROW_NUMBER() OVER (ORDER BY {priority_sql}) AS _priority_rank"
    else:
        rank_select = "NULL AS _priority_rank"

    select_cols = ", ".join(key_cols + strata_select + [rank_select])

    query = f"SELECT {select_cols} FROM {config['universe_table']} WHERE {where_clause} {order_clause}"
    logger.info("Sampling candidate pull (config_id=%s):\n%s", config.get("config_id"), query)

    return _run_source_query(db_conn, query)


def _case_key(row: dict, key_cols: list) -> str:
    """
    "col1=val1|col2=val2" for `key_cols`, in row-dict order -- sampling's
    analog of rules_engine/executor.py::build_src_key()/_format_src_key(),
    same fix applied for the same reason (see that function's docstring).

    `row`'s keys are always lowercased (see shared/db_ops.py::execute_query()/
    _run_source_query()), but `key_cols` comes straight from
    gre_sampling_config.key_columns as authored -- look up case-
    insensitively so casing in that column never silently breaks this.

    A key_cols entry not present in `row` at all (case-insensitively) is a
    config error -- key_columns naming a column _pull_candidates() doesn't
    actually SELECT -- not a real NULL value. Previously this silently
    fell back to dict.get()'s default of None, and EVERY affected row's
    case_key collapsed to the identical degenerate string (e.g.
    "claim_id=None") regardless of the row's real identity: distinct
    candidates became indistinguishable, is_selected/shortfall top-up in
    run_sampling() operated on one collapsed key instead of one per
    candidate, and gre_sample_selections (which has no unique index on
    case_key -- see sampling/schema.sql) silently accepted the resulting
    duplicates. This now raises instead, so run_sampling() fails loud
    (caught below, logged to gre_errors) rather than persisting
    indistinguishable rows.
    """
    def _fmt(c):
        key = c.lower()
        if key not in row:
            raise KeyError(
                f"key_columns entry '{c}' not found among this config's pulled "
                f"columns {sorted(row.keys())} -- gre_sampling_config.key_columns must "
                "name columns _pull_candidates() actually SELECTs (case-insensitive)."
            )
        v = row[key]
        return "NULL" if v is None else v
    return "|".join(f"{c}={_fmt(c)}" for c in key_cols)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def _persist_selections(meta_conn, meta_db: str, sample_run_id: str, config: dict,
                        levels: list, key_cols: list, candidates: list, selected_keys: set,
                        sample_cycle) -> None:
    """
    Persist every candidate considered (selected or not), plus one attrs
    row per candidate per stratification level, via bulk_insert() --
    batched executemany() instead of one INSERT+commit per row per table.
    Both tables are append-only with no unique index (a rerun always uses
    a fresh sample_run_id), so no duplicate-key fallback is needed here,
    unlike rules_engine/executor.py::_write_exceptions()'s
    bulk_insert_or_skip().

    Every row is inserted with etl_is_curr_ind='Y' -- this is the run's
    first (and only) INSERT, so it's unconditionally the current data for
    its sample_run_id. Any PRIOR sample_run_id sharing this run's
    (config_id, run_key) is deactivated separately, after this call
    succeeds -- see _deactivate_prior_sampling_runs().
    """
    sel_sql = f"""
        INSERT INTO {meta_db}.gre_sample_selections (
            sample_run_id, config_id, project_name, process_name, sample_cycle,
            case_key, priority_rank, excluded_flag, exclusion_reason, selected_flag,
            etl_is_curr_ind
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, ?, 'Y')
    """
    attr_sql = f"""
        INSERT INTO {meta_db}.gre_sample_selection_attrs (
            sample_run_id, case_key, strata_id, level_order, bucket_value, etl_is_curr_ind
        ) VALUES (?, ?, ?, ?, ?, 'Y')
    """

    sel_params = []
    attr_params = []
    for row in candidates:
        key = _case_key(row, key_cols)
        is_selected = key in selected_keys
        sel_params.append([
            sample_run_id, config.get("config_id"), config.get("project_name"),
            config.get("process_name"), sample_cycle, key, row.get("_priority_rank"),
            1 if is_selected else 0,
        ])
        for lvl in levels:
            bval = row.get(_bucket_key(lvl["strata_id"]))
            attr_params.append([sample_run_id, key, lvl["strata_id"], lvl["level_order"], str(bval)])

    bulk_insert(meta_conn, sel_sql, sel_params)
    bulk_insert(meta_conn, attr_sql, attr_params)


def _deactivate_prior_sampling_runs(meta_conn, meta_db: str, config_id, run_key: str,
                                    current_sample_run_id: str) -> int:
    """
    Soft-deactivate every OTHER sample_run_id's rows in
    gre_sample_selections/gre_sample_selection_attrs that share this run's
    (config_id, run_key) -- the sampling analog of rules_engine/executor.py::
    _write_exceptions()'s reconciliation, per the same "always re-execute,
    deactivate stale, activate new" design.

    run_key is not stored directly on gre_sample_selections (only embedded
    inside the sample_run_id string), so prior sample_run_id(s) for this
    (config_id, run_key) are found via gre_sampling_audit instead -- one
    row per sampling run, with sample_config_id and run_key recorded by
    _write_audit(). gre_sampling_audit holds sampling runs ONLY (it's this
    package's own table -- see sampling/schema.sql), so no run_type filter
    is needed here. Never deletes: a superseded run's rows stay in both
    tables with etl_is_curr_ind='N' for history/audit.

    Called AFTER this run's own rows are persisted as active (see
    run_sampling()), so a failure here never leaves a (config_id, run_key)
    with zero active rows -- worst case, both the old and new run are left
    marked active, which a later rerun's own deactivation pass will
    reconcile.

    Returns the number of prior sample_run_id's deactivated (0 if this is
    the first run for this (config_id, run_key)).
    """
    prior_runs = execute_query(
        meta_conn,
        f"""
        SELECT DISTINCT run_id
        FROM {meta_db}.gre_sampling_audit
        WHERE sample_config_id = ? AND run_key = ? AND run_id <> ?
        """,
        [config_id, run_key, current_sample_run_id],
    )
    prior_run_ids = [r["run_id"] for r in prior_runs]
    if not prior_run_ids:
        return 0

    for prior_run_id in prior_run_ids:
        # last_updated_datetime is bumped here (was NULL since insert) so
        # metadata_sync's incremental watermark (COALESCE(last_updated_
        # datetime, load_datetime)) picks up this flip -- load_datetime
        # itself is set once at insert and never touched again, same
        # convention as gre_exceptions.last_updated_datetime.
        execute_dml(
            meta_conn,
            f"""
            UPDATE {meta_db}.gre_sample_selections
            SET etl_is_curr_ind = 'N', last_updated_datetime = CURRENT_TIMESTAMP
            WHERE sample_run_id = ? AND etl_is_curr_ind = 'Y'
            """,
            [prior_run_id],
        )
        execute_dml(
            meta_conn,
            f"""
            UPDATE {meta_db}.gre_sample_selection_attrs
            SET etl_is_curr_ind = 'N', last_updated_datetime = CURRENT_TIMESTAMP
            WHERE sample_run_id = ? AND etl_is_curr_ind = 'Y'
            """,
            [prior_run_id],
        )

    return len(prior_run_ids)


def _write_audit(meta_conn, meta_db: str, sample_run_id: str, run_key: str, config: dict, method: str,
                 seed, target_vol: int, total_candidates: int, total_selected: int,
                 started_at: datetime, status: str, triggered_by: str = "SYSTEM") -> None:
    execute_dml(meta_conn, f"""
        INSERT INTO {meta_db}.gre_sampling_audit (
            run_id, run_key, started_at, ended_at, status,
            sample_config_id, sampling_method, random_seed,
            target_volume, total_candidates, total_selected, triggered_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        sample_run_id, run_key, started_at, datetime.now(), status,
        config.get("config_id"), method, seed,
        target_vol, total_candidates, total_selected, triggered_by,
    ])


def _log_sampling_error(meta_conn, meta_db: str, sample_run_id: str, run_key: str, config: dict,
                        error_type: str, message: str) -> None:
    """
    Thin wrapper over sampling/db_ops.py's log_error(), for config-dict
    callers -- writes to this package's own gre_sampling_errors table.
    """
    log_error(meta_conn, meta_db, sample_run_id, config.get("process_name"), run_key,
              error_type, message)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_sampling(config_id, run_key: str, cf, meta_conn=None, meta_db: str = None, seed: int = None,
                 run_params: dict = None, triggered_by: str = "SYSTEM") -> dict:
    """
    Execute one stratified sampling pass for `config_id`, scoped to
    `run_key` and whatever else `run_params` supplies (substituted into
    scope_sql's/exclusion_sql's "{key}" tokens -- same convention, and the
    same sampling.db_ops.py::_substitute_params() mechanism, as
    gre_rules.rule_syntax in rules_engine/).

    Parameters
    ----------
    config_id    : gre_sampling_config.config_id to run
    run_key      : opaque tracking/idempotency identifier for this run
                   (embedded into sample_run_id, and recorded on gre_sampling_audit) --
                   a batch id, a year+month pair, a specific date, or any
                   other column/combination the caller wants; build one via
                   shared/db_ops.py::build_run_key() or pass your own
                   string. Deliberately NOT merged into run_params -- see
                   run_params below -- if scope_sql/exclusion_sql need to
                   reference the run's tracking value, pass it explicitly
                   via run_params under whatever key they choose.
    cf           : ConnectionFactory, already loaded
    meta_conn    : adapter for the gre_ metadata store; defaults to
                   cf.get(gre_config.get_meta_connection_name())
    meta_db      : schema the gre_ tables live in; defaults to
                   gre_config.get_meta_db()
    seed         : explicit seed for RANDOM/SYSTEMATIC reproducibility; a
                   fresh one is generated and persisted if not supplied
    run_params   : optional dict of named values scope_sql/exclusion_sql can
                   reference via "{key}" tokens -- passed through exactly as
                   given, no reserved/required key. Lets each project scope
                   its candidate universe however it needs, the same way
                   rules_engine's run_params does.
    triggered_by : freeform string recorded on gre_sampling_audit AND folded into
                   sample_run_id -- same parameter/purpose as
                   rules_engine/runner.py::run_rule_group()'s triggered_by.
                   Defaults to "SYSTEM" for unattended/scheduled callers.

    Returns
    -------
    dict summary: sample_run_id, candidates, selected, target_volume,
    by_stratum, seed (None for RANKED)
    """
    from sampling import config as gre_config

    meta_db = meta_db or gre_config.get_meta_db()
    meta_conn = meta_conn or cf.get(gre_config.get_meta_connection_name())
    if meta_conn is None:
        raise RuntimeError(f"Metadata connection '{gre_config.get_meta_connection_name()}' unavailable.")

    resolved_params = dict(run_params or {})

    started_at = datetime.now()
    config, levels = load_sampling_config(meta_conn, meta_db, config_id)

    method = (config.get("sampling_method") or "RANKED").upper()
    if method not in VALID_METHODS:
        raise ValueError(f"config_id={config_id}: invalid sampling_method '{method}'.")

    rounding_mode = config.get("rounding_mode") or "FLOOR"
    target_vol = int(config.get("target_volume") or 150)
    key_cols = _key_columns(config)

    # Same shape/rationale as rules_engine/runner.py::_build_group_run_id()
    # -- see that function's docstring and shared/db_ops.py::
    # generate_run_id()'s. Folds in project_name (if set on this config),
    # an "attempt-N" label (N = count_prior_attempts() + 1, keyed on
    # (run_key, config_id)), and triggered_by, on top of the plain
    # sample_name::run_key shape this used to be:
    #
    #   {project_name}.{sample_name}::{run_key}::attempt-{N}::{triggered_by}::{timestamp}::{hex}
    #
    # `timestamp=started_at` keeps the id's embedded timestamp identical to
    # what _write_audit() below persists as gre_sampling_audit.started_at, rather
    # than a separate datetime.now() call drifting from it by however long
    # load_sampling_config() took.
    attempt_no = count_prior_attempts(meta_conn, meta_db, run_key, sample_config_id=config_id) + 1
    sample_name = config.get("sample_name", "SAMPLE")
    group_label = f"{config['project_name']}.{sample_name}" if config.get("project_name") else sample_name
    sample_run_id = generate_run_id(
        group_label, run_key, f"attempt-{attempt_no}", triggered_by, timestamp=started_at
    )

    db_conn = cf.get(config["source_type"])
    if db_conn is None:
        _log_sampling_error(meta_conn, meta_db, sample_run_id, run_key, config,
                            "CONNECTION_UNAVAILABLE", f"No connection '{config['source_type']}'")
        _write_audit(meta_conn, meta_db, sample_run_id, run_key, config, method, None,
                    target_vol, 0, 0, started_at, "ERROR", triggered_by=triggered_by)
        return {"sample_run_id": sample_run_id, "status": "ERROR", "candidates": 0, "selected": 0,
               "target_volume": target_vol, "by_stratum": {}, "seed": None}

    # A seed is only meaningful (and only persisted) for RANDOM/SYSTEMATIC --
    # RANKED is fully deterministic from priority_rank_sql alone.
    run_seed = None
    if method in ("RANDOM", "SYSTEMATIC"):
        run_seed = seed if seed is not None else random.SystemRandom().getrandbits(63)
    rng = random.Random(run_seed)

    try:
        candidates = _pull_candidates(db_conn, config, levels, resolved_params)
    except Exception as exc:
        logger.error("Sampling config_id=%s: candidate pull failed: %s", config_id, exc, exc_info=True)
        _log_sampling_error(meta_conn, meta_db, sample_run_id, run_key, config, "PULL_FAILURE", str(exc))
        _write_audit(meta_conn, meta_db, sample_run_id, run_key, config, method, run_seed,
                    target_vol, 0, 0, started_at, "ERROR", triggered_by=triggered_by)
        return {"sample_run_id": sample_run_id, "status": "ERROR", "candidates": 0, "selected": 0,
               "target_volume": target_vol, "by_stratum": {}, "seed": run_seed}

    if not candidates:
        _write_audit(meta_conn, meta_db, sample_run_id, run_key, config, method, run_seed,
                    target_vol, 0, 0, started_at, "COMPLETED", triggered_by=triggered_by)
        return {"sample_run_id": sample_run_id, "status": "COMPLETED", "candidates": 0, "selected": 0,
               "target_volume": target_vol, "by_stratum": {}, "seed": run_seed}

    # Stratify/select/persist, wrapped the same way the candidate pull
    # above is: a failure here (e.g. _case_key() raising on a
    # key_columns/config mismatch -- see its docstring) previously
    # propagated all the way out of run_sampling() uncaught, unlike every
    # other failure mode in this function, which logs to gre_errors and
    # writes an ERROR gre_sampling_audit row instead of crashing the caller.
    try:
        by_stratum: dict = {}
        selected = _stratify(candidates, levels, target_vol, method, rounding_mode, rng,
                             by_stratum=by_stratum)

        # Shortfall top-up: if stratified quotas under-filled relative to
        # target_vol (e.g. a thin cycle), top up from the remaining candidates
        # overall, using the SAME method as the rest of the run -- ranked by
        # remaining priority for RANKED/SYSTEMATIC, an additional random draw
        # for RANDOM.
        if len(selected) < target_vol:
            selected_keys = {_case_key(r, key_cols) for r in selected}
            remaining = [r for r in candidates if _case_key(r, key_cols) not in selected_keys]
            if method == "RANKED":
                remaining.sort(key=lambda r: r["_priority_rank"])
            shortfall = target_vol - len(selected)
            selected.extend(_select(remaining, shortfall, method, rng))

        selected = selected[:target_vol]
        selected_keys = {_case_key(r, key_cols) for r in selected}

        _persist_selections(meta_conn, meta_db, sample_run_id, config, levels, key_cols,
                            candidates, selected_keys, started_at.date())
    except Exception as exc:
        logger.error("Sampling config_id=%s: select/persist failed: %s", config_id, exc, exc_info=True)
        _log_sampling_error(meta_conn, meta_db, sample_run_id, run_key, config, "SELECT_PERSIST_FAILURE", str(exc))
        _write_audit(meta_conn, meta_db, sample_run_id, run_key, config, method, run_seed,
                    target_vol, len(candidates), 0, started_at, "ERROR", triggered_by=triggered_by)
        return {"sample_run_id": sample_run_id, "status": "ERROR", "candidates": len(candidates), "selected": 0,
               "target_volume": target_vol, "by_stratum": {}, "seed": run_seed}

    # This run's own rows are already persisted (and active) at this point --
    # only now look for and deactivate a PRIOR run of this same
    # (config_id, run_key), so a failure here never leaves the pair with
    # zero active rows (see _deactivate_prior_sampling_runs()'s docstring).
    # Deliberately non-fatal: reconciliation failing doesn't undo an
    # otherwise-successful sample.
    try:
        deactivated = _deactivate_prior_sampling_runs(meta_conn, meta_db, config_id, run_key, sample_run_id)
        if deactivated:
            logger.info(
                "Sampling config_id=%s run_key=%s: deactivated %d prior sample_run_id(s) "
                "superseded by %s.", config_id, run_key, deactivated, sample_run_id,
            )
    except Exception as exc:
        logger.error(
            "Sampling config_id=%s run_key=%s: deactivating prior sample_run_id(s) failed: %s",
            config_id, run_key, exc, exc_info=True,
        )
        _log_sampling_error(meta_conn, meta_db, sample_run_id, run_key, config,
                            "DEACTIVATE_PRIOR_RUNS_FAILURE", str(exc))

    _write_audit(meta_conn, meta_db, sample_run_id, run_key, config, method, run_seed,
                target_vol, len(candidates), len(selected), started_at, "COMPLETED",
                triggered_by=triggered_by)

    logger.info(
        "Sampling complete: sample_run_id=%s candidates=%d selected=%d/%d target (method=%s).",
        sample_run_id, len(candidates), len(selected), target_vol, method,
    )

    return {
        "sample_run_id": sample_run_id,
        "status": "COMPLETED",
        "candidates": len(candidates),
        "selected": len(selected),
        "target_volume": target_vol,
        "by_stratum": by_stratum,
        "seed": run_seed,
    }


# ---------------------------------------------------------------------------
# Multi-config orchestration (project_name/process_name fan-out)
# ---------------------------------------------------------------------------
#
# run_sampling() above is deliberately single-config: "one call = one
# gre_sampling_config row" keeps its audit bookkeeping simple. Driving every
# sampling config a process owns through the engine in one call is an
# orchestration concern layered on TOP of that contract, not a change to
# it -- mirrors rules_engine/runner.py's discover_rule_groups()/
# run_all_active_groups()/run_by_process_name() shape exactly.

def discover_sampling_configs(meta_conn, meta_db: str, project_name: str = None,
                              process_name: str = None) -> list:
    """
    Active gre_sampling_config.config_id values, optionally narrowed to one
    project_name and/or process_name. Returns config_ids sorted for a
    deterministic run order.
    """
    where = ["act_ind = 1"]
    params = []
    if project_name is not None:
        where.append("project_name = ?")
        params.append(project_name)
    if process_name is not None:
        where.append("process_name = ?")
        params.append(process_name)

    sql = f"""
        SELECT config_id
        FROM {meta_db}.gre_sampling_config
        WHERE {' AND '.join(where)}
        ORDER BY config_id
    """
    rows = execute_query(meta_conn, sql, params)
    return [r["config_id"] for r in rows]


def run_sampling_for_process_name(
    process_name: str,
    run_key: str,
    cf,
    meta_conn=None,
    meta_db: str = None,
    project_name: str = None,
    seed: int = None,
    run_params: dict = None,
    triggered_by: str = "SYSTEM",
) -> dict:
    """
    Thin convenience wrapper around run_sampling(): discovers every active
    gre_sampling_config scoped to one process_name (optionally further
    narrowed by project_name) and runs each, against the SAME
    run_key/run_params/seed. Mirrors rules_engine/runner.py's
    run_by_process_name() -- the common case of "run every sampling config
    this process owns" without the caller resolving meta_conn/meta_db or
    the matching config_ids themselves first.

    process_name : which gre_sampling_config.process_name to run every
                   active config for (required -- use run_sampling()
                   directly for one specific config_id, or
                   discover_sampling_configs() if you need the list of
                   matching config_ids without running them).
    run_key      : opaque tracking/idempotency identifier for this run --
                   see run_sampling()'s docstring. The SAME run_key is used
                   for every config found, so all of a process's samples
                   this cycle share one tracking value.
    cf           : a loaded db.connection_factory.ConnectionFactory.
    meta_conn    : metadata connection; defaults to
                   cf.get(sampling.config.get_meta_connection_name()).
    meta_db      : metadata schema/database name; defaults to
                   sampling.config.get_meta_db().
    project_name : optional further narrowing to one project within this
                   process_name; omit to run every project under it.
    seed         : explicit seed passed through to every run_sampling()
                   call (RANDOM/SYSTEMATIC only) -- omit to let each config
                   generate its own independent seed.
    run_params   : free-form dict for scope_sql/exclusion_sql {key}
                   substitution, passed through to every run_sampling()
                   call unchanged. run_key is deliberately NOT merged into
                   this -- see run_sampling()'s docstring.
    triggered_by : freeform string passed through to every run_sampling()
                   call unchanged -- see that function's docstring.

    Returns {"sampling_configs": {config_id: run_sampling()'s own summary
    dict, ...}} so a caller can inspect or aggregate per-config outcomes; a
    config that errors doesn't stop the remaining configs from running.
    Raises ValueError if no active config matches this process_name (and
    project_name, if given) -- most likely a typo'd process_name rather
    than a legitimately empty run.
    """
    from sampling import config as gre_config

    meta_conn = meta_conn or cf.get(gre_config.get_meta_connection_name())
    meta_db = meta_db or gre_config.get_meta_db()

    config_ids = discover_sampling_configs(meta_conn, meta_db, project_name=project_name,
                                           process_name=process_name)
    if not config_ids:
        raise ValueError(
            f"run_sampling_for_process_name: no active gre_sampling_config found for "
            f"process_name={process_name!r}"
            f"{f' project_name={project_name!r}' if project_name else ''} -- check "
            f"gre_sampling_config for a typo, or use discover_sampling_configs() directly "
            f"if an empty result is actually expected."
        )

    logger.info(
        "run_sampling_for_process_name: %d config(s) in scope (project_name=%s process_name=%s) "
        "for run_key=%s.",
        len(config_ids), project_name, process_name, run_key,
    )

    summaries = {}
    for config_id in config_ids:
        summaries[config_id] = run_sampling(
            config_id, run_key, cf,
            meta_conn=meta_conn, meta_db=meta_db, seed=seed, run_params=run_params,
            triggered_by=triggered_by,
        )

    return {"sampling_configs": summaries}
