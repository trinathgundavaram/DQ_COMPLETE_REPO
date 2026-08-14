"""
gre/sampling.py
----------------
Generic, config-driven stratified sampling -- a SEPARATE concern from rule
evaluation. Rules ask "is this row valid?" (pass/fail against a whole
universe); sampling asks "which N cases, out of a clean universe, are the
highest-value ones for a human reviewer this cycle?" A ranking/quota
problem, not a pass/fail problem -- its own module, own tables, and it can
run standalone with zero gre_rules rows defined for a project.

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

Algorithm
---------
pull candidates -> _stratify(candidates, levels, target_volume) recurses
one level at a time: bucket by that level's stratify_expr, compute each
bucket's target via gre_sampling_mix (a bucket_value absent from the mix
absorbs the "remainder" fraction -- see _target_for_bucket, identical rule
to the proven dq_* pattern), recurse into the next level with that smaller
target. Zero levels (or the bottom of the recursion) -> _select(), which
implements RANKED/RANDOM/SYSTEMATIC. Two levels reproduces the existing
dq_* UM behavior exactly (see tests/test_gre_sampling.py's regression
check, and gre/seed/um_sample.sql for the config_id=1 sample re-expressed
in this shape).

Reproducibility for RANDOM/SYSTEMATIC
--------------------------------------
ONE random seed is generated (or supplied) per sample_run_id and stored on
the gre_audit summary row. Buckets at every level are always processed in
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
import time
from datetime import datetime

from gre.executor import (
    execute_query, execute_dml, bulk_insert, log_error,
    _run_source_query, _substitute_batch_id,
)

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
        f"SELECT * FROM {meta_db}.gre_sampling_config WHERE config_id = ? AND active_flag = 1",
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


def _target_for_bucket(bucket_value: str, mix: dict, total: int, rounding_mode: str) -> int:
    """
    Same rule as core/stratified_sampling.py::_target_for_bucket, generalized
    to a config-driven rounding_mode instead of hardcoded floor(): a
    bucket_value present in `mix` gets that fraction of `total`; any
    bucket_value NOT in `mix` gets the remainder fraction
    (1 - sum(named fractions)) -- independently, same as the proven
    pattern, bounded in practice by the final target_volume truncation in
    run_sampling().
    """
    if not mix:
        return total
    if bucket_value in mix:
        return _round_target(total * mix[bucket_value], rounding_mode)
    named_fraction = sum(mix.values())
    remainder_fraction = max(0.0, 1.0 - named_fraction)
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

    buckets: dict = {}
    for row in candidates:
        bval = str(row.get(key))
        buckets.setdefault(bval, []).append(row)

    selected = []
    # Deterministic bucket order (sorted) -- required so a seeded rng
    # produces the exact same sequence of draws on every replay.
    for bval in sorted(buckets.keys()):
        brows = buckets[bval]
        btarget = _target_for_bucket(bval, level["mix"], target, rounding_mode)
        sub_path = f"{path}/{bval}" if path else bval
        selected.extend(_stratify(brows, levels, btarget, method, rounding_mode,
                                  rng, level_index + 1, by_stratum, sub_path))

    return selected


# ---------------------------------------------------------------------------
# Candidate pull
# ---------------------------------------------------------------------------

def _pull_candidates(db_conn, config: dict, levels: list, batch_id: str) -> list:
    key_cols = [c.strip() for c in config["key_columns"].split(",") if c.strip()]
    strata_select = [f"{lvl['stratify_expr']} AS {_bucket_key(lvl['strata_id'])}" for lvl in levels]
    select_cols = ", ".join(key_cols + strata_select) if strata_select else ", ".join(key_cols)

    scope = (config.get("scope_sql") or "").strip() or "1=1"
    scope = _substitute_batch_id(scope, batch_id)
    exclusion = (config.get("exclusion_sql") or "").strip()

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

    if priority_sql:
        order_clause = f"ORDER BY {priority_sql}"
    elif method == "SYSTEMATIC":
        # No priority given but SYSTEMATIC still needs a stable order to walk
        # an interval across -- fall back to key-column order.
        order_clause = f"ORDER BY {', '.join(key_cols)}"
    else:
        order_clause = ""

    query = f"SELECT {select_cols} FROM {config['universe_table']} WHERE {where_clause} {order_clause}"
    logger.info("Sampling candidate pull (config_id=%s):\n%s", config.get("config_id"), query)

    candidates = _run_source_query(db_conn, query)

    if method in _METHODS_REQUIRING_PRIORITY:
        for i, row in enumerate(candidates, start=1):
            row["_priority_rank"] = i
    else:
        for row in candidates:
            row["_priority_rank"] = None   # meaningless for RANDOM

    return candidates


def _case_key(row: dict, key_cols: list) -> str:
    return "|".join(f"{c}={row.get(c)}" for c in key_cols)


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
    unlike executor.py::_write_exceptions()'s bulk_insert_or_skip().
    """
    sel_sql = f"""
        INSERT INTO {meta_db}.gre_sample_selections (
            sample_run_id, config_id, project_name, process_name, sample_cycle,
            case_key, priority_rank, excluded_flag, exclusion_reason, selected_flag
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, ?)
    """
    attr_sql = f"""
        INSERT INTO {meta_db}.gre_sample_selection_attrs (
            sample_run_id, case_key, strata_id, level_order, bucket_value
        ) VALUES (?, ?, ?, ?, ?)
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


def _write_audit(meta_conn, meta_db: str, sample_run_id: str, config: dict, method: str,
                 seed, target_vol: int, total_candidates: int, total_selected: int,
                 started_at: datetime, status: str) -> None:
    execute_dml(meta_conn, f"""
        INSERT INTO {meta_db}.gre_audit (
            run_id, run_type, started_at, ended_at, status,
            sample_config_id, sampling_method, random_seed,
            target_volume, total_candidates, total_selected, triggered_by
        ) VALUES (?, 'SAMPLING', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'SYSTEM')
    """, [
        sample_run_id, started_at, datetime.now(), status,
        config.get("config_id"), method, seed,
        target_vol, total_candidates, total_selected,
    ])


def _log_sampling_error(meta_conn, meta_db: str, sample_run_id: str, config: dict,
                        error_type: str, message: str) -> None:
    """
    Thin wrapper over executor.py's shared log_error(): a sampling run has
    no rule_id/rule_group, so process_name is carried in the rule_group
    column for triage (same as the prior standalone implementation), just
    without a second copy of the INSERT+try/except body.
    """
    log_error(meta_conn, meta_db, sample_run_id, None, config.get("process_name"), None,
              error_type, message)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_sampling(config_id, batch_id: str, cf, meta_conn=None, meta_db: str = None, seed: int = None) -> dict:
    """
    Execute one stratified sampling pass for `config_id`, scoped to
    `batch_id` (substituted into scope_sql's {batch_id} token, same
    convention as gre_rules.rule_sql).

    Parameters
    ----------
    config_id  : gre_sampling_config.config_id to run
    batch_id   : this cycle's batch/cohort identifier
    cf         : ConnectionFactory, already loaded
    meta_conn  : adapter for the gre_ metadata store; defaults to
                 cf.get(gre_config.get_meta_connection_name())
    meta_db    : schema the gre_ tables live in; defaults to
                 gre_config.get_meta_db()
    seed       : explicit seed for RANDOM/SYSTEMATIC reproducibility; a
                 fresh one is generated and persisted if not supplied

    Returns
    -------
    dict summary: sample_run_id, candidates, selected, target_volume,
    by_stratum, seed (None for RANKED)
    """
    from gre import config as gre_config

    meta_db = meta_db or gre_config.get_meta_db()
    meta_conn = meta_conn or cf.get(gre_config.get_meta_connection_name())
    if meta_conn is None:
        raise RuntimeError(f"Metadata connection '{gre_config.get_meta_connection_name()}' unavailable.")

    started_at = datetime.now()
    config, levels = load_sampling_config(meta_conn, meta_db, config_id)

    method = (config.get("sampling_method") or "RANKED").upper()
    if method not in VALID_METHODS:
        raise ValueError(f"config_id={config_id}: invalid sampling_method '{method}'.")

    rounding_mode = config.get("rounding_mode") or "FLOOR"
    target_vol = int(config.get("target_volume") or 150)
    key_cols = [c.strip() for c in config["key_columns"].split(",") if c.strip()]

    sample_run_id = f"{config.get('sample_name', 'SAMPLE')}_{batch_id}_{started_at.strftime('%Y%m%d_%H%M%S')}"

    db_conn = cf.get(config["connection_name"])
    if db_conn is None:
        _log_sampling_error(meta_conn, meta_db, sample_run_id, config,
                            "CONNECTION_UNAVAILABLE", f"No connection '{config['connection_name']}'")
        _write_audit(meta_conn, meta_db, sample_run_id, config, method, None,
                    target_vol, 0, 0, started_at, "ERROR")
        return {"sample_run_id": sample_run_id, "status": "ERROR", "candidates": 0, "selected": 0,
               "target_volume": target_vol, "by_stratum": {}, "seed": None}

    # A seed is only meaningful (and only persisted) for RANDOM/SYSTEMATIC --
    # RANKED is fully deterministic from priority_rank_sql alone.
    run_seed = None
    if method in ("RANDOM", "SYSTEMATIC"):
        run_seed = seed if seed is not None else random.SystemRandom().getrandbits(63)
    rng = random.Random(run_seed)

    try:
        candidates = _pull_candidates(db_conn, config, levels, batch_id)
    except Exception as exc:
        logger.error("Sampling config_id=%s: candidate pull failed: %s", config_id, exc, exc_info=True)
        _log_sampling_error(meta_conn, meta_db, sample_run_id, config, "PULL_FAILURE", str(exc))
        _write_audit(meta_conn, meta_db, sample_run_id, config, method, run_seed,
                    target_vol, 0, 0, started_at, "ERROR")
        return {"sample_run_id": sample_run_id, "status": "ERROR", "candidates": 0, "selected": 0,
               "target_volume": target_vol, "by_stratum": {}, "seed": run_seed}

    if not candidates:
        _write_audit(meta_conn, meta_db, sample_run_id, config, method, run_seed,
                    target_vol, 0, 0, started_at, "COMPLETED")
        return {"sample_run_id": sample_run_id, "status": "COMPLETED", "candidates": 0, "selected": 0,
               "target_volume": target_vol, "by_stratum": {}, "seed": run_seed}

    by_stratum: dict = {}
    selected = _stratify(candidates, levels, target_vol, method, rounding_mode, rng, by_stratum=by_stratum)

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
    _write_audit(meta_conn, meta_db, sample_run_id, config, method, run_seed,
                target_vol, len(candidates), len(selected), started_at, "COMPLETED")

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
