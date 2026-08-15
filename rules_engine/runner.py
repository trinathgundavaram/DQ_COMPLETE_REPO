"""
rules_engine/runner.py
-------------------------
Orchestration entry point: run_rule_group(rule_group, batch_id, ...).

Single-threaded, sequential rule execution is still the DEFAULT and
requires zero config: a batch-readiness gate, checkpoint/resume, then a
sequencing_mode-aware pass over the rules, calling
rules_engine/executor.py::execute_rule() once per rule. Every rule commits
its own findings independently -- this file only ever decides run ORDER,
never commit/rollback. A dependency graph between rules is still out of
scope.

Parallel execution (opt-in)
-------------------------------
Raising GRE_MAX_PARALLEL_RULES above its default of 1 (see
shared/config.py) turns on a second path, used ONLY for
sequencing_mode='independent' groups: rules run concurrently across a
bounded thread pool, with per-connection concurrency additionally capped
by DQ_<NAME>_MAX_PARALLEL so a source system that can't tolerate much
concurrent load (e.g. an OLTP Postgres source, vs. a Teradata warehouse)
isn't hit harder just because the group-wide worker count went up. See
rules_engine/parallel.py's module docstring for the connection-pooling
mechanics, and _run_pending_parallel() below for how it plugs into this
file's existing checkpoint/resume and gre_audit bookkeeping.

sequencing_mode='sequential' groups NEVER take this path, regardless of
GRE_MAX_PARALLEL_RULES -- their whole point is a guaranteed run ORDER plus
on_failure=halt_group support, both of which are meaningless once rules
can finish out of order.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from shared import config as gre_config
from shared.db_ops import execute_query, execute_dml, build_run_params
from rules_engine.rules import load_rules
from rules_engine.executor import execute_rule, _log_error, _log_attempt
from rules_engine.parallel import build_pools, close_pools

logger = logging.getLogger(__name__)


def generate_run_id(rule_group: str, batch_id: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{rule_group}_{batch_id}_{ts}"


# ---------------------------------------------------------------------------
# Checkpoint / resume
# ---------------------------------------------------------------------------

def _already_succeeded_rule_ids(meta_conn, meta_db: str, rule_group: str, batch_id: str) -> set:
    """
    rule_ids that already have a SUCCESS attempt logged in gre_log for this
    (rule_group, batch_id). These are skipped on resume -- carrying over
    the legacy framework's get_failed_start_seq idea: a killed-and-rerun
    batch picks up after the last rule that actually completed, instead of
    re-running (and re-committing duplicate work against) rules that
    already succeeded.
    """
    rows = execute_query(
        meta_conn,
        f"""
        SELECT DISTINCT rule_id
        FROM {meta_db}.gre_log
        WHERE rule_group = ? AND batch_id = ? AND status = 'SUCCESS'
        """,
        [rule_group, batch_id],
    )
    return {r["rule_id"] for r in rows}


# ---------------------------------------------------------------------------
# gre_audit
# ---------------------------------------------------------------------------

def _start_audit(meta_conn, meta_db: str, run_id: str, rule_group: str, batch_id: str,
                  total_rules: int, triggered_by: str, rule_variant: str = None) -> None:
    execute_dml(meta_conn, f"""
        INSERT INTO {meta_db}.gre_audit (
            run_id, rule_group, batch_id, rule_variant, started_at, status,
            total_rules, rules_succeeded, rules_errored, triggered_by
        ) VALUES (?, ?, ?, ?, ?, 'RUNNING', ?, 0, 0, ?)
    """, [run_id, rule_group, batch_id, rule_variant, datetime.now(), total_rules, triggered_by])


def _finish_audit(meta_conn, meta_db: str, run_id: str, status: str,
                   rules_succeeded: int, rules_errored: int) -> None:
    execute_dml(meta_conn, f"""
        UPDATE {meta_db}.gre_audit
        SET ended_at = ?, status = ?, rules_succeeded = ?, rules_errored = ?
        WHERE run_id = ?
    """, [datetime.now(), status, rules_succeeded, rules_errored, run_id])


# ---------------------------------------------------------------------------
# Parallel execution (sequencing_mode='independent' only -- see module docstring)
# ---------------------------------------------------------------------------

def _run_one_pending_rule(rule, source_pools, meta_pool, meta_db, run_id, batch_id, resolved_params, total_cache):
    """
    One rule's worth of the ThreadPoolExecutor body -- acquire a source
    connection AND a metadata connection from their respective pools
    (never the single shared adapter cf.get() would hand back; see
    rules_engine/parallel.py's module docstring for why), run it through
    the exact same execute_rule() every rule goes through either way, then
    return both connections to their pools no matter what.

    A rule whose source connection pool never built even one adapter
    (source_pools[name].available is False) is handled by the caller
    BEFORE this function is ever invoked for that rule -- see
    _run_pending_parallel() below -- so by the time this runs, both pools
    passed in are guaranteed usable.
    """
    source_pool = source_pools[rule["source_connection"]]
    db_conn = source_pool.acquire()
    worker_meta_conn = meta_pool.acquire()
    try:
        status = execute_rule(rule, db_conn, worker_meta_conn, run_id, resolved_params, meta_db,
                              total_cache=total_cache)
    finally:
        source_pool.release(db_conn)
        meta_pool.release(worker_meta_conn)
    return rule["rule_id"], status


def _run_pending_parallel(pending, cf, meta_conn, meta_db, run_id, batch_id, resolved_params,
                          total_cache, max_workers):
    """
    The parallel counterpart to run_rule_group()'s default sequential
    for-loop, used only when sequencing_mode='independent' and
    GRE_MAX_PARALLEL_RULES > 1 (see the module docstring). Mirrors that
    loop's behavior rule-for-rule -- same CONNECTION_UNAVAILABLE handling,
    same results dict, same succeeded/errored counts -- just fanned out
    across a bounded thread pool instead of run one at a time.

    total_cache (shared, plain dict) is passed straight through to every
    concurrent execute_rule() call exactly as it is in the sequential
    path. Plain dict get/set/contains are atomic under the GIL, so this
    never corrupts -- the one accepted trade-off is that two rules racing
    to compute the SAME cache key (identical database_name/table_name/
    run_params) for the first time can, rarely, both run the identical
    COUNT(*) query before either has written the result. Both write the
    same correct value either way, so this is a harmless, self-resolving
    duplicate query on a narrow timing window, not a correctness issue --
    accepted here rather than adding lock-guarded memoization to
    executor.py::_compute_total() for what would only ever save one
    redundant query per cache key per run, at most.

    halt_group/on_failure is never consulted here, same as the sequential
    loop already does for 'independent' mode today -- see run_rule_group().

    Every pooled connection (source AND meta -- see rules_engine/
    parallel.py) is closed in the finally block below before this
    function returns, regardless of how the run ended.
    """
    results = {}
    succeeded = 0
    errored = 0

    unavailable = []
    runnable = []
    source_names = set()
    for rule in pending:
        source_names.add(rule["source_connection"])

    source_pools = build_pools(cf, source_names, max_workers)
    meta_pool = build_pools(cf, {gre_config.get_meta_connection_name()}, max_workers)[
        gre_config.get_meta_connection_name()
    ]

    try:
        # meta_pool failing to build even one connection is a harder stop
        # than a single source being unavailable: EVERY runnable rule
        # writes through it, and nothing can safely fall back to sharing
        # the single top-level `meta_conn` across concurrent workers (the
        # exact hazard this whole pooling design exists to avoid -- see
        # rules_engine/parallel.py's module docstring). Fail every pending
        # rule closed rather than acquire()-ing on an empty pool forever.
        if not meta_pool.available:
            logger.error(
                "Metadata connection '%s' could not be pooled for parallel execution "
                "(GRE_MAX_PARALLEL_RULES=%d) -- logging all %d pending rule(s) as ERROR.",
                gre_config.get_meta_connection_name(), max_workers, len(pending),
            )
            for rule in pending:
                _log_error(meta_conn, meta_db, run_id, rule, batch_id,
                           "CONNECTION_UNAVAILABLE",
                           f"No pooled connection '{gre_config.get_meta_connection_name()}' for parallel execution")
                _log_attempt(meta_conn, meta_db, run_id, rule, batch_id, "ERROR", 0, time.time(),
                            f"No pooled connection '{gre_config.get_meta_connection_name()}' for parallel execution")
                results[rule["rule_id"]] = "ERROR"
                errored += 1
            return results, succeeded, errored

        for rule in pending:
            if source_pools[rule["source_connection"]].available:
                runnable.append(rule)
            else:
                unavailable.append(rule)

        for rule in unavailable:
            logger.error(
                "rule_id=%s: source_connection '%s' unavailable -- logging as ERROR.",
                rule["rule_id"], rule["source_connection"],
            )
            _log_error(meta_conn, meta_db, run_id, rule, batch_id,
                       "CONNECTION_UNAVAILABLE", f"No connection '{rule['source_connection']}'")
            _log_attempt(meta_conn, meta_db, run_id, rule, batch_id, "ERROR", 0, time.time(),
                        f"No connection '{rule['source_connection']}'")
            results[rule["rule_id"]] = "ERROR"
            errored += 1

        if runnable:
            logger.info(
                "Running %d rule(s) in parallel (max_workers=%d) -- source connection(s): %s",
                len(runnable), max_workers, ", ".join(sorted(source_names)),
            )
            with ThreadPoolExecutor(max_workers=max_workers) as pool_exec:
                futures = [
                    pool_exec.submit(
                        _run_one_pending_rule, rule, source_pools, meta_pool, meta_db,
                        run_id, batch_id, resolved_params, total_cache,
                    )
                    for rule in runnable
                ]
                for future in as_completed(futures):
                    rule_id, status = future.result()
                    results[rule_id] = status
                    if status == "SUCCESS":
                        succeeded += 1
                    else:
                        errored += 1
    finally:
        close_pools(source_pools)
        close_pools({gre_config.get_meta_connection_name(): meta_pool})

    return results, succeeded, errored


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_rule_group(
    rule_group: str,
    batch_id: str,
    cf,
    meta_conn=None,
    meta_db: str = None,
    triggered_by: str = "SYSTEM",
    run_params: dict = None,
    rule_variant: str = None,
) -> dict:
    """
    Run every active rule in `rule_group` against `batch_id`.

    Parameters
    ----------
    rule_group   : which group of gre_rules to run
    batch_id     : the batch being evaluated -- the tracking/idempotency key
                   (gre_exceptions_uix, gre_log, gre_results, gre_audit) and
                   always present in the run_params every rule sees (see
                   run_params below).
    cf           : a db.connection_factory.ConnectionFactory, already loaded,
                   used to resolve each rule's source_connection to an adapter
    meta_conn    : adapter for the gre_ metadata store; defaults to
                   cf.get(gre_config.get_meta_connection_name())
    meta_db      : schema the gre_ tables live in; defaults to
                   gre_config.get_meta_db()
    triggered_by : freeform string recorded on gre_audit
    run_params   : optional dict of extra named values a rule's rule_sql can
                   reference via "{key}" tokens -- merged with batch_id via
                   shared.db_ops.build_run_params() (batch_id always wins
                   on key collision). The SAME dict also becomes the
                   equality filters for the auto-generated total-record
                   count (rules_engine/executor.py::_build_total_query()).
                   Lets each project scope its data however it needs
                   (month/year, run_type, a date range, a region column,
                   ...) without the engine having to know about any of
                   those column names. See shared/db_ops.py::
                   _substitute_params()'s docstring.
    rule_variant : optional extra selection level on top of rule_group/
                   table -- passed straight to rules_engine.rules.load_rules()
                   (see its docstring) and recorded on gre_audit for this
                   run. None (the default) loads only rules with
                   rule_variant IS NULL (universal rules for the group).

    Returns
    -------
    dict summary: run_id, status, total_rules, succeeded, errored, skipped_ready
    """
    meta_db = meta_db or gre_config.get_meta_db()
    meta_conn = meta_conn or cf.get(gre_config.get_meta_connection_name())
    if meta_conn is None:
        raise RuntimeError(
            f"Metadata connection '{gre_config.get_meta_connection_name()}' unavailable."
        )

    if not gre_config.check_batch_ready(rule_group, batch_id, meta_conn):
        logger.info("rule_group=%s batch_id=%s not ready -- skipping this run.", rule_group, batch_id)
        return {
            "run_id": None, "status": "NOT_READY", "total_rules": 0,
            "succeeded": 0, "errored": 0, "results": {},
        }

    rules = load_rules(meta_conn, meta_db, rule_group, rule_variant=rule_variant)
    if not rules:
        logger.info("No active rules for rule_group=%s.", rule_group)
        return {
            "run_id": None, "status": "NO_RULES", "total_rules": 0,
            "succeeded": 0, "errored": 0, "results": {},
        }

    resolved_params = build_run_params(batch_id, run_params)

    run_id = generate_run_id(rule_group, batch_id)
    _start_audit(meta_conn, meta_db, run_id, rule_group, batch_id, len(rules), triggered_by,
                 rule_variant=rule_variant)
    logger.info("Starting GRE run: %s (%d rule(s))", run_id, len(rules))

    # Checkpoint/resume: skip rules that already succeeded for this batch.
    done_ids = _already_succeeded_rule_ids(meta_conn, meta_db, rule_group, batch_id)
    pending = [r for r in rules if r["rule_id"] not in done_ids]
    if done_ids:
        logger.info(
            "Resuming run for batch_id=%s: %d rule(s) already succeeded, %d pending.",
            batch_id, len(done_ids), len(pending),
        )

    # sequencing_mode is expected to be consistent across a rule_group; take
    # it from the first rule (in seq_no order) and warn if the group
    # disagrees with itself rather than silently picking per-rule.
    sequencing_mode = (rules[0].get("sequencing_mode") or "independent").lower()
    if any((r.get("sequencing_mode") or "independent").lower() != sequencing_mode for r in rules):
        logger.warning(
            "rule_group=%s has mixed sequencing_mode values across rules -- "
            "using '%s' (from rule_id=%s, lowest seq_no).",
            rule_group, sequencing_mode, rules[0]["rule_id"],
        )

    results = {}
    succeeded = 0
    errored = 0
    halted = False

    # Shared for the whole run: several rules in a group often ask the
    # identical "how many rows are in this batch" question (same
    # database_name/table_name and run_params) -- one dict here, threaded
    # into every execute_rule() call, lets _compute_total() reuse that COUNT(*)
    # result instead of re-scanning the same rows once per rule. See
    # rules_engine/executor.py::_compute_total()'s docstring.
    total_cache = {}

    # Parallel path only ever applies to 'independent' groups (see the
    # module docstring) -- 'sequential' groups always take the loop below,
    # unchanged, regardless of GRE_MAX_PARALLEL_RULES.
    max_workers = min(gre_config.get_max_parallel_rules(), len(pending)) if pending else 1
    use_parallel = sequencing_mode == "independent" and max_workers > 1

    if use_parallel:
        results, succeeded, errored = _run_pending_parallel(
            pending, cf, meta_conn, meta_db, run_id, batch_id, resolved_params,
            total_cache, max_workers,
        )
    else:
        for rule in pending:
            db_conn = cf.get(rule["source_connection"])
            if db_conn is None:
                logger.error(
                    "rule_id=%s: source_connection '%s' unavailable -- logging as ERROR.",
                    rule["rule_id"], rule["source_connection"],
                )
                _log_error(meta_conn, meta_db, run_id, rule, batch_id,
                           "CONNECTION_UNAVAILABLE", f"No connection '{rule['source_connection']}'")
                _log_attempt(meta_conn, meta_db, run_id, rule, batch_id, "ERROR", 0, time.time(),
                            f"No connection '{rule['source_connection']}'")
                status = "ERROR"
            else:
                status = execute_rule(rule, db_conn, meta_conn, run_id, resolved_params, meta_db,
                                      total_cache=total_cache)

            results[rule["rule_id"]] = status

            if status == "SUCCESS":
                succeeded += 1
            else:
                errored += 1
                if sequencing_mode == "sequential":
                    on_failure = (rule.get("on_failure") or "skip_and_continue").lower()
                    if on_failure == "halt_group":
                        logger.warning(
                            "rule_id=%s errored with on_failure=halt_group -- "
                            "stopping further rules in group=%s for run=%s. "
                            "Already-committed findings from prior rules are untouched.",
                            rule["rule_id"], rule_group, run_id,
                        )
                        halted = True
                        break
                    # skip_and_continue: error already logged to gre_errors by execute_rule(); keep going.

    final_status = "HALTED" if halted else "COMPLETED"
    _finish_audit(meta_conn, meta_db, run_id, final_status, succeeded, errored)

    logger.info(
        "GRE run complete: %s | total=%d succeeded=%d errored=%d | %s",
        run_id, len(rules), succeeded, errored, final_status,
    )

    return {
        "run_id": run_id,
        "status": final_status,
        "total_rules": len(rules),
        "succeeded": succeeded,
        "errored": errored,
        "results": results,
    }
