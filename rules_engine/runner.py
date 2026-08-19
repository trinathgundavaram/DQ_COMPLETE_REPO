"""
rules_engine/runner.py
-------------------------
Orchestration entry point: run_rule_group(rule_group, run_key, ...).

Single-threaded, sequential rule execution is still the DEFAULT and
requires zero config: a run-readiness gate, checkpoint/resume, then a
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
by GRE_<TYPE>_MAX_PARALLEL so a source system that can't tolerate much
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
from shared.db_ops import execute_query, execute_dml
from rules_engine.rules import load_rules
from rules_engine.executor import execute_rule, _log_error, _log_attempt
from rules_engine.parallel import build_pools, close_pools

logger = logging.getLogger(__name__)


def generate_run_id(rule_group: str, run_key: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{rule_group}_{run_key}_{ts}"


# ---------------------------------------------------------------------------
# Checkpoint / resume
# ---------------------------------------------------------------------------

def _already_succeeded_rule_ids(meta_conn, meta_db: str, rule_group: str, run_key: str) -> set:
    """
    rule_ids that already have a SUCCESS attempt logged in gre_log for this
    (rule_group, run_key). These are skipped on resume -- carrying over
    the legacy framework's get_failed_start_seq idea: a killed-and-rerun
    run picks up after the last rule that actually completed, instead of
    re-running (and re-committing duplicate work against) rules that
    already succeeded.
    """
    rows = execute_query(
        meta_conn,
        f"""
        SELECT DISTINCT rule_id
        FROM {meta_db}.gre_log
        WHERE rule_group = ? AND run_key = ? AND status = 'SUCCESS'
        """,
        [rule_group, run_key],
    )
    return {r["rule_id"] for r in rows}


# ---------------------------------------------------------------------------
# gre_audit
# ---------------------------------------------------------------------------

def _start_audit(meta_conn, meta_db: str, run_id: str, rule_group: str, run_key: str,
                  total_rules: int, triggered_by: str, rule_variant: str = None,
                  project_name: str = None, process_name: str = None) -> None:
    execute_dml(meta_conn, f"""
        INSERT INTO {meta_db}.gre_audit (
            run_id, rule_group, project_name, process_name, run_key, rule_variant, started_at, status,
            total_rules, rules_succeeded, rules_errored, triggered_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'RUNNING', ?, 0, 0, ?)
    """, [run_id, rule_group, project_name, process_name, run_key, rule_variant,
          datetime.now(), total_rules, triggered_by])


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

def _run_one_pending_rule(rule, source_pools, meta_pool, meta_db, run_id, run_key, resolved_params, total_cache):
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
    source_pool = source_pools[rule["sql_dialect"]]
    db_conn = source_pool.acquire()
    worker_meta_conn = meta_pool.acquire()
    try:
        status = execute_rule(rule, db_conn, worker_meta_conn, run_id, run_key, resolved_params, meta_db,
                              total_cache=total_cache)
    finally:
        source_pool.release(db_conn)
        meta_pool.release(worker_meta_conn)
    return rule["rule_id"], status


def _run_pending_parallel(pending, cf, meta_conn, meta_db, run_id, run_key, resolved_params,
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
        source_names.add(rule["sql_dialect"])

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
                _log_error(meta_conn, meta_db, run_id, rule, run_key,
                           "CONNECTION_UNAVAILABLE",
                           f"No pooled connection '{gre_config.get_meta_connection_name()}' for parallel execution")
                _log_attempt(meta_conn, meta_db, run_id, rule, run_key, "ERROR", 0, time.time(),
                            f"No pooled connection '{gre_config.get_meta_connection_name()}' for parallel execution")
                results[rule["rule_id"]] = "ERROR"
                errored += 1
            return results, succeeded, errored

        for rule in pending:
            if source_pools[rule["sql_dialect"]].available:
                runnable.append(rule)
            else:
                unavailable.append(rule)

        for rule in unavailable:
            logger.error(
                "rule_id=%s: source_type '%s' unavailable -- logging as ERROR.",
                rule["rule_id"], rule["sql_dialect"],
            )
            _log_error(meta_conn, meta_db, run_id, rule, run_key,
                       "CONNECTION_UNAVAILABLE", f"No connection '{rule['sql_dialect']}'")
            _log_attempt(meta_conn, meta_db, run_id, rule, run_key, "ERROR", 0, time.time(),
                        f"No connection '{rule['sql_dialect']}'")
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
                        run_id, run_key, resolved_params, total_cache,
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
    run_key: str,
    cf,
    meta_conn=None,
    meta_db: str = None,
    triggered_by: str = "SYSTEM",
    run_params: dict = None,
    rule_variant: str = None,
) -> dict:
    """
    Run every active rule in `rule_group` for this `run_key`.

    Parameters
    ----------
    rule_group   : which group of gre_rules to run
    run_key      : opaque tracking/idempotency identifier for this run
                   (gre_exceptions_uix, gre_log, gre_results, gre_audit key
                   off this value) -- a batch id, a year+month pair, a
                   specific date, or any other column/combination the
                   caller wants; build one via shared/db_ops.py::
                   build_run_key() or pass your own string. Deliberately
                   NOT merged into run_params: run_params doubles as the
                   equality filters for the auto-generated total-record
                   count (see run_params below), and run_key is often NOT
                   a real column on a rule's table (e.g. a composite like
                   "2026_8"), so auto-injecting it there would silently
                   break that query for most tables. If a rule_sql needs
                   to reference the run's tracking value, pass it
                   explicitly via run_params under whatever key matches an
                   actual column.
    cf           : a db.connection_factory.ConnectionFactory, already loaded,
                   used to resolve each rule's sql_dialect (source_type) to an adapter
    meta_conn    : adapter for the gre_ metadata store; defaults to
                   cf.get(gre_config.get_meta_connection_name())
    meta_db      : schema the gre_ tables live in; defaults to
                   gre_config.get_meta_db()
    triggered_by : freeform string recorded on gre_audit
    run_params   : optional dict of named values a rule's rule_sql can
                   reference via "{key}" tokens -- passed through exactly
                   as given, no reserved/required key. The SAME dict also
                   becomes the equality filters for the auto-generated
                   total-record count
                   (rules_engine/executor.py::_build_total_query()). Lets
                   each project scope its data however it needs
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

    if not gre_config.check_run_ready(rule_group, run_key, meta_conn):
        logger.info("rule_group=%s run_key=%s not ready -- skipping this run.", rule_group, run_key)
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

    resolved_params = dict(run_params or {})

    # project_name/process_name are descriptive/reporting dimensions carried
    # on every gre_rules row (see rules_engine/schema.sql's design notes) --
    # rule_group is still the one literal key load_rules() filters on. Take
    # them from the first rule (lowest seq_no) and warn, same pattern as the
    # sequencing_mode consistency check below, if the group disagrees with
    # itself -- a rule_group is expected to belong to exactly one project/process.
    project_name = rules[0].get("project_name")
    process_name = rules[0].get("process_name")
    if any(r.get("project_name") != project_name or r.get("process_name") != process_name for r in rules):
        logger.warning(
            "rule_group=%s has mixed project_name/process_name values across rules -- "
            "using project_name=%s process_name=%s (from rule_id=%s, lowest seq_no) for gre_audit.",
            rule_group, project_name, process_name, rules[0]["rule_id"],
        )

    run_id = generate_run_id(rule_group, run_key)
    _start_audit(meta_conn, meta_db, run_id, rule_group, run_key, len(rules), triggered_by,
                 rule_variant=rule_variant, project_name=project_name, process_name=process_name)
    logger.info("Starting GRE run: %s (%d rule(s))", run_id, len(rules))

    # Checkpoint/resume: skip rules that already succeeded for this run_key.
    done_ids = _already_succeeded_rule_ids(meta_conn, meta_db, rule_group, run_key)
    pending = [r for r in rules if r["rule_id"] not in done_ids]
    if done_ids:
        logger.info(
            "Resuming run for run_key=%s: %d rule(s) already succeeded, %d pending.",
            run_key, len(done_ids), len(pending),
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
    # identical "how many rows are in this run" question (same
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
            pending, cf, meta_conn, meta_db, run_id, run_key, resolved_params,
            total_cache, max_workers,
        )
    else:
        for rule in pending:
            db_conn = cf.get(rule["sql_dialect"])
            if db_conn is None:
                logger.error(
                    "rule_id=%s: source_type '%s' unavailable -- logging as ERROR.",
                    rule["rule_id"], rule["sql_dialect"],
                )
                _log_error(meta_conn, meta_db, run_id, rule, run_key,
                           "CONNECTION_UNAVAILABLE", f"No connection '{rule['sql_dialect']}'")
                _log_attempt(meta_conn, meta_db, run_id, rule, run_key, "ERROR", 0, time.time(),
                            f"No connection '{rule['sql_dialect']}'")
                status = "ERROR"
            else:
                status = execute_rule(rule, db_conn, meta_conn, run_id, run_key, resolved_params, meta_db,
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


# ---------------------------------------------------------------------------
# Multi-group orchestration (project_name/process_name fan-out)
# ---------------------------------------------------------------------------
#
# run_rule_group() above is deliberately single-group: "one call = one
# rule_group" keeps its checkpoint/resume and gre_audit bookkeeping simple
# and easy to reason about. Driving multiple projects/processes through the
# engine in one operation (e.g. a nightly job covering several use cases) is
# an orchestration concern layered on TOP of that contract, not a change to
# it -- discover_rule_groups()/run_all_active_groups() below just find which
# rule_groups exist for a given project/process scope and call
# run_rule_group() once per group, unchanged.

def discover_rule_groups(meta_conn, meta_db: str, project_name: str = None,
                          process_name: str = None) -> list:
    """
    Distinct, active rule_group values in gre_rules, optionally narrowed to
    one project_name and/or process_name -- uses gre_rules_project_process_ix
    when either filter is supplied. Returns rule_group names sorted for a
    deterministic run order; callers needing a specific order should sort
    rule_groups themselves before passing them to run_all_active_groups().
    """
    where = ["active_flag = 1"]
    params = []
    if project_name is not None:
        where.append("project_name = ?")
        params.append(project_name)
    if process_name is not None:
        where.append("process_name = ?")
        params.append(process_name)

    sql = f"""
        SELECT DISTINCT rule_group
        FROM {meta_db}.gre_rules
        WHERE {' AND '.join(where)}
        ORDER BY rule_group
    """
    rows = execute_query(meta_conn, sql, params)
    return [r["rule_group"] for r in rows]


def run_all_active_groups(
    meta_conn,
    meta_db: str,
    run_key: str,
    cf,
    project_name: str = None,
    process_name: str = None,
    triggered_by: str = "SYSTEM",
    run_params: dict = None,
    rule_variant: str = None,
) -> dict:
    """
    Discover every active rule_group in scope (optionally filtered by
    project_name/process_name) and call run_rule_group() once per group,
    against the SAME run_key/run_params/rule_variant. Each group still gets
    its own run_id, its own gre_audit row, and its own checkpoint/resume --
    this is a thin fan-out, not a merged run.

    Returns {"rule_groups": {rule_group: run_rule_group()'s own summary dict, ...}}
    so a caller can inspect or aggregate per-group outcomes; a group that
    errors doesn't stop the remaining groups from running.
    """
    rule_groups = discover_rule_groups(meta_conn, meta_db, project_name=project_name,
                                        process_name=process_name)
    logger.info(
        "run_all_active_groups: %d rule_group(s) in scope (project_name=%s process_name=%s) for run_key=%s.",
        len(rule_groups), project_name, process_name, run_key,
    )

    summaries = {}
    for rule_group in rule_groups:
        summaries[rule_group] = run_rule_group(
            rule_group, run_key, cf,
            meta_conn=meta_conn, meta_db=meta_db, triggered_by=triggered_by,
            run_params=run_params, rule_variant=rule_variant,
        )

    return {"rule_groups": summaries}
