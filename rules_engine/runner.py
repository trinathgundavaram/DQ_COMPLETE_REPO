"""
rules_engine/runner.py
-------------------------
Orchestration entry point: run_rule_group(rule_group, run_key, ...).

Single-threaded, sequential rule execution is still the DEFAULT and
requires zero config: a run-readiness gate, then a sequencing_mode-aware
pass over EVERY active rule in the group, calling rules_engine/executor.py
::execute_rule() once per rule. Every rule commits its own findings
independently -- this file only ever decides run ORDER, never
commit/rollback. A dependency graph between rules is still out of scope.

Every call always re-executes every rule for its run_key -- there is no
checkpoint/resume skip of already-succeeded rules. A rerun of the same
run_key (deliberate, or a resumed run after a crash) re-runs the whole
group; rules_engine/executor.py::_write_exceptions() reconciles
gre_exceptions.etl_is_curr_ind against each attempt's true violation set
so a record that no longer violates gets deactivated instead of staying
marked current forever from an earlier attempt. See that function's
docstring for the reconciliation rules.

Parallel execution (opt-in)
-------------------------------
Raising GRE_MAX_PARALLEL_RULES above its default of 1 (see
rules_engine/config.py) turns on a second path, used ONLY for
sequencing_mode='independent' groups: rules run concurrently across a
bounded thread pool, with per-connection concurrency additionally capped
by GRE_<TYPE>_MAX_PARALLEL so a source system that can't tolerate much
concurrent load (e.g. an OLTP Postgres source, vs. a Teradata warehouse)
isn't hit harder just because the group-wide worker count went up. See
rules_engine/parallel.py's module docstring for the connection-pooling
mechanics, and _run_pending_parallel() below for how it plugs into this
file's existing per-rule execution and gre_rule_audit bookkeeping.

sequencing_mode='sequential' groups NEVER take this path, regardless of
GRE_MAX_PARALLEL_RULES -- their whole point is a guaranteed run ORDER plus
on_failure=halt_group support, both of which are meaningless once rules
can finish out of order.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from rules_engine import config as gre_config
from rules_engine.db_ops import (
    execute_query, execute_dml,
    generate_run_id as _generate_run_id,
    count_prior_attempts as _count_prior_attempts,
)
from rules_engine.rules import load_rules
from rules_engine.executor import execute_rule, _log_error, _log_attempt
from rules_engine.parallel import build_pools, close_pools

logger = logging.getLogger(__name__)


def generate_run_id(rule_group: str, run_key: str) -> str:
    """
    Plain 2-part run_id -- delegates to rules_engine/db_ops.py::generate_run_id()
    with just (rule_group, run_key) as label parts, e.g.
    "claims_dq::BATCH_2026_08_19::20260819T143022.183045::a1b2c3". Kept as
    a public, stable-signature function so any caller already importing
    rules_engine.runner.generate_run_id keeps working unchanged.

    run_rule_group() itself does NOT call this anymore -- it builds a
    richer run_id via _build_group_run_id() below (project_name, an
    "attempt-N" label, and triggered_by folded in). Call this directly
    only if you specifically want the plain, minimal shape.
    """
    return _generate_run_id(rule_group, run_key)


def _build_group_run_id(meta_conn, meta_db: str, rule_group: str, run_key: str,
                         project_name: str, triggered_by: str) -> str:
    """
    The run_id run_rule_group() actually mints -- richer than the plain
    generate_run_id(rule_group, run_key) above, folding in three more
    things a human scanning gre_log/gre_rule_audit wants without a join:

        {project_name}.{rule_group}::{run_key}::attempt-{N}::{triggered_by}::{timestamp}::{hex}

        e.g. UM_REVIEW.claims_dq::BATCH_2026_08_19::attempt-2::jsmith::20260819T151500.500000::f9e8d7

      - "{project_name}.{rule_group}" (just rule_group if project_name is
        NULL) -- which business process this run belongs to, not only
        which rule_group, without looking gre_rules up.
      - "attempt-{N}" -- N = count_prior_attempts() + 1: this reads as
        "the 2nd attempt at this run_key" directly, instead of requiring
        a human to compare two run_ids' timestamps to work out which
        attempt came first. See count_prior_attempts()'s docstring for
        why this is a label, not the uniqueness mechanism (the trailing
        hex suffix still is).
      - triggered_by -- who/what kicked this off (a login, a scheduler
        name, "SYSTEM"), already collected as a parameter here and
        recorded on gre_rule_audit -- folding it into the id too means
        it's visible on gre_log/gre_exceptions/gre_results rows as well,
        which don't otherwise carry it.

    Underlying shape/collision-safety is entirely generate_run_id()'s --
    this only decides WHICH label parts to pass it.
    """
    attempt_no = _count_prior_attempts(meta_conn, meta_db, run_key, rule_group=rule_group) + 1
    group_label = f"{project_name}.{rule_group}" if project_name else rule_group
    return _generate_run_id(group_label, run_key, f"attempt-{attempt_no}", triggered_by)


# ---------------------------------------------------------------------------
# gre_rule_audit -- rules_engine's OWN run-tracking table (rules_engine/
# schema.sql). sampling/ never reads or writes this table; its equivalent
# is sampling/sampling.py::_write_audit() against gre_sampling_audit,
# defined in sampling/schema.sql -- the two packages share no tables at
# all, see README.md's "Package separation".
# ---------------------------------------------------------------------------

def _start_audit(meta_conn, meta_db: str, run_id: str, rule_group: str, run_key: str,
                  total_rules: int, triggered_by: str, rule_variant: str = None,
                  project_name: str = None, process_name: str = None) -> None:
    execute_dml(meta_conn, f"""
        INSERT INTO {meta_db}.gre_rule_audit (
            run_id, rule_group, project_name, process_name, run_key, rule_variant, started_at, status,
            total_rules, rules_succeeded, rules_errored, triggered_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'RUNNING', ?, 0, 0, ?)
    """, [run_id, rule_group, project_name, process_name, run_key, rule_variant,
          datetime.now(), total_rules, triggered_by])


def _finish_audit(meta_conn, meta_db: str, run_id: str, status: str,
                   rules_succeeded: int, rules_errored: int) -> None:
    execute_dml(meta_conn, f"""
        UPDATE {meta_db}.gre_rule_audit
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
    to compute the SAME cache key (identical database_name/src_tbl_nm/
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

    # A running rule holds a source-role connection AND a meta-role
    # connection SIMULTANEOUSLY for its whole execution (see
    # _run_one_pending_rule() above), so when a rule's sql_dialect and the
    # metadata connection are the SAME named connection (e.g. both
    # "teradata" -- rules_engine/config.py's META_CONNECTION default), that one
    # source needs a genuinely separate pool object for each role: sharing
    # one pool between both acquire() calls would make a single worker
    # thread try to acquire two slots from its own pool and deadlock the
    # moment pool size is smaller than 2x the concurrent workers touching
    # it. But building each role's pool independently up to the SAME
    # GRE_<NAME>_MAX_PARALLEL cap (as this used to do, unconditionally)
    # lets the two pools' sizes double real concurrent sessions against
    # that one source past the cap the env var is meant to enforce. Fix:
    # keep two separate pool objects (no deadlock), but when the names
    # collide, split that source's cap between the two roles so their
    # combined size still honors GRE_<NAME>_MAX_PARALLEL.
    meta_name = gre_config.get_meta_connection_name()
    if meta_name in source_names:
        shared_cap = max(1, gre_config.get_max_parallel_for_connection(meta_name) // 2)
        cap_override = {meta_name: shared_cap}
    else:
        cap_override = None
    source_pools = build_pools(cf, source_names, max_workers, cap_override)
    meta_pool = build_pools(cf, {meta_name}, max_workers, cap_override)[meta_name]

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
        # source_pools and meta_pool are always separate ConnectionPool
        # objects (see above -- never the same instance, even when their
        # underlying names collide), so closing both here never
        # double-closes anything.
        close_pools(source_pools)
        close_pools({meta_name: meta_pool})

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
                   (gre_exceptions_uix, gre_log, gre_results, gre_rule_audit
                   key off this value) -- a batch id, a year+month pair, a
                   specific date, or any other column/combination the
                   caller wants; build one via rules_engine/db_ops.py::
                   build_run_key() or pass your own string. Deliberately
                   NOT merged into run_params: run_params doubles as the
                   equality filters for the auto-generated total-record
                   count (see run_params below), and run_key is often NOT
                   a real column on a rule's table (e.g. a composite like
                   "2026_8"), so auto-injecting it there would silently
                   break that query for most tables. If a rule_syntax needs
                   to reference the run's tracking value, pass it
                   explicitly via run_params under whatever key matches an
                   actual column.
    cf           : a db.connection_factory.ConnectionFactory, already loaded,
                   used to resolve each rule's sql_dialect (source_type) to an adapter
    meta_conn    : adapter for the gre_ metadata store; defaults to
                   cf.get(gre_config.get_meta_connection_name())
    meta_db      : schema the gre_ tables live in; defaults to
                   gre_config.get_meta_db()
    triggered_by : freeform string recorded on gre_rule_audit
    run_params   : optional dict of named values a rule's rule_syntax can
                   reference via "{key}" tokens -- passed through exactly
                   as given, no reserved/required key. The SAME dict also
                   becomes the equality filters for the auto-generated
                   total-record count
                   (rules_engine/executor.py::_build_total_query()). Lets
                   each project scope its data however it needs
                   (month/year, run_type, a date range, a region column,
                   ...) without the engine having to know about any of
                   those column names. See rules_engine/db_ops.py::
                   _substitute_params()'s docstring.
    rule_variant : optional extra selection level on top of rule_group/
                   table -- passed straight to rules_engine.rules.load_rules()
                   (see its docstring) and recorded on gre_rule_audit for
                   this run. None (the default) loads only rules with
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

    # Logged at INFO (not DEBUG) precisely because it's exactly what's
    # needed to catch a run silently pointed at the wrong Teradata
    # environment or meta schema -- e.g. the same schema NAME existing on
    # two different Teradata hosts, one with a column the other doesn't
    # have yet. meta_conn.host is None for file/S3 adapters (no remote
    # host) -- never the case for a real meta connection, but harmless
    # either way.
    logger.info(
        "run_rule_group starting: rule_group=%s run_key=%s meta_db=%s meta_host=%s",
        rule_group, run_key, meta_db, getattr(meta_conn, "host", None),
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
            "using project_name=%s process_name=%s (from rule_id=%s, lowest seq_no) for gre_rule_audit.",
            rule_group, project_name, process_name, rules[0]["rule_id"],
        )

    run_id = _build_group_run_id(meta_conn, meta_db, rule_group, run_key, project_name, triggered_by)
    _start_audit(meta_conn, meta_db, run_id, rule_group, run_key, len(rules), triggered_by,
                 rule_variant=rule_variant, project_name=project_name, process_name=process_name)
    logger.info("Starting GRE run: %s (%d rule(s))", run_id, len(rules))

    # Every rule always re-executes for its run_key -- no more skipping
    # rules that already have a SUCCESS attempt on file. This used to be
    # a checkpoint/resume optimization (skip work already committed after
    # a crash), but it also meant a genuine, deliberate rerun of the same
    # run_key (e.g. after fixing upstream data or a rule definition)
    # silently did nothing: gre_exceptions rows from the earlier attempt
    # stayed marked etl_is_curr_ind='Y' forever, even for records that no
    # longer violate. Re-executing every rule every time lets
    # rules_engine/executor.py::_write_exceptions() reconcile
    # etl_is_curr_ind against each attempt's TRUE current violation set
    # (see that function's docstring) -- the trade-off is that resuming
    # after a mid-run crash now re-runs already-succeeded rules too,
    # instead of picking up only from the failure point.
    pending = rules

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
    # database_name/src_tbl_nm and run_params) -- one dict here, threaded
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
# rule_group" keeps its run bookkeeping simple and easy to reason about.
# Driving multiple projects/processes through the
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
    where = ["act_ind = 1"]
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
    its own run_id and its own gre_rule_audit row -- this is a thin fan-out,
    not a merged run.

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
    return _run_rule_groups(rule_groups, meta_conn, meta_db, run_key, cf,
                             triggered_by, run_params, rule_variant)


def _run_rule_groups(rule_groups, meta_conn, meta_db: str, run_key: str, cf,
                      triggered_by: str, run_params: dict, rule_variant: str) -> dict:
    """
    Shared tail of run_all_active_groups()/run_by_process_name(): run
    run_rule_group() once per already-discovered rule_group and collect
    the summaries. Factored out so run_by_process_name() -- which must
    call discover_rule_groups() itself first, to raise ValueError on an
    empty match -- can hand its result straight in here instead of making
    run_all_active_groups() re-run the identical discover_rule_groups()
    query a second time.
    """
    summaries = {}
    for rule_group in rule_groups:
        summaries[rule_group] = run_rule_group(
            rule_group, run_key, cf,
            meta_conn=meta_conn, meta_db=meta_db, triggered_by=triggered_by,
            run_params=run_params, rule_variant=rule_variant,
        )
    return {"rule_groups": summaries}


def run_by_process_name(
    process_name: str,
    run_key: str,
    cf,
    meta_conn=None,
    meta_db: str = None,
    project_name: str = None,
    triggered_by: str = "SYSTEM",
    run_params: dict = None,
    rule_variant: str = None,
) -> dict:
    """
    Thin convenience wrapper around run_all_active_groups(), scoped to one
    process_name (e.g. "UNIVERSE_VALIDATION") -- the common case of "run
    everything this process owns" without the caller having to resolve
    meta_conn/meta_db themselves first.

    process_name : which gre_rules.process_name to run every active
                   rule_group for (required -- use run_all_active_groups()
                   directly if you want every process, or discover_rule_groups()
                   if you need the list of matching rule_groups without
                   running them).
    run_key      : opaque tracking/idempotency identifier for this run --
                   see run_rule_group()'s docstring. build_run_key() in
                   rules_engine/db_ops.py can build one from parts (a batch id, a
                   year+month pair, a specific date, or any combination).
    cf           : a loaded db.connection_factory.ConnectionFactory.
    meta_conn    : metadata connection to run against; defaults to
                   cf.get(rules_engine.config.get_meta_connection_name()) if not
                   supplied, so callers with a plain ConnectionFactory don't
                   need to resolve this themselves.
    meta_db      : metadata schema/database name; defaults to
                   rules_engine.config.get_meta_db() if not supplied.
    project_name : optional further narrowing to one project within this
                   process_name; omit to run every project under it.
    run_params   : free-form dict for rule_syntax {key} substitution -- see
                   run_rule_group()'s docstring. run_key is deliberately
                   NOT merged into this.

    Returns the same {"rule_groups": {...}} shape as run_all_active_groups().
    Raises ValueError if no active rule_group matches this process_name
    (and project_name, if given) -- most likely a typo'd process_name
    rather than a legitimately empty run, so this fails loudly instead of
    silently returning an empty result.
    """
    meta_conn = meta_conn or cf.get(gre_config.get_meta_connection_name())
    meta_db = meta_db or gre_config.get_meta_db()

    rule_groups = discover_rule_groups(meta_conn, meta_db, project_name=project_name,
                                        process_name=process_name)
    if not rule_groups:
        raise ValueError(
            f"run_by_process_name: no active rule_group found for process_name={process_name!r}"
            f"{f' project_name={project_name!r}' if project_name else ''} -- check gre_rules for a typo, "
            f"or use run_all_active_groups() directly if an empty result is actually expected."
        )
    logger.info(
        "run_by_process_name: %d rule_group(s) in scope (project_name=%s process_name=%s) for run_key=%s.",
        len(rule_groups), project_name, process_name, run_key,
    )
    # Reuses the discover_rule_groups() call above instead of going through
    # run_all_active_groups() (which would re-run that identical query).
    return _run_rule_groups(rule_groups, meta_conn, meta_db, run_key, cf,
                             triggered_by, run_params, rule_variant)
