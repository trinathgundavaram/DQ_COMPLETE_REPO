import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from rules_engine import config as gre_config
from rules_engine.db_ops import (
    execute_query, execute_dml,
    generate_run_id as _generate_run_id,
    count_prior_attempts as _count_prior_attempts,
    default_run_key as _default_run_key,
    build_extra_filters_clause,
)
from rules_engine.rules import load_rules
from rules_engine.executor import execute_rule, _log_error, _write_result_safe
from rules_engine.parallel import build_pools, close_pools

logger = logging.getLogger(__name__)


def _resolve_run_key(run_key: str, caller: str) -> str:
    if run_key:
        return run_key
    run_key = _default_run_key()
    logger.info("%s: no run_key passed -- defaulting to today's date, run_key=%s.", caller, run_key)
    return run_key


def _require_meta_conn(meta_conn, message: str):
    if meta_conn is None:
        raise RuntimeError(message)
    return meta_conn


def generate_run_id(rule_group: str, run_key: str) -> str:
    return _generate_run_id(rule_group, run_key)


def _build_group_run_id(meta_conn, meta_db: str, rule_group: str, run_key: str,
                         project_name: str, triggered_by: str) -> str:
    attempt_no = _count_prior_attempts(meta_conn, meta_db, run_key, rule_group=rule_group) + 1
    group_label = f"{project_name}.{rule_group}" if project_name else rule_group
    return _generate_run_id(group_label, run_key, f"attempt-{attempt_no}", triggered_by)


def _start_audit(meta_conn, meta_db: str, run_id: str, rule_group: str, run_key: str,
                  total_rules: int, triggered_by: str, rule_variant: str = None,
                  project_name: str = None, process_name: str = None,
                  run_params: dict = None, extra_filters: dict = None) -> None:
    execute_dml(meta_conn, f"""
        INSERT INTO {meta_db}.gre_rule_audit (
            run_id, rule_group, project_name, process_name, run_key, rule_variant,
            run_params, extra_filters, started_at, status,
            total_rules, rules_succeeded, rules_errored, triggered_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'RUNNING', ?, 0, 0, ?)
    """, [run_id, rule_group, project_name, process_name, run_key, rule_variant,
          json.dumps(run_params, default=str) if run_params else None,
          json.dumps(extra_filters, default=str) if extra_filters else None,
          datetime.now(), total_rules, triggered_by], log_params=True)


def _finish_audit(meta_conn, meta_db: str, run_id: str, status: str,
                   rules_succeeded: int, rules_errored: int) -> None:
    execute_dml(meta_conn, f"""
        UPDATE {meta_db}.gre_rule_audit
        SET ended_at = ?, status = ?, rules_succeeded = ?, rules_errored = ?
        WHERE run_id = ?
    """, [datetime.now(), status, rules_succeeded, rules_errored, run_id], log_params=True)


def _deactivate_all_active_for_run(meta_conn, meta_db: str, rule_group: str, run_key: str,
                                    current_run_id: str) -> None:
    try:
        execute_dml(
            meta_conn,
            f"""
            UPDATE {meta_db}.gre_results
            SET active_ind = 'N', last_updated_datetime = CURRENT_TIMESTAMP
            WHERE rule_group = ? AND run_key = ? AND run_id <> ? AND active_ind = 'Y'
            """,
            [rule_group, run_key, current_run_id],
            log_params=True,
        )
    except Exception as exc:
        logger.error(
            "Failed to blanket-deactivate prior gre_results rows for rule_group=%s run_key=%s: %s",
            rule_group, run_key, exc,
        )

    try:
        execute_dml(
            meta_conn,
            f"""
            UPDATE {meta_db}.gre_rule_errors
            SET active_ind = 'N', last_updated_datetime = CURRENT_TIMESTAMP
            WHERE rule_group = ? AND run_key = ? AND run_id <> ? AND active_ind = 'Y'
            """,
            [rule_group, run_key, current_run_id],
            log_params=True,
        )
    except Exception as exc:
        logger.error(
            "Failed to blanket-deactivate prior gre_rule_errors rows for rule_group=%s run_key=%s: %s",
            rule_group, run_key, exc,
        )

    try:
        execute_dml(
            meta_conn,
            f"""
            UPDATE {meta_db}.gre_exceptions
            SET etl_is_curr_ind = 'N', last_updated_by = 'SYSTEM', last_updated_datetime = CURRENT_TIMESTAMP
            WHERE rule_group = ? AND run_key = ? AND run_id <> ? AND etl_is_curr_ind = 'Y'
            """,
            [rule_group, run_key, current_run_id],
            log_params=True,
        )
    except Exception as exc:
        logger.error(
            "Failed to blanket-deactivate prior gre_exceptions rows for rule_group=%s run_key=%s: %s",
            rule_group, run_key, exc,
        )


def _run_one_pending_rule(rule, source_pools, meta_pool, meta_db, run_id, run_key, resolved_params, total_cache,
                          extra_filters=None, text_params=None, extra_filters_sql=None):
    source_pool = source_pools[rule["sql_dialect"]]
    db_conn = source_pool.acquire()
    worker_meta_conn = meta_pool.acquire()
    try:
        status = execute_rule(rule, db_conn, worker_meta_conn, run_id, run_key, resolved_params, meta_db,
                              total_cache=total_cache, extra_filters=extra_filters, text_params=text_params,
                              extra_filters_sql=extra_filters_sql)
    finally:
        source_pool.release(db_conn)
        meta_pool.release(worker_meta_conn)
    return rule["rule_id"], status


def _run_pending_parallel(pending, cf, meta_conn, meta_db, run_id, run_key, resolved_params,
                          total_cache, max_workers, extra_filters=None, text_params=None, extra_filters_sql=None):
    results = {}
    succeeded = 0
    errored = 0

    unavailable = []
    runnable = []
    source_names = set()
    for rule in pending:
        source_names.add(rule["sql_dialect"])


    meta_name = gre_config.get_meta_connection_name()
    if meta_name in source_names:
        shared_cap = max(1, gre_config.get_max_parallel_for_connection(meta_name) // 2)
        cap_override = {meta_name: shared_cap}
    else:
        cap_override = None
    source_pools = build_pools(cf, source_names, max_workers, cap_override)
    meta_pool = build_pools(cf, {meta_name}, max_workers, cap_override)[meta_name]

    try:


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
                _write_result_safe(meta_conn, meta_db, run_id, rule, run_key, time.time(), "ERROR",
                            error_message=f"No pooled connection '{gre_config.get_meta_connection_name()}' for parallel execution")
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
            _write_result_safe(meta_conn, meta_db, run_id, rule, run_key, time.time(), "ERROR",
                        error_message=f"No connection '{rule['sql_dialect']}'",
                        executed_sql=rule.get("rule_syntax"))
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
                        run_id, run_key, resolved_params, total_cache, extra_filters, text_params,
                        extra_filters_sql,
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
        close_pools({meta_name: meta_pool})

    return results, succeeded, errored


def run_rule_group(
    rule_group: str,
    run_key: str = None,
    cf=None,
    meta_conn=None,
    meta_db: str = None,
    triggered_by: str = "SYSTEM",
    run_params: dict = None,
    rule_variant: str = None,
    extra_filters: dict = None,
    text_params: dict = None,
) -> dict:
    if cf is None:
        raise RuntimeError("run_rule_group() needs a loaded ConnectionFactory (cf=...).")

    run_key = _resolve_run_key(run_key, "run_rule_group")

    meta_db = meta_db or gre_config.get_meta_db()
    meta_conn = meta_conn or cf.get(gre_config.get_meta_connection_name())
    meta_conn = _require_meta_conn(
        meta_conn, f"Metadata connection '{gre_config.get_meta_connection_name()}' unavailable."
    )


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


    try:
        extra_filters_sql = build_extra_filters_clause(extra_filters)
    except ValueError as exc:
        logger.error(
            "rule_group=%s: extra_filters=%s rejected -- failing this run before any rule executes: %s",
            rule_group, extra_filters, exc,
        )
        return {
            "run_id": None, "status": "INVALID_EXTRA_FILTERS", "total_rules": len(rules),
            "succeeded": 0, "errored": 0, "results": {}, "error": str(exc),
        }

    resolved_params = dict(run_params or {})


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
                 rule_variant=rule_variant, project_name=project_name, process_name=process_name,
                 run_params={**resolved_params, **(text_params or {})}, extra_filters=extra_filters)


    _deactivate_all_active_for_run(meta_conn, meta_db, rule_group, run_key, run_id)

    logger.info("Starting GRE run: %s (%d rule(s))", run_id, len(rules))


    pending = rules


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


    total_cache = {}


    max_workers = min(gre_config.get_max_parallel_rules(), len(pending)) if pending else 1
    use_parallel = sequencing_mode == "independent" and max_workers > 1

    if use_parallel:
        results, succeeded, errored = _run_pending_parallel(
            pending, cf, meta_conn, meta_db, run_id, run_key, resolved_params,
            total_cache, max_workers, extra_filters=extra_filters, text_params=text_params,
            extra_filters_sql=extra_filters_sql,
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
                _write_result_safe(meta_conn, meta_db, run_id, rule, run_key, time.time(), "ERROR",
                            error_message=f"No connection '{rule['sql_dialect']}'",
                            executed_sql=rule.get("rule_syntax"))
                status = "ERROR"
            else:
                status = execute_rule(rule, db_conn, meta_conn, run_id, run_key, resolved_params, meta_db,
                                      total_cache=total_cache, extra_filters=extra_filters, text_params=text_params,
                                      extra_filters_sql=extra_filters_sql)

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


def discover_rule_groups(meta_conn, meta_db: str, project_name: str = None,
                          process_name: str = None) -> list:
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
    rows = execute_query(meta_conn, sql, params, log_params=True)
    return [r["rule_group"] for r in rows]


def run_all_active_groups(
    meta_conn,
    meta_db: str,
    run_key: str = None,
    cf=None,
    project_name: str = None,
    process_name: str = None,
    triggered_by: str = "SYSTEM",
    run_params: dict = None,
    rule_variant: str = None,
    extra_filters: dict = None,
    text_params: dict = None,
) -> dict:
    run_key = _resolve_run_key(run_key, "run_all_active_groups")


    meta_conn = _require_meta_conn(
        meta_conn,
        f"Metadata connection '{gre_config.get_meta_connection_name()}' unavailable -- "
        "check its credentials/env vars (see the log above for the specific connection "
        "error) before retrying.",
    )

    rule_groups = discover_rule_groups(meta_conn, meta_db, project_name=project_name,
                                        process_name=process_name)
    logger.info(
        "run_all_active_groups: %d rule_group(s) in scope (project_name=%s process_name=%s) for run_key=%s.",
        len(rule_groups), project_name, process_name, run_key,
    )
    return _run_rule_groups(rule_groups, meta_conn, meta_db, run_key, cf,
                             triggered_by, run_params, rule_variant, extra_filters, text_params)


def _run_rule_groups(rule_groups, meta_conn, meta_db: str, run_key: str, cf,
                      triggered_by: str, run_params: dict, rule_variant: str,
                      extra_filters: dict = None, text_params: dict = None) -> dict:
    summaries = {}
    for rule_group in rule_groups:
        summaries[rule_group] = run_rule_group(
            rule_group, run_key, cf,
            meta_conn=meta_conn, meta_db=meta_db, triggered_by=triggered_by,
            run_params=run_params, rule_variant=rule_variant, extra_filters=extra_filters,
            text_params=text_params,
        )
    return {"rule_groups": summaries}


def run_by_process_name(
    process_name: str,
    run_key: str = None,
    cf=None,
    meta_conn=None,
    meta_db: str = None,
    project_name: str = None,
    triggered_by: str = "SYSTEM",
    run_params: dict = None,
    rule_variant: str = None,
    extra_filters: dict = None,
    text_params: dict = None,
) -> dict:
    if cf is None:
        raise RuntimeError("run_by_process_name() needs a loaded ConnectionFactory (cf=...).")

    run_key = _resolve_run_key(run_key, "run_by_process_name")

    meta_conn = meta_conn or cf.get(gre_config.get_meta_connection_name())
    meta_db = meta_db or gre_config.get_meta_db()


    meta_conn = _require_meta_conn(
        meta_conn,
        f"Metadata connection '{gre_config.get_meta_connection_name()}' unavailable -- "
        "check its credentials/env vars (see the log above for the specific connection "
        "error) before retrying.",
    )

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


    return _run_rule_groups(rule_groups, meta_conn, meta_db, run_key, cf,
                             triggered_by, run_params, rule_variant, extra_filters, text_params)


def run_by_scope(
    run_key: str = None,
    cf=None,
    meta_conn=None,
    meta_db: str = None,
    project_name: str = None,
    process_name: str = None,
    rule_group: str = None,
    rule_variant: str = None,
    triggered_by: str = "SYSTEM",
    run_params: dict = None,
    extra_filters: dict = None,
    text_params: dict = None,
) -> dict:
    if cf is None:
        raise RuntimeError("run_by_scope() needs a loaded ConnectionFactory (cf=...).")
    if not (project_name or process_name or rule_group):
        raise ValueError(
            "run_by_scope: at least one of project_name, process_name, or rule_group is "
            "required -- call run_all_active_groups() directly if an unscoped, engine-wide "
            "run is actually intended."
        )

    run_key = _resolve_run_key(run_key, "run_by_scope")

    meta_conn = meta_conn or cf.get(gre_config.get_meta_connection_name())
    meta_db = meta_db or gre_config.get_meta_db()


    meta_conn = _require_meta_conn(
        meta_conn,
        f"Metadata connection '{gre_config.get_meta_connection_name()}' unavailable -- "
        "check its credentials/env vars (see the log above for the specific connection "
        "error) before retrying.",
    )

    if rule_group:
        logger.info(
            "run_by_scope: rule_group=%s given -- running that group directly "
            "(project_name=%s process_name=%s rule_variant=%s -- project/process are used "
            "only for audit bookkeeping off the loaded rules, not for discovery).",
            rule_group, project_name, process_name, rule_variant,
        )
        summary = run_rule_group(
            rule_group, run_key, cf, meta_conn=meta_conn, meta_db=meta_db,
            triggered_by=triggered_by, run_params=run_params,
            rule_variant=rule_variant, extra_filters=extra_filters,
            text_params=text_params,
        )
        return {"rule_groups": {rule_group: summary}}

    rule_groups = discover_rule_groups(meta_conn, meta_db, project_name=project_name,
                                        process_name=process_name)
    if not rule_groups:
        raise ValueError(
            f"run_by_scope: no active rule_group found for project_name={project_name!r} "
            f"process_name={process_name!r} -- check gre_rules for a typo, or call "
            "run_all_active_groups() directly if an empty result is actually expected."
        )
    logger.info(
        "run_by_scope: %d rule_group(s) in scope (project_name=%s process_name=%s) for run_key=%s.",
        len(rule_groups), project_name, process_name, run_key,
    )
    return _run_rule_groups(rule_groups, meta_conn, meta_db, run_key, cf,
                             triggered_by, run_params, rule_variant, extra_filters, text_params)
