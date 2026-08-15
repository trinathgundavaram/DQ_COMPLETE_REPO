"""
rules_engine/runner.py
-------------------------
Orchestration entry point: run_rule_group(rule_group, batch_id, ...).

Deliberately NOT a thread pool and NOT a dependency graph -- both are
explicitly out of scope for this engine (see the prompt's CODE STYLE
section). This is a single-threaded loop: batch-readiness gate, checkpoint/
resume, then a sequencing_mode-aware pass over the rules, calling
rules_engine/executor.py::execute_rule() once per rule. Every rule commits
its own findings independently -- this file only ever decides run ORDER,
never commit/rollback.
"""

import logging
import time
from datetime import datetime

from shared import config as gre_config
from shared.db_ops import execute_query, execute_dml, build_run_params
from rules_engine.rules import load_rules
from rules_engine.executor import execute_rule, _log_error, _log_attempt

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
