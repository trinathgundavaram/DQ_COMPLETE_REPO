"""
rules_engine/engine.py
--------------
Main DQ execution engine.

On startup, any run older than DQ_STALE_RUN_HOURS that is still RUNNING is
marked ABORTED (covers crashes / SIGKILL mid-run).

Metadata connection name is read from DQ_META_CONNECTION env var (default
"teradata").

Thread-pool size is read from DQ_MAX_WORKERS (default 5).

Rules are loaded with ORDER BY priority ASC, rule_id ASC — lower priority
value runs first.

Rule dependency graph via depends_on_rule_id: after a parent completes,
dependent rules are submitted dynamically. If the parent FAILS / ERRORS /
SKIPs, all dependents are auto-skipped without running. Uses
concurrent.futures.wait(FIRST_COMPLETED) for dynamic future submission.

dry_run=True validates all rule SQL and table existence but writes nothing
to the database.

A pre-validation pass runs before parallel execution, validating SQL syntax
for every rule upfront and aborting on configurable DQ_PREVALIDATE_ABORT
(default False = log errors but continue).
"""

import logging
import os
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

from config.env_config import get_meta_db
from db.connection_factory import ConnectionFactory
from rules_engine.executor import (
    execute_query,
    execute_dml,
    execute_rule,
    record_rule_execution,
    validate_sql,
)
from rules_engine.rule_sql import build_query
from rules_engine.metrics import calculate_metrics, detect_and_log as detect_anomalies
from rules_engine.rule_lifecycle import is_suppressed, record_suppressed_execution, snapshot_changed_rules
from rules_engine.profiler import profile_tables_for_run
from utils.ids import generate_run_id
from utils.db_helpers import get_scope_id, find_scope_id
from utils.metadata_writers import log_message
from utils.alert import send_alert
from rules_engine import reporting
from rules_engine.rule_sql import check_dialect, DialectMismatchError, check_no_dml_ddl, UnsafeRuleSQLError, check_query_risk

# ── Configurable log level (DQ_LOG_LEVEL overrides; default INFO) ─────────────
_log_level = getattr(logging, os.getenv("DQ_LOG_LEVEL", "INFO").upper(), logging.INFO)
logging.basicConfig(
    level=_log_level,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Configurable constants (all env-overridable) ──────────────────────────────
META_CONNECTION   = os.getenv("DQ_META_CONNECTION",   "teradata")
MAX_WORKERS       = int(os.getenv("DQ_MAX_WORKERS",   "5"))
STALE_RUN_HOURS   = int(os.getenv("DQ_STALE_RUN_HOURS", "4"))
PREVALIDATE_ABORT = os.getenv("DQ_PREVALIDATE_ABORT", "false").lower() == "true"

# ── Module-level state for SIGTERM handler ────────────────────────────────────
# Fix: keyed by a unique per-invocation token (not shared scalars) so that
# multiple run_engine() calls active concurrently in the same process (e.g.
# a warm Lambda container reused for overlapping invocations, or a service
# fanning out several runs) each get their own cleanup entry instead of
# clobbering one another. Entries are removed as soon as their run_engine()
# call finishes (success, dry-run, or failure) so a SIGTERM arriving after a
# run has already completed — e.g. an idle long-lived cron_run_scheduler()
# process — cannot re-mark that already-finished run as ABORTED.
_active_runs: dict = {}
_active_runs_lock = threading.Lock()


def _register_active(token, cf=None, td=None, meta_db=None, run_id=None):
    with _active_runs_lock:
        _active_runs[token] = {"cf": cf, "td": td, "meta_db": meta_db, "run_id": run_id}


def _set_active_run_id(token, run_id):
    with _active_runs_lock:
        entry = _active_runs.get(token)
        if entry is not None:
            entry["run_id"] = run_id


def _unregister_active(token):
    with _active_runs_lock:
        _active_runs.pop(token, None)


def _sigterm_handler(signum, frame):
    """
    Mark every currently-active run as ABORTED and close its connections on
    SIGTERM/SIGINT. Iterates a snapshot of all registered runs so concurrent
    run_engine() invocations in this process are all cleaned up correctly,
    not just whichever one happened to register last.
    """
    logger.warning("Signal %d received — marking active run(s) ABORTED and shutting down.", signum)
    with _active_runs_lock:
        snapshot = list(_active_runs.values())
    for entry in snapshot:
        run_id, td, meta_db, cf = (entry.get("run_id"), entry.get("td"),
                                    entry.get("meta_db"), entry.get("cf"))
        if run_id and td and meta_db:
            try:
                _update_run_control(td, run_id, "ABORTED", meta_db)
            except Exception as exc:
                logger.error("SIGTERM handler: failed to update run control for %s: %s", run_id, exc)
        if cf:
            try:
                cf.close_all()
            except Exception as exc:
                logger.error("SIGTERM handler: failed to close connections: %s", exc)
    raise SystemExit(1)


# Register handler for SIGTERM (k8s) and SIGINT (Ctrl-C)
signal.signal(signal.SIGTERM, _sigterm_handler)
signal.signal(signal.SIGINT,  _sigterm_handler)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_engine(
    project: str,
    process: str,
    run_type: str,
    run_mode: str,
    batch_id: str = None,
    start_date=None,
    end_date=None,
    dry_run: bool = False,
):
    """
    Main entry point for the Data Quality Engine.

    Parameters
    ----------
    project    : project_name (matches dq_rules.project_name)
    process    : process_name (matches dq_rules.process_name)
    run_type   : e.g. MONTHLY, DAILY, WEEKLY — run-level concept only
    run_mode   : BATCH | DATE | FULL
    batch_id   : batch identifier (BATCH mode)
    start_date : YYYY-MM-DD inclusive start (DATE mode)
    end_date   : YYYY-MM-DD inclusive end   (DATE mode)
    dry_run    : when True, validate rules without writing any DB results
    """
    # Unique token for this run_engine() invocation — see _active_runs above.
    token = object()

    meta_db = get_meta_db()

    cf = ConnectionFactory()
    cf.load()
    td = cf.get(META_CONNECTION)   # env-driven metadata connection name

    # Register with SIGTERM handler so shutdown can clean up
    _register_active(token, cf=cf, td=td, meta_db=meta_db)

    if td is None:
        _unregister_active(token)
        raise RuntimeError(
            f"Metadata connection '{META_CONNECTION}' unavailable. "
            "Check DQ_META_CONNECTION and its credentials."
        )

    if dry_run:
        try:
            return _dry_run(cf, project, process, run_mode, batch_id, start_date, end_date, meta_db, td)
        finally:
            cf.close_all()
            _unregister_active(token)

    run_id     = generate_run_id(project, process, run_type, run_mode, batch_id, start_date, end_date)
    dataset_id = _build_dataset_id(run_mode, batch_id, start_date, end_date)
    _set_active_run_id(token, run_id)   # expose to SIGTERM handler

    run = {
        "run_id":       run_id,
        "scope_id":     get_scope_id(td, project, process, meta_db),
        "project_name": project,
        "process_name": process,
        "run_type":     run_type,
        "run_mode":     run_mode,
        "batch_id":     batch_id,
        "dataset_id":   dataset_id,
        "start_date":   start_date or "1900-01-01",
        "end_date":     end_date   or "2099-12-31",
    }

    logger.info("Starting DQ run: %s", run_id)

    # Clean up any stale RUNNING runs before starting
    _cleanup_stale_runs(td, meta_db)
    _insert_run_control(td, run, meta_db)

    total_rules  = 0
    failed_rules = 0

    try:
        rules = _load_rules(td, project, process, meta_db)  # sorted by priority
        total_rules = len(rules)
        log_message(td, run_id, "INFO", f"{total_rules} rules loaded.", meta_db=meta_db)
        logger.info("Rules loaded: %d", total_rules)

        if not rules:
            # No active rules for this project/process is a legitimate but
            # noteworthy outcome (e.g. onboarding before rules are seeded,
            # or everything got deactivated) -- log it instead of finishing
            # silently, and return the SAME dict shape every other exit
            # path returns (callers -- CLI, Lambda, Glue, Airflow, cron --
            # all call .get() on the result; a bare tuple here would raise
            # AttributeError in every one of them).
            logger.warning(
                "No active rules found for project=%s process=%s -- run_id=%s "
                "completed with zero rules evaluated.", project, process, run_id,
            )
            log_message(td, run_id, "WARN",
                        f"No active rules found for project={project} process={process}.",
                        meta_db=meta_db)
            _update_run_control(td, run_id, "COMPLETED", meta_db)
            return {
                "run_id":             run_id,
                "status":             "COMPLETED",
                "total_rules":        0,
                "failed_rules":       0,
                "data_issue_rules":   0,
                "engine_issue_rules": 0,
                "suppressed_rules":   0,
                "issue_count":        0,
                "results":            {},
            }

        # Archive changed rule definitions before any execution begins
        try:
            snapshot_changed_rules(td, rules, meta_db)
        except Exception as exc:
            logger.warning("Rule versioning failed (non-fatal): %s", exc)

        # Pre-validation pass
        invalid_codes = _pre_validate_rules(rules, cf, run, meta_db, td)
        if invalid_codes and PREVALIDATE_ABORT:
            msg = f"Pre-validation failed for {len(invalid_codes)} rule(s): {invalid_codes}"
            logger.error(msg)
            _update_run_control(td, run_id, "FAILED", meta_db)
            send_alert(f"DQ PRE-VALIDATION ABORTED\n\nRun: {run_id}\n\n{msg}", "ERROR")
            # Same dict shape as every other return -- see the no-rules
            # branch above for why a bare tuple here would break every caller.
            return {
                "run_id":             run_id,
                "status":             "FAILED",
                "total_rules":        total_rules,
                "failed_rules":       len(invalid_codes),
                "data_issue_rules":   0,
                "engine_issue_rules": len(invalid_codes),
                "suppressed_rules":   0,
                "issue_count":        _count_issues(td, run_id, meta_db),
                "results":            {code: "ERROR" for code in invalid_codes},
            }

        # Topological sort respecting depends_on_rule_id
        ordered_rules = _topological_sort(rules)

        # ── Parallel execution with dynamic dependency submission ─────────────
        completed_by_id: dict = {}   # rule_id  → status (written after each future)
        results:         dict = {}   # rule_code → status

        def run_single(rule: dict):
            source_system = (rule.get("source_system") or META_CONNECTION).lower()
            db_conn = cf.get(source_system)

            if db_conn is None:
                log_message(
                    td, run_id, "ERROR",
                    f"No connection for source_system='{source_system}'",
                    rule_id=rule.get("rule_id"),
                    rule_code=rule.get("rule_code"),
                    meta_db=meta_db,
                )
                return rule.get("rule_code"), rule.get("rule_id"), "ERROR"

            td_local = cf.new_connection(META_CONNECTION)
            if td_local is None:
                logger.error("Could not open per-thread metadata connection for rule %s.",
                             rule.get("rule_code"))
                return rule.get("rule_code"), rule.get("rule_id"), "ERROR"

            try:
                # Check suppression before doing any data work
                suppressed, supp_reason = is_suppressed(td_local, rule, meta_db)
                if suppressed:
                    record_suppressed_execution(td_local, run, rule, meta_db, supp_reason)
                    return rule.get("rule_code"), rule.get("rule_id"), "SUPPRESSED"

                status = execute_rule(rule, db_conn, td_local, run, meta_db)
                return rule.get("rule_code"), rule.get("rule_id"), status
            except Exception as exc:
                log_message(
                    td, run_id, "ERROR",
                    f"Fatal error in rule {rule.get('rule_code')}",
                    rule_id=rule.get("rule_id"),
                    rule_code=rule.get("rule_code"),
                    error_code="THREAD_FAILURE",
                    error_detail=str(exc),
                    meta_db=meta_db,
                )
                logger.error("Thread error for rule %s: %s",
                             rule.get("rule_code"), exc, exc_info=True)
                return rule.get("rule_code"), rule.get("rule_id"), "ERROR"
            finally:
                try:
                    td_local.close()
                except Exception:
                    pass

        # Determine which rules are ready to submit (no unfinished parent)
        submitted_ids: set = set()
        active_futures: dict = {}   # future → rule

        def _is_ready(rule: dict) -> bool:
            dep = rule.get("depends_on_rule_id")
            return dep is None or dep in completed_by_id

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:

            # Initial submission: all rules with no pending dependency
            for rule in ordered_rules:
                if _is_ready(rule):
                    f = pool.submit(run_single, rule)
                    active_futures[f] = rule
                    submitted_ids.add(rule["rule_id"])

            while active_futures:
                done, _ = wait(list(active_futures.keys()), return_when=FIRST_COMPLETED)

                for future in done:
                    rule = active_futures.pop(future)
                    try:
                        code, rule_id_val, status = future.result()
                    except Exception as exc:
                        # run_single() already wraps its own body in a broad
                        # try/except, so this only fires for something that
                        # escaped that -- rare, but exactly the kind of
                        # unexpected failure that needs a full traceback and
                        # a persisted record, not just a one-line log.
                        code        = rule.get("rule_code")
                        rule_id_val = rule.get("rule_id")
                        status      = "ERROR"
                        logger.error("Unhandled exception for rule %s: %s", code, exc, exc_info=True)
                        log_message(
                            td, run_id, "ERROR",
                            f"Unhandled exception for rule {code}",
                            rule_id=rule_id_val, rule_code=code,
                            error_code="UNHANDLED_EXCEPTION", error_detail=str(exc),
                            meta_db=meta_db,
                        )
                        record_rule_execution(
                            td, run, rule, rule.get("src_tbl_nm", ""),
                            0, 0, 0, 0.0, 0.0, "ERROR", 0.0, meta_db,
                        )

                    results[code]                = status
                    completed_by_id[rule_id_val] = status

                    if status in ("FAIL", "WARN", "ERROR", "SKIP", "SUPPRESSED"):
                        failed_rules += 1

                    # Submit / auto-skip rules whose parent just completed
                    for pending_rule in ordered_rules:
                        pid = pending_rule["rule_id"]
                        if pid in submitted_ids:
                            continue
                        if not _is_ready(pending_rule):
                            continue

                        dep = pending_rule.get("depends_on_rule_id")
                        if dep is not None and completed_by_id.get(dep) in ("FAIL", "ERROR", "SKIP"):
                            # Parent failed — auto-skip dependent rule
                            dep_status = completed_by_id[dep]
                            p_code     = pending_rule.get("rule_code")
                            logger.info(
                                "Auto-skipping rule %s — parent rule_id=%d status=%s.",
                                p_code, dep, dep_status,
                            )
                            log_message(
                                td, run_id, "INFO",
                                f"Rule {p_code} auto-skipped: parent {dep} {dep_status}",
                                rule_id=pid, rule_code=p_code, meta_db=meta_db,
                            )
                            # Write SKIP execution record for the skipped rule
                            record_rule_execution(
                                td, run, pending_rule,
                                pending_rule.get("src_tbl_nm", ""),
                                0, 0, 0, 0.0, 100.0, "SKIP", 0.0, meta_db,
                            )
                            results[p_code]      = "SKIP"
                            completed_by_id[pid] = "SKIP"
                            submitted_ids.add(pid)
                            failed_rules += 1
                        else:
                            # Parent passed (or no dependency) — submit
                            f = pool.submit(run_single, pending_rule)
                            active_futures[f] = pending_rule
                            submitted_ids.add(pid)

        # Any rule never submitted has an unsatisfiable dependency -- its
        # depends_on_rule_id points at a rule_id that never appeared in this
        # run's active rule set (deactivated, deleted, wrong scope, or a
        # stale/typo'd reference), so it can never become "ready". Left
        # alone it would silently vanish from the run: no execution record,
        # no issue, not counted anywhere. Surface it as a SKIP with a clear
        # config-error issue instead.
        for rule in ordered_rules:
            rid = rule["rule_id"]
            if rid in submitted_ids:
                continue
            dep = rule.get("depends_on_rule_id")
            p_code = rule.get("rule_code")
            msg = (
                f"Rule {p_code} never ran: depends_on_rule_id={dep} does not "
                f"reference an active rule in this run (deactivated, deleted, "
                f"different scope, or invalid rule_id)."
            )
            logger.error(msg)
            log_message(
                td, run_id, "ERROR", msg,
                rule_id=rid, rule_code=p_code,
                error_code="UNRESOLVED_DEPENDENCY",
                issue_type="UNRESOLVED_DEPENDENCY", table_name=rule.get("src_tbl_nm"),
                meta_db=meta_db,
            )
            record_rule_execution(
                td, run, rule, rule.get("src_tbl_nm", ""),
                0, 0, 0, 0.0, 100.0, "SKIP", 0.0, meta_db,
            )
            results[p_code]      = "SKIP"
            completed_by_id[rid] = "SKIP"
            failed_rules += 1

        # ── Data profiling (opt-in per table via dq_profile_config) ─────────
        try:
            profile_tables_for_run(cf, td, rules, run, meta_db)
        except Exception as exc:
            logger.error("Profiling failed (non-fatal): %s", exc, exc_info=True)
            log_message(td, run_id, "ERROR", f"Profiling error: {exc}",
                        error_code="PROFILING_FAILURE", error_detail=str(exc), meta_db=meta_db)

        # ── Post-run metrics ─────────────────────────────────────────────────
        try:
            metrics = calculate_metrics(td, run, meta_db)
            if metrics.get("breached"):
                logger.warning(
                    "DQ score dropped %.2f pp below %s baseline.",
                    metrics["deviation_pct"], run["run_type"],
                )
        except Exception as exc:
            logger.error("Metrics calculation failed: %s", exc, exc_info=True)
            log_message(td, run_id, "ERROR", f"Metrics error: {exc}",
                        error_code="METRICS_FAILURE", error_detail=str(exc), meta_db=meta_db)

        # ── Statistical anomaly detection (z-score / IQR) ────────────────────
        try:
            anomalies = detect_anomalies(td, run, meta_db)
            if anomalies:
                logger.warning(
                    "%d metric anomaly/anomalies detected for run %s.",
                    len(anomalies), run_id,
                )
        except Exception as exc:
            logger.error("Anomaly detection failed (non-fatal): %s", exc, exc_info=True)
            log_message(td, run_id, "ERROR", f"Anomaly detection error: {exc}",
                        error_code="ANOMALY_FAILURE", error_detail=str(exc), meta_db=meta_db)

        # ── Finalise ─────────────────────────────────────────────────────────
        issue_count  = _count_issues(td, run_id, meta_db)
        final_status = "COMPLETED" if failed_rules == 0 and issue_count == 0 else "COMPLETED_WITH_ISSUES"

        _update_run_control(td, run_id, final_status, meta_db)

        # Section 3.5: data findings (FAIL/WARN) and engine/rule failures
        # (ERROR/SKIP) are ALWAYS split into separate notification audiences
        # — never merged into one alert. See rules_engine/reporting.py.
        data_issue_rules   = sum(1 for v in results.values() if v in ("FAIL", "WARN"))
        engine_issue_rules = sum(1 for v in results.values() if v in ("ERROR", "SKIP"))
        suppressed_rules   = sum(1 for v in results.values() if v == "SUPPRESSED")

        try:
            reporting.notify_run_completion(
                td, run, meta_db, total_rules,
                data_issue_rules, engine_issue_rules, suppressed_rules, final_status,
            )
        except Exception as exc:
            logger.error("Notification routing failed (non-fatal): %s", exc, exc_info=True)

        # Section 3.6: static, immutable per-run audit report — opt-in via
        # env var so ad-hoc/local runs don't spam the report archive.
        if os.getenv("DQ_AUTO_AUDIT_REPORT", "false").lower() == "true":
            try:
                from rules_engine.reporting import generate_report
                out_dir = os.getenv("DQ_AUDIT_REPORT_DIR", "./dq_audit_reports")
                report_path = generate_report(td, meta_db, run_id, out_dir)
                logger.info("Audit report generated: %s", report_path)
            except Exception as exc:
                logger.error("Audit report generation failed (non-fatal): %s", exc, exc_info=True)

        logger.info(
            "DQ run complete: %s | rules=%d data_issues=%d engine_issues=%d suppressed=%d | %s",
            run_id, total_rules, data_issue_rules, engine_issue_rules, suppressed_rules, final_status,
        )

        summary = {
            "run_id":             run_id,
            "status":             final_status,
            "total_rules":        total_rules,
            "failed_rules":       failed_rules,
            "data_issue_rules":   data_issue_rules,
            "engine_issue_rules": engine_issue_rules,
            "suppressed_rules":   suppressed_rules,
            "issue_count":        issue_count,
            "results":            results,   # {rule_code: status} for every rule
        }
        return summary

    except Exception as exc:
        # Always log the original failure first, and with a traceback --
        # everything below this is best-effort cleanup, and none of it
        # should be able to hide *why* the run actually failed.
        logger.error("Engine failure for run %s: %s", run_id, exc, exc_info=True)

        # Each cleanup step below is independently guarded: if the run
        # failed because the metadata connection itself is unhealthy,
        # _update_run_control()/log_message()/send_alert() can each fail
        # too -- without isolation, the first one to raise would replace
        # this except block's re-raised exception with a confusing
        # secondary one and skip the remaining steps (and the alert)
        # entirely, silently.
        try:
            _update_run_control(td, run_id, "FAILED", meta_db)
        except Exception as cleanup_exc:
            logger.error("Could not mark run %s FAILED in dq_run_control: %s",
                        run_id, cleanup_exc, exc_info=True)

        try:
            log_message(td, run_id, "ERROR", "Engine-level failure",
                        error_code="ENGINE_FAILURE", error_detail=str(exc), meta_db=meta_db)
        except Exception as cleanup_exc:
            logger.error("Could not write engine-failure log message for run %s: %s",
                        run_id, cleanup_exc, exc_info=True)

        try:
            send_alert(
                f"DQ RUN FAILED\n\nRun ID: {run_id}\nProject: {project}\n"
                f"Process: {process}\nRun Type: {run_type}\nMode: {run_mode}\n\nError:\n{exc}",
                "ERROR",
            )
        except Exception as cleanup_exc:
            logger.error("Could not send failure alert for run %s: %s",
                        run_id, cleanup_exc, exc_info=True)

        raise

    finally:
        cf.close_all()
        _unregister_active(token)


# ---------------------------------------------------------------------------
# Dry-run mode
# ---------------------------------------------------------------------------

def _dry_run(cf, project, process, run_mode, batch_id, start_date, end_date, meta_db, td):
    """
    Validate all rules without writing any DB results.

    Checks: source connection reachable, table exists, SQL parses without error.
    Returns a dict summarising which rules passed/failed validation.
    """
    logger.info("[DRY RUN] project=%s process=%s", project, process)
    rules = _load_rules(td, project, process, meta_db)
    logger.info("[DRY RUN] %d rules loaded.", len(rules))

    run_stub = {
        "run_id":       "DRY_RUN",
        "project_name": project,
        "process_name": process,
        "run_type":     "DRY_RUN",
        "run_mode":     run_mode,
        "batch_id":     batch_id,
        "dataset_id":   "DRY_RUN",
        "start_date":   start_date or "1900-01-01",
        "end_date":     end_date   or "2099-12-31",
    }

    passed, failed_list = [], []

    for rule in rules:
        code          = rule.get("rule_code")
        source_system = (rule.get("source_system") or META_CONNECTION).lower()
        db_conn       = cf.get(source_system)

        if db_conn is None:
            logger.warning("[DRY RUN] FAIL %s — no connection '%s'.", code, source_system)
            failed_list.append((code, f"no connection '{source_system}'"))
            continue

        # Table existence
        try:
            if hasattr(db_conn, "prepare"):
                db_conn.prepare(rule)
            from utils.db_helpers import resolve_table
            table  = resolve_table(rule)
            cursor = db_conn.cursor()
            cursor.execute(f"SELECT 1 FROM {table} WHERE 1=0")
            cursor.close()
        except Exception as exc:
            logger.warning("[DRY RUN] FAIL %s — table check: %s", code, exc)
            failed_list.append((code, f"table error: {exc}"))
            continue

        # SQL validation
        try:
            # build_query() always returns level="ROW" now (every rule is
            # raw negative-SQL -- see rules_engine/rule_sql.py); the level is
            # kept for API stability, not because a different branch exists.
            sql, _level = build_query(rule, run_stub,
                                      getattr(db_conn, "source_type", "teradata"))
            validate_sql(db_conn, sql)
        except Exception as exc:
            logger.warning("[DRY RUN] FAIL %s — SQL error: %s", code, exc)
            failed_list.append((code, f"SQL error: {exc}"))
            continue

        logger.info("[DRY RUN] PASS %s", code)
        passed.append(code)

    # Fix: dry-run must expose a "status" key so all execution wrappers
    # (CLI exit code, Lambda HTTP status, Glue exit code, Airflow exception)
    # can correctly detect validation failures. Any rule that failed
    # validation (bad SQL, missing table, no connection) means the dry run
    # itself did not pass — mirrors the "FAILED" status used elsewhere for
    # non-viable runs, distinct from COMPLETED_WITH_ISSUES which is reserved
    # for legitimate data-quality findings on a real run.
    status = "FAILED" if failed_list else "COMPLETED"

    summary = {
        "status":       status,
        "total":        len(rules),
        "passed":       len(passed),
        "failed":       len(failed_list),
        "failed_rules": failed_list,
    }
    logger.info("[DRY RUN] Complete — %d/%d rules validated OK.", len(passed), len(rules))
    return summary


# ---------------------------------------------------------------------------
# Pre-validation pass
# ---------------------------------------------------------------------------

def _pre_validate_rules(rules: list, cf, run: dict, meta_db: str, td) -> list:
    """
    Validate SQL syntax for every rule before execution starts.

    Returns a list of rule_codes that failed validation.
    Errors are written to dq_run_logs (issue_type set for triageable ones).
    """
    invalid = []
    logger.info("Pre-validating %d rules...", len(rules))

    for rule in rules:
        code          = rule.get("rule_code")
        source_system = (rule.get("source_system") or META_CONNECTION).lower()
        db_conn       = cf.get(source_system)

        if db_conn is None:
            invalid.append(code)
            log_message(td, run["run_id"], "ERROR",
                        f"Pre-validation: no connection '{source_system}' for rule {code}",
                        rule_id=rule.get("rule_id"), rule_code=code,
                        error_code="PREVALIDATION", meta_db=meta_db)
            continue

        source_type_val = getattr(db_conn, "source_type", "teradata")

        # Fix (Section 4/8): dialect mismatch must fail BEFORE execution —
        # checked first, distinct from generic SQL_SYNTAX issues so it is
        # unambiguous in dq_run_logs (issue_type='DIALECT_MISMATCH').
        try:
            check_dialect(rule, source_type_val)
        except DialectMismatchError as exc:
            invalid.append(code)
            log_message(td, run["run_id"], "ERROR",
                        f"Pre-validation dialect mismatch in rule {code}: {exc}",
                        rule_id=rule.get("rule_id"), rule_code=code,
                        error_code="DIALECT_MISMATCH", error_detail=str(exc),
                        issue_type="DIALECT_MISMATCH", table_name=rule.get("src_tbl_nm"),
                        meta_db=meta_db)
            continue

        # rule_syntax must be read-only — checked before it ever touches a
        # live connection (see rules_engine/rule_sql.py::check_no_dml_ddl for why a
        # generic SQL_SYNTAX failure isn't good enough: a data-modifying
        # CTE still parses as a SELECT and would otherwise only be caught
        # by validate_sql()'s dry-run below, by which point the write has
        # already happened).
        try:
            check_no_dml_ddl(rule.get("rule_syntax") or "", code)
        except UnsafeRuleSQLError as exc:
            invalid.append(code)
            log_message(td, run["run_id"], "ERROR",
                        f"Pre-validation unsafe rule_syntax in rule {code}: {exc}",
                        rule_id=rule.get("rule_id"), rule_code=code,
                        error_code="UNSAFE_RULE_SQL", error_detail=str(exc),
                        issue_type="UNSAFE_RULE_SQL", table_name=rule.get("src_tbl_nm"),
                        meta_db=meta_db)
            continue

        # Query-cost heuristics — advisory only, never blocks the run (see
        # rules_engine/rule_sql.py::check_query_risk's docstring for why).
        # Logged to dq_run_logs (issue_type='QUERY_RISK') so it's visible
        # without stopping anything; the actual protection against a
        # runaway query is executor.py's per-query timeout
        # (DQ_QUERY_TIMEOUT_SECONDS), a separate layer.
        for warning in check_query_risk(rule):
            log_message(td, run["run_id"], "WARN",
                        f"Pre-validation warning for rule {code}: {warning}",
                        rule_id=rule.get("rule_id"), rule_code=code,
                        issue_type="QUERY_RISK", table_name=rule.get("src_tbl_nm"),
                        meta_db=meta_db)
            logger.warning("Rule %s query-risk warning: %s", code, warning)

        try:
            if hasattr(db_conn, "prepare"):
                db_conn.prepare(rule)
            # build_query() always returns level="ROW" now (every rule is
            # raw negative-SQL -- see rules_engine/rule_sql.py); the level is
            # kept for API stability, not because a different branch exists.
            sql, _level = build_query(rule, run, source_type_val)
            validate_sql(db_conn, sql)
        except Exception as exc:
            invalid.append(code)
            log_message(td, run["run_id"], "ERROR",
                        f"Pre-validation SQL error in rule {code}: {exc}",
                        rule_id=rule.get("rule_id"), rule_code=code,
                        error_code="PREVALIDATION", error_detail=str(exc),
                        issue_type="SQL_SYNTAX", table_name=rule.get("src_tbl_nm"),
                        meta_db=meta_db)

    if invalid:
        logger.warning("Pre-validation: %d rule(s) failed — %s", len(invalid), invalid)
    else:
        logger.info("Pre-validation: all rules OK.")

    return invalid


# ---------------------------------------------------------------------------
# Rule ordering
# ---------------------------------------------------------------------------

def _topological_sort(rules: list) -> list:
    """
    Return rules in an order that respects depends_on_rule_id relationships
    and priority ordering within the same dependency level.

    Cycles are detected and the cyclic rule is appended at the end to
    prevent deadlock.
    """
    rule_by_id = {r["rule_id"]: r for r in rules}
    visited    = set()
    result     = []

    def visit(rule, ancestors=None):
        rid = rule["rule_id"]
        if rid in visited:
            return
        if ancestors and rid in ancestors:
            # Break the cycle here to avoid infinite recursion. The rule
            # itself is still scheduled: this call is a nested lookahead
            # from an earlier, still-on-the-stack visit() for one of its
            # own dependents, and that outer call adds it to `result` once
            # this nested call returns.
            logger.warning(
                "Dependency cycle detected at rule_id=%d (%s) — breaking cycle; "
                "check depends_on_rule_id for a circular reference.",
                rid, rule.get("rule_code"),
            )
            return
        ancestors = (ancestors or set()) | {rid}
        dep_id = rule.get("depends_on_rule_id")
        if dep_id and dep_id in rule_by_id and dep_id not in visited:
            visit(rule_by_id[dep_id], ancestors)
        visited.add(rid)
        result.append(rule)

    # Sort by priority first so within each dependency level, lower priority
    # values run first.
    for rule in sorted(rules, key=lambda r: (r.get("priority", 100), r["rule_id"])):
        visit(rule)

    return result


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _build_dataset_id(run_mode: str, batch_id, start_date, end_date) -> str:
    """Build a clean dataset_id — never returns 'None_None'."""
    mode = (run_mode or "FULL").upper()
    if mode == "BATCH" and batch_id:
        return str(batch_id)
    if mode == "DATE" and start_date and end_date:
        return f"{start_date}_{end_date}"
    return "FULL"


def _cleanup_stale_runs(td, meta_db: str):
    """
    Mark any run still RUNNING after STALE_RUN_HOURS as ABORTED.
    Protects against crashes / SIGKILL leaving phantom RUNNING rows.
    """
    try:
        execute_dml(td, f"""
            UPDATE {meta_db}.dq_run_control
            SET status   = 'ABORTED',
                end_time = CURRENT_TIMESTAMP
            WHERE status     = 'RUNNING'
              AND start_time < CURRENT_TIMESTAMP - {STALE_RUN_HOURS} * INTERVAL '1' HOUR
        """)
        logger.debug("Stale-run cleanup complete (threshold: %dh).", STALE_RUN_HOURS)
    except Exception as exc:
        logger.warning("Stale-run cleanup failed (non-fatal): %s", exc)


def _insert_run_control(td, run: dict, meta_db: str):
    sql = f"""
        INSERT INTO {meta_db}.dq_run_control (
            run_id, scope_id, run_type, run_mode,
            batch_id, dataset_id, start_date, end_date,
            triggered_by, start_time, status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'SYSTEM', CURRENT_TIMESTAMP, 'RUNNING', CURRENT_TIMESTAMP)
    """
    execute_dml(td, sql, [
        run["run_id"],
        run["scope_id"],
        run["run_type"],
        run["run_mode"],
        run.get("batch_id") or "",
        run.get("dataset_id") or "",
        run["start_date"],
        run["end_date"],
    ])
    logger.info("Run control inserted: %s", run["run_id"])


def _load_rules(td, project: str, process: str, meta_db: str) -> list:
    """
    Load active rules for project/process, ordered by priority then rule_id.
    ORDER BY priority ASC ensures lower-priority-value rules run first.

    dq_rules is keyed by scope_id, not raw project_name/process_name — the
    scope lookup is read-only (find_scope_id, not get_scope_id): a
    project/process nobody has ever run rules for simply has no scope row,
    which correctly yields zero rules rather than creating one.
    project_name/process_name are joined back in so each returned rule dict
    still exposes those keys, since a lot of downstream code (profiler
    config matching, rule_tester printouts, etc.) reads rule.get("project_name").
    """
    scope_id = find_scope_id(td, project, process, meta_db)
    if scope_id is None:
        return []
    sql = f"""
        SELECT r.*, s.project_name, s.process_name
        FROM {meta_db}.dq_rules r
        JOIN {meta_db}.dq_scope s ON s.scope_id = r.scope_id
        WHERE r.scope_id    = ?
          AND r.active_flag = 1
        ORDER BY r.priority ASC, r.rule_id ASC
    """
    return execute_query(td, sql, [scope_id])


def _count_issues(td, run_id: str, meta_db: str) -> int:
    # dq_rule_issues was folded into dq_run_logs (see utils/metadata_writers.py
    # module docstring) -- a row with issue_type set is exactly the set of
    # rows dq_rule_issues used to hold.
    rows = execute_query(
        td,
        f"SELECT COUNT(*) AS cnt FROM {meta_db}.dq_run_logs "
        f"WHERE run_id = ? AND issue_type IS NOT NULL",
        [run_id],
    )
    return rows[0]["cnt"] if rows else 0


def _update_run_control(td, run_id: str, status: str, meta_db: str):
    # run_id traces back to CLI/Lambda-event project/process/batch_id input
    # (see utils/ids.py::generate_run_id) — parameterized rather than
    # f-string interpolated so a stray quote in a project name can't ever
    # reach the SQL text, not just because it currently can't.
    execute_dml(td, f"""
        UPDATE {meta_db}.dq_run_control
        SET end_time = CURRENT_TIMESTAMP,
            status   = ?
        WHERE run_id = ?
    """, [status, run_id])
    logger.info("Run control updated: %s -> %s", run_id, status)

