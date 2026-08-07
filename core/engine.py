"""
core/engine.py
--------------
Main DQ execution engine.

Fix #4  (v2): On startup, mark any run older than DQ_STALE_RUN_HOURS that is
still RUNNING as ABORTED (covers crashes / SIGKILL mid-run).

Fix #5  (v2): Metadata connection name is read from DQ_META_CONNECTION env var
(default "teradata") — no more hardcoded string literal.

Fix #10 (v2): Thread-pool size read from DQ_MAX_WORKERS (default 5).

Fix #11 (v2): Rules loaded with ORDER BY priority ASC, rule_id ASC — lower
priority value runs first.

Fix #12 (v2): Rule dependency graph — depends_on_rule_id column.  After a
parent completes, dependent rules are submitted dynamically.  If the parent
FAILS / ERRORS / SKIPs, all dependents are auto-skipped without running.
Uses concurrent.futures.wait(FIRST_COMPLETED) for dynamic future submission.

Fix #16 (v2): dry_run=True mode — validates all rule SQL and table existence
but writes nothing to the database.

Fix #17 (v2): Pre-validation pass before parallel execution — validates SQL
syntax for every rule upfront and aborts on configurable DQ_PREVALIDATE_ABORT
(default False = log errors but continue).
"""

import logging
import os
import signal
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime

from config.env_config import get_meta_db
from db.connection_factory import ConnectionFactory
from core.executor import (
    execute_query,
    execute_dml,
    execute_rule,
    record_rule_execution,
    validate_sql,
)
from core.rule_sql import build_query
from core.metrics import calculate_metrics, detect_and_log as detect_anomalies
from core.rule_lifecycle import is_suppressed, record_suppressed_execution, snapshot_changed_rules
from core.profiler import profile_tables_for_run
from utils.ids import generate_run_id
from utils.metadata_writers import log_message
from utils.alert import send_alert
from core import reporting
from utils.validation import validate_table_exists
from utils.metadata_writers import log_issue
from core.rule_sql import check_dialect, DialectMismatchError

# ── Configurable log level (DQ_LOG_LEVEL overrides; default INFO) ─────────────
_log_level = getattr(logging, os.getenv("DQ_LOG_LEVEL", "INFO").upper(), logging.INFO)
logging.basicConfig(
    level=_log_level,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Configurable constants (all env-overridable) ──────────────────────────────
META_CONNECTION   = os.getenv("DQ_META_CONNECTION",   "teradata")  # fix #5
MAX_WORKERS       = int(os.getenv("DQ_MAX_WORKERS",   "5"))        # fix #10
STALE_RUN_HOURS   = int(os.getenv("DQ_STALE_RUN_HOURS", "4"))     # fix #4
PREVALIDATE_ABORT = os.getenv("DQ_PREVALIDATE_ABORT", "false").lower() == "true"  # fix #17

# ── Module-level state for SIGTERM handler ────────────────────────────────────
_active_run_id: str = None
_active_cf:     "ConnectionFactory" = None
_active_td      = None
_active_meta_db: str = None


def _sigterm_handler(signum, frame):
    """
    Mark the active run as ABORTED and close all connections on SIGTERM/SIGINT.
    Prevents runs being left in RUNNING state on container shutdown or k8s eviction.
    """
    logger.warning("Signal %d received — marking run ABORTED and shutting down.", signum)
    if _active_run_id and _active_td and _active_meta_db:
        try:
            _update_run_control(_active_td, _active_run_id, "ABORTED", _active_meta_db)
        except Exception as exc:
            logger.error("SIGTERM handler: failed to update run control: %s", exc)
    if _active_cf:
        try:
            _active_cf.close_all()
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
    dry_run: bool = False,    # fix #16
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
    global _active_run_id, _active_cf, _active_td, _active_meta_db

    meta_db = get_meta_db()

    cf = ConnectionFactory()
    cf.load()
    td = cf.get(META_CONNECTION)   # fix #5: env-driven metadata connection name

    # Register with SIGTERM handler so shutdown can clean up
    _active_cf     = cf
    _active_td     = td
    _active_meta_db = meta_db

    if td is None:
        raise RuntimeError(
            f"Metadata connection '{META_CONNECTION}' unavailable. "
            "Check DQ_META_CONNECTION and its credentials."
        )

    if dry_run:
        try:
            return _dry_run(cf, project, process, run_mode, batch_id, start_date, end_date, meta_db, td)
        finally:
            cf.close_all()

    run_id     = generate_run_id(project, process, run_type, run_mode, batch_id, start_date, end_date)
    dataset_id = _build_dataset_id(run_mode, batch_id, start_date, end_date)
    _active_run_id = run_id   # expose to SIGTERM handler

    run = {
        "run_id":       run_id,
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

    # fix #4: clean up any stale RUNNING runs before starting
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
            _update_run_control(td, run_id, "COMPLETED", meta_db)
            return run_id, "COMPLETED"

        # Archive changed rule definitions before any execution begins
        try:
            snapshot_changed_rules(td, rules, meta_db)
        except Exception as exc:
            logger.warning("Rule versioning failed (non-fatal): %s", exc)

        # fix #17: pre-validation pass
        invalid_codes = _pre_validate_rules(rules, cf, run, meta_db, td)
        if invalid_codes and PREVALIDATE_ABORT:
            msg = f"Pre-validation failed for {len(invalid_codes)} rule(s): {invalid_codes}"
            logger.error(msg)
            _update_run_control(td, run_id, "FAILED", meta_db)
            send_alert(f"DQ PRE-VALIDATION ABORTED\n\nRun: {run_id}\n\n{msg}", "ERROR")
            return run_id, "FAILED"

        # fix #12: topological sort respecting depends_on_rule_id
        ordered_rules = _topological_sort(rules)

        # ── Parallel execution with dynamic dependency submission (fix #12) ───
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
                        code        = rule.get("rule_code")
                        rule_id_val = rule.get("rule_id")
                        status      = "ERROR"
                        logger.error("Unhandled exception for rule %s: %s", code, exc)

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
        # — never merged into one alert. See core/reporting.py.
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
                from core.reporting import generate_report
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
        _update_run_control(td, run_id, "FAILED", meta_db)
        log_message(td, run_id, "ERROR", "Engine-level failure",
                    error_code="ENGINE_FAILURE", error_detail=str(exc), meta_db=meta_db)
        send_alert(
            f"DQ RUN FAILED\n\nRun ID: {run_id}\nProject: {project}\n"
            f"Process: {process}\nRun Type: {run_type}\nMode: {run_mode}\n\nError:\n{exc}",
            "ERROR",
        )
        logger.error("Engine failure for run %s: %s", run_id, exc, exc_info=True)
        raise

    finally:
        cf.close_all()


# ---------------------------------------------------------------------------
# Dry-run mode  (fix #16)
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
            _q, _level = build_query(rule, run_stub,
                                     getattr(db_conn, "source_type", "teradata"))
            if _level != "SCHEMA":
                validate_sql(db_conn, _q)
        except Exception as exc:
            logger.warning("[DRY RUN] FAIL %s — SQL error: %s", code, exc)
            failed_list.append((code, f"SQL error: {exc}"))
            continue

        logger.info("[DRY RUN] PASS %s", code)
        passed.append(code)

    summary = {
        "total":        len(rules),
        "passed":       len(passed),
        "failed":       len(failed_list),
        "failed_rules": failed_list,
    }
    logger.info("[DRY RUN] Complete — %d/%d rules validated OK.", len(passed), len(rules))
    return summary


# ---------------------------------------------------------------------------
# Pre-validation pass  (fix #17)
# ---------------------------------------------------------------------------

def _pre_validate_rules(rules: list, cf, run: dict, meta_db: str, td) -> list:
    """
    Validate SQL syntax for every rule before execution starts.

    Returns a list of rule_codes that failed validation.
    Errors are written to dq_run_logs and dq_rule_issues.
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
        # unambiguous in dq_rule_issues.
        try:
            check_dialect(rule, source_type_val)
        except DialectMismatchError as exc:
            invalid.append(code)
            log_message(td, run["run_id"], "ERROR",
                        f"Pre-validation dialect mismatch in rule {code}: {exc}",
                        rule_id=rule.get("rule_id"), rule_code=code,
                        error_code="DIALECT_MISMATCH", error_detail=str(exc),
                        meta_db=meta_db)
            log_issue(td, run, rule, "DIALECT_MISMATCH",
                      f"Pre-validation failed: {exc}", str(exc), meta_db=meta_db)
            continue

        try:
            if hasattr(db_conn, "prepare"):
                db_conn.prepare(rule)
            _q, _level = build_query(rule, run, source_type_val)
            if _level != "SCHEMA":
                validate_sql(db_conn, _q)
        except Exception as exc:
            invalid.append(code)
            log_message(td, run["run_id"], "ERROR",
                        f"Pre-validation SQL error in rule {code}: {exc}",
                        rule_id=rule.get("rule_id"), rule_code=code,
                        error_code="PREVALIDATION", error_detail=str(exc),
                        meta_db=meta_db)
            log_issue(td, run, rule, "SQL_SYNTAX",
                      f"Pre-validation failed: {exc}", str(exc), meta_db=meta_db)

    if invalid:
        logger.warning("Pre-validation: %d rule(s) failed — %s", len(invalid), invalid)
    else:
        logger.info("Pre-validation: all rules OK.")

    return invalid


# ---------------------------------------------------------------------------
# Rule ordering  (fix #11 + #12)
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
            logger.warning(
                "Dependency cycle detected at rule_id=%d (%s) — appending at end.",
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
    Fix #4: Mark any run still RUNNING after STALE_RUN_HOURS as ABORTED.
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
            run_id, project_name, process_name, run_type, run_mode,
            batch_id, dataset_id, start_date, end_date,
            triggered_by, start_time, status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'SYSTEM', CURRENT_TIMESTAMP, 'RUNNING', CURRENT_TIMESTAMP)
    """
    execute_dml(td, sql, [
        run["run_id"],
        run["project_name"],
        run["process_name"],
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
    fix #11: ORDER BY priority ASC ensures lower-priority-value rules run first.
    """
    sql = f"""
        SELECT *
        FROM {meta_db}.dq_rules
        WHERE project_name = '{project}'
          AND process_name = '{process}'
          AND active_flag  = 1
        ORDER BY priority ASC, rule_id ASC
    """
    return execute_query(td, sql)


def _count_issues(td, run_id: str, meta_db: str) -> int:
    rows = execute_query(
        td,
        f"SELECT COUNT(*) AS cnt FROM {meta_db}.dq_rule_issues WHERE run_id = '{run_id}'"
    )
    return rows[0]["cnt"] if rows else 0


def _update_run_control(td, run_id: str, status: str, meta_db: str):
    execute_dml(td, f"""
        UPDATE {meta_db}.dq_run_control
        SET end_time = CURRENT_TIMESTAMP,
            status   = '{status}'
        WHERE run_id = '{run_id}'
    """)
    logger.info("Run control updated: %s -> %s", run_id, status)

