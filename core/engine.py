import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from config.env_config import get_meta_db
from db.connection_factory import ConnectionFactory
from core.executor import execute_query, execute_dml, execute_rule
from core.metrics import calculate_metrics
from utils.id_builder import generate_run_id
from utils.logger import log_message
from utils.alert import send_alert

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

MAX_WORKERS = 5


def run_engine(
    project: str,
    process: str,
    run_type: str,
    run_mode: str,
    batch_id: str = None,
    start_date=None,
    end_date=None,
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
    """
    meta_db = get_meta_db()

    cf = ConnectionFactory()
    cf.load()
    td = cf.get("teradata")   # primary metadata connection (main thread only)

    if td is None:
        raise RuntimeError("Teradata metadata connection unavailable. Aborting.")

    run_id     = generate_run_id(project, process, run_type, run_mode, batch_id, start_date, end_date)
    dataset_id = _build_dataset_id(run_mode, batch_id, start_date, end_date)

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
    _insert_run_control(td, run, meta_db)

    total_rules  = 0
    failed_rules = 0

    try:
        rules = _load_rules(td, project, process, meta_db)
        total_rules = len(rules)
        log_message(td, run_id, "INFO", f"{total_rules} rules loaded.", meta_db=meta_db)
        logger.info("Rules loaded: %d", total_rules)

        # ── Parallel rule execution ──────────────────────────────────────────
        # Each thread gets its OWN Teradata metadata connection so concurrent
        # writes to dq_rule_execution / dq_exceptions / dq_run_logs never
        # share connection state.
        results: dict = {}   # rule_code → status

        def run_single(rule: dict):
            source_system = (rule.get("source_system") or "teradata").lower()
            db_conn = cf.get(source_system)

            if db_conn is None:
                log_message(
                    td, run_id, "ERROR",
                    f"No DB connection for source_system='{source_system}'",
                    rule_id=rule.get("rule_id"),
                    rule_code=rule.get("rule_code"),
                    meta_db=meta_db,
                )
                return rule.get("rule_code"), "ERROR"

            # Dedicated connection for this thread's metadata writes
            td_local = cf.new_connection("teradata")
            if td_local is None:
                logger.error("Could not open per-thread metadata connection for rule %s.",
                             rule.get("rule_code"))
                return rule.get("rule_code"), "ERROR"

            try:
                status = execute_rule(rule, db_conn, td_local, run, meta_db)
                return rule.get("rule_code"), status
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
                return rule.get("rule_code"), "ERROR"
            finally:
                try:
                    td_local.close()
                except Exception:
                    pass

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(run_single, r): r for r in rules}
            for future in as_completed(futures):
                try:
                    code, status = future.result()
                    results[code] = status
                    if status in ("FAIL", "WARN", "ERROR"):
                        failed_rules += 1
                except Exception as exc:
                    rule = futures[future]
                    logger.error("Unhandled exception for rule %s: %s",
                                 rule.get("rule_code"), exc)
                    failed_rules += 1

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

        # ── Finalise ─────────────────────────────────────────────────────────
        issue_count  = _count_issues(td, run_id, meta_db)
        final_status = "COMPLETED" if failed_rules == 0 and issue_count == 0 else "COMPLETED_WITH_ISSUES"

        _update_run_control(td, run_id, final_status, meta_db)
        _send_completion_alert(run, total_rules, failed_rules, issue_count, final_status)

        logger.info("DQ run complete: %s | rules=%d failed=%d | %s",
                    run_id, total_rules, failed_rules, final_status)
        return run_id, final_status

    except Exception as exc:
        _update_run_control(td, run_id, "FAILED", meta_db)
        log_message(td, run_id, "ERROR", "Engine-level failure",
                    error_code="ENGINE_FAILURE", error_detail=str(exc), meta_db=meta_db)
        send_alert(
            f"❌ DQ RUN FAILED\n\nRun ID: {run_id}\nProject: {project}\n"
            f"Process: {process}\nRun Type: {run_type}\nMode: {run_mode}\n\nError:\n{exc}",
            "ERROR",
        )
        logger.error("Engine failure for run %s: %s", run_id, exc, exc_info=True)
        raise


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


def _insert_run_control(td, run: dict, meta_db: str):
    batch_id   = (run.get("batch_id")   or "").replace("'", "''")
    dataset_id = (run.get("dataset_id") or "").replace("'", "''")
    sql = f"""
        INSERT INTO {meta_db}.dq_run_control (
            run_id, project_name, process_name, run_type, run_mode,
            batch_id, dataset_id, start_date, end_date,
            triggered_by, start_time, status, created_at
        ) VALUES (
            '{run["run_id"]}',
            '{run["project_name"]}',
            '{run["process_name"]}',
            '{run["run_type"]}',
            '{run["run_mode"]}',
            '{batch_id}',
            '{dataset_id}',
            DATE '{run["start_date"]}',
            DATE '{run["end_date"]}',
            'SYSTEM',
            CURRENT_TIMESTAMP,
            'RUNNING',
            CURRENT_TIMESTAMP
        )
    """
    execute_dml(td, sql)
    logger.info("Run control inserted: %s", run["run_id"])


def _load_rules(td, project: str, process: str, meta_db: str) -> list:
    """
    Load all active rules for project/process.
    run_type is NOT a rule attribute — same rules run for any run_type.
    """
    sql = f"""
        SELECT *
        FROM {meta_db}.dq_rules
        WHERE project_name = '{project}'
          AND process_name = '{process}'
          AND active_flag  = 1
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
    logger.info("Run control updated: %s → %s", run_id, status)


def _send_completion_alert(run: dict, total_rules: int, failed_rules: int,
                           issue_count: int, final_status: str):
    run_id  = run["run_id"]
    project = run["project_name"]
    process = run["process_name"]

    if failed_rules > 0 or issue_count > 0:
        send_alert(
            f"⚠️ DQ RUN COMPLETED WITH ISSUES\n\n"
            f"Run ID      : {run_id}\n"
            f"Project     : {project}\n"
            f"Process     : {process}\n"
            f"Run Type    : {run['run_type']}\n"
            f"Mode        : {run['run_mode']}\n\n"
            f"Total Rules : {total_rules}\n"
            f"Failed Rules: {failed_rules}\n"
            f"Issues      : {issue_count}\n\n"
            f"Status: {final_status}",
            "WARN",
        )
    else:
        send_alert(
            f"✅ DQ RUN SUCCESS\n\n"
            f"Run ID   : {run_id}\n"
            f"Project  : {project}\n"
            f"Process  : {process}\n"
            f"Run Type : {run['run_type']}\n"
            f"Mode     : {run['run_mode']}\n\n"
            f"Total Rules: {total_rules}\n\n"
            f"Status: COMPLETED",
            "INFO",
        )
