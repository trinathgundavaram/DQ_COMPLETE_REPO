import os
import time
import logging
from datetime import datetime, date

from core.query_builder import build_query, build_count_query
from core.evaluator import evaluate_rule
from utils.table_resolver import resolve_table
from utils.validation import validate_table_exists
from utils.json_builder import build_json_pk, build_pk_string
from utils.issue_logger import log_issue
from utils.logger import log_message

logger = logging.getLogger(__name__)

# Maximum exception rows captured per rule — prevents OOM on large result sets.
# Override via env var DQ_MAX_EXCEPTIONS (0 = unlimited, use with care).
MAX_EXCEPTIONS = int(os.getenv("DQ_MAX_EXCEPTIONS", "10000"))
EXCEPTION_CHUNK = 500   # rows per executemany batch


# ---------------------------------------------------------------------------
# Low-level DB helpers
# ---------------------------------------------------------------------------

def execute_query(conn, query: str) -> list:
    """Run a SELECT and return rows as list-of-dicts (keys lowercased)."""
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [c[0].lower() for c in cursor.description]
    rows = [dict(zip(columns, r)) for r in cursor.fetchall()]
    cursor.close()
    return rows


def execute_dml(conn, query: str):
    """Execute a DML statement (INSERT, UPDATE, MERGE) with no return value."""
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()


def bulk_insert(conn, sql: str, data: list):
    """Batch-insert rows in chunks using executemany."""
    if not data:
        return
    cursor = conn.cursor()
    for i in range(0, len(data), EXCEPTION_CHUNK):
        cursor.executemany(sql, data[i : i + EXCEPTION_CHUNK])
    conn.commit()
    cursor.close()


def validate_sql(db_conn, query: str):
    """Dry-run a query to catch syntax errors before full execution."""
    test = f"SELECT * FROM ({query}) t WHERE 1=0"
    cursor = db_conn.cursor()
    cursor.execute(test)
    cursor.close()


# ---------------------------------------------------------------------------
# Rule execution
# ---------------------------------------------------------------------------

def execute_rule(rule: dict, db_conn, td_conn, run: dict, meta_db: str) -> str:
    """
    Execute one DQ rule end-to-end.

    Parameters
    ----------
    rule    : rule dict from dq_rules
    db_conn : source system connection  (data queries)
    td_conn : Teradata metadata connection (writes to dq_* tables)
              Must be a DEDICATED connection — never shared across threads.
    run     : run context dict
    meta_db : metadata schema name

    Returns
    -------
    Status string: "PASS" | "FAIL" | "WARN" | "ERROR" | "SKIP"
    """
    start = time.time()

    # ── STEP 0: Source preparation ────────────────────────────────────────────
    # For FileAdapter: loads the CSV/Excel file into DuckDB (idempotent, thread-safe).
    # For all DB adapters: no-op.
    if hasattr(db_conn, "prepare"):
        try:
            db_conn.prepare(rule)
        except Exception as exc:
            log_issue(td_conn, run, rule, "CONFIG_ERROR",
                      f"Source preparation failed: {exc}", str(exc), meta_db=meta_db)
            log_message(td_conn, run["run_id"], "ERROR",
                        f"Source preparation failed for rule {rule.get('rule_code')}",
                        rule_id=rule.get("rule_id"), rule_code=rule.get("rule_code"),
                        error_code="SOURCE_PREP", error_detail=str(exc), meta_db=meta_db)
            return "ERROR"

    # ── STEP 1: Table validation ──────────────────────────────────────────────
    if not validate_table_exists(db_conn, td_conn, rule, run, log_issue, log_message, meta_db):
        return "SKIP"

    table = resolve_table(rule)
    query = build_query(rule, run)

    # ── STEP 2: SQL syntax validation ─────────────────────────────────────────
    try:
        validate_sql(db_conn, query)
    except Exception as exc:
        log_issue(td_conn, run, rule, "SQL_SYNTAX",
                  "Invalid rule SQL syntax", str(exc), meta_db=meta_db)
        log_message(td_conn, run["run_id"], "ERROR",
                    f"SQL validation failed for rule {rule.get('rule_code')}",
                    rule_id=rule.get("rule_id"), rule_code=rule.get("rule_code"),
                    error_code="SQL_SYNTAX", error_detail=str(exc), meta_db=meta_db)
        return "ERROR"

    # ── STEP 3: Fetch total count (same filter scope as rule) ─────────────────
    count_query = build_count_query(rule, run)
    try:
        total = execute_query(db_conn, count_query)[0]["total_count"]
    except Exception as exc:
        log_issue(td_conn, run, rule, "DATA_RUNTIME",
                  "Count query failed", str(exc), meta_db=meta_db)
        log_message(td_conn, run["run_id"], "ERROR",
                    f"Count query failed for rule {rule.get('rule_code')}",
                    rule_id=rule.get("rule_id"), rule_code=rule.get("rule_code"),
                    error_code="DATA_RUNTIME", error_detail=str(exc), meta_db=meta_db)
        return "ERROR"

    # ── STEP 4: Fetch failed rows (with cap to avoid OOM) ─────────────────────
    try:
        failed_rows = _fetch_failed_rows(db_conn, query)
    except Exception as exc:
        log_issue(td_conn, run, rule, "DATA_RUNTIME",
                  "Runtime error during rule execution", str(exc), meta_db=meta_db)
        log_message(td_conn, run["run_id"], "ERROR",
                    f"Execution error: {rule.get('rule_code')}",
                    rule_id=rule.get("rule_id"), rule_code=rule.get("rule_code"),
                    error_code="DATA_RUNTIME", error_detail=str(exc), meta_db=meta_db)
        return "ERROR"

    failed      = len(failed_rows)
    passed      = max(total - failed, 0)
    failure_pct = round((failed / total * 100), 6) if total else 0.0
    pass_pct    = round(100 - failure_pct, 6)

    # ── STEP 5: Evaluate ──────────────────────────────────────────────────────
    status = evaluate_rule(
        total, failed,
        rule.get("threshold_pct"),
        rule.get("threshold_count"),
        rule.get("severity"),
    )

    exec_time = round(time.time() - start, 4)
    now       = datetime.utcnow()
    run_date  = now.date()
    run_month = date(run_date.year, run_date.month, 1)

    logger.info(
        "rule=%s | total=%d | failed=%d | pct=%.2f%% | status=%s | %.4fs",
        rule.get("rule_code"), total, failed, failure_pct, status, exec_time,
    )

    # ── STEP 6: Insert execution record ───────────────────────────────────────
    exec_sql = f"""
        INSERT INTO {meta_db}.dq_rule_execution (
            run_id, rule_id, rule_code, project_name, process_name,
            table_name, run_type, run_mode, batch_id, dataset_id,
            start_date, end_date,
            total_records, failed_records, passed_records,
            failure_pct, pass_pct, severity, status,
            execution_time, run_timestamp, run_date, run_month, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """
    bulk_insert(td_conn, exec_sql, [(
        run["run_id"],
        rule["rule_id"],
        rule["rule_code"],
        rule["project_name"],
        rule.get("process_name"),
        table,
        run.get("run_type"),
        run.get("run_mode"),
        run.get("batch_id"),
        run.get("dataset_id"),
        run.get("start_date"),
        run.get("end_date"),
        total,
        failed,
        passed,
        failure_pct,
        pass_pct,
        rule.get("severity"),
        status,
        exec_time,
        now,
        run_date,
        run_month,
    )])

    # ── STEP 7: Insert exceptions (chunked, capped) ───────────────────────────
    if failed > 0 and rule.get("primary_key_columns"):
        _insert_exceptions(td_conn, run, rule, table, failed_rows, meta_db)

    return status


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _fetch_failed_rows(db_conn, query: str) -> list:
    """
    Fetch failed rows with a cap at MAX_EXCEPTIONS to prevent OOM.
    Uses fetchmany for memory-efficient retrieval.
    """
    cursor = db_conn.cursor()
    cursor.execute(query)
    columns = [c[0].lower() for c in cursor.description]

    rows = []
    cap  = MAX_EXCEPTIONS if MAX_EXCEPTIONS > 0 else float("inf")

    while True:
        batch = cursor.fetchmany(EXCEPTION_CHUNK)
        if not batch:
            break
        for r in batch:
            rows.append(dict(zip(columns, r)))
            if len(rows) >= cap:
                logger.warning(
                    "Exception cap reached (%d). Remaining failures recorded in "
                    "dq_rule_execution counts but not in dq_exceptions.",
                    MAX_EXCEPTIONS,
                )
                cursor.close()
                return rows
    cursor.close()
    return rows


def _insert_exceptions(td_conn, run: dict, rule: dict, table: str,
                        failed_rows: list, meta_db: str):
    """Batch-insert exception rows into dq_exceptions."""
    exc_sql = f"""
        INSERT INTO {meta_db}.dq_exceptions (
            run_id, rule_id, rule_code, project_name, process_name,
            table_name, run_type, run_mode, batch_id, dataset_id,
            key_json, primary_key_str, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """
    exc_rows = [
        (
            run["run_id"],
            rule["rule_id"],
            rule["rule_code"],
            rule["project_name"],
            rule.get("process_name"),
            table,
            run.get("run_type"),
            run.get("run_mode"),
            run.get("batch_id"),
            run.get("dataset_id"),
            build_json_pk(rule, r),
            build_pk_string(rule, r),
        )
        for r in failed_rows
    ]
    bulk_insert(td_conn, exc_sql, exc_rows)
    logger.info("Inserted %d exceptions for rule=%s.", len(exc_rows), rule.get("rule_code"))
