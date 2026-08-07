"""
rules_engine/executor.py
----------------
Single-rule execution engine.

A separate COUNT(*) subquery on the rule SQL gives the true failure count
even when exception rows are capped by MAX_EXCEPTIONS — failed_records /
failure_pct are always accurate.

SKIP status is recorded in dq_rule_execution (zero counts, status='SKIP')
so skipped rules are visible in metrics and the DQ score denominator is
correct.

execute_query / execute_dml accept an optional `params` argument for
parameterised ? placeholders — no f-string value injection.

Source queries (count + rule) are wrapped in a tenacity retry decorator
with exponential back-off. Retry count is controlled by
DQ_QUERY_MAX_RETRIES (default 3). Metadata writes are NOT retried, to
prevent duplicate inserts.

Check-type integration:
    execute_rule():
      - passes db_conn.source_type to build_query()
      - branches on the returned 'level':
          ROW    → original path (count + failed-count + exceptions)
          TABLE  → runs the full query; 0 rows = PASS, ≥1 = FAIL; total=1
          SCHEMA → calls _check_column_exists() against the DB catalog

evaluate_rule() (PASS/FAIL/WARN decision) lives in this file too — it has
exactly one caller (this module) so keeping it alongside execute_rule()
means the "run the query, then judge the result" logic reads top to bottom
in one place instead of two.
"""

import os
import time
import logging
from datetime import datetime, date

from rules_engine.rule_sql import build_query, build_count_query
from utils.db_helpers import resolve_table
from utils.validation import validate_table_exists
from utils.ids import build_json_pk, build_pk_string
from utils.metadata_writers import log_issue
from utils.metadata_writers import log_message
from rules_engine.rule_sql import check_dialect, DialectMismatchError, check_no_dml_ddl, UnsafeRuleSQLError

logger = logging.getLogger(__name__)

# ── Tunable constants ─────────────────────────────────────────────────────────
MAX_EXCEPTIONS  = int(os.getenv("DQ_MAX_EXCEPTIONS",   "10000"))
EXCEPTION_CHUNK = int(os.getenv("DQ_EXCEPTION_CHUNK",  "500"))
MAX_RETRIES     = int(os.getenv("DQ_QUERY_MAX_RETRIES", "3"))

# ── Optional tenacity retry ───────────────────────────────────────────────────
try:
    from tenacity import (
        retry,
        stop_after_attempt,
        wait_exponential,
        retry_if_exception_type,
    )

    def _source_retry(fn):
        """Retry wrapper for transient source-DB errors."""
        return retry(
            stop=stop_after_attempt(MAX_RETRIES),
            wait=wait_exponential(multiplier=1, min=2, max=15),
            retry=retry_if_exception_type(Exception),
            reraise=True,
        )(fn)

except ImportError:
    logger.warning(
        "tenacity not installed — source query retries disabled. "
        "Install with: pip install tenacity"
    )

    def _source_retry(fn):   # no-op passthrough
        return fn


# ---------------------------------------------------------------------------
# Low-level DB helpers
# ---------------------------------------------------------------------------

def execute_query(conn, query: str, params=None) -> list:
    """Run a SELECT and return rows as list-of-dicts (column names lowercased).

    Returns an empty list if cursor.description is None (e.g. a DML statement
    was accidentally passed) rather than crashing with TypeError.
    """
    cursor = conn.cursor()
    if params is not None:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    if cursor.description is None:
        cursor.close()
        return []
    columns = [c[0].lower() for c in cursor.description]
    rows    = [dict(zip(columns, r)) for r in cursor.fetchall()]
    cursor.close()
    return rows


def execute_dml(conn, query: str, params=None):
    """Execute a DML statement (INSERT / UPDATE / MERGE) with no return value."""
    cursor = conn.cursor()
    if params is not None:
        cursor.execute(query, params)
    else:
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
    test   = f"SELECT * FROM ({query}) dq_syntax_check WHERE 1=0"
    cursor = db_conn.cursor()
    cursor.execute(test)
    cursor.close()


# ---------------------------------------------------------------------------
# Retry-wrapped source query helpers  (fix #9)
# ---------------------------------------------------------------------------

@_source_retry
def _count_total(db_conn, count_query: str) -> int:
    """Return total in-scope record count with retry on transient errors."""
    rows = execute_query(db_conn, count_query)
    return int(rows[0]["total_count"] if rows else 0)


@_source_retry
def _count_failed(db_conn, rule_query: str) -> int:
    """
    Return the TRUE count of failed rows by running COUNT(*) on the rule query.

    Called separately from _fetch_failed_rows so the recorded failed_records
    is always accurate, even when exception capture is capped.
    """
    sql  = f"SELECT COUNT(*) AS cnt FROM ({rule_query}) dq_failed_sub"
    rows = execute_query(db_conn, sql)
    return int(rows[0]["cnt"] if rows else 0)


@_source_retry
def _run_table_check(db_conn, rule_query: str) -> int:
    """
    Run a TABLE-level check query and return the row count.
    0 rows → PASS, ≥1 rows → FAIL.
    """
    rows = execute_query(db_conn, rule_query)
    return len(rows)


@_source_retry
def _fetch_failed_rows(db_conn, query: str) -> list:
    """
    Fetch failed rows for exception capture, capped at MAX_EXCEPTIONS.

    Note: this count may be LESS than the true failed count when the cap is hit.
    Always use _count_failed() for the authoritative failure count.
    """
    cursor  = db_conn.cursor()
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
                    "Exception cap reached (%d). Failure COUNT is still accurate "
                    "(in dq_rule_execution) but dq_exceptions only has the first %d rows.",
                    MAX_EXCEPTIONS, MAX_EXCEPTIONS,
                )
                cursor.close()
                return rows

    cursor.close()
    return rows


# ---------------------------------------------------------------------------
# SCHEMA check: COLUMN_EXISTS
# ---------------------------------------------------------------------------

def _check_column_exists(db_conn, source_type: str, rule: dict) -> bool:
    """
    Query the DB catalog to verify that check_column exists in the rule's table.

    Returns True if the column is found, False if not.
    Raises on connection/query errors (caller handles ERROR status).
    """
    col    = (rule.get("check_column") or "").strip()
    table  = (rule.get("src_tbl_nm") or "").strip()
    db_nm  = (rule.get("src_db_name") or "").strip()
    schema = (rule.get("src_schema") or "").strip()

    if not col or not table:
        raise ValueError(
            "COLUMN_EXISTS requires both check_column and src_tbl_nm to be set "
            f"(rule_code={rule.get('rule_code')})."
        )

    st = (source_type or "").lower()

    if st == "teradata":
        db_part = db_nm or schema
        sql = (
            "SELECT COUNT(*) AS cnt FROM DBC.ColumnsV "
            "WHERE UPPER(DatabaseName) = UPPER(?) "
            "AND UPPER(TableName)    = UPPER(?) "
            "AND UPPER(ColumnName)   = UPPER(?)"
        )
        rows = execute_query(db_conn, sql, [db_part, table, col])

    elif st in ("postgresql", "postgres", "aurora"):
        sc = schema or "public"
        sql = (
            "SELECT COUNT(*) AS cnt FROM information_schema.columns "
            "WHERE LOWER(table_schema) = LOWER(%s) "
            "AND LOWER(table_name)   = LOWER(%s) "
            "AND LOWER(column_name)  = LOWER(%s)"
        )
        rows = execute_query(db_conn, sql, [sc, table, col])

    elif st in ("sqlserver", "mssql"):
        sc = schema or "dbo"
        sql = (
            "SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE LOWER(TABLE_SCHEMA) = LOWER(?) "
            "AND LOWER(TABLE_NAME)   = LOWER(?) "
            "AND LOWER(COLUMN_NAME)  = LOWER(?)"
        )
        rows = execute_query(db_conn, sql, [sc, table, col])

    else:
        # DuckDB / file — query the DuckDB pragma
        view_name = table.split(".")[-1]   # strip schema prefix if any
        sql       = (
            f"SELECT COUNT(*) AS cnt FROM pragma_table_info('{view_name}') "
            f"WHERE LOWER(name) = LOWER(?)"
        )
        rows = execute_query(db_conn, sql, [col])

    cnt = int(rows[0]["cnt"] if rows else 0)
    return cnt > 0


# ---------------------------------------------------------------------------
# Pass/fail/warn decision
# ---------------------------------------------------------------------------

# Severity values (case-insensitive) that resolve a breach to WARN rather
# than FAIL. Everything NOT in this set is fail-worthy, so a project can
# invent any severity vocabulary it wants (e.g. "Compliance Flag",
# "Timeliness") without touching this module — only the "this is just a
# soft warning" set needs listing here.
_SOFT_SEVERITIES = {"WARN", "WARNING", "INFO", "NOTICE"}


def evaluate_rule(
    total: int,
    failed: int,
    threshold_pct=None,             # float | None
    threshold_count=None,           # int   | None
    severity: str = "WARN",
    require_rows: bool = False,     # True -> empty table is a breach
    threshold_operator: str = "OR", # 'OR' | 'AND'
) -> str:
    """
    Decide PASS / FAIL / WARN for one rule execution.

    1. total == 0, require_rows=False -> PASS  (no data; not required)
    2. total == 0, require_rows=True  -> FAIL/WARN  (data was expected)
    3. failed == 0                     -> PASS
    4. threshold_operator='OR'  (default) -> breach if pct OR count exceeded
    5. threshold_operator='AND'            -> breach only if BOTH exceeded
    6. No thresholds configured             -> any failure is a breach
    7. Thresholds configured but not met     -> PASS
    """
    severity           = (severity           or "WARN").upper()
    threshold_operator = (threshold_operator or "OR" ).upper()

    if total == 0:
        if require_rows:
            outcome = "WARN" if severity in _SOFT_SEVERITIES else "FAIL"
            logger.info("total=0 with require_rows=True -> %s.", outcome)
            return outcome
        logger.info("total=0 -- PASS (no data; require_rows=False).")
        return "PASS"

    if failed == 0:
        return "PASS"

    pct = (failed / total) * 100
    count_breached = (threshold_count is not None) and (failed > threshold_count)
    pct_breached   = (threshold_pct   is not None) and (pct   > threshold_pct)

    if threshold_count is not None or threshold_pct is not None:
        if threshold_operator == "AND":
            both_set = (threshold_count is not None) and (threshold_pct is not None)
            breached = (both_set and count_breached and pct_breached) if both_set \
                       else (count_breached or pct_breached)
        else:
            breached = count_breached or pct_breached
    else:
        breached = True   # no thresholds configured -- any failure is a breach

    if breached:
        outcome = "WARN" if severity in _SOFT_SEVERITIES else "FAIL"
        logger.info(
            "Breach | pct=%.2f threshold_pct=%s count=%d threshold_count=%s operator=%s severity=%s -> %s",
            pct, threshold_pct, failed, threshold_count, threshold_operator, severity, outcome,
        )
        return outcome

    logger.info(
        "No breach | pct=%.2f threshold_pct=%s count=%d threshold_count=%s operator=%s -> PASS",
        pct, threshold_pct, failed, threshold_count, threshold_operator,
    )
    return "PASS"


# ---------------------------------------------------------------------------
# Rule execution record helpers  (fix #2)
# ---------------------------------------------------------------------------

def record_rule_execution(
    td_conn,
    run: dict,
    rule: dict,
    table: str,
    total: int,
    failed: int,
    passed: int,
    failure_pct: float,
    pass_pct: float,
    status: str,
    exec_time: float,
    meta_db: str,
):
    """
    Insert one row into dq_rule_execution.

    Used for both normal results AND SKIP records so the metrics denominator
    (total_rules) always counts every rule that was attempted.
    """
    now       = datetime.utcnow()
    run_date  = now.date()
    run_month = date(run_date.year, run_date.month, 1)

    # project_name/process_name/run_type/run_mode/batch_id/dataset_id/
    # start_date/end_date are NOT stored here — they're fixed once at run
    # start and available via a JOIN to dq_run_control on run_id. rule_code/
    # severity ARE stored: they're a frozen snapshot of what the (mutable)
    # dq_rules row said at execution time, not derivable after the fact.
    sql = f"""
        INSERT INTO {meta_db}.dq_rule_execution (
            run_id, rule_id, rule_code, table_name,
            total_records, failed_records, passed_records,
            failure_pct, pass_pct, severity, status,
            execution_time, run_timestamp, run_date, run_month, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """
    bulk_insert(td_conn, sql, [(
        run["run_id"],
        rule.get("rule_id"),
        rule.get("rule_code"),
        table,
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


# ---------------------------------------------------------------------------
# Rule execution
# ---------------------------------------------------------------------------

def execute_rule(rule: dict, db_conn, td_conn, run: dict, meta_db: str) -> str:
    """
    Execute one DQ rule end-to-end.

    Parameters
    ----------
    rule    : rule dict from dq_rules
    db_conn : source-system adapter  (data queries; read-only from this function)
    td_conn : Teradata metadata adapter (writes to dq_* tables)
              Must be a DEDICATED connection — never shared across threads.
    run     : run context dict
    meta_db : metadata schema name

    Returns
    -------
    "PASS" | "FAIL" | "WARN" | "ERROR" | "SKIP"
    """
    start       = time.time()
    source_type = getattr(db_conn, "source_type", "teradata")

    # ── STEP -1: Dialect guard (Section 4/8) — fail fast, never mid-run ──────
    # Defense-in-depth: rules_engine/engine.py::_pre_validate_rules already runs this
    # same check before the run starts. This second check protects rules
    # added/changed after pre-validation ran, or executed via a path that
    # skips pre-validation (e.g. rule_tester.py single-rule harness).
    try:
        check_dialect(rule, source_type)
    except DialectMismatchError as exc:
        log_issue(td_conn, run, rule, "DIALECT_MISMATCH", str(exc), meta_db=meta_db)
        log_message(td_conn, run["run_id"], "ERROR",
                    f"Dialect mismatch for rule {rule.get('rule_code')}: {exc}",
                    rule_id=rule.get("rule_id"), rule_code=rule.get("rule_code"),
                    error_code="DIALECT_MISMATCH", error_detail=str(exc), meta_db=meta_db)
        table = rule.get("src_tbl_nm", "UNKNOWN")
        record_rule_execution(td_conn, run, rule, table, 0, 0, 0,
                              0.0, 0.0, "ERROR", round(time.time() - start, 4), meta_db)
        logger.error("Dialect mismatch — rule skipped: %s", exc, exc_info=True)
        return "ERROR"

    # ── STEP -0.5: Write-statement guard — same defense-in-depth rationale
    # as the dialect check above (rules_engine/engine.py::_pre_validate_rules
    # already ran this; this protects rules changed after pre-validation
    # or executed via a path that skips it). ─────────────────────────────
    try:
        check_no_dml_ddl(rule.get("rule_syntax") or "", rule.get("rule_code"))
    except UnsafeRuleSQLError as exc:
        log_issue(td_conn, run, rule, "UNSAFE_RULE_SQL", str(exc), meta_db=meta_db)
        log_message(td_conn, run["run_id"], "ERROR",
                    f"Unsafe rule_syntax for rule {rule.get('rule_code')}: {exc}",
                    rule_id=rule.get("rule_id"), rule_code=rule.get("rule_code"),
                    error_code="UNSAFE_RULE_SQL", error_detail=str(exc), meta_db=meta_db)
        table = rule.get("src_tbl_nm", "UNKNOWN")
        record_rule_execution(td_conn, run, rule, table, 0, 0, 0,
                              0.0, 0.0, "ERROR", round(time.time() - start, 4), meta_db)
        logger.error("Unsafe rule_syntax — rule skipped: %s", exc, exc_info=True)
        return "ERROR"

    # ── STEP 0: Source preparation ────────────────────────────────────────────
    # For FileAdapter: loads CSV/Excel into DuckDB (idempotent, thread-safe).
    # For DB adapters: no-op.
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
            logger.error("Source preparation failed for rule %s: %s",
                        rule.get("rule_code"), exc, exc_info=True)
            table = rule.get("src_tbl_nm", "UNKNOWN")
            record_rule_execution(td_conn, run, rule, table, 0, 0, 0,
                                  0.0, 0.0, "ERROR", round(time.time() - start, 4), meta_db)
            return "ERROR"

    # ── SCHEMA-level: COLUMN_EXISTS (no table existence check needed for this) ─
    ct = (rule.get("check_type") or "").strip().upper()
    if ct == "COLUMN_EXISTS":
        return _execute_schema_check(rule, db_conn, td_conn, run, meta_db,
                                     source_type, start)

    # ── STEP 1: Table validation ──────────────────────────────────────────────
    if not validate_table_exists(db_conn, td_conn, rule, run, log_issue, log_message, meta_db):
        # Write a SKIP row so metrics counts every rule
        table = rule.get("src_tbl_nm", "UNKNOWN")
        record_rule_execution(td_conn, run, rule, table, 0, 0, 0,
                              0.0, 100.0, "SKIP", round(time.time() - start, 4), meta_db)
        return "SKIP"

    table = resolve_table(rule)

    # ── STEP 2: Build SQL + determine level ───────────────────────────────────
    try:
        query, level = build_query(rule, run, source_type)
    except ValueError as exc:
        log_issue(td_conn, run, rule, "CONFIG_ERROR",
                  f"Rule SQL build failed: {exc}", str(exc), meta_db=meta_db)
        log_message(td_conn, run["run_id"], "ERROR",
                    f"SQL build failed for rule {rule.get('rule_code')}",
                    rule_id=rule.get("rule_id"), rule_code=rule.get("rule_code"),
                    error_code="CONFIG_ERROR", error_detail=str(exc), meta_db=meta_db)
        record_rule_execution(td_conn, run, rule, table, 0, 0, 0,
                              0.0, 0.0, "ERROR", round(time.time() - start, 4), meta_db)
        return "ERROR"

    # ── TABLE level: aggregate / freshness / volume checks ────────────────────
    if level == "TABLE":
        return _execute_table_check(
            rule, db_conn, td_conn, run, meta_db, table, query, start
        )

    # ── ROW level: standard row-by-row check ─────────────────────────────────
    return _execute_row_check(
        rule, db_conn, td_conn, run, meta_db, table, query, start
    )


# ---------------------------------------------------------------------------
# Level-specific execution paths
# ---------------------------------------------------------------------------

def _execute_schema_check(
    rule, db_conn, td_conn, run, meta_db, source_type, start
) -> str:
    """Handle COLUMN_EXISTS — checks DB catalog; no data query."""
    table = resolve_table(rule)
    try:
        exists = _check_column_exists(db_conn, source_type, rule)
    except Exception as exc:
        log_issue(td_conn, run, rule, "SCHEMA_ERROR",
                  f"COLUMN_EXISTS catalog query failed: {exc}", str(exc), meta_db=meta_db)
        log_message(td_conn, run["run_id"], "ERROR",
                    f"COLUMN_EXISTS failed for rule {rule.get('rule_code')}",
                    rule_id=rule.get("rule_id"), rule_code=rule.get("rule_code"),
                    error_code="SCHEMA_ERROR", error_detail=str(exc), meta_db=meta_db)
        logger.error("COLUMN_EXISTS catalog query failed for rule %s: %s",
                     rule.get("rule_code"), exc, exc_info=True)
        record_rule_execution(td_conn, run, rule, table, 0, 0, 0,
                              0.0, 0.0, "ERROR", round(time.time() - start, 4), meta_db)
        return "ERROR"

    failed     = 0 if exists else 1
    status     = evaluate_rule(
        total=1, failed=failed,
        threshold_pct=rule.get("threshold_pct"),
        threshold_count=rule.get("threshold_count"),
        severity=rule.get("severity"),
        require_rows=False,
        threshold_operator=rule.get("threshold_operator", "OR"),
    )
    exec_time  = round(time.time() - start, 4)

    logger.info(
        "rule=%s | SCHEMA | column_exists=%s | status=%s | %.4fs",
        rule.get("rule_code"), exists, status, exec_time,
    )
    record_rule_execution(td_conn, run, rule, table, 1, failed, 1 - failed,
                          float(failed * 100), float((1 - failed) * 100),
                          status, exec_time, meta_db)
    return status


def _execute_table_check(
    rule, db_conn, td_conn, run, meta_db, table, query, start
) -> str:
    """Handle TABLE-level checks (FRESHNESS, ROW_COUNT_*, AGGREGATE_RANGE, etc.)."""
    # TABLE checks always work on a "logical property of the table" — total = 1.
    total = 1

    # ── SQL syntax validation ─────────────────────────────────────────────────
    # (wrap in SELECT … WHERE 1=0 to force a parse-only pass)
    try:
        validate_sql(db_conn, query)
    except Exception as exc:
        log_issue(td_conn, run, rule, "SQL_SYNTAX",
                  "Invalid TABLE-check SQL", str(exc), meta_db=meta_db)
        log_message(td_conn, run["run_id"], "ERROR",
                    f"SQL validation failed for rule {rule.get('rule_code')}",
                    rule_id=rule.get("rule_id"), rule_code=rule.get("rule_code"),
                    error_code="SQL_SYNTAX", error_detail=str(exc), meta_db=meta_db)
        logger.error("Invalid TABLE-check SQL for rule %s: %s",
                     rule.get("rule_code"), exc, exc_info=True)
        record_rule_execution(td_conn, run, rule, table, 0, 0, 0,
                              0.0, 0.0, "ERROR", round(time.time() - start, 4), meta_db)
        return "ERROR"

    # ── Run the TABLE-level query ─────────────────────────────────────────────
    try:
        row_count = _run_table_check(db_conn, query)
    except Exception as exc:
        log_issue(td_conn, run, rule, "DATA_RUNTIME",
                  f"TABLE-level check query failed: {exc}", str(exc), meta_db=meta_db)
        log_message(td_conn, run["run_id"], "ERROR",
                    f"TABLE check failed for rule {rule.get('rule_code')}",
                    rule_id=rule.get("rule_id"), rule_code=rule.get("rule_code"),
                    error_code="DATA_RUNTIME", error_detail=str(exc), meta_db=meta_db)
        logger.error("TABLE-level check query failed for rule %s: %s",
                     rule.get("rule_code"), exc, exc_info=True)
        record_rule_execution(td_conn, run, rule, table, total, 0, total,
                              0.0, 100.0, "ERROR", round(time.time() - start, 4), meta_db)
        return "ERROR"

    # 0 rows returned = condition met = PASS
    # ≥1 rows returned = condition violated = FAIL (or WARN based on severity)
    failed = 1 if row_count > 0 else 0
    passed = total - failed

    failure_pct = float(failed * 100)
    pass_pct    = float(passed * 100)

    status    = evaluate_rule(
        total=total, failed=failed,
        threshold_pct=rule.get("threshold_pct"),
        threshold_count=rule.get("threshold_count"),
        severity=rule.get("severity"),
        require_rows=False,   # TABLE checks don't use require_rows
        threshold_operator=rule.get("threshold_operator", "OR"),
    )
    exec_time = round(time.time() - start, 4)

    logger.info(
        "rule=%s | TABLE | violation=%s | status=%s | %.4fs",
        rule.get("rule_code"), bool(failed), status, exec_time,
    )
    record_rule_execution(td_conn, run, rule, table, total, failed, passed,
                          failure_pct, pass_pct, status, exec_time, meta_db)
    return status


def _execute_row_check(
    rule, db_conn, td_conn, run, meta_db, table, query, start
) -> str:
    """Handle ROW-level checks (original behaviour)."""
    # ── SQL syntax validation ─────────────────────────────────────────────────
    try:
        validate_sql(db_conn, query)
    except Exception as exc:
        log_issue(td_conn, run, rule, "SQL_SYNTAX",
                  "Invalid rule SQL syntax", str(exc), meta_db=meta_db)
        log_message(td_conn, run["run_id"], "ERROR",
                    f"SQL validation failed for rule {rule.get('rule_code')}",
                    rule_id=rule.get("rule_id"), rule_code=rule.get("rule_code"),
                    error_code="SQL_SYNTAX", error_detail=str(exc), meta_db=meta_db)
        logger.error("Invalid rule SQL syntax for rule %s: %s",
                     rule.get("rule_code"), exc, exc_info=True)
        record_rule_execution(td_conn, run, rule, table, 0, 0, 0,
                              0.0, 0.0, "ERROR", round(time.time() - start, 4), meta_db)
        return "ERROR"

    # ── STEP 3: Total record count ────────────────────────────────────────────
    count_query = build_count_query(rule, run)
    try:
        total = _count_total(db_conn, count_query)
    except Exception as exc:
        log_issue(td_conn, run, rule, "DATA_RUNTIME",
                  "Count query failed", str(exc), meta_db=meta_db)
        log_message(td_conn, run["run_id"], "ERROR",
                    f"Count query failed for rule {rule.get('rule_code')}",
                    rule_id=rule.get("rule_id"), rule_code=rule.get("rule_code"),
                    error_code="DATA_RUNTIME", error_detail=str(exc), meta_db=meta_db)
        logger.error("Count query failed for rule %s: %s",
                     rule.get("rule_code"), exc, exc_info=True)
        record_rule_execution(td_conn, run, rule, table, 0, 0, 0,
                              0.0, 0.0, "ERROR", round(time.time() - start, 4), meta_db)
        return "ERROR"

    # ── STEP 4: TRUE failed-record count  (fix #1) ───────────────────────────
    try:
        failed = _count_failed(db_conn, query)
    except Exception as exc:
        log_issue(td_conn, run, rule, "DATA_RUNTIME",
                  "Failed-count query failed", str(exc), meta_db=meta_db)
        log_message(td_conn, run["run_id"], "ERROR",
                    f"Failed-count query failed for rule {rule.get('rule_code')}",
                    rule_id=rule.get("rule_id"), rule_code=rule.get("rule_code"),
                    error_code="DATA_RUNTIME", error_detail=str(exc), meta_db=meta_db)
        logger.error("Failed-count query failed for rule %s: %s",
                     rule.get("rule_code"), exc, exc_info=True)
        record_rule_execution(td_conn, run, rule, table, total, 0, total,
                              0.0, 100.0, "ERROR", round(time.time() - start, 4), meta_db)
        return "ERROR"

    passed      = max(total - failed, 0)
    failure_pct = round((failed / total * 100), 6) if total else 0.0
    pass_pct    = round(100.0 - failure_pct, 6)

    # ── STEP 5: Evaluate ──────────────────────────────────────────────────────
    status = evaluate_rule(
        total, failed,
        rule.get("threshold_pct"),
        rule.get("threshold_count"),
        rule.get("severity"),
        require_rows=bool(rule.get("require_rows", 0)),
        threshold_operator=rule.get("threshold_operator", "OR"),
    )

    exec_time = round(time.time() - start, 4)

    logger.info(
        "rule=%s | total=%d | failed=%d | pct=%.2f%% | status=%s | %.4fs",
        rule.get("rule_code"), total, failed, failure_pct, status, exec_time,
    )

    # ── STEP 6: Insert execution record ───────────────────────────────────────
    record_rule_execution(td_conn, run, rule, table, total, failed, passed,
                          failure_pct, pass_pct, status, exec_time, meta_db)

    # ── STEP 7: Capture exception rows (capped) ───────────────────────────────
    if failed > 0 and rule.get("primary_key_columns"):
        try:
            failed_rows = _fetch_failed_rows(db_conn, query)
            _insert_exceptions(td_conn, run, rule, table, failed_rows, meta_db)
        except Exception as exc:
            log_issue(td_conn, run, rule, "DATA_RUNTIME",
                      "Exception row capture failed (counts still accurate)",
                      str(exc), meta_db=meta_db)
            logger.warning(
                "Exception capture failed for rule %s (counts are still accurate): %s",
                rule.get("rule_code"), exc, exc_info=True,
            )

    return status


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _insert_exceptions(td_conn, run: dict, rule: dict, table: str,
                        failed_rows: list, meta_db: str):
    """Batch-insert captured exception rows into dq_exceptions."""
    if not failed_rows:
        return

    # Same reasoning as record_rule_execution above: project/process/
    # run_type/run_mode/batch_id/dataset_id are derivable via run_id ->
    # dq_run_control, so they're not repeated on every exception row.
    exc_sql = f"""
        INSERT INTO {meta_db}.dq_exceptions (
            run_id, rule_id, rule_code, table_name,
            key_json, primary_key_str, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """
    exc_rows = [
        (
            run["run_id"],
            rule.get("rule_id"),
            rule.get("rule_code"),
            table,
            build_json_pk(rule, r),
            build_pk_string(rule, r),
        )
        for r in failed_rows
    ]
    bulk_insert(td_conn, exc_sql, exc_rows)
    logger.info(
        "Inserted %d exception rows for rule=%s (true failed count may be higher).",
        len(exc_rows), rule.get("rule_code"),
    )
