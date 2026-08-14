"""
gre/executor.py
----------------
Single-rule execution: run one rule's negative SQL SELECT, write every
violating row to gre_exceptions through one shared path (never the rule's
own INSERT), then evaluate the rule-level threshold and upsert gre_results.

Everything here commits independently, per row and per rule -- there is no
shared transaction spanning rules, or even spanning the exception rows of
one rule. See execute_rule() for why that's safe under a crash/resume.

Low-level DB helpers (execute_query/execute_dml) and the tenacity retry
decorator are written fresh in this file rather than imported from
core/executor.py -- importing anything under core/ would violate this
project's scope boundary (only db/adapters.py and db/connection_factory.py
are reusable). The patterns are intentionally the same; the code is not
shared.
"""

import logging
import os
import time
from datetime import datetime

logger = logging.getLogger(__name__)

# ── Tunable constants ───────────────────────────────────────────────────────
MAX_RETRIES = int(os.getenv("GRE_QUERY_MAX_RETRIES", "3"))

# Severity values (case-insensitive) that resolve a breach to WARN rather
# than FAIL -- same convention as core/executor.py's _SOFT_SEVERITIES, so a
# project can invent any severity vocabulary it wants without touching code.
_SOFT_SEVERITIES = {"WARN", "WARNING", "INFO", "NOTICE"}

# ── Optional tenacity retry (mirrors core/executor.py's _source_retry) ─────
try:
    from tenacity import (
        retry,
        stop_after_attempt,
        wait_exponential,
        retry_if_exception_type,
    )

    def _source_retry(fn):
        return retry(
            stop=stop_after_attempt(MAX_RETRIES),
            wait=wait_exponential(multiplier=1, min=2, max=15),
            retry=retry_if_exception_type(Exception),
            reraise=True,
        )(fn)

except ImportError:
    logger.warning("tenacity not installed -- source query retries disabled.")

    def _source_retry(fn):
        return fn


# ---------------------------------------------------------------------------
# Low-level DB helpers
# ---------------------------------------------------------------------------

def execute_query(conn, query: str, params=None) -> list:
    """Run a SELECT and return rows as list-of-dicts (column names lowercased)."""
    cursor = conn.cursor()
    if params is not None:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    if cursor.description is None:
        cursor.close()
        return []
    columns = [c[0].lower() for c in cursor.description]
    rows = [dict(zip(columns, r)) for r in cursor.fetchall()]
    cursor.close()
    return rows


def execute_dml(conn, query: str, params=None):
    """Execute one DML statement and commit immediately -- every call is its own transaction."""
    cursor = conn.cursor()
    if params is not None:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    conn.commit()
    cursor.close()


_DUPLICATE_KEY_MARKERS = (
    "unique", "duplicate", "already exists", "constraint",
)


def _is_duplicate_key_error(exc: Exception) -> bool:
    """
    Best-effort, driver-agnostic detection of a unique-constraint violation.

    Different drivers (teradatasql, psycopg2, duckdb) raise different
    exception classes for this, so this checks the message text rather than
    a fixed set of classes -- crude, but it's the same "catch the
    duplicate-key error" mechanism dq_metrics_summary_uix relies on,
    generalised across dialects instead of hardcoded to one.
    """
    msg = str(exc).lower()
    return any(marker in msg for marker in _DUPLICATE_KEY_MARKERS)


def _insert_or_skip(conn, sql: str, params: list) -> bool:
    """
    INSERT one row; commit on success. On a duplicate-key error, roll back
    and treat it as "already committed by an earlier attempt" -- skip,
    don't overwrite. Any other error is re-raised.

    This -- not delete-then-insert -- is the idempotency mechanism, modeled
    on dq_metrics_summary_uix: a crash mid-write never leaves a half-deleted
    row, because nothing is ever deleted here.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        conn.commit()
        return True
    except Exception as exc:
        try:
            conn.commit()   # release the aborted statement so the connection stays usable
        except Exception:
            pass
        if _is_duplicate_key_error(exc):
            logger.debug("Duplicate key on insert -- already recorded, skipping. (%s)", exc)
            return False
        raise
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Batch-id token substitution (v1 batch-scoping mechanism -- see
# gre/schema.sql header for why there's no filter_column system)
# ---------------------------------------------------------------------------

def _substitute_batch_id(sql: str, batch_id: str) -> str:
    """
    Replace every literal "{batch_id}" token in `sql` with the escaped
    batch_id value. Rule authors are responsible for their own quoting
    (e.g. `WHERE batch_id = '{batch_id}'`) -- this only does the swap.
    """
    return sql.replace("{batch_id}", (batch_id or "").replace("'", "''"))


# ---------------------------------------------------------------------------
# Retry-wrapped source query
# ---------------------------------------------------------------------------

@_source_retry
def _run_source_query(db_conn, sql: str) -> list:
    return execute_query(db_conn, sql)


# ---------------------------------------------------------------------------
# Natural key
# ---------------------------------------------------------------------------

def build_natural_key(rule: dict, row: dict) -> str:
    """
    "col1=val1|col2=val2" from rule['natural_key_columns'] -- this engine's
    analog of dq_rules.primary_key_columns / utils/ids.py::build_pk_string.
    Every violating row must produce one of these; it's what makes
    gre_exceptions_uix (rule_id, batch_id, natural_key_value) meaningful.
    """
    cols = [c.strip() for c in (rule.get("natural_key_columns") or "").split(",") if c.strip()]
    if not cols:
        raise ValueError(
            f"rule_id={rule.get('rule_id')} has no natural_key_columns -- "
            "every rule must declare one to write idempotent exception rows."
        )
    # Explicit None -> 'NULL' (not just a missing-key default) so a
    # genuinely NULL key column is stable and human-readable in
    # gre_exceptions.natural_key_value, not the string "None".
    def _fmt(c):
        v = row.get(c, "NULL")
        return "NULL" if v is None else v
    return "|".join(f"{c}={_fmt(c)}" for c in cols)


# ---------------------------------------------------------------------------
# Threshold evaluation
# ---------------------------------------------------------------------------

def evaluate_threshold(
    total: int,
    failed: int,
    threshold_pct=None,
    threshold_count=None,
    threshold_operator: str = "OR",
    severity: str = "Data Validation Error",
) -> dict:
    """
    Decide the rule-level PASS/FAIL/WARN verdict and whether a gre_results
    row should be written at all.

    Returns a dict:
        status                   "PASS" | "FAIL" | "WARN"
        write_result              bool -- False means: don't insert/update
                                   gre_results for this rule+batch at all
        threshold_pct_used        effective value actually applied (or None)
        threshold_count_used      effective value actually applied (or None)
        threshold_operator_used   effective value actually applied (or None)

    Rules (mirrors core/executor.py::evaluate_rule's pct/count/operator
    logic, plus the rule-level breach semantics from the prompt):

      1. total == 0            -> PASS, write_result=False (nothing to score)
      2. threshold configured  -> breach if failure_pct > threshold_pct,
                                   or failed > threshold_count, combined per
                                   threshold_operator (OR=either, AND=both
                                   set-and-breached). Comparison is strictly
                                   greater-than -- exactly AT the threshold
                                   is still a PASS. Always write a row
                                   (PASS/FAIL/WARN) so trending is possible.
      3. no threshold configured -> fallback: breach ONLY when
                                   failed == total (every in-scope record
                                   failed). This is its OWN inclusive check,
                                   not "threshold_pct=100 with >" -- pct can
                                   never exceed 100, so that comparison would
                                   silently never fire. Write a row only when
                                   this fallback actually breaches.
    """
    severity = (severity or "").upper()
    threshold_operator = (threshold_operator or "OR").upper()

    if total == 0:
        return {
            "status": "PASS",
            "write_result": False,
            "threshold_pct_used": None,
            "threshold_count_used": None,
            "threshold_operator_used": None,
        }

    has_threshold = threshold_pct is not None or threshold_count is not None

    if has_threshold:
        pct = (failed / total) * 100
        pct_breached = threshold_pct is not None and pct > threshold_pct
        count_breached = threshold_count is not None and failed > threshold_count

        if threshold_operator == "AND":
            both_set = threshold_pct is not None and threshold_count is not None
            breached = (pct_breached and count_breached) if both_set else (pct_breached or count_breached)
        else:
            breached = pct_breached or count_breached

        status = ("WARN" if severity in _SOFT_SEVERITIES else "FAIL") if breached else "PASS"

        return {
            "status": status,
            "write_result": True,
            "threshold_pct_used": threshold_pct,
            "threshold_count_used": threshold_count,
            "threshold_operator_used": threshold_operator,
        }

    # No threshold configured -- explicit all-in-scope-failed fallback.
    breached = failed == total
    if not breached:
        return {
            "status": "PASS",
            "write_result": False,
            "threshold_pct_used": None,
            "threshold_count_used": None,
            "threshold_operator_used": None,
        }

    status = "WARN" if severity in _SOFT_SEVERITIES else "FAIL"
    return {
        "status": status,
        "write_result": True,
        "threshold_pct_used": None,
        "threshold_count_used": None,
        "threshold_operator_used": None,
    }


# ---------------------------------------------------------------------------
# gre_exceptions / gre_results writers
# ---------------------------------------------------------------------------

def _write_exceptions(meta_conn, meta_db: str, rule: dict, run_id: str, batch_id: str, rows: list) -> int:
    """
    Write every violating row to gre_exceptions via the one shared path.
    Rows whose natural key already exists for this (rule_id, batch_id) are
    silently skipped -- see _insert_or_skip(). Returns how many NEW rows
    were inserted this call (not the total on file).
    """
    sql = f"""
        INSERT INTO {meta_db}.gre_exceptions (
            run_id, rule_id, table_name, element_name, source_name,
            issue_desc, batch_id, natural_key_value
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    inserted = 0
    for row in rows:
        nk = build_natural_key(rule, row)
        issue_desc = f"Rule '{rule.get('rule_name')}' violated (natural_key={nk})"
        params = [
            run_id,
            rule.get("rule_id"),
            rule.get("table_name"),
            rule.get("element_name"),
            rule.get("source_connection"),
            issue_desc,
            batch_id,
            nk,
        ]
        if _insert_or_skip(meta_conn, sql, params):
            inserted += 1
    return inserted


def _count_exceptions(meta_conn, meta_db: str, rule_id, batch_id: str) -> int:
    """
    Authoritative failed_records: a fresh COUNT(*) against gre_exceptions
    for this (rule_id, batch_id), taken AFTER the write step. This is what
    makes a rerun correct -- rows skipped as duplicates are still counted
    exactly once, whether they were written this attempt or a prior one.
    """
    rows = execute_query(
        meta_conn,
        f"SELECT COUNT(*) AS cnt FROM {meta_db}.gre_exceptions WHERE rule_id = ? AND batch_id = ?",
        [rule_id, batch_id],
    )
    return int(rows[0]["cnt"]) if rows else 0


def _upsert_result(meta_conn, meta_db: str, row: dict) -> None:
    """
    Upsert one gre_results row for (rule_id, batch_id): INSERT, and on the
    unique-index duplicate-key error, UPDATE in place instead -- unlike
    gre_exceptions, this table is a summary row, not row-level history, so
    a rerun should overwrite it rather than accumulate duplicates.
    """
    insert_sql = f"""
        INSERT INTO {meta_db}.gre_results (
            rule_id, batch_id, run_id, total_records, failed_records,
            failure_pct, threshold_pct_used, threshold_count_used,
            threshold_operator_used, severity, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = [
        row["rule_id"], row["batch_id"], row["run_id"], row["total_records"],
        row["failed_records"], row["failure_pct"], row["threshold_pct_used"],
        row["threshold_count_used"], row["threshold_operator_used"],
        row["severity"], row["status"],
    ]
    try:
        execute_dml(meta_conn, insert_sql, params)
        return
    except Exception as exc:
        if not _is_duplicate_key_error(exc):
            raise

    update_sql = f"""
        UPDATE {meta_db}.gre_results
        SET run_id = ?, total_records = ?, failed_records = ?, failure_pct = ?,
            threshold_pct_used = ?, threshold_count_used = ?,
            threshold_operator_used = ?, severity = ?, status = ?,
            evaluated_at = CURRENT_TIMESTAMP
        WHERE rule_id = ? AND batch_id = ?
    """
    execute_dml(meta_conn, update_sql, [
        row["run_id"], row["total_records"], row["failed_records"], row["failure_pct"],
        row["threshold_pct_used"], row["threshold_count_used"], row["threshold_operator_used"],
        row["severity"], row["status"], row["rule_id"], row["batch_id"],
    ])


def _log_attempt(meta_conn, meta_db: str, run_id: str, rule: dict, batch_id: str,
                  status: str, rowcount: int, start_time: float, error_message: str = None) -> None:
    """Insert one gre_log row for this execution attempt. Never raises."""
    sql = f"""
        INSERT INTO {meta_db}.gre_log (
            run_id, rule_id, rule_group, batch_id, seq_no,
            start_time, end_time, status, rowcount, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        execute_dml(meta_conn, sql, [
            run_id, rule.get("rule_id"), rule.get("rule_group"), batch_id, rule.get("seq_no"),
            datetime.fromtimestamp(start_time), datetime.now(), status, rowcount, error_message,
        ])
    except Exception as exc:
        logger.error("Failed to write gre_log row for rule_id=%s: %s", rule.get("rule_id"), exc)


def _log_error(meta_conn, meta_db: str, run_id: str, rule: dict, batch_id: str,
                error_type: str, message: str, detail: str = None) -> None:
    """Insert one gre_errors row. Never raises -- an errors-table failure must not mask the real error."""
    sql = f"""
        INSERT INTO {meta_db}.gre_errors (
            run_id, rule_id, rule_group, batch_id, error_type, error_message, error_detail
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    try:
        execute_dml(meta_conn, sql, [
            run_id, rule.get("rule_id"), rule.get("rule_group"), batch_id, error_type, message, detail,
        ])
    except Exception as exc:
        logger.error("Failed to write gre_errors row for rule_id=%s: %s", rule.get("rule_id"), exc)


# ---------------------------------------------------------------------------
# Total in-scope record count
# ---------------------------------------------------------------------------

def _compute_total(db_conn, rule: dict, batch_id: str) -> int:
    """
    total_records = scope_sql result if the rule defines one, else
    COUNT(*) on table_name scoped to the current batch via batch_id_column
    (default 'batch_id'). Both paths go through the same {batch_id}
    substitution as rule_sql.
    """
    scope_sql = (rule.get("scope_sql") or "").strip()
    if scope_sql:
        query = _substitute_batch_id(scope_sql, batch_id)
    else:
        col = rule.get("batch_id_column") or "batch_id"
        query = _substitute_batch_id(
            f"SELECT COUNT(*) AS total_count FROM {rule['table_name']} WHERE {col} = '{{batch_id}}'",
            batch_id,
        )

    rows = _run_source_query(db_conn, query)
    if not rows:
        return 0
    first_row = rows[0]
    first_value = next(iter(first_row.values()))
    return int(first_value or 0)


# ---------------------------------------------------------------------------
# Rule execution
# ---------------------------------------------------------------------------

def execute_rule(rule: dict, db_conn, meta_conn, run_id: str, batch_id: str, meta_db: str) -> str:
    """
    Execute one rule end-to-end: run rule_sql, write every violating row to
    gre_exceptions, evaluate the rule-level threshold, upsert gre_results,
    and log the attempt.

    Parameters
    ----------
    rule      : one row from gre_rules (dict)
    db_conn   : SourceAdapter for rule['source_connection'] -- READS ONLY
    meta_conn : SourceAdapter for the gre_ metadata store -- all writes go here
    run_id    : id for this run (assigned by gre/runner.py)
    batch_id  : the batch being evaluated
    meta_db   : schema name the gre_ tables live in

    Returns
    -------
    "SUCCESS" | "ERROR"  -- an EXECUTION outcome (did the rule run without
    crashing), never the PASS/FAIL/WARN data verdict, which lives in
    gre_results. This is what gre/runner.py's on_failure logic acts on.

    Commit model
    ------------
    Every violating row commits independently (_insert_or_skip). The
    gre_results upsert and the gre_log attempt row are their own separate
    commits too. Nothing here is wrapped in one transaction, by design --
    a crash after row 500 of 1000 leaves the first 500 committed, and a
    rerun of this same rule/batch picks up exactly where it left off
    because of the gre_exceptions_uix natural-key uniqueness.
    """
    start = time.time()

    try:
        query = _substitute_batch_id(rule["rule_sql"], batch_id)
        violating_rows = _run_source_query(db_conn, query)
    except Exception as exc:
        logger.error("Rule %s: rule_sql failed: %s", rule.get("rule_id"), exc, exc_info=True)
        _log_error(meta_conn, meta_db, run_id, rule, batch_id, "SQL_RUNTIME", str(exc))
        _log_attempt(meta_conn, meta_db, run_id, rule, batch_id, "ERROR", 0, start, str(exc))
        return "ERROR"

    try:
        _write_exceptions(meta_conn, meta_db, rule, run_id, batch_id, violating_rows)
        failed = _count_exceptions(meta_conn, meta_db, rule.get("rule_id"), batch_id)
    except Exception as exc:
        logger.error("Rule %s: writing exceptions failed: %s", rule.get("rule_id"), exc, exc_info=True)
        _log_error(meta_conn, meta_db, run_id, rule, batch_id, "WRITE_FAILURE", str(exc))
        _log_attempt(meta_conn, meta_db, run_id, rule, batch_id, "ERROR", 0, start, str(exc))
        return "ERROR"

    try:
        total = _compute_total(db_conn, rule, batch_id)
    except Exception as exc:
        logger.error("Rule %s: scope/count query failed: %s", rule.get("rule_id"), exc, exc_info=True)
        _log_error(meta_conn, meta_db, run_id, rule, batch_id, "SCOPE_QUERY_FAILURE", str(exc))
        _log_attempt(meta_conn, meta_db, run_id, rule, batch_id, "ERROR", failed, start, str(exc))
        return "ERROR"

    verdict = evaluate_threshold(
        total, failed,
        threshold_pct=rule.get("threshold_pct"),
        threshold_count=rule.get("threshold_count"),
        threshold_operator=rule.get("threshold_operator", "OR"),
        severity=rule.get("severity"),
    )

    if verdict["write_result"]:
        failure_pct = round((failed / total * 100), 6) if total else 0.0
        try:
            _upsert_result(meta_conn, meta_db, {
                "rule_id": rule.get("rule_id"),
                "batch_id": batch_id,
                "run_id": run_id,
                "total_records": total,
                "failed_records": failed,
                "failure_pct": failure_pct,
                "threshold_pct_used": verdict["threshold_pct_used"],
                "threshold_count_used": verdict["threshold_count_used"],
                "threshold_operator_used": verdict["threshold_operator_used"],
                "severity": rule.get("severity"),
                "status": verdict["status"],
            })
        except Exception as exc:
            logger.error("Rule %s: gre_results upsert failed: %s", rule.get("rule_id"), exc, exc_info=True)
            _log_error(meta_conn, meta_db, run_id, rule, batch_id, "RESULTS_WRITE_FAILURE", str(exc))
            _log_attempt(meta_conn, meta_db, run_id, rule, batch_id, "ERROR", failed, start, str(exc))
            return "ERROR"

    logger.info(
        "rule_id=%s | total=%d failed=%d | verdict=%s (write_result=%s) | %.3fs",
        rule.get("rule_id"), total, failed, verdict["status"], verdict["write_result"], time.time() - start,
    )
    _log_attempt(meta_conn, meta_db, run_id, rule, batch_id, "SUCCESS", failed, start)
    return "SUCCESS"
