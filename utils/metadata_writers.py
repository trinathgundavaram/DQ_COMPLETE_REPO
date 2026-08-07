"""
utils/metadata_writers.py
----------------------------
Writers for the two "something went wrong with the ENGINE" tables:

    log_message(...) -> dq_run_logs     — general structured run log
    log_issue(...)   -> dq_rule_issues  — non-fatal rule-level problem

Both write to `td` (the METADATA connection) — never the source connection
being validated — and use parameterised `?` placeholders (no manual SQL
escaping). meta_db is resolved lazily per call, not cached at import time,
so DQ_ENV/DQ_META_DB changes after import are respected.

Deliberately separate from dq_exceptions (utils/ids.py builds the keys
for those) — a data finding and an engine problem are never the same row,
see core/executor.py and DESIGN.md.
"""

import logging

from config.env_config import get_meta_db

logger = logging.getLogger(__name__)


def log_message(
    td,
    run_id: str,
    level: str,
    message: str,
    rule_id=None,
    rule_code=None,
    error_code: str = None,
    error_detail: str = None,
    meta_db: str = None,
):
    """Insert a structured log entry into dq_run_logs. Never raises."""
    meta_db = meta_db or get_meta_db()

    sql = f"""
        INSERT INTO {meta_db}.dq_run_logs
            (run_id, rule_id, rule_code, log_level, message,
             error_code, error_detail, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """
    try:
        cursor = td.cursor()
        cursor.execute(sql, [
            run_id, rule_id, rule_code or None, level, message,
            error_code or None, error_detail or None,
        ])
        td.commit()
        cursor.close()
    except Exception as exc:
        logger.error("Failed to insert run log: %s", exc)

    py_level = getattr(logging, level.upper(), logging.INFO)
    logger.log(py_level, "[%s] run=%s rule=%s | %s", level, run_id, rule_code, message)


def log_issue(
    td,
    run: dict,
    rule: dict,
    issue_type: str,
    message: str,
    detail: str = None,
    meta_db: str = None,
):
    """Insert a row into dq_rule_issues for a non-fatal rule-level problem. Never raises."""
    meta_db = meta_db or get_meta_db()

    sql = f"""
        INSERT INTO {meta_db}.dq_rule_issues
            (run_id, rule_id, rule_code, project_name, process_name,
             table_name, issue_type, issue_message, error_detail, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """
    try:
        cursor = td.cursor()
        cursor.execute(sql, [
            run.get("run_id") or "",
            rule.get("rule_id"),
            rule.get("rule_code") or None,
            rule.get("project_name") or None,
            rule.get("process_name") or None,
            rule.get("src_tbl_nm") or None,
            issue_type,
            message,
            detail or None,
        ])
        td.commit()
        cursor.close()
        logger.warning("[ISSUE] %s | rule=%s | %s", issue_type, rule.get("rule_code"), message)
    except Exception as exc:
        logger.error("Failed to insert issue log: %s", exc)
