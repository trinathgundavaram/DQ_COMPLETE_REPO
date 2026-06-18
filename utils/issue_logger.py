"""
utils/issue_logger.py
---------------------
Rule-issue writer for dq_rule_issues.

Fix #6 (v2): VALUES are now fully parameterised with ? placeholders.
No manual single-quote escaping — driver handles all escaping internally.
`td` must be the metadata Teradata connection, NOT the source connection.
"""

import logging
from config.env_config import get_meta_db

logger = logging.getLogger(__name__)

# meta_db resolved lazily inside each call — not cached at import time.


def log_issue(
    td,
    run: dict,
    rule: dict,
    issue_type: str,
    message: str,
    detail: str = None,
    meta_db: str = None,
):
    """
    Insert a row into dq_rule_issues for non-fatal rule-level problems.

    Uses parameterised ? placeholders — no manual escaping needed.
    `td` must be the metadata Teradata connection, NOT the source connection.
    """
    meta_db = meta_db or get_meta_db()

    rule_id    = rule.get("rule_id")          # may be None → SQL NULL
    rule_code  = rule.get("rule_code") or None
    project    = rule.get("project_name") or None
    process    = rule.get("process_name") or None
    table_name = rule.get("src_tbl_nm") or None
    run_id     = run.get("run_id") or ""
    detail_val = detail or None

    sql = f"""
        INSERT INTO {meta_db}.dq_rule_issues
            (run_id, rule_id, rule_code, project_name, process_name,
             table_name, issue_type, issue_message, error_detail, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """
    try:
        cursor = td.cursor()
        cursor.execute(sql, [
            run_id,
            rule_id,
            rule_code,
            project,
            process,
            table_name,
            issue_type,
            message,
            detail_val,
        ])
        td.commit()
        cursor.close()
        logger.warning("[ISSUE] %s | rule=%s | %s", issue_type, rule_code, message)
    except Exception as exc:
        logger.error("Failed to insert issue log: %s", exc)
