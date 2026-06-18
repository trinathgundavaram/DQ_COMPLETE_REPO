"""
utils/logger.py
---------------
Structured run-log writer for dq_run_logs.

Fix #6 (v2): VALUES are now fully parameterised with ? placeholders.
No manual single-quote escaping — driver handles all escaping internally.
"""

import logging
from config.env_config import get_meta_db

logger = logging.getLogger(__name__)

# NOTE: meta_db is resolved lazily inside each call (not at import time)
# so that DQ_ENV changes after import are respected and startup order
# does not matter.


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
    """
    Insert a structured log entry into dq_run_logs.

    Uses parameterised ? placeholders — no manual escaping needed.
    Failures are logged to stderr but never re-raised.
    """
    meta_db = meta_db or get_meta_db()

    # None → SQL NULL for nullable integer / varchar columns
    rule_id_val   = rule_id if rule_id is not None else None
    rule_code_val = rule_code or None
    error_code_val   = error_code or None
    error_detail_val = error_detail or None

    sql = f"""
        INSERT INTO {meta_db}.dq_run_logs
            (run_id, rule_id, rule_code, log_level, message,
             error_code, error_detail, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """
    try:
        cursor = td.cursor()
        cursor.execute(sql, [
            run_id,
            rule_id_val,
            rule_code_val,
            level,
            message,
            error_code_val,
            error_detail_val,
        ])
        td.commit()
        cursor.close()
    except Exception as exc:
        logger.error("Failed to insert run log: %s", exc)

    py_level = getattr(logging, level.upper(), logging.INFO)
    logger.log(py_level, "[%s] run=%s rule=%s | %s", level, run_id, rule_code, message)
