import logging
from config.env_config import get_meta_db

logger = logging.getLogger(__name__)

# Resolved once at import time from the active environment.
# Callers may still override per-call if needed.
_DEFAULT_META_DB = get_meta_db()


def log_message(
    td,
    run_id: str,
    level: str,
    message: str,
    rule_id=None,
    rule_code=None,
    error_code: str = None,
    error_detail: str = None,
    meta_db: str = None,          # None → use env-resolved default (never DEV hardcode)
):
    """
    Insert a structured log entry into dq_run_logs.
    Failures are printed to stderr but never re-raised.
    """
    meta_db = meta_db or _DEFAULT_META_DB

    safe_msg    = (message      or "").replace("'", "''")
    safe_detail = (error_detail or "").replace("'", "''")
    safe_code   = (error_code   or "").replace("'", "''")
    safe_rcode  = (rule_code    or "").replace("'", "''")

    rule_id_sql = str(rule_id) if rule_id is not None else "NULL"
    rcode_sql   = f"'{safe_rcode}'" if rule_code is not None else "NULL"

    sql = f"""
        INSERT INTO {meta_db}.dq_run_logs
            (run_id, rule_id, rule_code, log_level, message,
             error_code, error_detail, created_at)
        VALUES (
            '{run_id}', {rule_id_sql}, {rcode_sql},
            '{level}', '{safe_msg}',
            '{safe_code}', '{safe_detail}',
            CURRENT_TIMESTAMP
        )
    """
    try:
        cursor = td.cursor()
        cursor.execute(sql)
        td.commit()
        cursor.close()
    except Exception as exc:
        logger.error("Failed to insert run log: %s", exc)

    py_level = getattr(logging, level.upper(), logging.INFO)
    logger.log(py_level, "[%s] run=%s rule=%s | %s", level, run_id, rule_code, message)
