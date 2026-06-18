import logging
from config.env_config import get_meta_db

logger = logging.getLogger(__name__)

_DEFAULT_META_DB = get_meta_db()


def log_issue(
    td,                           # metadata connection (always Teradata)
    run: dict,
    rule: dict,
    issue_type: str,
    message: str,
    detail: str = None,
    meta_db: str = None,          # None → env-resolved default (never DEV hardcode)
):
    """
    Insert a row into dq_rule_issues for non-fatal rule-level problems.
    `td` must be the metadata Teradata connection, NOT the source connection.
    """
    meta_db = meta_db or _DEFAULT_META_DB

    safe_msg    = (message or "").replace("'", "''")
    safe_detail = (detail  or "").replace("'", "''")
    run_id      = (run.get("run_id") or "").replace("'", "''")
    rule_id     = rule.get("rule_id")
    rule_code   = (rule.get("rule_code")    or "").replace("'", "''")
    project     = (rule.get("project_name") or "").replace("'", "''")
    process     = (rule.get("process_name") or "").replace("'", "''")
    table_name  = (rule.get("src_tbl_nm")   or "").replace("'", "''")

    rule_id_sql = str(rule_id) if rule_id is not None else "NULL"

    sql = f"""
        INSERT INTO {meta_db}.dq_rule_issues
            (run_id, rule_id, rule_code, project_name, process_name,
             table_name, issue_type, issue_message, error_detail, created_at)
        VALUES (
            '{run_id}', {rule_id_sql}, '{rule_code}',
            '{project}', '{process}', '{table_name}',
            '{issue_type}', '{safe_msg}', '{safe_detail}',
            CURRENT_TIMESTAMP
        )
    """
    try:
        cursor = td.cursor()
        cursor.execute(sql)
        td.commit()
        cursor.close()
        logger.warning("[ISSUE] %s | rule=%s | %s", issue_type, rule_code, message)
    except Exception as exc:
        logger.error("Failed to insert issue log: %s", exc)
