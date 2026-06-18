import logging
from utils.table_resolver import resolve_table

logger = logging.getLogger(__name__)


def validate_table_exists(
    db_conn,        # source connection — used only for the SELECT 1 probe
    td_conn,        # metadata connection — used for logging issues/messages
    rule: dict,
    run: dict,
    log_issue_fn,
    log_message_fn,
    meta_db: str,
) -> bool:
    """
    Verify the rule's source table is reachable by executing a no-op query.
    Issue and message logs are written to `td_conn` (metadata), NOT `db_conn`.
    Returns True if accessible, False otherwise.
    """
    try:
        table = resolve_table(rule)
    except ValueError as exc:
        log_issue_fn(td_conn, run, rule, "CONFIG_ERROR", str(exc), meta_db=meta_db)
        return False

    try:
        cursor = db_conn.cursor()
        cursor.execute(f"SELECT 1 FROM {table} WHERE 1=0")
        cursor.close()
        return True

    except Exception as exc:
        msg = f"Table or schema not found: {table}"
        log_issue_fn(
            td_conn, run, rule,
            "TABLE_NOT_FOUND", msg, str(exc),
            meta_db=meta_db,
        )
        log_message_fn(
            td_conn,
            run["run_id"],
            "ERROR",
            f"Table validation failed: {table}",
            rule_id=rule.get("rule_id"),
            rule_code=rule.get("rule_code"),
            error_code="TABLE_VALIDATION",
            error_detail=str(exc),
            meta_db=meta_db,
        )
        logger.error("Table validation failed for rule %s: %s", rule.get("rule_code"), exc)
        return False
