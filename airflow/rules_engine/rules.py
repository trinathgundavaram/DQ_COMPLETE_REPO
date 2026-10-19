import logging

from rules_engine.config import resolve_database_name, resolve_env_tokens
from rules_engine.db_ops import execute_query

logger = logging.getLogger(__name__)


def load_rules(meta_conn, meta_db: str, rule_group: str, rule_variant: str = None) -> list:
    if rule_variant:
        sql = f"""
            SELECT *
            FROM {meta_db}.gre_rules
            WHERE rule_group   = ?
              AND act_ind      = 1
              AND (rule_variant IS NULL OR rule_variant = ?)
            ORDER BY seq_no ASC, rule_id ASC
        """
        params = [rule_group, rule_variant]
    else:


        sql = f"""
            SELECT *
            FROM {meta_db}.gre_rules
            WHERE rule_group   = ?
              AND act_ind      = 1
            ORDER BY seq_no ASC, rule_id ASC
        """
        params = [rule_group]

    rows = execute_query(meta_conn, sql, params, log_params=True)


    for row in rows:
        row["database_name"] = resolve_database_name(row.get("database_name"))
        row["rule_syntax"] = resolve_env_tokens(row.get("rule_syntax"))

    logger.info(
        "Loaded %d active rule(s) for rule_group=%s%s.",
        len(rows), rule_group, f" rule_variant={rule_variant}" if rule_variant else "",
    )
    return rows
