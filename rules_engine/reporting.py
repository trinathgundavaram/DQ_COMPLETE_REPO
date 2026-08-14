"""
rules_engine/reporting.py
----------------------------
A report is just a query against gre_results, optionally joined out to
gre_exceptions for the underlying records -- no separate report-building
machinery. Two thin helpers cover both.
"""

from shared.db_ops import execute_query


def get_breaches(meta_conn, meta_db: str, run_id: str) -> list:
    """Every rule that breached its threshold (FAIL or WARN) for a given run."""
    return execute_query(
        meta_conn,
        f"""
        SELECT *
        FROM {meta_db}.gre_results
        WHERE run_id = ? AND status IN ('FAIL', 'WARN')
        ORDER BY status, rule_id
        """,
        [run_id],
    )


def get_records_for_result(meta_conn, meta_db: str, rule_id, batch_id: str) -> list:
    """
    Every gre_exceptions record behind one gre_results verdict -- the drill-
    down join described in the prompt: gre_results -> gre_exceptions on
    (rule_id, batch_id), filtered to the current record version.
    """
    return execute_query(
        meta_conn,
        f"""
        SELECT e.*
        FROM {meta_db}.gre_exceptions e
        WHERE e.rule_id = ? AND e.batch_id = ? AND e.etl_is_curr_ind = 'Y'
        ORDER BY e.record_id
        """,
        [rule_id, batch_id],
    )
