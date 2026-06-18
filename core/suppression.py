"""
core/suppression.py
-------------------
Rule suppression (snooze) management.

Operators insert a row into dq_rule_suppressions to temporarily silence a
rule — e.g. "source system has an upstream incident, expected to resolve by
Friday."  An expiry timestamp (expires_at) causes the suppression to lift
automatically; omit it for manual-only lift.

A suppression is considered ACTIVE when:
    lifted_at IS NULL
    AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)

Engine integration
------------------
engine.py calls is_suppressed() inside run_single() before execute_rule().
If suppressed, record_suppressed_execution() writes a SUPPRESSED status row
to dq_rule_execution so that the metrics denominator remains accurate.

Public API
----------
is_suppressed(td_conn, rule, meta_db)  -> (bool, str)
record_suppressed_execution(td_conn, run, rule, meta_db, reason)

Managing suppressions (via direct SQL or a UI tool)
----------------------------------------------------
    -- Suppress a rule until a specific time:
    INSERT INTO {meta_db}.dq_rule_suppressions
        (suppression_id, rule_id, rule_code, reason, suppressed_by, expires_at)
    VALUES (NEXT_ID, 42, 'CLAIMS_NULL_CHECK', 'Upstream incident TICK-999',
            'ops_team', '2024-04-05 09:00:00');

    -- Lift manually before expiry:
    UPDATE {meta_db}.dq_rule_suppressions
    SET lifted_at = CURRENT_TIMESTAMP, lifted_by = 'ops_team'
    WHERE suppression_id = NEXT_ID AND lifted_at IS NULL;
"""

import logging
from typing import Tuple

logger = logging.getLogger(__name__)


def is_suppressed(td_conn, rule: dict, meta_db: str) -> Tuple[bool, str]:
    """
    Return (True, reason) if the rule has an active suppression, else (False, "").

    Errors in the suppression lookup are treated as non-suppressed (fail open)
    so a misconfigured suppressions table never blocks rule execution.
    """
    from core.executor import execute_query

    rule_id = rule.get("rule_id")
    if rule_id is None:
        return False, ""

    try:
        rows = execute_query(
            td_conn,
            f"""
            SELECT reason, expires_at
            FROM {meta_db}.dq_rule_suppressions
            WHERE rule_id   = ?
              AND lifted_at IS NULL
              AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
            ORDER BY suppressed_at DESC
            """,
            [rule_id],
        )
    except Exception as exc:
        logger.warning(
            "Suppression lookup failed for rule %s — treating as not suppressed: %s",
            rule.get("rule_code"), exc,
        )
        return False, ""

    if not rows:
        return False, ""

    reason = (rows[0].get("reason") or "").strip() or "Suppressed (no reason recorded)"
    expires = rows[0].get("expires_at")
    expiry_note = f" (expires {expires})" if expires else " (no expiry)"
    logger.info(
        "Rule %s is SUPPRESSED%s: %s",
        rule.get("rule_code"), expiry_note, reason,
    )
    return True, reason


def record_suppressed_execution(
    td_conn,
    run: dict,
    rule: dict,
    meta_db: str,
    reason: str,
):
    """
    Write a SUPPRESSED status row to dq_rule_execution.

    Keeps the metrics denominator accurate — suppressed rules are counted as
    evaluated so the DQ score and failure-rate metrics remain meaningful.
    """
    from core.executor import record_rule_execution
    from utils.logger import log_message

    table = rule.get("src_tbl_nm") or "UNKNOWN"
    try:
        record_rule_execution(
            td_conn, run, rule, table,
            total=0, failed=0, passed=0,
            failure_pct=0.0, pass_pct=0.0,
            status="SUPPRESSED", exec_time=0.0,
            meta_db=meta_db,
        )
        log_message(
            td_conn,
            run["run_id"],
            "INFO",
            f"Rule {rule.get('rule_code')} suppressed — {reason}",
            rule_id=rule.get("rule_id"),
            rule_code=rule.get("rule_code"),
            meta_db=meta_db,
        )
    except Exception as exc:
        logger.error(
            "Failed to record SUPPRESSED execution for rule %s: %s",
            rule.get("rule_code"), exc,
        )
