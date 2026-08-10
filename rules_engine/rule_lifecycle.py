"""
rules_engine/rule_lifecycle.py
-------------------------
Rule governance that happens ONCE per run, on the main thread, before/around
execution — as opposed to rules_engine/executor.py which runs each rule. Two
concerns, both about a rule's lifecycle rather than its data checks:

Suppression (snooze)
----------------------
Temporarily silence a rule (e.g. "upstream incident, expected fixed by
Friday") without touching its definition. is_suppressed() is checked per
rule before execute_rule() runs; if suppressed, record_suppressed_execution()
still writes a SUPPRESSED row to dq_rule_execution so the metrics
denominator stays accurate — a suppressed rule counts as "evaluated," just
not run.

    A suppression is ACTIVE when:
        lifted_at IS NULL AND (expires_at IS NULL OR expires_at > NOW)

    -- Suppress a rule until a specific time:
    INSERT INTO {meta_db}.dq_rule_suppressions
        (suppression_id, rule_id, rule_code, reason, suppressed_by, expires_at)
    VALUES (NEXT_ID, 42, 'CLAIMS_NULL_CHECK', 'Upstream incident TICK-999',
            'ops_team', '2024-04-05 09:00:00');

    -- Lift manually before expiry:
    UPDATE {meta_db}.dq_rule_suppressions
    SET lifted_at = CURRENT_TIMESTAMP, lifted_by = 'ops_team'
    WHERE suppression_id = NEXT_ID AND lifted_at IS NULL;

Versioning (forensic history)
--------------------------------
Whenever a rule's tracked fields change, the CURRENT state is archived to
dq_rule_versions BEFORE the run's rules execute — this answers "what did
this rule say when it flagged that case a year ago?" (audit defensibility).
snapshot_changed_rules() runs once per engine run; get_version_at_run()
looks up the version active at a given timestamp.

Public API
----------
is_suppressed(td_conn, rule, meta_db) -> (bool, str)
record_suppressed_execution(td_conn, run, rule, meta_db, reason)
snapshot_changed_rules(td_conn, rules, meta_db) -> int
get_version_at_run(td_conn, rule_id, run_timestamp, meta_db) -> dict | None
"""

import logging
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)


# =============================================================================
# Suppression
# =============================================================================

def is_suppressed(td_conn, rule: dict, meta_db: str) -> Tuple[bool, str]:
    """
    (True, reason) if the rule has an active suppression, else (False, "").
    A lookup failure is treated as not-suppressed (fail open) so a
    misconfigured suppressions table never blocks rule execution.
    """
    from rules_engine.executor import execute_query

    rule_id = rule.get("rule_id")
    if rule_id is None:
        return False, ""

    try:
        rows = execute_query(td_conn, f"""
            SELECT reason, expires_at
            FROM {meta_db}.dq_rule_suppressions
            WHERE rule_id   = ?
              AND lifted_at IS NULL
              AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
            ORDER BY suppressed_at DESC
        """, [rule_id])
    except Exception as exc:
        logger.warning("Suppression lookup failed for rule %s — treating as not suppressed: %s",
                       rule.get("rule_code"), exc)
        return False, ""

    if not rows:
        return False, ""

    reason  = (rows[0].get("reason") or "").strip() or "Suppressed (no reason recorded)"
    expires = rows[0].get("expires_at")
    logger.info("Rule %s is SUPPRESSED (%s): %s", rule.get("rule_code"),
               f"expires {expires}" if expires else "no expiry", reason)
    return True, reason


def record_suppressed_execution(td_conn, run: dict, rule: dict, meta_db: str, reason: str):
    """Write a SUPPRESSED status row so the metrics denominator stays accurate."""
    from rules_engine.executor import record_rule_execution
    from utils.metadata_writers import log_message

    table = rule.get("src_tbl_nm") or "UNKNOWN"
    try:
        record_rule_execution(td_conn, run, rule, table, total=0, failed=0, passed=0,
                              failure_pct=0.0, pass_pct=0.0, status="SUPPRESSED",
                              exec_time=0.0, meta_db=meta_db)
        log_message(td_conn, run["run_id"], "INFO",
                    f"Rule {rule.get('rule_code')} suppressed — {reason}",
                    rule_id=rule.get("rule_id"), rule_code=rule.get("rule_code"), meta_db=meta_db)
    except Exception as exc:
        logger.error("Failed to record SUPPRESSED execution for rule %s: %s", rule.get("rule_code"), exc)


# =============================================================================
# Versioning
# =============================================================================

_TRACKED_FIELDS: List[str] = [
    "rule_syntax", "check_type", "filter_sql", "threshold_pct",
    "threshold_count", "threshold_operator", "severity", "active_flag",
]


def snapshot_changed_rules(td_conn, rules: list, meta_db: str) -> int:
    """Archive a dq_rule_versions row for every rule whose tracked fields changed. Returns count archived."""
    from rules_engine.executor import execute_query, bulk_insert

    versioned = 0
    for rule in rules:
        try:
            if _needs_snapshot(td_conn, rule, meta_db, execute_query):
                _write_snapshot(td_conn, rule, meta_db, execute_query, bulk_insert)
                versioned += 1
        except Exception as exc:
            logger.warning("Could not snapshot rule %s (non-fatal): %s", rule.get("rule_code"), exc)

    logger.info("Rule versioning: archived %d changed rule(s) of %d.", versioned, len(rules))
    return versioned


def get_version_at_run(td_conn, rule_id: int, run_timestamp: str, meta_db: str) -> Optional[dict]:
    """Return the rule version active at run_timestamp, or None. Forensic lookup."""
    from rules_engine.executor import execute_query

    try:
        rows = execute_query(td_conn, f"""
            SELECT * FROM {meta_db}.dq_rule_versions
            WHERE rule_id = ? AND changed_at <= ?
            ORDER BY version_num DESC
        """, [rule_id, run_timestamp])
        return rows[0] if rows else None
    except Exception as exc:
        logger.error("get_version_at_run failed for rule_id=%s: %s", rule_id, exc)
        return None


def _needs_snapshot(td_conn, rule: dict, meta_db: str, execute_query) -> bool:
    try:
        rows = execute_query(td_conn, f"""
            SELECT {', '.join(_TRACKED_FIELDS)}
            FROM {meta_db}.dq_rule_versions
            WHERE rule_id = ?
            ORDER BY version_num DESC
        """, [rule.get("rule_id")])
    except Exception as exc:
        logger.warning("Could not query dq_rule_versions for rule %s: %s", rule.get("rule_code"), exc)
        return True   # assume needs snapshot on error

    if not rows:
        return True   # never been snapshotted

    latest = rows[0]
    return any(str(rule.get(f) or "").strip() != str(latest.get(f) or "").strip() for f in _TRACKED_FIELDS)


def _write_snapshot(td_conn, rule: dict, meta_db: str, execute_query, bulk_insert):
    try:
        rows = execute_query(td_conn, f"""
            SELECT COALESCE(MAX(version_num), 0) AS max_ver
            FROM {meta_db}.dq_rule_versions WHERE rule_id = ?
        """, [rule.get("rule_id")])
        max_ver = int((rows[0].get("max_ver") if rows else 0) or 0)
    except Exception as exc:
        # COALESCE(MAX(...), 0) always returns exactly one row, so this can
        # only fail on a genuine query error (connection drop, permissions,
        # table issue) -- never on "no versions yet". Silently assuming
        # max_ver=0 here would archive this snapshot as version_num=1/
        # change_type=CREATED even when real versions already exist,
        # corrupting the forensic audit trail this table exists for (see
        # DESIGN.md) with no trace of why. Log it loudly before falling
        # back so the corruption risk is at least visible in the logs.
        logger.error(
            "Could not determine current version_num for rule %s (rule_id=%s) — "
            "defaulting to version 1. This may create a duplicate/incorrect "
            "version_num if versions already exist: %s",
            rule.get("rule_code"), rule.get("rule_id"), exc, exc_info=True,
        )
        max_ver = 0

    next_ver    = max_ver + 1
    change_type = "CREATED" if max_ver == 0 else "MODIFIED"

    sql = f"""
        INSERT INTO {meta_db}.dq_rule_versions (
            rule_id, rule_code, version_num, change_type,
            rule_syntax, check_type, filter_sql, threshold_pct,
            threshold_count, threshold_operator, severity, active_flag, changed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """
    bulk_insert(td_conn, sql, [(
        rule.get("rule_id"), rule.get("rule_code"), next_ver, change_type,
        rule.get("rule_syntax"), rule.get("check_type"), rule.get("filter_sql"),
        rule.get("threshold_pct"), rule.get("threshold_count"), rule.get("threshold_operator"),
        rule.get("severity"), rule.get("active_flag"),
    )])
    logger.info("Rule %s -> version %d archived (%s).", rule.get("rule_code"), next_ver, change_type)
