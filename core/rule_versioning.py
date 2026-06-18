"""
core/rule_versioning.py
-----------------------
Automatic rule definition archiving.

When any tracked field of a rule changes, the CURRENT state is archived to
dq_rule_versions before execution begins.  This answers the forensic question:
  "What did rule CLAIMS_NULL_CHECK look like when it failed on 2024-Q3?"

Architecture
------------
snapshot_changed_rules() is called ONCE at the start of each run, on the main
thread, before parallel rule execution begins.  It compares each active rule
against the latest version in dq_rule_versions and writes a new row if any
tracked field has changed.

Tracked fields (any change triggers a new version row)
-------------------------------------------------------
rule_syntax, check_type, check_column, check_params,
filter_sql, join_sql,
threshold_pct, threshold_count, threshold_operator,
severity, active_flag

Public API
----------
snapshot_changed_rules(td_conn, rules, meta_db)  -> int  (# rules versioned)
get_version_at_run(td_conn, rule_id, run_timestamp, meta_db)  -> dict | None
"""

import logging
from typing import List, Optional

logger = logging.getLogger(__name__)

_TRACKED_FIELDS: List[str] = [
    "rule_syntax",
    "check_type",
    "check_column",
    "check_params",
    "filter_sql",
    "join_sql",
    "threshold_pct",
    "threshold_count",
    "threshold_operator",
    "severity",
    "active_flag",
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def snapshot_changed_rules(td_conn, rules: list, meta_db: str) -> int:
    """
    Archive a new version row for every rule whose tracked fields have changed
    since the last snapshot (or have never been snapshotted at all).

    Parameters
    ----------
    td_conn  : Teradata metadata connection (main-thread connection)
    rules    : list of rule dicts loaded from dq_rules
    meta_db  : metadata schema name

    Returns
    -------
    Number of rules that were archived.
    """
    from core.executor import execute_query, bulk_insert

    versioned = 0
    for rule in rules:
        try:
            if _needs_snapshot(td_conn, rule, meta_db, execute_query):
                _write_snapshot(td_conn, rule, meta_db, execute_query, bulk_insert)
                versioned += 1
        except Exception as exc:
            # Non-fatal — versioning failure must not block rule execution
            logger.warning(
                "Could not snapshot rule %s (non-fatal): %s",
                rule.get("rule_code"), exc,
            )

    if versioned:
        logger.info("Rule versioning: archived %d changed rule(s).", versioned)
    else:
        logger.debug("Rule versioning: no changes detected across %d rules.", len(rules))
    return versioned


def get_version_at_run(
    td_conn,
    rule_id: int,
    run_timestamp: str,
    meta_db: str,
) -> Optional[dict]:
    """
    Return the rule version that was active at run_timestamp.

    Useful for forensic analysis — answers "what did this rule look like
    when that failure happened?"

    Parameters
    ----------
    td_conn        : metadata connection
    rule_id        : dq_rules.rule_id
    run_timestamp  : ISO timestamp string, e.g. "2024-03-15 14:32:00"
    meta_db        : metadata schema name

    Returns
    -------
    The most recent version row with changed_at <= run_timestamp, or None.
    """
    from core.executor import execute_query

    try:
        rows = execute_query(
            td_conn,
            f"""
            SELECT *
            FROM {meta_db}.dq_rule_versions
            WHERE rule_id  = ?
              AND changed_at <= ?
            ORDER BY version_num DESC
            """,
            [rule_id, run_timestamp],
        )
        return rows[0] if rows else None
    except Exception as exc:
        logger.error(
            "get_version_at_run failed for rule_id=%s: %s", rule_id, exc
        )
        return None


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _needs_snapshot(td_conn, rule: dict, meta_db: str, execute_query) -> bool:
    """
    Return True if the rule has no archived version or any tracked field differs
    from the latest archived version.
    """
    try:
        rows = execute_query(
            td_conn,
            f"""
            SELECT {', '.join(_TRACKED_FIELDS)}
            FROM {meta_db}.dq_rule_versions
            WHERE rule_id = ?
            ORDER BY version_num DESC
            """,
            [rule.get("rule_id")],
        )
    except Exception as exc:
        logger.warning(
            "Could not query dq_rule_versions for rule %s: %s",
            rule.get("rule_code"), exc,
        )
        return True   # assume needs snapshot on error

    if not rows:
        return True   # never been snapshotted

    latest = rows[0]
    for field in _TRACKED_FIELDS:
        current  = str(rule.get(field) or "").strip()
        archived = str(latest.get(field) or "").strip()
        if current != archived:
            logger.debug(
                "Rule %s: field '%s' changed — will archive new version.",
                rule.get("rule_code"), field,
            )
            return True
    return False


def _write_snapshot(td_conn, rule: dict, meta_db: str, execute_query, bulk_insert):
    """Write a new version row and increment the version counter."""
    # Determine next version number
    try:
        rows = execute_query(
            td_conn,
            f"""
            SELECT COALESCE(MAX(version_num), 0) AS max_ver
            FROM {meta_db}.dq_rule_versions
            WHERE rule_id = ?
            """,
            [rule.get("rule_id")],
        )
        max_ver = int((rows[0].get("max_ver") if rows else 0) or 0)
    except Exception:
        max_ver = 0

    next_ver    = max_ver + 1
    change_type = "CREATED" if max_ver == 0 else "MODIFIED"

    sql = f"""
        INSERT INTO {meta_db}.dq_rule_versions (
            rule_id, rule_code, version_num, change_type,
            rule_syntax, check_type, check_column, check_params,
            filter_sql, join_sql,
            threshold_pct, threshold_count, threshold_operator,
            severity, active_flag,
            changed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """
    bulk_insert(td_conn, sql, [(
        rule.get("rule_id"),
        rule.get("rule_code"),
        next_ver,
        change_type,
        rule.get("rule_syntax"),
        rule.get("check_type"),
        rule.get("check_column"),
        rule.get("check_params"),
        rule.get("filter_sql"),
        rule.get("join_sql"),
        rule.get("threshold_pct"),
        rule.get("threshold_count"),
        rule.get("threshold_operator"),
        rule.get("severity"),
        rule.get("active_flag"),
    )])

    logger.info(
        "Rule %s → version %d archived (%s).",
        rule.get("rule_code"), next_ver, change_type,
    )
