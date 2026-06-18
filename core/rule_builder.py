"""
core/rule_builder.py
--------------------
Bridges the rule catalog (dq_rules) to the check_types SQL generators.

Public API
----------
build_rule_sql(rule, table, filter_sql, source_type)
    -> (sql: str, level: str)
       sql   — ready-to-run SQL (ROW: WHERE fragment, TABLE: full SELECT,
               SCHEMA: empty string — executor handles catalog query)
       level — "ROW" | "TABLE" | "SCHEMA"

get_check_level(rule)
    -> "ROW" | "TABLE" | "SCHEMA"

Backward compatibility
----------------------
If rule["check_type"] is None/empty the function falls back to
rule["rule_syntax"] and returns level="ROW" (historic behaviour).
"""

import logging
from typing import Optional, Tuple

from core.check_types import (
    CHECK_CATALOG,
    get_level,
    _parse_params,  # re-export for convenience
)

logger = logging.getLogger(__name__)

_ALIAS = "t"   # must match query_builder.py


# ─────────────────────────────────────────────────────────────────────────────
# Public helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_check_level(rule: dict) -> str:
    """
    Return the execution level for a rule: "ROW", "TABLE", or "SCHEMA".

    Logic:
      1. If check_type is set → look up in CHECK_CATALOG.
      2. If check_type is unknown → log warning, default to ROW.
      3. If check_type is absent/NULL → fall back to legacy ROW (uses rule_syntax).
    """
    ct = (rule.get("check_type") or "").strip().upper()
    if not ct:
        return "ROW"   # legacy path
    return get_level(ct)   # returns "ROW" by default for unknown types


def build_rule_sql(
    rule: dict,
    table: str,
    filter_sql: str,
    source_type: str,
) -> Tuple[str, str]:
    """
    Generate the SQL expression for a rule and return (sql, level).

    Parameters
    ----------
    rule        : dict row from dq_rules (must include check_type, check_column,
                  check_params, and rule_syntax fields)
    table       : fully-qualified table name (database.schema.table or similar)
    filter_sql  : verbatim WHERE clause fragment already resolved by query_builder
                  ("1=1" when no filter is active)
    source_type : adapter source type string, e.g. "teradata", "postgresql",
                  "databricks", "sqlserver", "file"

    Returns
    -------
    (sql, level) where level is "ROW" | "TABLE" | "SCHEMA"
    Raises ValueError when required check_params fields are missing.
    """
    ct = (rule.get("check_type") or "").strip().upper()

    # ── LEGACY / CUSTOM path ─────────────────────────────────────────────────
    if not ct:
        sql = (rule.get("rule_syntax") or "").strip()
        if not sql:
            raise ValueError(
                f"Rule {rule.get('rule_code')} has no check_type and no rule_syntax."
            )
        return sql, "ROW"

    # ── SCHEMA level (handled by executor — no SQL from here) ─────────────────
    if ct == "COLUMN_EXISTS":
        return "", "SCHEMA"

    # ── Look up in catalog ────────────────────────────────────────────────────
    spec = CHECK_CATALOG.get(ct)
    if spec is None:
        # Unknown check_type: fall back to rule_syntax as a custom ROW check
        logger.warning(
            "Unknown check_type '%s' for rule %s — falling back to rule_syntax.",
            ct, rule.get("rule_code"),
        )
        sql = (rule.get("rule_syntax") or "").strip()
        if not sql:
            raise ValueError(
                f"Unknown check_type '{ct}' and no rule_syntax fallback "
                f"(rule_code={rule.get('rule_code')})."
            )
        return sql, "ROW"

    level  = spec["level"]
    params = _parse_params(rule)
    fn     = spec["fn"]

    try:
        if level == "ROW":
            sql = fn(rule, table, _ALIAS, filter_sql, params, source_type)
        elif level == "TABLE":
            sql = fn(rule, table, filter_sql, params, source_type)
        else:
            # Should not reach here for non-SCHEMA types
            sql = ""

    except (ValueError, KeyError) as exc:
        raise ValueError(
            f"Error generating SQL for rule {rule.get('rule_code')} "
            f"(check_type={ct}): {exc}"
        ) from exc

    return sql, level


# ─────────────────────────────────────────────────────────────────────────────
# Validation helper (used by pre-validate pass in engine.py)
# ─────────────────────────────────────────────────────────────────────────────

def validate_rule_params(rule: dict) -> Optional[str]:
    """
    Return an error string if the rule is misconfigured, or None if OK.

    Checks:
    - Known check_type has required check_params keys present.
    - check_column is set when the check type needs it.
    - rule_syntax is present when check_type is absent.

    This is a STATIC check (no DB access).
    """
    ct = (rule.get("check_type") or "").strip().upper()

    if not ct:
        if not (rule.get("rule_syntax") or "").strip():
            return "check_type is not set and rule_syntax is empty."
        return None

    if ct == "COLUMN_EXISTS":
        if not (rule.get("check_column") or "").strip():
            return "COLUMN_EXISTS requires check_column."
        return None

    spec = CHECK_CATALOG.get(ct)
    if spec is None:
        if not (rule.get("rule_syntax") or "").strip():
            return f"Unknown check_type '{ct}' and rule_syntax is empty."
        return None

    params   = _parse_params(rule)
    required = spec.get("required", [])
    missing  = [k for k in required if k not in params]
    if missing:
        return (
            f"check_type '{ct}' requires check_params fields: "
            f"{', '.join(missing)}"
        )

    return None
