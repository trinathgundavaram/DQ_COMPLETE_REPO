"""
core/query_builder.py
---------------------
Builds DQ rule SQL queries and incremental WHERE filters.

Check-type integration (v4):
    build_query() now delegates to core.rule_builder for check-type aware SQL
    generation.  It returns a (sql, level) tuple:
        level = "ROW"    → sql is a WHERE fragment; executor wraps in SELECT *
        level = "TABLE"  → sql is a full SELECT; executor runs it directly
        level = "SCHEMA" → sql is "" ; executor runs catalog query instead

Fix #15 (v2): `filter_sql` CLOB column — when populated it is used verbatim
as the WHERE clause, bypassing filter_column/filter_type entirely.  This
supports compound filters (multi-column, expressions, subqueries) that the
single-column design cannot express.

Priority chain for filter selection:
    1. rule.filter_sql      — verbatim SQL fragment (highest priority)
    2. rule.filter_type     — per-rule mode override (DATE / BATCH / FULL)
    3. run.run_mode         — global run-level mode fallback

Date literals use plain ISO strings ('YYYY-MM-DD') instead of
Teradata-specific DATE 'literal' syntax — works across Teradata,
PostgreSQL, Databricks, SQL Server, and DuckDB via implicit cast.
"""

import logging
from typing import Tuple

from utils.table_resolver import resolve_table
from core.rule_builder import build_rule_sql

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Filter builder
# ─────────────────────────────────────────────────────────────────────────────

def build_filter(rule: dict, run: dict) -> str:
    """
    Build the incremental WHERE filter clause.

    Returns a SQL fragment safe to embed inside AND (...).
    Never returns an empty string — falls back to '1=1' (full scan).
    """
    # ── 1. filter_sql: verbatim override (compound / custom expressions) ──────
    filter_sql = (rule.get("filter_sql") or "").strip()
    if filter_sql:
        logger.debug("Using filter_sql override for rule %s.", rule.get("rule_code"))
        return filter_sql

    col = (rule.get("filter_column") or "").strip()
    if not col:
        return "1=1"

    # ── 2. filter_type (per-rule) → falls back to run_mode (global) ──────────
    mode = (rule.get("filter_type") or run.get("run_mode") or "FULL").upper()

    if mode == "BATCH":
        batch_id = (run.get("batch_id") or "").replace("'", "''")
        return f"{col} = '{batch_id}'"

    if mode == "DATE":
        start = (run.get("start_date") or "1900-01-01").replace("'", "''")
        end   = (run.get("end_date")   or "2099-12-31").replace("'", "''")
        # Plain string literals work across all supported source DBs
        # (Teradata, PostgreSQL, Databricks, SQL Server, DuckDB).
        return f"{col} BETWEEN '{start}' AND '{end}'"

    return "1=1"


# ─────────────────────────────────────────────────────────────────────────────
# Count query (ROW level only)
# ─────────────────────────────────────────────────────────────────────────────

def build_count_query(rule: dict, run: dict) -> str:
    """
    Build a COUNT(*) query with the SAME filter as the rule query.

    This ensures total_records and failure_pct are computed over the
    same scoped dataset as the rule rows.

    Only used for ROW-level rules.  TABLE-level rules use total=1 (the
    property being checked is a single table characteristic).
    """
    table      = resolve_table(rule)
    join_sql   = (rule.get("join_sql") or "").strip()
    filter_cnd = build_filter(rule, run)

    join_clause = f"\n    {join_sql}" if join_sql else ""

    return (
        f"SELECT COUNT(*) AS total_count\n"
        f"    FROM {table} t"
        f"{join_clause}\n"
        f"    WHERE ({filter_cnd})"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main query builder
# ─────────────────────────────────────────────────────────────────────────────

def build_query(
    rule: dict,
    run: dict,
    source_type: str = "teradata",
) -> Tuple[str, str]:
    """
    Assemble the DQ rule SQL query via the check-type system.

    Returns
    -------
    (sql, level)
        ROW    — sql is a WHERE fragment (failing rows).
                 Executor wraps as: SELECT * FROM {table} t
                                    WHERE ({sql}) AND ({filter})
        TABLE  — sql is a complete SELECT ready to run.
                 0 rows returned → PASS; ≥1 rows → FAIL.
                 total_records = 1 (the table-level property).
        SCHEMA — sql = "" (executor queries the DB catalog for COLUMN_EXISTS).

    Raises ValueError when the rule has neither a check_type nor a rule_syntax.
    """
    table      = resolve_table(rule)
    filter_cnd = build_filter(rule, run)
    join_sql   = (rule.get("join_sql") or "").strip()

    # Delegate SQL generation to rule_builder (handles check_type + fallback)
    sql, level = build_rule_sql(rule, table, filter_cnd, source_type)

    if level == "SCHEMA":
        return "", "SCHEMA"

    if level == "TABLE":
        logger.debug(
            "Built TABLE-level query for rule_code=%s:\n%s",
            rule.get("rule_code"), sql,
        )
        return sql, "TABLE"

    # ── ROW level: wrap WHERE fragment in a SELECT * ──────────────────────────
    if not sql:
        raise ValueError(
            f"rule_id={rule.get('rule_id')} ({rule.get('rule_code')}): "
            "produced empty WHERE clause."
        )

    join_clause = f"\n    {join_sql}" if join_sql else ""

    row_query = (
        f"SELECT *\n"
        f"    FROM {table} t"
        f"{join_clause}\n"
        f"    WHERE ({sql}) AND ({filter_cnd})"
    )

    logger.debug(
        "Built ROW-level query for rule_code=%s:\n%s",
        rule.get("rule_code"), row_query,
    )
    return row_query, "ROW"
