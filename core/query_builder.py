import logging
from utils.table_resolver import resolve_table

logger = logging.getLogger(__name__)


def build_filter(rule: dict, run: dict) -> str:
    """
    Build the incremental WHERE filter clause.

    Priority:
      1. rule.filter_type  — per-rule override (DATE / BATCH / FULL)
      2. run.run_mode      — global run-level mode (DATE / BATCH / FULL)

    This lets individual rules opt into a different filter strategy than
    the rest of the run (e.g. a rule that always needs a date window even
    in a BATCH run).

    Returns a SQL fragment safe to embed inside AND (...).
    """
    col = (rule.get("filter_column") or "").strip()
    if not col:
        return "1=1"

    # rule-level override wins; fall back to run_mode
    mode = (rule.get("filter_type") or run.get("run_mode") or "FULL").upper()

    if mode == "BATCH":
        batch_id = (run.get("batch_id") or "").replace("'", "''")
        return f"{col} = '{batch_id}'"

    if mode == "DATE":
        start = (run.get("start_date") or "1900-01-01").replace("'", "''")
        end   = (run.get("end_date")   or "2099-12-31").replace("'", "''")
        # Plain string literals work across Teradata, PostgreSQL, SQL Server,
        # Databricks, and DuckDB — implicit cast to DATE when col is a date type.
        return f"{col} BETWEEN '{start}' AND '{end}'"

    return "1=1"


def build_count_query(rule: dict, run: dict) -> str:
    """
    Build a COUNT(*) query with the SAME filter as the rule query so that
    total_records and failure_pct are computed over the same scoped dataset.
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


def build_query(rule: dict, run: dict) -> str:
    """
    Assemble the DQ rule SQL query.

        SELECT *
        FROM <resolved_table> t
        [<join_sql>]
        WHERE (<rule_syntax>) AND (<filter_condition>)
    """
    rule_syntax = (rule.get("rule_syntax") or "").strip()
    if not rule_syntax:
        raise ValueError(
            f"rule_id={rule.get('rule_id')} ({rule.get('rule_code')}): rule_syntax is empty."
        )

    table      = resolve_table(rule)
    join_sql   = (rule.get("join_sql") or "").strip()
    filter_cnd = build_filter(rule, run)

    join_clause = f"\n    {join_sql}" if join_sql else ""

    query = (
        f"SELECT *\n"
        f"    FROM {table} t"
        f"{join_clause}\n"
        f"    WHERE ({rule_syntax}) AND ({filter_cnd})"
    )

    logger.debug("Built query for rule_code=%s:\n%s", rule.get("rule_code"), query)
    return query
