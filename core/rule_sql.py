"""
core/rule_sql.py
-------------------
Everything involved in turning one dq_rules row into ready-to-run,
dialect-safe SQL: the run-scope filter, the three rule-authoring paths, and
the fail-fast dialect check. These three concerns (filter, SQL generation,
dialect safety) always run together for every rule, so they live in one
file that reads top to bottom as "how do we get valid SQL for this rule."

Three rule-authoring paths, checked in this order
----------------------------------------------------
1. RAW SQL (sql_dialect is set) — the primary/recommended path. rule_syntax
   is a COMPLETE, self-contained negative-SQL SELECT that returns the rows
   violating the rule (its own joins/subqueries/CTEs, any valid SQL for the
   declared dialect) — zero rows returned means the rule passed. check_type,
   if also set, is used ONLY as a dashboard/taxonomy classification tag
   (see core/check_types.py) — it does not affect SQL generation here.

2. Built-in check_type (check_type set, sql_dialect NOT set) — declarative
   generation via core/check_types.py's 23 built-in generators, which
   already emit dialect-correct SQL per source_type. Good for simple,
   single-table checks that don't need custom SQL.

3. Legacy custom fragment (neither set) — rule_syntax is treated as a
   WHERE-clause fragment and wrapped as:
       SELECT * FROM {table} t {join} WHERE ({fragment}) AND ({filter})
   Kept for backward compatibility with pre-dialect-guard rule definitions.

Dialect safety
--------------
Every raw-SQL rule (path 1) declares sql_dialect ('teradata' | 'postgres' |
'ansi'). check_dialect() compares it against the target connection's
source_type (DIALECT_COMPATIBILITY below) and raises DialectMismatchError
on a mismatch — called from core/engine.py (pre-validation, load-time) and
core/executor.py (defense-in-depth, immediately before execution) so a
mismatch is always caught before a query reaches the database, never as a
confusing mid-run syntax error. 'ansi' is accepted everywhere by
definition. Rules using path 2/3 have no sql_dialect and are exempt — the
check_type generators can't have a dialect mismatch by construction.

Public API
----------
build_filter(rule, run) -> str
build_count_query(rule, run) -> str
build_query(rule, run, source_type) -> (sql: str, level: str)
validate_rule_params(rule) -> str | None       (static, no DB access)
check_dialect(rule, source_type) -> None       (raises DialectMismatchError)
"""

import logging
from typing import Optional, Tuple

from core.check_types import CHECK_CATALOG, get_level, _parse_params
from utils.db_helpers import resolve_table

logger = logging.getLogger(__name__)

_ALIAS = "t"


# =============================================================================
# Run-scope filter
# =============================================================================

def build_filter(rule: dict, run: dict) -> str:
    """
    Build the incremental WHERE filter clause for a rule's run scope.
    Never returns an empty string — falls back to '1=1' (full scan).

    Priority: rule.filter_sql (verbatim override) > rule.filter_column +
    (rule.filter_type or run.run_mode).
    """
    filter_sql = (rule.get("filter_sql") or "").strip()
    if filter_sql:
        return filter_sql

    col = (rule.get("filter_column") or "").strip()
    if not col:
        return "1=1"

    mode = (rule.get("filter_type") or run.get("run_mode") or "FULL").upper()

    if mode == "BATCH":
        batch_id = (run.get("batch_id") or "").replace("'", "''")
        return f"{col} = '{batch_id}'"

    if mode == "DATE":
        # Plain ISO string literals work across every supported source DB
        # via implicit cast (Teradata, PostgreSQL, DuckDB/S3).
        start = (run.get("start_date") or "1900-01-01").replace("'", "''")
        end   = (run.get("end_date")   or "2099-12-31").replace("'", "''")
        return f"{col} BETWEEN '{start}' AND '{end}'"

    return "1=1"


def build_count_query(rule: dict, run: dict) -> str:
    """COUNT(*) with the SAME filter as the rule query, for accurate failure_pct."""
    table       = resolve_table(rule)
    join_clause = f"\n    {rule.get('join_sql', '').strip()}" if (rule.get("join_sql") or "").strip() else ""
    return (
        f"SELECT COUNT(*) AS total_count\n"
        f"    FROM {table} t{join_clause}\n"
        f"    WHERE ({build_filter(rule, run)})"
    )


# =============================================================================
# SQL generation (the three authoring paths)
# =============================================================================

def is_raw_sql_rule(rule: dict) -> bool:
    """True when the rule is authored via the raw-SQL path (path 1)."""
    return bool((rule.get("sql_dialect") or "").strip())


def get_check_level(rule: dict) -> str:
    """"ROW" | "TABLE" | "SCHEMA" — which shape of query/result this rule produces."""
    if is_raw_sql_rule(rule):
        return "ROW"
    ct = (rule.get("check_type") or "").strip().upper()
    return get_level(ct) if ct else "ROW"


def build_query(rule: dict, run: dict, source_type: str = "teradata") -> Tuple[str, str]:
    """
    Assemble the full query for a rule.

    Returns (sql, level):
        ROW    — ready-to-run SELECT (executor wraps in COUNT(*)/fetch subqueries)
        TABLE  — complete SELECT; 0 rows = PASS, >=1 rows = FAIL; total_records = 1
        SCHEMA — sql = "" ; executor queries the DB catalog directly (COLUMN_EXISTS)
    """
    table      = resolve_table(rule)
    filter_cnd = build_filter(rule, run)
    join_sql   = (rule.get("join_sql") or "").strip()

    sql, level = build_rule_sql(rule, table, filter_cnd, source_type)

    if level == "SCHEMA":
        return "", "SCHEMA"
    if level == "TABLE":
        return sql, "TABLE"

    # ROW level from a check_type generator or the legacy fragment path
    # returns a WHERE fragment that still needs the FROM/JOIN wrapped
    # around it. Raw-SQL rules (path 1) already return a complete query
    # via _build_raw_sql() and pass straight through unchanged.
    if is_raw_sql_rule(rule):
        return sql, "ROW"

    if not sql:
        raise ValueError(f"rule_id={rule.get('rule_id')} ({rule.get('rule_code')}): produced empty WHERE clause.")

    join_clause = f"\n    {join_sql}" if join_sql else ""
    row_query = f"SELECT *\n    FROM {table} t{join_clause}\n    WHERE ({sql}) AND ({filter_cnd})"
    return row_query, "ROW"


def build_rule_sql(rule: dict, table: str, filter_sql: str, source_type: str) -> Tuple[str, str]:
    """
    Generate SQL for one rule via whichever of the 3 authoring paths applies.
    Returns (sql, level). Raises ValueError on missing required fields.
    """
    if is_raw_sql_rule(rule):
        return _build_raw_sql(rule, filter_sql)

    ct = (rule.get("check_type") or "").strip().upper()

    if not ct:   # path 3: legacy fragment
        sql = (rule.get("rule_syntax") or "").strip()
        if not sql:
            raise ValueError(f"Rule {rule.get('rule_code')} has no sql_dialect, check_type, or rule_syntax.")
        return sql, "ROW"

    if ct == "COLUMN_EXISTS":
        return "", "SCHEMA"

    spec = CHECK_CATALOG.get(ct)   # path 2: built-in check_type generator
    if spec is None:
        logger.warning("Unknown check_type '%s' for rule %s — falling back to rule_syntax.",
                       ct, rule.get("rule_code"))
        sql = (rule.get("rule_syntax") or "").strip()
        if not sql:
            raise ValueError(f"Unknown check_type '{ct}' and no rule_syntax fallback (rule_code={rule.get('rule_code')}).")
        return sql, "ROW"

    level, params, fn = spec["level"], _parse_params(rule), spec["fn"]
    try:
        if level == "ROW":
            sql = fn(rule, table, _ALIAS, filter_sql, params, source_type)
        elif level == "TABLE":
            sql = fn(rule, table, filter_sql, params, source_type)
        else:
            sql = ""
    except (ValueError, KeyError) as exc:
        raise ValueError(f"Error generating SQL for rule {rule.get('rule_code')} (check_type={ct}): {exc}") from exc

    return sql, level


def _build_raw_sql(rule: dict, filter_sql: str) -> Tuple[str, str]:
    """
    Return the rule's own negative-SQL SELECT verbatim. If the rule opted
    into run-scoping (filter_column/filter_sql set), wrap it as an outer
    subquery rather than asking every rule author to duplicate scoping
    logic:

        SELECT * FROM ( <rule_syntax> ) dq_raw_sql WHERE (<filter>)

    The rule's own SELECT list must include whatever column the filter
    references (e.g. a pull_date/batch column).
    """
    sql = (rule.get("rule_syntax") or "").strip()
    if not sql:
        raise ValueError(f"Rule {rule.get('rule_code')} declares sql_dialect='{rule.get('sql_dialect')}' but rule_syntax is empty.")

    if sql.endswith(";"):
        sql = sql[:-1].rstrip()

    fc = (filter_sql or "").strip()
    if fc and fc != "1=1":
        return f"SELECT * FROM (\n{sql}\n) dq_raw_sql WHERE ({fc})", "ROW"
    return sql, "ROW"


def validate_rule_params(rule: dict) -> Optional[str]:
    """
    Static (no DB access) check that a rule is internally well-formed.
    Returns an error string, or None if OK. Dialect-vs-connection
    compatibility is a separate, DB-aware check — see check_dialect() below.
    """
    if is_raw_sql_rule(rule):
        dialect = (rule.get("sql_dialect") or "").strip().lower()
        if dialect not in VALID_DIALECTS:
            return f"sql_dialect '{dialect}' is invalid — must be one of {', '.join(sorted(VALID_DIALECTS))}."
        if not (rule.get("rule_syntax") or "").strip():
            return "sql_dialect is set but rule_syntax is empty."
        if not (rule.get("primary_key_columns") or "").strip():
            return ("Raw SQL rules must declare primary_key_columns (the key_columns "
                   "identifying the audited entity in the rule's own SELECT).")
        return None

    ct = (rule.get("check_type") or "").strip().upper()

    if not ct:
        return None if (rule.get("rule_syntax") or "").strip() else \
               "sql_dialect/check_type are not set and rule_syntax is empty."

    if ct == "COLUMN_EXISTS":
        return None if (rule.get("check_column") or "").strip() else "COLUMN_EXISTS requires check_column."

    spec = CHECK_CATALOG.get(ct)
    if spec is None:
        return None if (rule.get("rule_syntax") or "").strip() else f"Unknown check_type '{ct}' and rule_syntax is empty."

    missing = [k for k in spec.get("required", []) if k not in _parse_params(rule)]
    return f"check_type '{ct}' requires check_params fields: {', '.join(missing)}" if missing else None


# =============================================================================
# Dialect safety
# =============================================================================

class DialectMismatchError(Exception):
    """Raised when a rule's declared sql_dialect cannot run against its target connection."""


VALID_DIALECTS = {"teradata", "postgres", "ansi"}

# source_type (from the adapter / dq_connections.source_type) -> set of
# sql_dialect values that are safe to execute against it. 'ansi' is always
# accepted everywhere by definition.
DIALECT_COMPATIBILITY = {
    "teradata":   {"teradata", "ansi"},
    "postgresql": {"postgres", "ansi"},
    "postgres":   {"postgres", "ansi"},   # alias
    "aurora":     {"postgres", "ansi"},   # Aurora PG-compatible
    # S3 landed files are queried through DuckDB, which implements a
    # Postgres-flavoured SQL surface — 'postgres'-dialect rules run
    # unmodified against it.
    "s3":         {"postgres", "ansi"},
    "duckdb":     {"postgres", "ansi"},   # alias — internal adapter name
}


def check_dialect(rule: dict, source_type: str) -> None:
    """
    Raise DialectMismatchError if rule['sql_dialect'] cannot run against a
    connection reporting the given source_type. No-op for rules with no
    sql_dialect set (path 2/3 — exempt by construction) or an
    unrecognised source_type (let the query itself surface any real error).
    """
    dialect = (rule.get("sql_dialect") or "").strip().lower()
    if not dialect:
        return

    if dialect not in VALID_DIALECTS:
        raise DialectMismatchError(
            f"{rule.get('rule_code')}: invalid sql_dialect '{dialect}'. "
            f"Must be one of: {', '.join(sorted(VALID_DIALECTS))}."
        )

    st = (source_type or "").strip().lower()
    allowed = DIALECT_COMPATIBILITY.get(st)
    if allowed is None:
        logger.warning("Dialect check skipped for rule %s — unrecognised source_type '%s'.",
                       rule.get("rule_code"), source_type)
        return

    if dialect not in allowed:
        raise DialectMismatchError(
            f"{rule.get('rule_code')} is written for '{dialect}', cannot run against a "
            f"'{st}' connection. Allowed dialects for '{st}': {', '.join(sorted(allowed))}."
        )
