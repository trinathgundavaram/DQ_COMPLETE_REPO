"""
rules_engine/rule_sql.py
-------------------
Everything involved in turning one dq_rules row into ready-to-run,
dialect-safe SQL: the run-scope filter, raw negative-SQL assembly, and the
fail-fast dialect check. These concerns (filter, SQL assembly, dialect
safety) always run together for every rule, so they live in one file that
reads top to bottom as "how do we get valid SQL for this rule."

Rule authoring model: raw negative SQL only
--------------------------------------------
Every rule declares sql_dialect ('teradata' | 'postgres' | 'ansi') and a
COMPLETE, self-contained negative-SQL SELECT in rule_syntax that returns
the rows violating the rule -- its own joins/subqueries/CTEs, any valid
SQL for the declared dialect. Zero rows returned means the rule passed.
Both fields are required (see validate_rule_params()).

check_type may optionally also be set -- it is a plain free-text
classification/taxonomy tag (e.g. for dashboard grouping/filtering) and
never affects SQL generation or execution.

filter_column / filter_type / filter_sql are an optional run-scoping
layer, independent of the rule's own SQL: they let a rule auto-scope to
the current run's batch/date window without the rule author hand-coding
date literals into rule_syntax every time (see build_filter() /
_build_raw_sql() below). Set neither and the rule runs unscoped
(full-table) every time -- see check_query_risk() for why that's flagged.

An earlier version of this engine also supported a declarative
"check_type generates the SQL for you" path and a legacy WHERE-fragment
path (see git history / DESIGN.md if archaeology is ever needed). Both
were removed: every rule in production use was already authored as raw
SQL, and the declarative paths only added surface area (a whole generator
library, extra dq_rules/dq_rule_versions columns, a dq_check_catalog
reference table) that nothing exercised.

Dialect safety
--------------
check_dialect() compares a rule's declared sql_dialect against the target
connection's source_type (DIALECT_COMPATIBILITY below) and raises
DialectMismatchError on a mismatch -- called from rules_engine/engine.py
(pre-validation, load-time) and rules_engine/executor.py (defense-in-depth,
immediately before execution) so a mismatch is always caught before a
query reaches the database, never as a confusing mid-run syntax error.
'ansi' is accepted everywhere by definition.

Public API
----------
build_filter(rule, run) -> str
build_count_query(rule, run) -> str
build_query(rule, run, source_type) -> (sql: str, level: str)   level is always "ROW"
validate_rule_params(rule) -> str | None       (static, no DB access)
check_dialect(rule, source_type) -> None       (raises DialectMismatchError)
check_no_dml_ddl(rule_syntax, rule_code) -> None   (raises UnsafeRuleSQLError)
check_query_risk(rule) -> list[str]            (advisory cost warnings, never raises)
"""

import logging
import re
from typing import Optional, Tuple

from utils.db_helpers import resolve_table

logger = logging.getLogger(__name__)


# =============================================================================
# Write-statement guard (rule_syntax must be read-only)
# =============================================================================
#
# rule_syntax is free-text SQL that ends up embedded in queries the engine
# executes against a live connection (see _build_raw_sql below, and
# rules_engine/executor.py's _count_failed/_fetch_failed_rows, which wrap it as
# `SELECT COUNT(*) FROM (<rule_syntax>) x`). A bare DML/DDL statement in
# that position is usually just a syntax error once wrapped in a subquery
# -- but a data-modifying CTE (e.g. Postgres/DuckDB's
# `WITH t AS (DELETE FROM foo RETURNING *) SELECT * FROM t`) still parses
# as a SELECT overall and would execute the DELETE as a side effect the
# moment the CTE is evaluated, including during validate_sql()'s syntax
# dry-run. check_no_dml_ddl() closes that gap by rejecting any banned
# keyword appearing outside a string literal or comment, regardless of
# where in the statement it appears.

class UnsafeRuleSQLError(Exception):
    """Raised when rule_syntax contains a DML/DDL statement rule authors must never write."""


_BANNED_SQL_KEYWORDS = {
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "MERGE",
    "CREATE", "GRANT", "REVOKE", "EXEC", "EXECUTE", "CALL", "COPY",
    "ATTACH", "DETACH", "VACUUM", "REPLACE", "PRAGMA",
}


def _strip_sql_noise(sql: str) -> str:
    """
    Strip string literals and comments so keyword scanning doesn't
    false-positive on a banned word inside a quoted value (e.g.
    WHERE status = 'DELETE_REQUESTED') or a comment. Not a full SQL
    parser -- just enough to not choke on what rule authors actually write.
    """
    out = []
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'" and j + 1 < n and sql[j + 1] == "'":
                    j += 2
                    continue
                if sql[j] == "'":
                    j += 1
                    break
                j += 1
            i = j
            continue
        if ch == "-" and i + 1 < n and sql[i + 1] == "-":
            nl = sql.find("\n", i)
            i = n if nl == -1 else nl + 1
            continue
        if ch == "/" and i + 1 < n and sql[i + 1] == "*":
            end = sql.find("*/", i + 2)
            i = n if end == -1 else end + 2
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def check_no_dml_ddl(rule_syntax: str, rule_code: str = "") -> None:
    """
    Raise UnsafeRuleSQLError if rule_syntax contains a DML/DDL keyword
    outside a string literal or comment. Rule authors get a full
    negative-SQL SELECT (subqueries, joins, CTEs feeding a SELECT) --
    never a statement that writes to the database.

    Called from both validate_rule_params() (load-time / pre-validation,
    same call site as check_dialect() in rules_engine/engine.py) and directly
    from _build_raw_sql() below (execution-time defense in depth, same
    pattern as check_dialect()'s second call site in rules_engine/executor.py).
    """
    cleaned = _strip_sql_noise(rule_syntax or "")
    words = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", cleaned.upper())
    hit = next((w for w in words if w in _BANNED_SQL_KEYWORDS), None)
    if hit:
        raise UnsafeRuleSQLError(
            f"{rule_code or '(unknown rule)'}: rule_syntax contains a disallowed "
            f"DML/DDL keyword '{hit}'. Rules may only read data (SELECT, including "
            f"CTEs that feed a SELECT) -- they must never write to the database."
        )


# =============================================================================
# Query-cost heuristics (advisory, not blocking)
# =============================================================================
#
# check_no_dml_ddl() above answers "is this rule SAFE to run at all".
# check_query_risk() answers a different question: "is this rule likely to
# be EXPENSIVE against the source system" -- an unscoped SELECT * on a
# billion-row table is perfectly safe SQL that can still burden a shared
# source connection for everyone else using it. These are static, no-DB
# text heuristics (no EXPLAIN / live cost estimate -- that would need a
# connection and a dialect-specific plan parser per source_type, which is
# real future work -- see DESIGN.md's follow-ups). They're intentionally
# advisory (warnings logged to dq_rule_issues, never block a run) because
# every one of them has a legitimate use (e.g. a full scan of a genuinely
# small reference table is fine) -- the goal is to make a rule author
# consciously confirm that, not to guess wrong and block a valid rule.
# The runtime backstop that actually protects the source system regardless
# of whether a rule LOOKED risky is executor.py's query timeout
# (DQ_QUERY_TIMEOUT_SECONDS) -- this is a second, independent layer.

_IN_LIST_WARN_THRESHOLD = 500   # items in a single literal IN (...) list


def check_query_risk(rule: dict) -> list:
    """
    Scan a rule's SQL text for patterns that tend to produce expensive
    queries. Returns a list of human-readable warning strings (empty list
    = nothing flagged). Never raises.

      - no scoping filter at all (filter_sql and filter_column both empty
        -- build_filter() would fall back to the full-table '1=1')
      - SELECT * with no visible WHERE anywhere in the statement
      - an explicit CROSS JOIN
      - a literal IN (...) list with more than _IN_LIST_WARN_THRESHOLD items
    """
    warnings = []

    no_filter = (
        not (rule.get("filter_sql") or "").strip()
        and not (rule.get("filter_column") or "").strip()
    )
    if no_filter:
        warnings.append(
            "No filter_sql or filter_column set -- this rule scans the full "
            "table on every run with no scoping. Confirm that's intentional "
            "(fine for a small reference table; expensive on a large fact table)."
        )

    raw = _strip_sql_noise(rule.get("rule_syntax") or "")
    upper = raw.upper()

    if re.search(r"SELECT\s+\*", upper) and "WHERE" not in upper:
        warnings.append(
            "SELECT * with no WHERE clause anywhere in rule_syntax -- likely "
            "reads every row and every column. Consider selecting only the "
            "columns the rule needs and adding a scoping condition."
        )

    if re.search(r"\bCROSS\s+JOIN\b", upper):
        warnings.append(
            "Explicit CROSS JOIN found -- produces a row for every "
            "combination of both sides. Confirm that's intentional, not an "
            "accidental cartesian product from a missing join condition."
        )

    for match in re.finditer(r"\bIN\s*\(([^()]*)\)", raw, re.IGNORECASE):
        item_count = match.group(1).count(",") + 1
        if item_count > _IN_LIST_WARN_THRESHOLD:
            warnings.append(
                f"Literal IN (...) list with ~{item_count} items -- some "
                f"source DBs cap or slow down badly on very large IN lists. "
                f"Consider a temp/joined table of values instead."
            )
            break   # one warning is enough even if multiple large lists exist

    return warnings


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
    table = resolve_table(rule)
    return (
        f"SELECT COUNT(*) AS total_count\n"
        f"    FROM {table} t\n"
        f"    WHERE ({build_filter(rule, run)})"
    )


# =============================================================================
# SQL generation — raw negative SQL only
# =============================================================================

def is_raw_sql_rule(rule: dict) -> bool:
    """
    True when sql_dialect is set. Every valid rule is a raw-SQL rule now
    (see module docstring) -- kept as a named check because
    validate_rule_params() and callers use it to produce a clear error
    for a rule missing sql_dialect, rather than hardcoding the check inline.
    """
    return bool((rule.get("sql_dialect") or "").strip())


def build_query(rule: dict, run: dict, source_type: str = "teradata") -> Tuple[str, str]:
    """
    Assemble the full query for a rule.

    Returns (sql, level) -- level is always "ROW" (ready-to-run SELECT;
    executor wraps it in COUNT(*)/fetch subqueries). Kept as a tuple for
    API stability with executor.py/rule_tester.py rather than changing
    every call site to a bare string.
    """
    filter_cnd = build_filter(rule, run)
    return build_rule_sql(rule, filter_cnd)


def build_rule_sql(rule: dict, filter_sql: str) -> Tuple[str, str]:
    """Generate SQL for one rule. Returns (sql, "ROW"). Raises ValueError if malformed."""
    if not is_raw_sql_rule(rule):
        raise ValueError(
            f"Rule {rule.get('rule_code')} has no sql_dialect set. Every rule must "
            f"declare sql_dialect ('teradata' | 'postgres' | 'ansi') and a complete "
            f"negative-SQL SELECT in rule_syntax -- see rules_engine/rule_sql.py's "
            f"module docstring."
        )
    return _build_raw_sql(rule, filter_sql)


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

    check_no_dml_ddl(sql, rule.get("rule_code"))

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
    dialect = (rule.get("sql_dialect") or "").strip().lower()
    if not dialect:
        return (
            "sql_dialect is not set. Every rule must declare sql_dialect "
            f"('teradata' | 'postgres' | 'ansi') and a negative-SQL SELECT "
            f"in rule_syntax."
        )
    if dialect not in VALID_DIALECTS:
        return f"sql_dialect '{dialect}' is invalid — must be one of {', '.join(sorted(VALID_DIALECTS))}."

    rule_syntax = (rule.get("rule_syntax") or "").strip()
    if not rule_syntax:
        return "sql_dialect is set but rule_syntax is empty."

    if not (rule.get("primary_key_columns") or "").strip():
        return ("Rules must declare primary_key_columns (the key_columns "
               "identifying the audited entity in the rule's own SELECT).")

    try:
        check_no_dml_ddl(rule_syntax, rule.get("rule_code"))
    except UnsafeRuleSQLError as exc:
        return str(exc)

    return None


# =============================================================================
# Dialect safety
# =============================================================================

class DialectMismatchError(Exception):
    """Raised when a rule's declared sql_dialect cannot run against its target connection."""


VALID_DIALECTS = {"teradata", "postgres", "ansi"}

# source_type (from the adapter / config/connections.yaml) -> set of
# sql_dialect values that are safe to execute against it. 'ansi' is always
# accepted everywhere by definition.
DIALECT_COMPATIBILITY = {
    "teradata":   {"teradata", "ansi"},
    "postgresql": {"postgres", "ansi"},
    "postgres":   {"postgres", "ansi"},   # alias
    "aurora":     {"postgres", "ansi"},   # Aurora PG-compatible
    # S3 landed files and local flat files (FileAdapter) are both queried
    # through DuckDB, which implements a Postgres-flavoured SQL surface —
    # 'postgres'-dialect rules run unmodified against either.
    "s3":         {"postgres", "ansi"},
    "file":       {"postgres", "ansi"},   # FileAdapter (db/adapters.py) — also DuckDB
    "duckdb":     {"postgres", "ansi"},   # alias — internal adapter name
}


def check_dialect(rule: dict, source_type: str) -> None:
    """
    Raise DialectMismatchError if rule['sql_dialect'] cannot run against a
    connection reporting the given source_type. No-op for a rule with no
    sql_dialect set (validate_rule_params() already flags that as a
    separate, clearer config error) or an unrecognised source_type (let
    the query itself surface any real error).
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
