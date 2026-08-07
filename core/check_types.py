"""
core/check_types.py
-------------------
Built-in check type catalog for the DQ framework.

Every check type produces SQL that identifies FAILING rows (ROW level) or a
full query whose row count is the failure count (TABLE level).

Check levels
------------
ROW     — generates a WHERE-clause fragment.  The executor wraps it as:
              SELECT * FROM {table} t WHERE ({fragment}) AND ({filter})
          total_records = actual table row count
          failed_records = rows matching the WHERE fragment

TABLE   — generates a complete SQL query returning 0 rows (PASS) or ≥1 rows
          (FAIL).  Typically an aggregate query with a HAVING clause.
          total_records = 1  (the "table property" is the unit being tested)
          failed_records = COUNT(*) of the generated query  (0 or 1)

SCHEMA  — metadata check (column existence).  Handled specially by executor;
          no SQL is generated here — executor queries the DB catalog.

Cross-database compatibility
-----------------------------
All generators receive `source_type` and emit dialect-appropriate SQL.
Supported source types: teradata, postgresql, aurora, databricks, sqlserver, file.

Adding a new check type
-----------------------
1. Write a generator function with signature:
       ROW:   fn(rule, table, alias, filter_sql, params, source_type) -> str
       TABLE: fn(rule, table, filter_sql, params, source_type) -> str
2. Register it in CHECK_CATALOG below.
"""

import json
import logging

logger = logging.getLogger(__name__)

_ALIAS = "t"   # outer table alias used throughout core/rule_sql.py

# ─── Dimension constants ──────────────────────────────────────────────────────
COMPLETENESS  = "COMPLETENESS"
UNIQUENESS    = "UNIQUENESS"
VALIDITY      = "VALIDITY"
CONSISTENCY   = "CONSISTENCY"
TIMELINESS    = "TIMELINESS"
VOLUME        = "VOLUME"
ACCURACY      = "ACCURACY"
SCHEMA        = "SCHEMA"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _stddev_fn(source_type: str) -> str:
    """Return the sample standard deviation function name for each DB."""
    return "STDEV" if source_type in ("sqlserver", "mssql") else "STDDEV_SAMP"


def _freshness_threshold(max_age_hours: int, source_type: str) -> str:
    """Return a CURRENT_TIMESTAMP - N hours expression per database dialect."""
    st = source_type.lower()
    if st == "teradata":
        return f"CURRENT_TIMESTAMP - {max_age_hours} * INTERVAL '1' HOUR"
    if st in ("postgresql", "postgres", "aurora"):
        return f"CURRENT_TIMESTAMP - INTERVAL '{max_age_hours} hours'"
    if st == "databricks":
        return f"TIMESTAMPADD(HOUR, -{max_age_hours}, CURRENT_TIMESTAMP)"
    if st in ("sqlserver", "mssql"):
        return f"DATEADD(hour, -{max_age_hours}, GETDATE())"
    # DuckDB / file
    return f"CURRENT_TIMESTAMP - INTERVAL '{max_age_hours} hours'"


def _parse_params(rule: dict) -> dict:
    raw = rule.get("check_params") or "{}"
    try:
        return json.loads(raw) if isinstance(raw, str) else (raw or {})
    except (json.JSONDecodeError, TypeError) as exc:
        logger.warning("Invalid check_params JSON for rule %s: %s",
                       rule.get("rule_code"), exc)
        return {}


def _col(rule: dict, key: str = "check_column") -> str:
    """Return the primary check column from the rule dict."""
    return (rule.get(key) or "").strip()


def _cols(rule: dict) -> list:
    """Return all check columns (comma-separated check_column)."""
    raw = _col(rule)
    return [c.strip() for c in raw.split(",") if c.strip()]


def _filter_clause(filter_sql: str) -> str:
    """Return ' AND (filter)' or '' when filter is trivially true."""
    if filter_sql and filter_sql.strip() not in ("1=1", ""):
        return f" AND ({filter_sql})"
    return ""


def _require_col(rule: dict, check_type: str) -> str:
    c = _col(rule)
    if not c:
        raise ValueError(
            f"{check_type} requires check_column to be set "
            f"(rule_code={rule.get('rule_code')})."
        )
    return c


# ─────────────────────────────────────────────────────────────────────────────
# ROW-level generators  (return WHERE-clause fragment identifying FAILING rows)
# ─────────────────────────────────────────────────────────────────────────────

def _not_null(rule, table, alias, filter_sql, params, source_type):
    """Fails for rows where the column IS NULL."""
    col = _require_col(rule, "NOT_NULL")
    return f"{alias}.{col} IS NULL"


def _not_empty(rule, table, alias, filter_sql, params, source_type):
    """Fails for rows where the column is NULL or blank/whitespace."""
    col = _require_col(rule, "NOT_EMPTY")
    return (
        f"({alias}.{col} IS NULL "
        f"OR TRIM(CAST({alias}.{col} AS VARCHAR(4000))) = '')"
    )


def _unique(rule, table, alias, filter_sql, params, source_type):
    """Fails for rows whose check_column value appears more than once in scope."""
    col = _require_col(rule, "UNIQUE")
    fc  = _filter_clause(filter_sql)
    return (
        f"(SELECT COUNT(*) FROM {table} _u "
        f"WHERE _u.{col} = {alias}.{col}{fc}) > 1"
    )


def _unique_combination(rule, table, alias, filter_sql, params, source_type):
    """Fails for rows whose combination of check_columns appears more than once."""
    cols = _cols(rule)
    if not cols:
        raise ValueError(
            f"UNIQUE_COMBINATION requires at least two columns in check_column "
            f"(rule_code={rule.get('rule_code')})."
        )
    joins = " AND ".join(f"_u.{c} = {alias}.{c}" for c in cols)
    fc    = _filter_clause(filter_sql)
    return (
        f"(SELECT COUNT(*) FROM {table} _u "
        f"WHERE {joins}{fc}) > 1"
    )


def _regex_match(rule, table, alias, filter_sql, params, source_type):
    """Fails for rows where the column does NOT match the regex pattern."""
    col     = _require_col(rule, "REGEX_MATCH")
    pattern = params.get("pattern", "")
    if not pattern:
        raise ValueError(
            f"REGEX_MATCH requires check_params.pattern "
            f"(rule_code={rule.get('rule_code')})."
        )
    # Escape single-quotes inside the pattern
    p = pattern.replace("'", "''")
    st = source_type.lower()

    if st == "teradata":
        return (
            f"REGEXP_SIMILAR(CAST({alias}.{col} AS VARCHAR(4000)), '{p}', 'i') = 0"
        )
    if st in ("postgresql", "postgres", "aurora"):
        return f"NOT ({alias}.{col} ~* '{p}')"
    if st == "databricks":
        return f"NOT RLIKE(CAST({alias}.{col} AS STRING), '(?i){p}')"
    if st in ("sqlserver", "mssql"):
        # SQL Server has no native regex — emit LIKE with a note
        like_p = (pattern
                  .replace(".*", "%").replace(".+", "_%")
                  .replace("^", "").replace("$", "")
                  .replace("'", "''"))
        logger.warning(
            "REGEX_MATCH on SQL Server uses LIKE approximation — "
            "complex patterns may not behave as expected (rule %s).",
            rule.get("rule_code"),
        )
        return f"{alias}.{col} NOT LIKE '{like_p}'"
    # DuckDB / file
    return f"NOT REGEXP_MATCHES(CAST({alias}.{col} AS VARCHAR), '{p}')"


def _in_list(rule, table, alias, filter_sql, params, source_type):
    """Fails for rows where check_column value is NOT in the allowed list."""
    col    = _require_col(rule, "IN_LIST")
    values = params.get("values", [])
    if not values:
        raise ValueError(
            f"IN_LIST requires check_params.values (list) "
            f"(rule_code={rule.get('rule_code')})."
        )
    quoted = ", ".join(f"'{str(v).replace(chr(39), chr(39)*2)}'" for v in values)
    return f"{alias}.{col} NOT IN ({quoted})"


def _not_in_list(rule, table, alias, filter_sql, params, source_type):
    """Fails for rows where check_column value IS in the forbidden list."""
    col    = _require_col(rule, "NOT_IN_LIST")
    values = params.get("values", [])
    if not values:
        raise ValueError(
            f"NOT_IN_LIST requires check_params.values (list) "
            f"(rule_code={rule.get('rule_code')})."
        )
    quoted = ", ".join(f"'{str(v).replace(chr(39), chr(39)*2)}'" for v in values)
    return f"{alias}.{col} IN ({quoted})"


def _range_check(rule, table, alias, filter_sql, params, source_type):
    """Fails for rows where check_column is outside [min_value, max_value]."""
    col = _require_col(rule, "RANGE_CHECK")
    lo  = params.get("min_value")
    hi  = params.get("max_value")
    if lo is None or hi is None:
        raise ValueError(
            f"RANGE_CHECK requires check_params.min_value and max_value "
            f"(rule_code={rule.get('rule_code')})."
        )
    return f"({alias}.{col} < {lo} OR {alias}.{col} > {hi})"


def _min_value(rule, table, alias, filter_sql, params, source_type):
    """Fails for rows where check_column < min_value."""
    col = _require_col(rule, "MIN_VALUE")
    lo  = params.get("min_value")
    if lo is None:
        raise ValueError(
            f"MIN_VALUE requires check_params.min_value "
            f"(rule_code={rule.get('rule_code')})."
        )
    return f"{alias}.{col} < {lo}"


def _max_value(rule, table, alias, filter_sql, params, source_type):
    """Fails for rows where check_column > max_value."""
    col = _require_col(rule, "MAX_VALUE")
    hi  = params.get("max_value")
    if hi is None:
        raise ValueError(
            f"MAX_VALUE requires check_params.max_value "
            f"(rule_code={rule.get('rule_code')})."
        )
    return f"{alias}.{col} > {hi}"


def _positive_value(rule, table, alias, filter_sql, params, source_type):
    """Fails for rows where check_column is zero, negative, or NULL."""
    col = _require_col(rule, "POSITIVE_VALUE")
    return f"({alias}.{col} IS NULL OR {alias}.{col} <= 0)"


def _non_negative(rule, table, alias, filter_sql, params, source_type):
    """Fails for rows where check_column is negative or NULL."""
    col = _require_col(rule, "NON_NEGATIVE")
    return f"({alias}.{col} IS NULL OR {alias}.{col} < 0)"


def _cross_column(rule, table, alias, filter_sql, params, source_type):
    """
    Fails when an inter-column relationship is violated.

    check_params.expression : SQL boolean expression using column names
                              (without table alias) that must be TRUE to PASS.
                              Failing rows are those where the expression is FALSE or NULL.

    Example:
        check_params = {"expression": "end_date >= start_date"}
        → fails when end_date < start_date OR either is NULL
    """
    expr = params.get("expression", "").strip()
    if not expr:
        raise ValueError(
            f"CROSS_COLUMN requires check_params.expression "
            f"(rule_code={rule.get('rule_code')})."
        )
    # Prefix bare column names with alias (simple heuristic: wrap with NOT)
    return f"NOT ({expr})"


def _conditional(rule, table, alias, filter_sql, params, source_type):
    """
    Fails when a conditional (IF → THEN) relationship is violated.

    check_params:
        if_column    : column to test
        if_value     : value that triggers the rule (can be a list for IN check)
        then_column  : column that must satisfy the condition when if_column matches
        then_operator: IS NULL | IS NOT NULL | = | != | > | < | IN
        then_value   : value for then_operator (not needed for IS NULL / IS NOT NULL)

    Example: if status = 'ACTIVE' then effective_date IS NOT NULL
        check_params = {
            "if_column": "status",
            "if_value": "ACTIVE",
            "then_column": "effective_date",
            "then_operator": "IS NOT NULL"
        }
    """
    if_col  = params.get("if_column", "").strip()
    if_val  = params.get("if_value")
    th_col  = params.get("then_column", "").strip()
    th_op   = params.get("then_operator", "IS NOT NULL").strip().upper()
    th_val  = params.get("then_value")

    if not if_col or not th_col:
        raise ValueError(
            f"CONDITIONAL requires check_params.if_column and then_column "
            f"(rule_code={rule.get('rule_code')})."
        )

    # Build IF condition
    if isinstance(if_val, list):
        quoted = ", ".join(f"'{str(v).replace(chr(39), chr(39)*2)}'" for v in if_val)
        if_cond = f"{alias}.{if_col} IN ({quoted})"
    else:
        if_cond = f"{alias}.{if_col} = '{str(if_val).replace(chr(39), chr(39)*2)}'"

    # Build THEN condition (negated — we want rows that VIOLATE the rule)
    if th_op in ("IS NOT NULL",):
        then_fail = f"{alias}.{th_col} IS NULL"
    elif th_op in ("IS NULL",):
        then_fail = f"{alias}.{th_col} IS NOT NULL"
    elif th_op in ("=", "!=", ">", "<", ">=", "<="):
        q_val = f"'{str(th_val).replace(chr(39), chr(39)*2)}'" if isinstance(th_val, str) else str(th_val)
        negated = {"=": "!=", "!=": "=", ">": "<=", "<": ">=", ">=": "<", "<=": ">"}[th_op]
        then_fail = f"{alias}.{th_col} {negated} {q_val}"
    elif th_op == "IN":
        if isinstance(th_val, list):
            quoted = ", ".join(f"'{str(v).replace(chr(39), chr(39)*2)}'" for v in th_val)
        else:
            quoted = f"'{str(th_val).replace(chr(39), chr(39)*2)}'"
        then_fail = f"{alias}.{th_col} NOT IN ({quoted})"
    else:
        raise ValueError(
            f"Unsupported then_operator '{th_op}' in CONDITIONAL "
            f"(rule_code={rule.get('rule_code')})."
        )

    return f"({if_cond} AND {then_fail})"


def _referential_integrity(rule, table, alias, filter_sql, params, source_type):
    """
    Fails for rows where check_column has no matching value in a reference table.

    check_params.ref_table  : fully-qualified reference table name
    check_params.ref_column : column in ref_table to match against

    NULLs in check_column always fail (foreign key semantics).
    Add NOT_NULL as a separate rule if NULLs are acceptable.
    """
    col      = _require_col(rule, "REFERENTIAL_INTEGRITY")
    ref_tbl  = params.get("ref_table", "").strip()
    ref_col  = params.get("ref_column", "").strip()
    if not ref_tbl or not ref_col:
        raise ValueError(
            f"REFERENTIAL_INTEGRITY requires check_params.ref_table and ref_column "
            f"(rule_code={rule.get('rule_code')})."
        )
    return (
        f"NOT EXISTS ("
        f"SELECT 1 FROM {ref_tbl} _ri "
        f"WHERE _ri.{ref_col} = {alias}.{col}"
        f")"
    )


def _outlier_check(rule, table, alias, filter_sql, params, source_type):
    """
    Fails for rows where check_column deviates more than N standard deviations
    from the in-scope mean.  Default n_stddev = 3.

    check_params.n_stddev : number of standard deviations (default 3)
    """
    col      = _require_col(rule, "OUTLIER_CHECK")
    n_stddev = float(params.get("n_stddev", 3.0))
    fc       = _filter_clause(filter_sql)
    sdfn     = _stddev_fn(source_type)

    mean_subq = f"(SELECT AVG({col}) FROM {table} _os WHERE 1=1{fc})"
    sdev_subq = f"(SELECT {sdfn}({col}) FROM {table} _os WHERE 1=1{fc})"

    return (
        f"ABS({alias}.{col} - {mean_subq}) > {n_stddev} * COALESCE({sdev_subq}, 0)"
    )


# ─────────────────────────────────────────────────────────────────────────────
# TABLE-level generators  (return complete SQL; 0 rows = PASS, ≥1 = FAIL)
# ─────────────────────────────────────────────────────────────────────────────

def _freshness(rule, table, filter_sql, params, source_type):
    """
    Fails when the most recent value of check_column is older than max_age_hours.

    check_params.max_age_hours : maximum acceptable data age in hours (default 24)
    """
    col          = _require_col(rule, "FRESHNESS")
    max_age_hrs  = int(params.get("max_age_hours", 24))
    threshold    = _freshness_threshold(max_age_hrs, source_type)
    where_part   = f"WHERE ({filter_sql})" if filter_sql and filter_sql != "1=1" else ""

    return (
        f"SELECT\n"
        f"    MAX({_ALIAS}.{col}) AS last_updated,\n"
        f"    {max_age_hrs}       AS max_age_hours_required\n"
        f"FROM {table} {_ALIAS}\n"
        f"{where_part}\n"
        f"HAVING MAX({_ALIAS}.{col}) < {threshold}"
    )


def _min_row_count(rule, table, filter_sql, params, source_type):
    """Fails when the row count (in scope) is below min_rows."""
    min_rows   = int(params.get("min_rows", 1))
    where_part = f"WHERE ({filter_sql})" if filter_sql and filter_sql != "1=1" else ""

    return (
        f"SELECT COUNT(*) AS actual_count, {min_rows} AS min_required\n"
        f"FROM {table} {_ALIAS}\n"
        f"{where_part}\n"
        f"HAVING COUNT(*) < {min_rows}"
    )


def _max_row_count(rule, table, filter_sql, params, source_type):
    """Fails when the row count (in scope) exceeds max_rows."""
    max_rows   = int(params.get("max_rows", 0))
    where_part = f"WHERE ({filter_sql})" if filter_sql and filter_sql != "1=1" else ""

    return (
        f"SELECT COUNT(*) AS actual_count, {max_rows} AS max_allowed\n"
        f"FROM {table} {_ALIAS}\n"
        f"{where_part}\n"
        f"HAVING COUNT(*) > {max_rows}"
    )


def _row_count_range(rule, table, filter_sql, params, source_type):
    """Fails when the row count is outside [min_rows, max_rows]."""
    min_rows   = int(params.get("min_rows", 0))
    max_rows   = int(params.get("max_rows", 0))
    where_part = f"WHERE ({filter_sql})" if filter_sql and filter_sql != "1=1" else ""

    return (
        f"SELECT COUNT(*) AS actual_count,\n"
        f"       {min_rows} AS min_required,\n"
        f"       {max_rows} AS max_allowed\n"
        f"FROM {table} {_ALIAS}\n"
        f"{where_part}\n"
        f"HAVING COUNT(*) < {min_rows} OR COUNT(*) > {max_rows}"
    )


def _aggregate_range(rule, table, filter_sql, params, source_type):
    """
    Fails when an aggregate function on check_column is outside [min_value, max_value].

    check_params:
        aggregate : SUM | AVG | MIN | MAX | COUNT | STDDEV  (default AVG)
        min_value : lower bound (required)
        max_value : upper bound (required)
    """
    col        = _require_col(rule, "AGGREGATE_RANGE")
    agg        = params.get("aggregate", "AVG").upper()
    lo         = params.get("min_value")
    hi         = params.get("max_value")
    where_part = f"WHERE ({filter_sql})" if filter_sql and filter_sql != "1=1" else ""

    allowed = {"SUM", "AVG", "MIN", "MAX", "COUNT", "STDDEV", "STDDEV_SAMP", "STDEV"}
    if agg not in allowed:
        raise ValueError(
            f"AGGREGATE_RANGE: unsupported aggregate '{agg}'. "
            f"Allowed: {sorted(allowed)} (rule_code={rule.get('rule_code')})."
        )
    # Normalise STDDEV per source DB
    if agg in ("STDDEV", "STDDEV_SAMP", "STDEV"):
        agg = _stddev_fn(source_type)

    conditions = []
    if lo is not None:
        conditions.append(f"{agg}({col}) < {lo}")
    if hi is not None:
        conditions.append(f"{agg}({col}) > {hi}")
    if not conditions:
        raise ValueError(
            f"AGGREGATE_RANGE requires at least one of min_value or max_value "
            f"(rule_code={rule.get('rule_code')})."
        )
    having = " OR ".join(conditions)

    return (
        f"SELECT {agg}({col}) AS actual_value,\n"
        f"       {lo if lo is not None else 'NULL'} AS min_expected,\n"
        f"       {hi if hi is not None else 'NULL'} AS max_expected\n"
        f"FROM {table} {_ALIAS}\n"
        f"{where_part}\n"
        f"HAVING {having}"
    )


def _sum_match(rule, table, filter_sql, params, source_type):
    """
    Fails when SUM(check_column) deviates from SUM(ref_column) in ref_table
    by more than tolerance_pct %.

    check_params:
        ref_table      : fully-qualified reference table
        ref_column     : column to sum in ref_table  (default = check_column)
        tolerance_pct  : allowed % deviation (default 0 = exact match)
        ref_filter     : optional WHERE clause for ref_table
    """
    col         = _require_col(rule, "SUM_MATCH")
    ref_tbl     = params.get("ref_table", "").strip()
    ref_col     = params.get("ref_column", col).strip()
    tol_pct     = float(params.get("tolerance_pct", 0.0))
    ref_filter  = params.get("ref_filter", "1=1").strip() or "1=1"
    where_part  = f"({filter_sql})" if filter_sql and filter_sql != "1=1" else "1=1"

    if not ref_tbl:
        raise ValueError(
            f"SUM_MATCH requires check_params.ref_table "
            f"(rule_code={rule.get('rule_code')})."
        )

    return (
        f"WITH _primary AS (\n"
        f"    SELECT COALESCE(SUM({col}), 0) AS total\n"
        f"    FROM {table} {_ALIAS} WHERE {where_part}\n"
        f"),\n"
        f"_reference AS (\n"
        f"    SELECT COALESCE(SUM({ref_col}), 0) AS total\n"
        f"    FROM {ref_tbl} WHERE {ref_filter}\n"
        f")\n"
        f"SELECT\n"
        f"    p.total AS primary_sum,\n"
        f"    r.total AS reference_sum,\n"
        f"    ABS(p.total - r.total) AS absolute_diff,\n"
        f"    CASE WHEN r.total <> 0\n"
        f"         THEN ABS(p.total - r.total) / ABS(r.total) * 100.0\n"
        f"         ELSE NULL END AS pct_diff\n"
        f"FROM _primary p, _reference r\n"
        f"WHERE ABS(p.total - r.total) > ({tol_pct} / 100.0) * ABS(NULLIF(r.total, 0))"
    )


def _count_match(rule, table, filter_sql, params, source_type):
    """
    Fails when COUNT(*) deviates from COUNT(*) in ref_table by more than tolerance_pct %.

    check_params:
        ref_table      : fully-qualified reference table
        tolerance_pct  : allowed % deviation (default 0 = exact match)
        ref_filter     : optional WHERE clause for ref_table
    """
    ref_tbl    = params.get("ref_table", "").strip()
    tol_pct    = float(params.get("tolerance_pct", 0.0))
    ref_filter = params.get("ref_filter", "1=1").strip() or "1=1"
    where_part = f"({filter_sql})" if filter_sql and filter_sql != "1=1" else "1=1"

    if not ref_tbl:
        raise ValueError(
            f"COUNT_MATCH requires check_params.ref_table "
            f"(rule_code={rule.get('rule_code')})."
        )

    return (
        f"WITH _primary AS (\n"
        f"    SELECT COUNT(*) AS cnt FROM {table} {_ALIAS} WHERE {where_part}\n"
        f"),\n"
        f"_reference AS (\n"
        f"    SELECT COUNT(*) AS cnt FROM {ref_tbl} WHERE {ref_filter}\n"
        f")\n"
        f"SELECT\n"
        f"    p.cnt AS primary_count,\n"
        f"    r.cnt AS reference_count,\n"
        f"    ABS(p.cnt - r.cnt) AS absolute_diff\n"
        f"FROM _primary p, _reference r\n"
        f"WHERE ABS(p.cnt - r.cnt) > ({tol_pct} / 100.0) * NULLIF(r.cnt, 0)"
    )


# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA-level check  (handled by executor — no SQL generated here)
# ─────────────────────────────────────────────────────────────────────────────

def _column_exists(rule, table, alias, filter_sql, params, source_type):
    """Executor handles COLUMN_EXISTS specially via DB catalog queries."""
    raise NotImplementedError(
        "COLUMN_EXISTS is handled by the executor, not the query builder."
    )


# ─────────────────────────────────────────────────────────────────────────────
# CHECK CATALOG
# ─────────────────────────────────────────────────────────────────────────────
# Each entry:
#   dimension  : DQ dimension string
#   level      : ROW | TABLE | SCHEMA
#   description: human-readable summary
#   required   : list of required check_params keys  (check_column is implicit)
#   optional   : list of optional check_params keys
#   fn         : generator function (see signatures above)
# ─────────────────────────────────────────────────────────────────────────────

CHECK_CATALOG = {

    # ── COMPLETENESS ──────────────────────────────────────────────────────────
    "NOT_NULL": {
        "dimension":   COMPLETENESS,
        "level":       "ROW",
        "description": "Column must not contain NULL values",
        "required":    [],
        "optional":    [],
        "fn":          _not_null,
    },
    "NOT_EMPTY": {
        "dimension":   COMPLETENESS,
        "level":       "ROW",
        "description": "Column must not be NULL or blank/whitespace",
        "required":    [],
        "optional":    [],
        "fn":          _not_empty,
    },

    # ── UNIQUENESS ────────────────────────────────────────────────────────────
    "UNIQUE": {
        "dimension":   UNIQUENESS,
        "level":       "ROW",
        "description": "Column values must be unique within the scoped dataset",
        "required":    [],
        "optional":    [],
        "fn":          _unique,
    },
    "UNIQUE_COMBINATION": {
        "dimension":   UNIQUENESS,
        "level":       "ROW",
        "description": "Combination of check_columns must be unique (natural key check)",
        "required":    [],   # check_column must contain comma-separated list
        "optional":    [],
        "fn":          _unique_combination,
    },

    # ── VALIDITY ──────────────────────────────────────────────────────────────
    "REGEX_MATCH": {
        "dimension":   VALIDITY,
        "level":       "ROW",
        "description": "Column must match the given regular expression pattern",
        "required":    ["pattern"],
        "optional":    [],
        "fn":          _regex_match,
    },
    "IN_LIST": {
        "dimension":   VALIDITY,
        "level":       "ROW",
        "description": "Column value must be one of the allowed values",
        "required":    ["values"],
        "optional":    [],
        "fn":          _in_list,
    },
    "NOT_IN_LIST": {
        "dimension":   VALIDITY,
        "level":       "ROW",
        "description": "Column value must NOT be any of the forbidden values",
        "required":    ["values"],
        "optional":    [],
        "fn":          _not_in_list,
    },
    "RANGE_CHECK": {
        "dimension":   VALIDITY,
        "level":       "ROW",
        "description": "Column must be within [min_value, max_value] (inclusive)",
        "required":    ["min_value", "max_value"],
        "optional":    [],
        "fn":          _range_check,
    },
    "MIN_VALUE": {
        "dimension":   VALIDITY,
        "level":       "ROW",
        "description": "Column must be >= min_value",
        "required":    ["min_value"],
        "optional":    [],
        "fn":          _min_value,
    },
    "MAX_VALUE": {
        "dimension":   VALIDITY,
        "level":       "ROW",
        "description": "Column must be <= max_value",
        "required":    ["max_value"],
        "optional":    [],
        "fn":          _max_value,
    },
    "POSITIVE_VALUE": {
        "dimension":   VALIDITY,
        "level":       "ROW",
        "description": "Column must be > 0 (NULL also fails)",
        "required":    [],
        "optional":    [],
        "fn":          _positive_value,
    },
    "NON_NEGATIVE": {
        "dimension":   VALIDITY,
        "level":       "ROW",
        "description": "Column must be >= 0 (NULL also fails)",
        "required":    [],
        "optional":    [],
        "fn":          _non_negative,
    },

    # ── CONSISTENCY ───────────────────────────────────────────────────────────
    "CROSS_COLUMN": {
        "dimension":   CONSISTENCY,
        "level":       "ROW",
        "description": "A SQL boolean expression across two or more columns must be TRUE",
        "required":    ["expression"],
        "optional":    [],
        "fn":          _cross_column,
    },
    "CONDITIONAL": {
        "dimension":   CONSISTENCY,
        "level":       "ROW",
        "description": "IF column A has value X, THEN column B must satisfy a condition",
        "required":    ["if_column", "if_value", "then_column", "then_operator"],
        "optional":    ["then_value"],
        "fn":          _conditional,
    },
    "REFERENTIAL_INTEGRITY": {
        "dimension":   CONSISTENCY,
        "level":       "ROW",
        "description": "Column value must exist in the reference table (foreign key check)",
        "required":    ["ref_table", "ref_column"],
        "optional":    [],
        "fn":          _referential_integrity,
    },

    # ── ACCURACY / STATISTICAL ────────────────────────────────────────────────
    "OUTLIER_CHECK": {
        "dimension":   ACCURACY,
        "level":       "ROW",
        "description": "Fails rows that deviate more than N standard deviations from the in-scope mean",
        "required":    [],
        "optional":    ["n_stddev"],
        "fn":          _outlier_check,
    },

    # ── TIMELINESS ────────────────────────────────────────────────────────────
    "FRESHNESS": {
        "dimension":   TIMELINESS,
        "level":       "TABLE",
        "description": "MAX(check_column) must be within max_age_hours of current time",
        "required":    ["max_age_hours"],
        "optional":    [],
        "fn":          _freshness,
    },

    # ── VOLUME ────────────────────────────────────────────────────────────────
    "MIN_ROW_COUNT": {
        "dimension":   VOLUME,
        "level":       "TABLE",
        "description": "Row count must be >= min_rows",
        "required":    ["min_rows"],
        "optional":    [],
        "fn":          _min_row_count,
    },
    "MAX_ROW_COUNT": {
        "dimension":   VOLUME,
        "level":       "TABLE",
        "description": "Row count must be <= max_rows",
        "required":    ["max_rows"],
        "optional":    [],
        "fn":          _max_row_count,
    },
    "ROW_COUNT_RANGE": {
        "dimension":   VOLUME,
        "level":       "TABLE",
        "description": "Row count must be within [min_rows, max_rows]",
        "required":    ["min_rows", "max_rows"],
        "optional":    [],
        "fn":          _row_count_range,
    },
    "AGGREGATE_RANGE": {
        "dimension":   ACCURACY,
        "level":       "TABLE",
        "description": "Aggregate(check_column) — SUM/AVG/MIN/MAX/COUNT — must be within [min_value, max_value]",
        "required":    ["aggregate"],
        "optional":    ["min_value", "max_value"],
        "fn":          _aggregate_range,
    },

    # ── CROSS-TABLE ACCURACY ──────────────────────────────────────────────────
    "SUM_MATCH": {
        "dimension":   ACCURACY,
        "level":       "TABLE",
        "description": "SUM(check_column) must match SUM(ref_column) in ref_table within tolerance_pct %",
        "required":    ["ref_table"],
        "optional":    ["ref_column", "tolerance_pct", "ref_filter"],
        "fn":          _sum_match,
    },
    "COUNT_MATCH": {
        "dimension":   CONSISTENCY,
        "level":       "TABLE",
        "description": "COUNT(*) must match COUNT(*) in ref_table within tolerance_pct %",
        "required":    ["ref_table"],
        "optional":    ["tolerance_pct", "ref_filter"],
        "fn":          _count_match,
    },

    # ── SCHEMA ────────────────────────────────────────────────────────────────
    "COLUMN_EXISTS": {
        "dimension":   SCHEMA,
        "level":       "SCHEMA",
        "description": "Verifies that check_column exists in the source table before any row-level rules run",
        "required":    [],
        "optional":    [],
        "fn":          _column_exists,   # not called — executor handles this path
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Public helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_spec(check_type: str) -> dict:
    """Return the catalog entry for check_type, or None if unknown."""
    return CHECK_CATALOG.get((check_type or "").upper())


def list_check_types() -> list:
    """Return sorted list of all registered check type names."""
    return sorted(CHECK_CATALOG.keys())


def get_dimension(check_type: str) -> str:
    """Return the DQ dimension for a check type, or 'CUSTOM' for unknown."""
    spec = get_spec(check_type)
    return spec["dimension"] if spec else "CUSTOM"


def get_level(check_type: str) -> str:
    """Return ROW | TABLE | SCHEMA | CUSTOM for a given check_type string."""
    spec = get_spec(check_type)
    return spec["level"] if spec else "ROW"   # fallback: treat as ROW (uses rule_syntax)
