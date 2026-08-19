"""
rules_engine/reporting.py
----------------------------
A report is just a query against gre_results, optionally joined out to
gre_exceptions for the underlying records -- no separate report-building
machinery.

get_breaches() and get_records_for_result() stay inside the gre_ metadata
store. get_source_records_for_rule() goes one step further: it ties each
gre_exceptions row back to the actual SOURCE record it came from, at
report/analysis time -- see that function's docstring for why this is a
live re-join rather than a stored copy.
"""

import logging

from shared.db_ops import execute_query, _run_source_query, _escape_sql_literal, EXCEPTION_CHUNK
from rules_engine.executor import parse_natural_key, _format_natural_key

logger = logging.getLogger(__name__)


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

    This returns gre_exceptions' OWN columns only (natural_key_value,
    issue_desc, ...) -- not the source record itself. For the full source
    row behind each of these, see get_source_records_for_rule() below.
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


# ---------------------------------------------------------------------------
# Tie-back: gre_exceptions -> the actual source record
# ---------------------------------------------------------------------------

def _build_natural_key_where(keys: list) -> str:
    """
    Build a WHERE-clause fragment that matches every parsed natural key
    (dicts from parse_natural_key(), one per gre_exceptions row) in one
    query.

    A single-column natural key (the common case, and by far the cheapest
    query shape) becomes a plain "col IN (...)"; a composite key becomes
    an OR of per-record ANDs, since there's no portable way to express
    "match any of these N (col1, col2) pairs" as a single IN-list across
    every dialect this engine supports. Either shape is chunked by the
    caller (see get_source_records_for_rule()) so one call never has to
    match more than EXCEPTION_CHUNK records at once.
    """
    if not keys:
        return "1=0"   # no keys -> match nothing, never an unfiltered scan

    cols = list(keys[0].keys())

    if len(cols) == 1:
        col = cols[0]
        values = [k[col] for k in keys]
        parts = []
        non_null = [v for v in values if v is not None]
        if non_null:
            in_list = ", ".join(f"'{_escape_sql_literal(v)}'" for v in non_null)
            parts.append(f"{col} IN ({in_list})")
        if any(v is None for v in values):
            parts.append(f"{col} IS NULL")
        return " OR ".join(parts)

    record_clauses = []
    for key in keys:
        conds = [
            f"{col} IS NULL" if val is None else f"{col} = '{_escape_sql_literal(val)}'"
            for col, val in key.items()
        ]
        record_clauses.append("(" + " AND ".join(conds) + ")")
    return " OR ".join(record_clauses)


def get_source_records_for_rule(cf, meta_conn, meta_db: str, rule_id, batch_id: str) -> list:
    """
    Every SOURCE record behind one rule's failures for one batch -- "pull
    the 50 records that failed rule 1" for a dashboard, an analyst
    review, or a downstream share-out.

    gre_exceptions deliberately never stores the violating row's own data
    (see rules_engine/schema.sql's header on that table) -- only enough
    to re-identify it: database_name/table_name/source_name (copied from
    the rule at write time) plus natural_key_value. A row that fails
    every rule in a 10-rule group would otherwise get its full column set
    captured 10 times, once per rule, purely because the SAME source data
    is already sitting right there in the source table. This function
    re-joins back to that LIVE source table at report/analysis time
    instead: parse each gre_exceptions row's natural_key_value, batch the
    parsed keys into EXCEPTION_CHUNK-sized groups, and pull the matching
    rows straight from source_name/database_name/table_name via the same
    ConnectionFactory every rule run already uses.

    Each returned dict is the source row's own columns, PLUS this
    finding's context under underscore-prefixed keys (won't collide with
    any real source column name):
        _record_id           gre_exceptions.record_id, to cite back to it
        _rule_id              the rule_id passed in
        _natural_key_value    the same key gre_exceptions stored
        _issue_desc            gre_exceptions.issue_desc
        _exception_flag         gre_exceptions.exception_flag (compliance
                                 disposition -- 'OPEN' etc.)

    Trade-off vs. a stored snapshot: this reflects the source table's
    CURRENT state, not its state at the moment the rule ran. If a record
    has since been corrected or deleted upstream, this may return updated
    data for that key, or fewer rows than gre_exceptions has on file for
    this rule/batch (the missing ones are logged at info level below, not
    silently dropped). Accepted here in exchange for not duplicating
    source data into gre_exceptions at all -- see the module docstring.
    A caller than genuinely needs point-in-time accuracy regardless of
    upstream changes needs a different design (a stored snapshot column),
    not this function.
    """
    exceptions = execute_query(
        meta_conn,
        f"""
        SELECT record_id, natural_key_value, database_name, table_name, source_name,
               issue_desc, exception_flag
        FROM {meta_db}.gre_exceptions
        WHERE rule_id = ? AND batch_id = ? AND etl_is_curr_ind = 'Y'
        """,
        [rule_id, batch_id],
    )
    if not exceptions:
        return []

    # Grouped by (database_name, table_name, source_name) defensively --
    # in practice every gre_exceptions row for one rule_id shares the same
    # triple (a rule targets exactly one table), but nothing here assumes
    # that rather than handling it correctly if it's ever not the case.
    groups = {}
    for exc in exceptions:
        key = (exc.get("database_name"), exc.get("table_name"), exc.get("source_name"))
        groups.setdefault(key, []).append(exc)

    records = []
    for (database_name, table_name, source_name), group in groups.items():
        db_conn = cf.get(source_name)
        if db_conn is None:
            raise RuntimeError(
                f"Source connection '{source_name}' unavailable -- cannot tie "
                f"rule_id={rule_id} batch_id={batch_id} exceptions back to source records."
            )

        # source_name is gre_exceptions' copy of the rule's sql_dialect
        # (source_type) at write time -- a file/S3 rule needs its DuckDB
        # view re-registered here the same way execute_rule() does, via
        # the shim dict below (all qualified_name()/prepare() need is
        # database_name/table_name); a no-op for teradata/postgres.
        table_ref_rule = {"database_name": database_name, "table_name": table_name}
        db_conn.prepare(table_ref_rule)
        table_ref = db_conn.qualified_name(table_ref_rule)

        keys = [parse_natural_key(exc["natural_key_value"]) for exc in group]
        cols = list(keys[0].keys())
        by_natural_key = {exc["natural_key_value"]: exc for exc in group}
        matched_keys = set()

        for chunk_start in range(0, len(keys), EXCEPTION_CHUNK):
            chunk = keys[chunk_start:chunk_start + EXCEPTION_CHUNK]
            where = _build_natural_key_where(chunk)
            query = f"SELECT * FROM {table_ref} WHERE {where}"
            for row in _run_source_query(db_conn, query):
                nk = _format_natural_key(cols, row)
                exc = by_natural_key.get(nk)
                matched_keys.add(nk)
                merged = dict(row)
                merged["_record_id"] = exc.get("record_id") if exc else None
                merged["_rule_id"] = rule_id
                merged["_natural_key_value"] = nk
                merged["_issue_desc"] = exc.get("issue_desc") if exc else None
                merged["_exception_flag"] = exc.get("exception_flag") if exc else None
                records.append(merged)

        missing = set(by_natural_key) - matched_keys
        if missing:
            logger.info(
                "get_source_records_for_rule: %d of %d exception(s) for rule_id=%s "
                "batch_id=%s no longer match a row in %s.%s (likely corrected/deleted "
                "upstream since the rule ran).",
                len(missing), len(group), rule_id, batch_id, database_name, table_name,
            )

    return records
