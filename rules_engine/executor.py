"""
rules_engine/executor.py
--------------------------
Single-rule execution: run one rule's negative SQL SELECT, write every
violating row to gre_exceptions through one shared path (never the rule's
own INSERT), then evaluate the rule-level threshold and upsert gre_results.

Everything here commits independently, per row and per rule -- there is no
shared transaction spanning rules, or even spanning the exception rows of
one rule. See execute_rule() for why that's safe under a crash/resume.

Built on rules_engine/db_ops.py
---------------------------
The low-level DB helpers (execute_query/execute_dml/bulk_insert*), {key}
run_params substitution (_substitute_params), and the retry-wrapped
source query all live in rules_engine/db_ops.py -- used identically
by sampling/sampling.py. Everything in THIS file is specific to rule
evaluation: the single-scan optimization, natural-key building, threshold
evaluation, and the gre_exceptions/gre_results/gre_log writers.

Big-dataset path (single-scan evaluation + bulk writes)
----------------------------------------------------------
A few things do NOT scale to a rule matching millions of rows in the
original v1 shape, and are fixed here the same way core/executor.py's
proven fix #1 fixes the equivalent problem for the dq_* engine:

  1. Writing violating rows one INSERT-plus-commit at a time
     (_insert_or_skip per row) means one network round trip per row.
     rules_engine/db_ops.py's bulk_insert_or_skip() batches this into
     GRE_EXCEPTION_CHUNK-sized executemany() calls -- see _write_exceptions().
  2. Fetching every violating row with one unbounded fetchall(), then
     deriving failed_records from a COUNT(*) against the *destination*
     table after the write, ties the accuracy of failed_records to
     whatever exception-detail capture happens to write. _scan_violations()
     runs rule_syntax ONCE, streamed via fetchmany() in EXCEPTION_CHUNK-
     sized batches rather than one giant fetchall(), producing both the
     true failed count AND the full row list for gre_exceptions detail
     capture from that same pass -- so a rule that matches 10 million rows
     is still scanned only once instead of twice.

     There is deliberately NO cap on how many violating rows get a
     gre_exceptions detail row: every violating row is captured, in full,
     every attempt -- compliance/audit review needs the complete record
     set, not a sample of it. (An earlier version of this file capped
     detail capture at GRE_MAX_EXCEPTIONS and only kept failed_records
     exact past the cap; that capping behavior has been removed --
     `rows` returned by _scan_violations() is always the complete
     violation set.) The trade-off this makes: `rows` is held in memory
     for the whole scan, so a rule matching an extremely large number of
     rows needs memory proportional to that count -- accepted here in
     exchange for gre_exceptions never silently missing a violating
     record. See execute_rule()'s docstring for how this affects
     _write_exceptions()'s deactivation reconciliation (it can now always
     run, unconditionally, on every attempt -- see that function's
     docstring).
  3. Running rule_syntax AGAIN just to count it (a separate COUNT(*)-wrapped
     query) means every rule scanned its own base data twice per attempt.
     _scan_violations() replaces that two-query design (formerly
     _count_failed() + _fetch_violating_rows()) with the one combined scan
     described above -- roughly halving read load against the source
     table/connection for every rule, with no rule-authoring or schema
     change at all.
"""

import logging
import time
from datetime import datetime

from rules_engine.db_ops import (
    execute_dml, execute_query, bulk_insert_or_skip, bulk_execute, _is_duplicate_key_error,
    _substitute_params, _run_source_query, _escape_sql_literal,
    log_error, EXCEPTION_CHUNK,
)

logger = logging.getLogger(__name__)

# Severity values (case-insensitive) that resolve a breach to WARN rather
# than FAIL -- same convention as core/executor.py's _SOFT_SEVERITIES, so a
# project can invent any severity vocabulary it wants without touching code.
_SOFT_SEVERITIES = {"WARN", "WARNING", "INFO", "NOTICE"}


def _log_error(meta_conn, meta_db: str, run_id: str, rule: dict, run_key: str,
                error_type: str, message: str, detail: str = None) -> None:
    """Rule-dict convenience wrapper around rules_engine/db_ops.py::log_error(), for gre_rules-keyed callers."""
    log_error(meta_conn, meta_db, run_id, rule.get("rule_id"), rule.get("rule_group"),
              run_key, error_type, message, detail)


# ---------------------------------------------------------------------------
# Single-scan rule evaluation
# ---------------------------------------------------------------------------

# Reuses rules_engine/db_ops.py's retry decorator indirectly via _run_source_query
# for the total-record count path; _scan_violations runs its own cursor
# loop directly (streaming fetchmany(), not a single fetchall()) so it is
# NOT wrapped in _source_retry -- a retry here would re-run a
# partially-streamed scan from the top, which is wasteful for a rule
# matching a large row count. A transient failure here is instead treated
# as a hard error for this attempt (see execute_rule()'s STEP 1).
def _scan_violations(db_conn, query: str) -> tuple:
    """
    Run rule_syntax ONCE, streamed via fetchmany() in EXCEPTION_CHUNK-sized
    batches rather than one giant fetchall(), instead of the old two-query
    design (a separate COUNT(*)-wrapped query, then rule_syntax run again
    in full to fetch detail rows) -- roughly halving read load against the
    source table/connection for every rule, since both old queries
    evaluated the identical predicate against the identical rows.

    Returns (failed, rows):
      failed : count of every row the query returns -- always == len(rows).
      rows   : EVERY violating row, uncapped, for gre_exceptions detail
               capture -- there is no ceiling here (see the module
               docstring's "Big-dataset path" section): compliance/audit
               review needs the complete violation set, not a sample, so
               nothing is ever dropped from `rows` regardless of how many
               rows rule_syntax matches. This does mean memory use for
               `rows` scales with the violation count for a single
               attempt -- a rule matching an extremely large number of
               rows needs correspondingly more memory to hold them all
               before _write_exceptions() bulk-writes them.

    Trade-off vs. the two-query design: previously, a failure that hit
    ONLY the detail-fetch step -- AFTER a separate COUNT(*) had already
    succeeded -- didn't fail the rule; the true count was already safely
    in hand, so the rule could still be scored correctly with capture
    just skipped. With one shared query, a failure anywhere during the
    scan means there's no independently-obtained count to fall back on,
    so execute_rule() now has to treat any failure here as a hard ERROR
    for the rule (see its STEP 1 below). This mainly matters for rules
    whose result rows are unusually wide/expensive to materialize (many
    columns, large text fields), where a bare COUNT(*) might succeed even
    if pulling full rows hits a resource limit -- accepted here in
    exchange for cutting the common-case scan cost in half.
    """
    cursor = db_conn.cursor()
    try:
        cursor.execute(query)
        if cursor.description is None:
            return 0, []
        columns = [c[0].lower() for c in cursor.description]

        failed = 0
        rows = []
        while True:
            batch = cursor.fetchmany(EXCEPTION_CHUNK)
            if not batch:
                break
            for r in batch:
                failed += 1
                rows.append(dict(zip(columns, r)))
        return failed, rows
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Natural key
# ---------------------------------------------------------------------------

def _format_src_key(cols: list, row: dict) -> str:
    """
    "col1=val1|col2=val2" for `cols`, in order, from `row` -- the actual
    encoding logic behind build_src_key() below, factored out so
    rules_engine/reporting.py can recompute the identical string from a
    row it fetched straight from the source table (not through a `rule`
    dict), to match it back to the gre_exceptions row it came from.

    `row`'s keys are always lowercased (see _scan_violations()/
    rules_engine/db_ops.py::execute_query()), but `cols` may not be -- it comes
    straight from gre_rules.src_key_cols (build_src_key()) or from a
    previously-stored src_key_value's original casing
    (rules_engine/reporting.py::get_source_records_for_rule()). Look up
    case-insensitively so authoring casing never silently breaks this.

    A `cols` entry not present in `row` at all (case-insensitively) is a
    rule misconfiguration -- src_key_cols naming a column rule_syntax
    doesn't actually SELECT -- not a real NULL value, and is NOT written
    as "NULL": that used to collapse into the same literal string as a
    genuinely NULL column and was indistinguishable afterward. This now
    raises instead, so callers fail loudly (logged to gre_log/gre_errors
    via execute_rule()'s STEP 3) rather than writing/matching on a
    corrupted key.
    """
    def _fmt(c):
        key = c.lower()
        if key not in row:
            raise KeyError(
                f"src_key_cols column '{c}' not found among this rule's result "
                f"columns {sorted(row.keys())} -- gre_rules.src_key_cols must name "
                "columns rule_syntax actually SELECTs (case-insensitive)."
            )
        v = row[key]
        return "NULL" if v is None else v
    return "|".join(f"{c}={_fmt(c)}" for c in cols)


def build_src_key(rule: dict, row: dict) -> str:
    """
    "col1=val1|col2=val2" from rule['src_key_cols'] -- this engine's
    analog of dq_rules.primary_key_columns / utils/ids.py::build_pk_string.
    Every violating row must produce one of these; it's what makes
    gre_exceptions_uix (rule_id, run_key, src_key_value) meaningful.
    """
    cols = [c.strip() for c in (rule.get("src_key_cols") or "").split(",") if c.strip()]
    if not cols:
        raise ValueError(
            f"rule_id={rule.get('rule_id')} has no src_key_cols -- "
            "every rule must declare one to write idempotent exception rows."
        )
    return _format_src_key(cols, row)


def parse_src_key(src_key_value: str) -> dict:
    """
    Best-effort inverse of build_src_key()/_format_src_key():
    "col1=val1|col2=val2" -> {"col1": "val1", "col2": "val2"}, used by
    rules_engine/reporting.py to re-derive the WHERE filter that ties a
    gre_exceptions row back to its source record.

    The literal string 'NULL' round-trips back to Python None (matching
    build_src_key()'s own encoding of a genuinely NULL key column) --
    which means a key column whose REAL value is the literal text "NULL"
    is indistinguishable from a true NULL after parsing. This is a
    pre-existing constraint of the delimited-string encoding itself (not
    introduced here); pick src_key_cols that won't hold that value.

    Splits on the literal "|" and the FIRST "=" in each segment -- key
    column NAMES never contain either, but a key VALUE containing "|"
    will not round-trip correctly, for the same reason. Empty input (a
    row with no columns, which build_src_key() never actually
    produces since it requires at least one column) returns {}.
    """
    parsed = {}
    for segment in (src_key_value or "").split("|"):
        if not segment:
            continue
        col, _, val = segment.partition("=")
        parsed[col] = None if val == "NULL" else val
    return parsed


# Dialects whose source table is itself a durable, directly-queryable
# object an analyst can connect to and run a stored SQL string against
# LATER, independent of this Python process -- see
# build_source_tieback_sql()'s docstring for why 'file'/'s3' are excluded.
_TIEBACK_SQL_DIALECTS = {"teradata", "postgres"}


def _tieback_split_expr(dialect: str, src_key_value_expr: str, token_index: int, token_count: int) -> str:
    """
    dialect-appropriate "give me the value half of the token_index'th
    '|'-delimited COLUMN=VALUE segment of src_key_value_expr" expression.
    1-indexed (both STRTOK and split_part are 1-indexed) -- token_index=1
    for a single-column key skips the outer split entirely (nothing to
    split on '|' when there's only ever one segment).
    """
    token = src_key_value_expr if token_count == 1 else None
    if dialect == "teradata":
        if token is None:
            token = f"STRTOK({src_key_value_expr}, '|', {token_index})"
        return f"STRTOK({token}, '=', 2)"
    # postgres -- and, incidentally, duckdb, which implements the same
    # split_part(string, delimiter, position) signature, so this branch
    # would also work if _TIEBACK_SQL_DIALECTS ever grows to include it.
    if token is None:
        token = f"split_part({src_key_value_expr}, '|', {token_index})"
    return f"split_part({token}, '=', 2)"


def build_source_tieback_sql(rule: dict, run_key: str, meta_db: str) -> str:
    """
    Build (never execute) the SQL TEXT that joins this rule's source table
    directly to its gre_exceptions rows for `run_key`, parsing
    src_key_value back into its original column(s) in-database via a
    dialect-appropriate string-split function -- the generic, automated
    version of the STRTOK join an analyst would otherwise hand-write in
    Toad each time (see the conversation this generalizes, and
    rules_engine/reporting.py::get_source_records_for_rule() for the
    Python fetch-and-join equivalent of the same tie-back).

    Returns SQL text ONLY -- this never opens a connection or runs
    anything; see execute_rule()'s STEP 3.5 for where the returned string
    gets persisted (gre_results.source_tieback_sql), so an analyst can
    pull it straight out of gre_results and paste it into Toad/whatever
    SQL client, without having to re-derive src_key_cols/database_name/
    src_tbl_nm/rule_id by hand every time.

    Only meaningful for a rule whose source table is itself a durable
    object an analyst can query LATER, independent of this Python
    process's lifetime:
      - 'teradata' -- joins straight to {meta_db}.gre_exceptions, same
        Teradata instance, using STRTOK.
      - 'postgres' -- uses split_part; ASSUMES metadata_sync has mirrored
        gre_exceptions into that Postgres schema (see
        metadata_sync/README.md) -- the caller/analyst is responsible for
        qualifying {meta_db} with wherever that mirror actually lives if
        it differs from the rule's own database_name.
      - 'file'/'s3' -- these run against an ephemeral DuckDB view
        registered only for the duration of one Python process (see
        db/connection_factory.py's FileAdapter/S3Adapter.prepare()) --
        there is no persistent table for a stored SQL string to reference
        after the process exits, so this returns None for those (and for
        any other/unrecognized sql_dialect) rather than emitting SQL that
        can never actually be run.

    Also returns None if the rule has no src_key_cols (same "can't build
    a natural key" case build_src_key() raises on) -- there is nothing to
    join on.
    """
    dialect = (rule.get("sql_dialect") or "").lower()
    if dialect not in _TIEBACK_SQL_DIALECTS:
        return None

    cols = [c.strip() for c in (rule.get("src_key_cols") or "").split(",") if c.strip()]
    if not cols:
        return None

    rule_id = rule.get("rule_id")
    database_name = rule.get("database_name")
    src_tbl_nm = rule.get("src_tbl_nm")
    src_table = f"{database_name}.{src_tbl_nm}" if database_name else src_tbl_nm

    conditions = []
    for i, col in enumerate(cols, start=1):
        value_expr = _tieback_split_expr(dialect, "e.src_key_value", i, len(cols))
        # A genuinely NULL key column is stored as the literal string
        # "NULL" (see _format_src_key()'s docstring), not a real SQL NULL
        # -- fall back to an IS NULL comparison so those rows still join
        # instead of silently never matching (NULL = 'NULL' is never true
        # in SQL). This used to be written as a CASE expression
        # ("CASE WHEN {value_expr} = 'NULL' THEN s.{col} IS NULL ELSE
        # s.{col} = {value_expr} END") -- that is invalid SQL in both
        # target dialects: a CASE expression's THEN/ELSE branches must
        # each return a scalar VALUE, and "s.{col} IS NULL" is a boolean
        # PREDICATE, not a value, so this raised a syntax error the
        # moment an analyst actually ran the generated SQL (Teradata has
        # no first-class BOOLEAN type to return here at all; Postgres
        # rejects a bare IS NULL as a CASE result for the same reason).
        # An OR of two mutually-exclusive AND'd predicates expresses the
        # identical logic and is valid, portable boolean SQL in both
        # dialects.
        conditions.append(
            f"(({value_expr} = 'NULL' AND s.{col} IS NULL) "
            f"OR ({value_expr} <> 'NULL' AND s.{col} = {value_expr}))"
        )
    where_join = "\n  AND ".join(conditions)

    return (
        f"SELECT s.*, e.record_id AS _record_id, e.rule_id AS _rule_id, e.rule_nm AS _rule_nm,\n"
        f"       e.process_name AS _process_name, e.project_name AS _project_name,\n"
        f"       e.src_key_value AS _src_key_value, e.issue_desc AS _issue_desc,\n"
        f"       e.exception_flag AS _exception_flag\n"
        f"FROM {src_table} s\n"
        f"JOIN {meta_db}.gre_exceptions e\n"
        f"  ON {where_join}\n"
        f"WHERE e.rule_id = {rule_id}\n"
        f"  AND e.run_key = '{_escape_sql_literal(run_key)}'\n"
        f"  AND e.etl_is_curr_ind = 'Y'"
    )


# ---------------------------------------------------------------------------
# Threshold evaluation
# ---------------------------------------------------------------------------

def evaluate_threshold(
    total: int,
    failed: int,
    threshold_pct=None,
    threshold_count=None,
    threshold_operator: str = "OR",
    severity: str = "Data Validation Error",
) -> dict:
    """
    Decide the rule-level PASS/FAIL/WARN verdict and whether a gre_results
    row should be written at all.

    Returns a dict:
        status                   "PASS" | "FAIL" | "WARN"
        write_result              bool -- False means: don't insert/update
                                   gre_results for this rule+run_key at all
        threshold_pct_used        effective value actually applied (or None)
        threshold_count_used      effective value actually applied (or None)
        threshold_operator_used   effective value actually applied (or None)

    Rules (mirrors core/executor.py::evaluate_rule's pct/count/operator
    logic, plus the rule-level breach semantics from the prompt):

      1. total == 0            -> PASS, write_result=False (nothing to score)
      2. threshold configured  -> breach if failure_pct > threshold_pct,
                                   or failed > threshold_count, combined per
                                   threshold_operator (OR=either, AND=both
                                   set-and-breached). Comparison is strictly
                                   greater-than -- exactly AT the threshold
                                   is still a PASS. Always write a row
                                   (PASS/FAIL/WARN) so trending is possible.
      3. no threshold configured -> fallback: breach ONLY when
                                   failed == total (every in-scope record
                                   failed). This is its OWN inclusive check,
                                   not "threshold_pct=100 with >" -- pct can
                                   never exceed 100, so that comparison would
                                   silently never fire. Write a row only when
                                   this fallback actually breaches.
    """
    severity = (severity or "").upper()
    threshold_operator = (threshold_operator or "OR").upper()

    if total == 0:
        return {
            "status": "PASS",
            "write_result": False,
            "threshold_pct_used": None,
            "threshold_count_used": None,
            "threshold_operator_used": None,
        }

    has_threshold = threshold_pct is not None or threshold_count is not None

    if has_threshold:
        pct = (failed / total) * 100
        pct_breached = threshold_pct is not None and pct > threshold_pct
        count_breached = threshold_count is not None and failed > threshold_count

        if threshold_operator == "AND":
            both_set = threshold_pct is not None and threshold_count is not None
            breached = (pct_breached and count_breached) if both_set else (pct_breached or count_breached)
        else:
            breached = pct_breached or count_breached

        status = ("WARN" if severity in _SOFT_SEVERITIES else "FAIL") if breached else "PASS"

        return {
            "status": status,
            "write_result": True,
            "threshold_pct_used": threshold_pct,
            "threshold_count_used": threshold_count,
            "threshold_operator_used": threshold_operator,
        }

    # No threshold configured -- explicit all-in-scope-failed fallback.
    breached = failed == total
    if not breached:
        return {
            "status": "PASS",
            "write_result": False,
            "threshold_pct_used": None,
            "threshold_count_used": None,
            "threshold_operator_used": None,
        }

    status = "WARN" if severity in _SOFT_SEVERITIES else "FAIL"
    return {
        "status": status,
        "write_result": True,
        "threshold_pct_used": None,
        "threshold_count_used": None,
        "threshold_operator_used": None,
    }


# ---------------------------------------------------------------------------
# gre_exceptions / gre_results writers
# ---------------------------------------------------------------------------

def _write_exceptions(meta_conn, meta_db: str, rule: dict, run_id: str, run_key: str, rows: list,
                       run_params: dict = None) -> dict:
    """
    Reconcile gre_exceptions for (rule_id, run_key) against THIS attempt's
    true violation set, instead of a blind append-only insert. A rerun of
    a rule that already has exception rows on file for this run_key (see
    rules_engine/runner.py's run_rule_group() -- every run now
    re-executes every rule for its run_key rather than skipping
    already-succeeded ones) must make etl_is_curr_ind='Y' reflect the
    LATEST execution's state, not the union of every execution that ever
    ran:

      - a src_key_value violating NOW with no existing row      -> INSERT, etl_is_curr_ind='Y'
      - a src_key_value violating NOW whose existing row is 'N' -> reactivate to 'Y'
      - a src_key_value violating NOW whose existing row is 'Y' -> already current, left alone
      - an existing 'Y' row whose src_key_value is NOT in this attempt's violation set
        (fixed since the row was last written)                 -> deactivate to 'N'

    Soft-deactivation (never delete) preserves full history of what was
    ever flagged and when it got fixed -- rules_engine/reporting.py
    already filters on etl_is_curr_ind='Y', so nothing on the read side
    needs to change for deactivated rows to stop showing as open.

    `rows` is always this attempt's COMPLETE violation set -- there is no
    detail-capture cap on _scan_violations() any more (see that function's
    docstring), so deactivation below always runs unconditionally: an
    existing 'Y' row not present in `rows` is safely known to have been
    fixed, never "maybe just not captured this time." (An earlier version
    of this file skipped deactivation entirely when a MAX_EXCEPTIONS cap
    meant `rows` was only a partial view -- that whole conditional no
    longer applies, since `rows` can no longer be partial.)

    Rows are de-duplicated by src key WITHIN this call first (a
    rule_syntax that legitimately returns the same src key twice in one
    pull would otherwise be treated as two separate changes).

    rule_nm/dgr_nbr/universe_version are copied straight from `rule`
    (gre_rules), same as element_name/project_name/process_name above --
    purely descriptive, NULL if the rule row doesn't set them.
    run_type/batch_schedule are copied from `run_params` ONLY if the
    caller happened to supply those exact keys for this run -- also purely
    descriptive, and never required (run_params has no reserved keys, see
    execute_rule()'s docstring).

    Returns {"inserted": N, "reactivated": N, "deactivated": N}.
    """
    run_params = run_params or {}
    rule_id = rule.get("rule_id")

    # de-dup this attempt's violating rows by src key
    new_rows_by_key = {}
    for row in rows:
        nk = build_src_key(rule, row)
        new_rows_by_key.setdefault(nk, row)

    # every gre_exceptions row currently on file for (rule_id, run_key),
    # regardless of its current etl_is_curr_ind -- needed to tell "new"
    # from "reactivate" from "already current" from "now fixed"
    existing = execute_query(
        meta_conn,
        f"""
        SELECT record_id, src_key_value, etl_is_curr_ind
        FROM {meta_db}.gre_exceptions
        WHERE rule_id = ? AND run_key = ?
        """,
        [rule_id, run_key],
    )
    existing_by_key = {r["src_key_value"]: r for r in existing}

    to_insert = []
    to_reactivate = []
    for nk, row in new_rows_by_key.items():
        existing_row = existing_by_key.get(nk)
        if existing_row is None:
            issue_desc = f"Rule '{rule.get('rule_nm')}' violated (src_key={nk})"
            to_insert.append([
                run_id,
                rule_id,
                rule.get("database_name"),
                rule.get("src_tbl_nm"),
                rule.get("project_name"),
                rule.get("process_name"),
                rule.get("element_name"),
                rule.get("sql_dialect"),
                issue_desc,
                run_key,
                nk,
                rule.get("rule_nm"),
                rule.get("dgr_nbr"),
                rule.get("universe_version"),
                run_params.get("run_type"),
                run_params.get("batch_schedule"),
            ])
        elif existing_row["etl_is_curr_ind"] != "Y":
            to_reactivate.append([run_id, run_key, existing_row["record_id"]])

    insert_sql = f"""
        INSERT INTO {meta_db}.gre_exceptions (
            run_id, rule_id, database_name, src_tbl_nm, project_name, process_name,
            element_name, source_name, issue_desc, run_key, src_key_value,
            rule_nm, dgr_nbr, universe_version, run_type, batch_schedule
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    inserted = bulk_insert_or_skip(meta_conn, insert_sql, to_insert) if to_insert else 0

    reactivate_sql = f"""
        UPDATE {meta_db}.gre_exceptions
        SET etl_is_curr_ind = 'Y', run_id = ?, last_updated_by = 'SYSTEM', last_updated_datetime = CURRENT_TIMESTAMP
        WHERE run_key = ? AND record_id = ?
    """
    reactivated = bulk_execute(meta_conn, reactivate_sql, to_reactivate) if to_reactivate else 0

    # `rows` is always this attempt's complete violation set (see this
    # function's docstring), so it's always safe to deactivate an existing
    # 'Y' row that isn't in it -- no cap-related "maybe not captured"
    # ambiguity to guard against any more.
    deactivated = 0
    to_deactivate = [
        [r["record_id"]]
        for nk, r in existing_by_key.items()
        if r["etl_is_curr_ind"] == "Y" and nk not in new_rows_by_key
    ]
    if to_deactivate:
        deactivate_sql = f"""
            UPDATE {meta_db}.gre_exceptions
            SET etl_is_curr_ind = 'N', last_updated_by = 'SYSTEM', last_updated_datetime = CURRENT_TIMESTAMP
            WHERE record_id = ?
        """
        deactivated = bulk_execute(meta_conn, deactivate_sql, to_deactivate)

    return {"inserted": inserted, "reactivated": reactivated, "deactivated": deactivated}


def _upsert_result(meta_conn, meta_db: str, row: dict) -> None:
    """
    Upsert one gre_results row for (rule_id, run_key): INSERT, and on the
    unique-index duplicate-key error, UPDATE in place instead -- unlike
    gre_exceptions, this table is a summary row, not row-level history, so
    a rerun should overwrite it rather than accumulate duplicates.

    active_ind is always written as 'Y' here (insert AND update): the
    unique index (rule_id, run_key) already guarantees there is never more
    than one gre_results row for a given rule+run_key, so there is never
    a "stale" gre_results row left behind by a rerun to deactivate --
    unlike gre_log/gre_errors below, which are append-only across reruns
    of the same run_key under a NEW run_id and so genuinely need one. The
    column is still carried here (see rules_engine/schema.sql) so every
    gre_ table this feature touches exposes the same active_ind
    vocabulary a downstream report can filter on uniformly, and so a
    future move away from upsert-in-place (e.g. keeping gre_results
    history too) wouldn't need a new column added.
    """
    insert_sql = f"""
        INSERT INTO {meta_db}.gre_results (
            rule_id, run_key, run_id, project_name, process_name, total_records, failed_records,
            failure_pct, threshold_pct_used, threshold_count_used,
            threshold_operator_used, severity, status, source_tieback_sql, active_ind
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Y')
    """
    params = [
        row["rule_id"], row["run_key"], row["run_id"], row.get("project_name"), row.get("process_name"),
        row["total_records"], row["failed_records"], row["failure_pct"], row["threshold_pct_used"],
        row["threshold_count_used"], row["threshold_operator_used"],
        row["severity"], row["status"], row.get("source_tieback_sql"),
    ]
    try:
        execute_dml(meta_conn, insert_sql, params)
        return
    except Exception as exc:
        try:
            meta_conn.commit()   # release the aborted INSERT so the connection stays usable for the UPDATE below
        except Exception:
            pass
        if not _is_duplicate_key_error(exc):
            raise

    update_sql = f"""
        UPDATE {meta_db}.gre_results
        SET run_id = ?, project_name = ?, process_name = ?, total_records = ?, failed_records = ?,
            failure_pct = ?, threshold_pct_used = ?, threshold_count_used = ?,
            threshold_operator_used = ?, severity = ?, status = ?, source_tieback_sql = ?,
            active_ind = 'Y', evaluated_at = CURRENT_TIMESTAMP
        WHERE rule_id = ? AND run_key = ?
    """
    execute_dml(meta_conn, update_sql, [
        row["run_id"], row.get("project_name"), row.get("process_name"), row["total_records"],
        row["failed_records"], row["failure_pct"],
        row["threshold_pct_used"], row["threshold_count_used"], row["threshold_operator_used"],
        row["severity"], row["status"], row.get("source_tieback_sql"), row["rule_id"], row["run_key"],
    ])


def _deactivate_prior_log_attempts(meta_conn, meta_db: str, rule_id, run_key: str, current_run_id: str) -> None:
    """
    Soft-deactivate every gre_log row for (rule_id, run_key) left over from
    an EARLIER run_id -- i.e. a previous, separate run of this same
    run_key (rules_engine/runner.py::generate_run_id() mints a brand new
    run_id every call, even for a repeated run_key). Mirrors
    sampling/sampling.py::_deactivate_prior_sampling_runs()'s "always
    re-execute, deactivate stale, activate new" pattern, applied here to
    gre_log instead of gre_sample_selections.

    Deliberately scoped to run_id <> current_run_id, NOT status: an ERROR
    attempt from an earlier run_id is exactly as stale as a SUCCESS one
    once this run_key has been re-run -- the LATEST run_id's own attempt
    (whatever its status) is what should read as "active" for this
    rule_id/run_key, not a mix of whichever old rows happened to say
    SUCCESS. Never deletes -- gre_log keeps full history for audit; only
    active_ind flips.

    Called once per rule per attempt, immediately before _log_attempt()
    inserts this attempt's own row -- see execute_rule()'s call sites
    below. Never raises: a failure here must not mask the real attempt
    outcome, same contract as _log_attempt() itself.
    """
    try:
        execute_dml(
            meta_conn,
            f"""
            UPDATE {meta_db}.gre_log
            SET active_ind = 'N', last_updated_datetime = CURRENT_TIMESTAMP
            WHERE rule_id = ? AND run_key = ? AND run_id <> ? AND active_ind = 'Y'
            """,
            [rule_id, run_key, current_run_id],
        )
    except Exception as exc:
        logger.error(
            "Failed to deactivate prior gre_log attempts for rule_id=%s run_key=%s: %s",
            rule_id, run_key, exc,
        )


def _log_attempt(meta_conn, meta_db: str, run_id: str, rule: dict, run_key: str,
                  status: str, rowcount: int, start_time: float, error_message: str = None) -> None:
    """
    Insert one gre_log row for this execution attempt, after deactivating
    any gre_log row(s) left active for this (rule_id, run_key) from an
    earlier run_id -- see _deactivate_prior_log_attempts()'s docstring.
    This attempt's own row is always inserted with active_ind='Y': it is,
    by definition, the newest attempt for this rule_id/run_key the moment
    it's written. Never raises.
    """
    rule_id = rule.get("rule_id")
    _deactivate_prior_log_attempts(meta_conn, meta_db, rule_id, run_key, run_id)

    sql = f"""
        INSERT INTO {meta_db}.gre_log (
            run_id, rule_id, rule_group, project_name, process_name, run_key, seq_no,
            start_time, end_time, status, rowcount, error_message, active_ind
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Y')
    """
    try:
        execute_dml(meta_conn, sql, [
            run_id, rule_id, rule.get("rule_group"), rule.get("project_name"),
            rule.get("process_name"), run_key, rule.get("seq_no"),
            datetime.fromtimestamp(start_time), datetime.now(), status, rowcount, error_message,
        ])
    except Exception as exc:
        logger.error("Failed to write gre_log row for rule_id=%s: %s", rule.get("rule_id"), exc)


# ---------------------------------------------------------------------------
# Total in-scope record count
# ---------------------------------------------------------------------------

def _build_total_query(table_ref: str, run_params: dict) -> str:
    """
    "SELECT COUNT(*) AS total_count FROM {table_ref} WHERE k1 = '{k1}' AND
    k2 = '{k2}' ..." built straight from run_params' OWN keys -- there is
    no separate scope_sql to hand-author or keep in sync. The dict that
    scopes rule_syntax already says what's "in scope" for this run, so
    re-deriving that as a second, independently-written SQL blob was pure
    duplication with real drift risk (rule_syntax's WHERE and scope_sql's
    WHERE could silently disagree over time). Every key present in
    run_params is treated as a literal column name on this rule's table
    and applied as an equality filter, AND'd together -- a project's
    run_params IS its scoping definition, for the rule_syntax substitution
    AND the denominator alike.

    table_ref is the adapter's own FROM-clause identifier for this rule
    (SourceAdapter.qualified_name()) -- "database_name.src_tbl_nm" for a
    real database, or the prepared view name for a file/S3 source.

    Sorted key order makes the resulting query text deterministic
    regardless of run_params' own insertion order, so _compute_total()'s
    total_cache key is stable for two calls with the same effective
    filters. Escaping/quoting is delegated to _substitute_params() (same
    logic rule_syntax substitution uses) rather than reimplemented here.
    """
    where_clause = " AND ".join(f"{key} = '{{{key}}}'" for key in sorted(run_params)) or "1=1"
    template = f"SELECT COUNT(*) AS total_count FROM {table_ref} WHERE {where_clause}"
    return _substitute_params(template, run_params)


def _compute_total(db_conn, rule: dict, run_params: dict, total_cache: dict = None) -> int:
    """
    total_records = COUNT(*) FROM the rule's table (db_conn.qualified_name(rule)),
    filtered by the SAME run_params dict used to scope rule_syntax -- see
    _build_total_query()'s docstring.

    total_cache: an optional dict shared across every rule in one
    run_rule_group() call (see runner.py), keyed by (sql_dialect,
    effective query text). Multiple rules in a group very often ask the
    identical question -- same table + the same run_params -- so caching
    by the actual resolved query (not just src_tbl_nm) reuses the
    COUNT(*) result across every rule that would otherwise re-run the
    exact same scan. A fresh cache per run means this never sees stale
    data across runs; within one run the source isn't expected to change
    mid-run anyway (the same assumption gre_exceptions' idempotency
    already relies on). Callers that don't pass a cache (e.g. direct
    execute_rule() calls in tests) get the old always-fresh-query
    behavior unchanged.
    """
    query = _build_total_query(db_conn.qualified_name(rule), run_params)

    cache_key = (rule.get("sql_dialect"), query)
    if total_cache is not None and cache_key in total_cache:
        return total_cache[cache_key]

    rows = _run_source_query(db_conn, query)
    if not rows:
        total = 0
    else:
        first_row = rows[0]
        first_value = next(iter(first_row.values()))
        total = int(first_value or 0)

    if total_cache is not None:
        total_cache[cache_key] = total
    return total


# ---------------------------------------------------------------------------
# Rule execution
# ---------------------------------------------------------------------------

def execute_rule(rule: dict, db_conn, meta_conn, run_id: str, run_key: str, run_params: dict, meta_db: str,
                  total_cache: dict = None) -> str:
    """
    Execute one rule end-to-end: prepare its source (file/S3 rules register
    their DuckDB view here), substitute run_params into rule_syntax, run it,
    write every violating row to gre_exceptions, evaluate the rule-level
    threshold, upsert gre_results, and log the attempt.

    Parameters
    ----------
    rule        : one row from gre_rules (dict)
    db_conn     : SourceAdapter for rule['sql_dialect'] -- READS ONLY
    meta_conn   : SourceAdapter for the gre_ metadata store -- all writes go here
    run_id      : id for this run (assigned by rules_engine/runner.py)
    run_key     : opaque tracking/idempotency identifier for this run --
                  gre_exceptions/gre_log/gre_results key off this value
                  (see rules_engine/schema.sql's gre_exceptions_uix /
                  gre_results_uix). Not required to appear in run_params --
                  build it however fits your data (a batch id, a
                  year+month pair, a specific date, or any other
                  column/combination) via rules_engine/db_ops.py::build_run_key(),
                  or pass your own string directly.
    run_params  : dict of named values substituted into rule_syntax's "{key}"
                  tokens (see rules_engine/db_ops.py::_substitute_params()) AND
                  used, key-for-key, as the equality filters for the
                  auto-generated total-record count (see
                  _compute_total()/_build_total_query()). Has no
                  reserved/required key -- entirely up to the rule author
                  what it contains. Two keys get an extra, optional
                  courtesy: if present, "run_type" and "batch_schedule"
                  are ALSO copied onto gre_exceptions.run_type/
                  batch_schedule (purely descriptive columns, see
                  rules_engine/schema.sql) -- this doesn't make them
                  reserved, it's just where those two particular values
                  land if you choose to use them.
    meta_db     : schema name the gre_ tables live in
    total_cache : optional dict, shared across every rule in one
                  run_rule_group() call, that memoizes _compute_total()'s
                  COUNT(*) result -- see _compute_total()'s docstring.
                  None (the default) disables caching, e.g. for direct
                  single-rule calls in tests.

    Returns
    -------
    "SUCCESS" | "ERROR"  -- an EXECUTION outcome (did the rule run without
    crashing), never the PASS/FAIL/WARN data verdict, which lives in
    gre_results. This is what rules_engine/runner.py's on_failure logic
    acts on.

    Commit model
    ------------
    Every violating row commits independently, in GRE_EXCEPTION_CHUNK-sized
    batches (bulk_insert_or_skip). The gre_results upsert and the gre_log
    attempt row are their own separate commits too. Nothing here is
    wrapped in one transaction, by design -- a crash mid-write leaves
    whatever chunks already committed in place, and a rerun of this same
    rule/run_key picks up exactly where it left off because of the
    gre_exceptions_uix natural-key uniqueness.

    Big-dataset path
    -----------------
    rule_syntax is scanned ONCE (STEP 1, _scan_violations) instead of once
    for a COUNT(*) and again for detail-row capture -- see that function's
    docstring for the exact trade-off this makes. failed_records comes
    from that same scan's true row count, and gre_exceptions detail
    capture gets EVERY one of those rows, uncapped -- there is no
    MAX_EXCEPTIONS ceiling any more (see _scan_violations()'s docstring).
    Because count and capture share one query, a failure during STEP 1
    fails the whole rule (there's no independently-obtained count to fall
    back on) -- whereas a failure specifically WRITING the already-fetched
    rows (STEP 3) is still logged but non-fatal, since `failed`/`total`
    are already known by then. total_cache (STEP 2) similarly avoids a
    redundant COUNT(*) scan when several rules in a group ask the same
    "how many rows are in this run" question.
    """
    start = time.time()

    # ── STEP 0: prepare the source -- fail fast, never mid-run ───────────────
    # No-op for teradata/postgres; a file/S3 rule registers its DuckDB view
    # here, driven entirely by rule['database_name']/rule['src_tbl_nm'] --
    # see db/connection_factory.py's FileAdapter/S3Adapter.prepare().
    try:
        db_conn.prepare(rule)
    except Exception as exc:
        logger.error("Rule %s: source prepare failed: %s", rule.get("rule_id"), exc, exc_info=True)
        _log_error(meta_conn, meta_db, run_id, rule, run_key, "SOURCE_PREPARE_ERROR", str(exc))
        _log_attempt(meta_conn, meta_db, run_id, rule, run_key, "ERROR", 0, start, str(exc))
        return "ERROR"

    # ── STEP 0b: run_params substitution -- fail fast, never mid-run ─────────
    try:
        query = _substitute_params(rule["rule_syntax"], run_params)
    except ValueError as exc:
        logger.error("Rule %s: run_params substitution failed: %s", rule.get("rule_id"), exc)
        _log_error(meta_conn, meta_db, run_id, rule, run_key, "PARAM_SUBSTITUTION_ERROR", str(exc))
        _log_attempt(meta_conn, meta_db, run_id, rule, run_key, "ERROR", 0, start, str(exc))
        return "ERROR"

    # ── STEP 1: ONE scan of rule_syntax -- TRUE failed count + all rows ──────
    try:
        failed, violating_rows = _scan_violations(db_conn, query)
    except Exception as exc:
        logger.error("Rule %s: rule_syntax scan failed: %s", rule.get("rule_id"), exc, exc_info=True)
        _log_error(meta_conn, meta_db, run_id, rule, run_key, "SQL_RUNTIME", str(exc))
        _log_attempt(meta_conn, meta_db, run_id, rule, run_key, "ERROR", 0, start, str(exc))
        return "ERROR"

    # ── STEP 2: total in-scope record count (memoized across the run_group) ──
    try:
        total = _compute_total(db_conn, rule, run_params, total_cache=total_cache)
    except Exception as exc:
        logger.error("Rule %s: scope/count query failed: %s", rule.get("rule_id"), exc, exc_info=True)
        _log_error(meta_conn, meta_db, run_id, rule, run_key, "SCOPE_QUERY_FAILURE", str(exc))
        _log_attempt(meta_conn, meta_db, run_id, rule, run_key, "ERROR", 0, start, str(exc))
        return "ERROR"

    # ── STEP 3: reconcile gre_exceptions against this attempt -- best effort ─
    # A failure here is logged but never fails the rule: `failed`/`total`
    # above are already correct (from the STEP 1 scan) and independent of
    # whether the write itself succeeds. Always runs -- even with ZERO
    # violating_rows -- because a rule that's now fully clean (used to
    # violate, doesn't anymore) still needs its previously-active
    # gre_exceptions rows deactivated; skipping this call when
    # violating_rows is empty (the old behavior) meant a clean rerun could
    # never close out stale exceptions from an earlier attempt. See
    # rules_engine/runner.py -- every run_rule_group() call now
    # re-executes every rule for its run_key (no more silent
    # already-succeeded skip), so this reconciliation runs on every rerun.
    reconcile = {"inserted": 0, "reactivated": 0, "deactivated": 0}
    try:
        reconcile = _write_exceptions(meta_conn, meta_db, rule, run_id, run_key, violating_rows,
                                      run_params=run_params)
    except Exception as exc:
        logger.warning(
            "Rule %s: exception-row reconciliation failed (failed_records is still accurate): %s",
            rule.get("rule_id"), exc, exc_info=True,
        )
        _log_error(meta_conn, meta_db, run_id, rule, run_key, "WRITE_FAILURE", str(exc))

    verdict = evaluate_threshold(
        total, failed,
        threshold_pct=rule.get("threshold_pct"),
        threshold_count=rule.get("threshold_count"),
        threshold_operator=rule.get("threshold_operator", "OR"),
        severity=rule.get("severity"),
    )

    if verdict["write_result"]:
        failure_pct = round((failed / total * 100), 6) if total else 0.0

        # Build (never execute) the STRTOK/split_part join text that ties
        # this rule's gre_exceptions rows back to their live source
        # records -- see build_source_tieback_sql()'s docstring. Best-
        # effort: a failure here must never take down an otherwise-
        # successful rule attempt, so it's logged and the result row is
        # still written with source_tieback_sql=NULL rather than erroring
        # the whole rule out.
        try:
            source_tieback_sql = build_source_tieback_sql(rule, run_key, meta_db)
        except Exception as exc:
            logger.warning(
                "Rule %s: source_tieback_sql generation failed (non-fatal): %s",
                rule.get("rule_id"), exc, exc_info=True,
            )
            source_tieback_sql = None

        try:
            _upsert_result(meta_conn, meta_db, {
                "rule_id": rule.get("rule_id"),
                "run_key": run_key,
                "run_id": run_id,
                "project_name": rule.get("project_name"),
                "process_name": rule.get("process_name"),
                "total_records": total,
                "failed_records": failed,
                "failure_pct": failure_pct,
                "threshold_pct_used": verdict["threshold_pct_used"],
                "threshold_count_used": verdict["threshold_count_used"],
                "threshold_operator_used": verdict["threshold_operator_used"],
                "severity": rule.get("severity"),
                "status": verdict["status"],
                "source_tieback_sql": source_tieback_sql,
            })
        except Exception as exc:
            logger.error("Rule %s: gre_results upsert failed: %s", rule.get("rule_id"), exc, exc_info=True)
            _log_error(meta_conn, meta_db, run_id, rule, run_key, "RESULTS_WRITE_FAILURE", str(exc))
            _log_attempt(meta_conn, meta_db, run_id, rule, run_key, "ERROR", failed, start, str(exc))
            return "ERROR"

    logger.info(
        "rule_id=%s | total=%d failed=%d inserted=%d reactivated=%d deactivated=%d | "
        "verdict=%s (write_result=%s) | %.3fs",
        rule.get("rule_id"), total, failed, reconcile["inserted"], reconcile["reactivated"],
        reconcile["deactivated"], verdict["status"], verdict["write_result"], time.time() - start,
    )
    # gre_log.rowcount = `failed` -- the TRUE, exact count of violating rows
    # from THIS attempt's own scan (rules_engine/schema.sql's comment:
    # "violating rows written to gre_exceptions this attempt"), identical
    # to what gets written as gre_results.failed_records for the same
    # attempt. This used to log `written` (= reconcile["inserted"] +
    # reconcile["reactivated"]) instead -- the count of rows that CHANGED
    # to active THIS attempt, not the count of rows active as of this
    # attempt. Those agree only on a rule's very first run: on any rerun
    # where the violation set is unchanged (nothing newly broke, nothing
    # got fixed), inserted=reactivated=0, so the old logic reported
    # rowcount=0 for an attempt that still had `failed` genuine, currently-
    # active violations on file -- a real rule with open violations reading
    # as "0 rows" in gre_log every stable rerun, silently disagreeing with
    # gre_results.failed_records and with a COUNT(*) against gre_exceptions
    # itself. `failed` cannot "double-count" anything (the concern the old
    # comment raised): it is a fresh COUNT from this attempt's own scan,
    # not an accumulator across attempts. See
    # tests/test_rules_engine_executor.py::
    # test_execute_rule_gre_log_rowcount_matches_failed_records_on_unchanged_rerun.
    _log_attempt(meta_conn, meta_db, run_id, rule, run_key, "SUCCESS", failed, start)
    return "SUCCESS"
