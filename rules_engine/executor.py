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
evaluation, and the gre_exceptions/gre_results writers (gre_results is
both the per-attempt log and the rule-level verdict -- see _write_result()'s
docstring for why those merged into one table).

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
    build_extra_filters_clause, log_error, EXCEPTION_CHUNK,
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
              run_key, error_type, message, detail, rule_variant=rule.get("rule_variant"))


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
def _scan_violations(db_conn, query: str, src_key_cols: list = None) -> tuple:
    """
    Run rule_syntax ONCE, streamed via fetchmany() in EXCEPTION_CHUNK-sized
    batches rather than one giant fetchall(), instead of the old two-query
    design (a separate COUNT(*)-wrapped query, then rule_syntax run again
    in full to fetch detail rows) -- roughly halving read load against the
    source table/connection for every rule, since both old queries
    evaluated the identical predicate against the identical rows.

    src_key_cols : the rule's gre_rules.src_key_cols, already split into a
        list (e.g. ["claim_id"]) -- see build_src_key()'s docstring for
        where this normally comes from. When given (and at least one entry
        matches an actual result column), each row is PROJECTED down to
        just these columns before being retained in `rows`, instead of
        keeping every column rule_syntax happened to SELECT. This is safe
        because _write_exceptions() -- the only consumer of `rows` -- only
        ever reads a row through build_src_key(rule, row), which itself
        only touches rule['src_key_cols']; nothing downstream needs any
        other column's value. For a rule_syntax that SELECTs many/wide
        columns (or `SELECT *`) but keys on just one or two of them, this
        turns retained memory for a big violation set from O(violations x
        every selected column) into O(violations x key columns only) --
        often a large reduction. Pass None (or an empty/fully-unmatched
        list) to keep the old full-row behavior -- e.g. for a caller that
        doesn't know src_key_cols yet, or wants to preserve prior exact
        behavior for some other reason.

        A src_key_cols entry that doesn't match any actual result column
        (rule misconfiguration -- src_key_cols naming a column rule_syntax
        doesn't SELECT) is simply not projected either -- exactly like the
        old full-row capture, where that column was equally absent from
        `row`. _write_exceptions()'s build_src_key() call still raises the
        same KeyError in the same place either way, so this projection
        never changes error behavior, only what's retained in memory for
        the columns that DO exist.

    Returns (failed, rows):
      failed : count of every row the query returns -- always == len(rows).
      rows   : EVERY violating row, uncapped, for gre_exceptions detail
               capture -- there is no ceiling here (see the module
               docstring's "Big-dataset path" section): compliance/audit
               review needs the complete violation set, not a sample, so
               nothing is ever dropped from `rows` regardless of how many
               rows rule_syntax matches. Each row is either the full
               fetched row, or just its src_key_cols columns (see
               src_key_cols above) -- either way this DOES mean memory use
               for `rows` scales with the violation count for a single
               attempt: a rule matching an extremely large number of rows
               still needs memory proportional to that count (times
               whichever column set was retained) before
               _write_exceptions() bulk-writes them.

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
    exchange for cutting the common-case scan cost in half. Note the
    src_key_cols projection above only reduces what's RETAINED after each
    EXCEPTION_CHUNK-sized batch is fetched, not what's fetched over the
    wire per batch -- the driver still returns every SELECTed column for
    each of the (bounded, EXCEPTION_CHUNK-sized) rows in flight at any one
    time, same as before.
    """
    lowered_key_cols = {c.strip().lower() for c in (src_key_cols or []) if c.strip()}

    cursor = db_conn.cursor()
    try:
        cursor.execute(query)
        if cursor.description is None:
            return 0, []
        columns = [c[0].lower() for c in cursor.description]

        # Indices of columns actually worth projecting down to -- see
        # src_key_cols above. Empty (falsy) when src_key_cols wasn't
        # passed, or named nothing this query actually SELECTs, in which
        # case every row is kept in full (unchanged prior behavior).
        key_idx = [i for i, c in enumerate(columns) if c in lowered_key_cols]

        failed = 0
        rows = []
        while True:
            batch = cursor.fetchmany(EXCEPTION_CHUNK)
            if not batch:
                break
            for r in batch:
                failed += 1
                if key_idx:
                    rows.append({columns[i]: r[i] for i in key_idx})
                else:
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
    raises instead, so callers fail loudly (logged to gre_results/gre_rule_errors
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


def split_src_key_cols(rule: dict) -> list:
    """
    gre_rules.src_key_cols ("claim_id, batch_id") -> ["claim_id", "batch_id"]
    -- the one place this split happens, shared by build_src_key() below
    and _scan_violations()'s memory-projection optimization (STEP 1 in
    execute_rule()), so the two can never quietly drift out of sync on
    what counts as a natural-key column for this rule.
    """
    return [c.strip() for c in (rule.get("src_key_cols") or "").split(",") if c.strip()]


def build_src_key(rule: dict, row: dict) -> str:
    """
    "col1=val1|col2=val2" from rule['src_key_cols'] -- this engine's
    analog of dq_rules.primary_key_columns / utils/ids.py::build_pk_string.
    Every violating row must produce one of these; it's what makes
    gre_exceptions_uix (rule_id, run_key, src_key_value) meaningful.
    """
    cols = split_src_key_cols(rule)
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

    cols = split_src_key_cols(rule)
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
      3. no threshold configured -> fallback: treat both thresholds as
                                   effectively 0, not 100 -- breach as soon
                                   as ANY record fails (failed > 0), not
                                   only when every in-scope record does.
                                   Leaving threshold_pct/threshold_count
                                   NULL is meant to mean "no tolerance for
                                   failure was ever configured for this
                                   rule," which reads most naturally as
                                   "breach on the first failure," not
                                   "breach only once literally everything
                                   fails." (An earlier version of this
                                   fallback required failed == total --
                                   the "threshold_pct=100" reading was
                                   rejected on its own terms since a
                                   percentage can never exceed 100 under a
                                   strict ">" comparison, but requiring
                                   100% failure to breach turned out to be
                                   just as surprising a default in
                                   practice, so the fallback now treats the
                                   NULLs as 0 instead.) Write a row only
                                   when this fallback actually breaches --
                                   a genuinely clean (zero-failure) attempt
                                   with no threshold configured still
                                   writes nothing, same as before.
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

    # No threshold configured -- treat threshold_count as an effective 0
    # (see this function's docstring): ANY failure breaches. threshold_
    # count_used reports that effective 0 (not None) so gre_results shows
    # what was actually applied, same as the has_threshold branch above
    # always reports its real effective values -- a reviewer scanning
    # gre_results shouldn't have to separately know "NULL here means 0
    # was applied" out of band.
    breached = failed > 0
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
        "threshold_count_used": 0,
        "threshold_operator_used": threshold_operator,
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
    pull would otherwise be treated as two separate changes). Two
    diagnostics wrap this de-dup step, both purely additive (neither
    changes gre_exceptions content or the rule's PASS/FAIL/WARN verdict):

      - Structural fail-fast: before the de-dup loop runs at all, `rows[0]`
        is checked once for every configured src_key_cols column. A
        missing column is a property of the query (rule_syntax never
        SELECTs it), identical for every row, so it's validated once up
        front and raises immediately -- instead of surfacing N rows deep
        into the loop, aborting reconciliation for every row after
        whichever one the DB driver happened to return first.
      - Per-row resilience + visibility: build_src_key() is called inside
        a try/except per row, so any OTHER (non-structural) per-row
        failure skips just that row rather than aborting the whole
        attempt; skipped rows are counted and logged once as
        KEY_BUILD_FAILURE (gre_rule_errors), not one row per failure.
        Rows that build a key successfully but collide with an
        already-seen key (src_key_cols isn't a true natural key for this
        table, or multiple rows are all-NULL in every key column and
        collapse onto the same "NULL"-encoded string) are still merged
        first-encountered-wins via setdefault() -- unchanged -- but the
        collapse is now counted and logged once as KEY_NOT_DISTINCT.

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

    # Fail fast on a structural misconfiguration (src_key_cols naming a
    # column rule_syntax never SELECTs) -- this is a property of the query,
    # identical for every row it returns, so check it ONCE against the
    # first row rather than discovering it N rows deep in the loop below,
    # where it would otherwise abort processing for every row after it.
    if rows:
        key_cols = split_src_key_cols(rule)
        missing = [c for c in key_cols if c.strip().lower() not in rows[0]]
        if missing:
            raise KeyError(
                f"rule_id={rule_id}: src_key_cols {missing} not found among "
                f"result columns {sorted(rows[0].keys())} -- gre_rules.src_key_cols "
                "must name columns rule_syntax actually SELECTs (case-insensitive)."
            )

    # de-dup this attempt's violating rows by src key. Per-row try/except
    # so one anomalous row can't abort reconciliation for every other row
    # in this attempt -- the structural (whole-query) failure case is
    # already caught above before this loop runs.
    new_rows_by_key = {}
    key_build_errors = 0
    for row in rows:
        try:
            nk = build_src_key(rule, row)
        except Exception:
            key_build_errors += 1
            continue
        new_rows_by_key.setdefault(nk, row)

    if key_build_errors:
        logger.warning(
            "rule_id=%s run_key=%s: %d/%d violating rows failed key-building "
            "and were skipped this attempt",
            rule_id, run_key, key_build_errors, len(rows),
        )
        _log_error(
            meta_conn, meta_db, run_id, rule, run_key, "KEY_BUILD_FAILURE",
            f"{key_build_errors}/{len(rows)} violating rows failed key-building",
        )

    if len(rows) > len(new_rows_by_key) + key_build_errors:
        collapsed = len(rows) - key_build_errors - len(new_rows_by_key)
        logger.warning(
            "rule_id=%s run_key=%s: %d violating rows collapsed into %d "
            "distinct src_key_value(s) -- src_key_cols may not be a true "
            "natural key for this rule's table",
            rule_id, run_key, len(rows), len(new_rows_by_key),
        )
        _log_error(
            meta_conn, meta_db, run_id, rule, run_key, "KEY_NOT_DISTINCT",
            f"{collapsed} duplicate-key row(s) collapsed into existing "
            f"src_key_value entries ({len(new_rows_by_key)} distinct keys "
            f"from {len(rows)} violating rows)",
        )

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
        log_params=True,
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
                rule.get("rule_group"),
                rule.get("rule_variant"),
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

    # rule_group/rule_variant are copied straight from `rule` (gre_rules),
    # same as project_name/process_name -- purely descriptive, so a
    # gre_exceptions row can be filtered/reported on by rule_group or
    # rule_variant without a join back to gre_rules (which may have since
    # changed or been deactivated). See run_by_scope()'s docstring
    # (rules_engine/runner.py) for why rule_variant on the RUN's request
    # is a separate thing from this RULE's own rule_variant value.
    insert_sql = f"""
        INSERT INTO {meta_db}.gre_exceptions (
            run_id, rule_id, database_name, src_tbl_nm, project_name, process_name,
            rule_group, rule_variant, element_name, source_name, issue_desc, run_key,
            src_key_value, rule_nm, dgr_nbr, universe_version, run_type, batch_schedule
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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


def _write_result(meta_conn, meta_db: str, run_id: str, rule: dict, run_key: str, start_time: float,
                   status: str, total_records=None, failed_records: int = 0, failure_pct=None,
                   threshold_pct_used=None, threshold_count_used=None, threshold_operator_used=None,
                   source_tieback_sql: str = None, error_message: str = None,
                   executed_sql: str = None) -> None:
    """
    Insert ONE gre_results row for this execution attempt -- the single
    consolidated replacement for what used to be a gre_log row (one per
    attempt: execution status, rowcount, start/end timing) PLUS a
    separately-upserted gre_results row (one per rule_id+run_key: the
    PASS/FAIL/WARN data verdict). The two tables carried the exact same
    grain in practice and nearly the same columns, and gre_log's status
    ('SUCCESS'/'ERROR') was easy to misread as the data verdict when it
    only ever meant "the attempt ran to completion without raising" -- a
    rule that legitimately FAILED its threshold still logged
    status='SUCCESS' in gre_log every time, while the real verdict lived
    only in gre_results.status. This function writes ONE row using
    gre_results' verdict semantics (PASS | FAIL | WARN for a completed
    evaluation, ERROR when the attempt itself couldn't produce a verdict
    at all -- see the STEP 0/0b/1/2 failure paths in execute_rule()), so
    there's exactly one place to look for "did this rule pass, fail, or
    blow up," and exactly one column meaning "status."

    Every row this attempt's rule_group produces for this run_key was
    already blanket-deactivated up front, once, before any rule in the
    group started executing -- see rules_engine/runner.py::
    _deactivate_all_active_for_run()'s docstring for why that's a single
    broad pass instead of a per-rule deactivate-then-insert here. This
    function therefore always just INSERTs (never UPDATEs, never
    deactivates anything itself): unlike the old gre_results
    upsert-in-place, this keeps full attempt history the way gre_log used
    to, with active_ind marking which row is current for this
    (rule_id, run_key). A rerun of the same run_key -- the same kind of
    run, just re-executed -- never deletes a prior attempt's row; the
    upfront blanket pass deactivates it, and this call adds the new one.

    Raises on a genuine write failure (does NOT swallow it) -- unlike the
    old _log_attempt()'s "never raises" contract, because this is now the
    ONLY place an attempt's outcome is recorded at all; a caller that
    needs this call to be best-effort should go through the
    _write_result_safe() wrapper below instead of catching here.

    executed_sql : the ACTUAL SQL text that ran for this attempt (rule_syntax
                   AFTER _substitute_params() resolved every {key}/$key
                   token), or -- for the two failure points before
                   substitution runs at all -- the RAW, unsubstituted
                   rule_syntax, so an unresolved token is still visible.
                   See rules_engine/schema.sql's gre_results.executed_sql
                   column comment for the full rationale.
    """
    rule_id = rule.get("rule_id")

    sql = f"""
        INSERT INTO {meta_db}.gre_results (
            run_id, rule_id, rule_group, rule_variant, project_name, process_name, run_key, seq_no,
            start_time, end_time, total_records, failed_records, failure_pct,
            threshold_pct_used, threshold_count_used, threshold_operator_used,
            severity, status, error_message, executed_sql, source_tieback_sql, active_ind
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Y')
    """
    execute_dml(meta_conn, sql, [
        run_id, rule_id, rule.get("rule_group"), rule.get("rule_variant"),
        rule.get("project_name"), rule.get("process_name"),
        run_key, rule.get("seq_no"), datetime.fromtimestamp(start_time), datetime.now(),
        total_records, failed_records, failure_pct, threshold_pct_used, threshold_count_used,
        threshold_operator_used, rule.get("severity"), status, error_message, executed_sql,
        source_tieback_sql,
    ], log_params=True)


def _write_result_safe(meta_conn, meta_db: str, run_id: str, rule: dict, run_key: str, start_time: float,
                        status: str, **kwargs) -> None:
    """
    Best-effort wrapper around _write_result() -- logs and swallows any
    failure instead of raising. Used for the STEP 0/0b/1/2 early-failure
    paths (the attempt's own tracking row must never crash execute_rule()
    on top of the real error that already happened) and for the trivial/
    no-write-required verdict branch. The main verdict-path call in
    execute_rule() calls _write_result() directly instead, specifically so
    a write failure THERE can be caught, turned into its own
    RESULTS_WRITE_FAILURE gre_rule_errors row, and reported as this
    attempt's real outcome.
    """
    try:
        _write_result(meta_conn, meta_db, run_id, rule, run_key, start_time, status, **kwargs)
    except Exception as exc:
        logger.error("Failed to write gre_results row for rule_id=%s: %s", rule.get("rule_id"), exc)


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
                  total_cache: dict = None, extra_filters: dict = None, text_params: dict = None) -> str:
    """
    Execute one rule end-to-end: prepare its source (file/S3 rules register
    their DuckDB view here), substitute run_params into rule_syntax, splice
    in extra_filters (if the rule opts in), run it, write every violating
    row to gre_exceptions, evaluate the rule-level threshold, upsert
    gre_results, and log the attempt.

    Parameters
    ----------
    rule        : one row from gre_rules (dict)
    db_conn     : SourceAdapter for rule['sql_dialect'] -- READS ONLY
    meta_conn   : SourceAdapter for the gre_ metadata store -- all writes go here
    run_id      : id for this run (assigned by rules_engine/runner.py)
    run_key     : opaque tracking/idempotency identifier for this run --
                  gre_exceptions/gre_results key off this value
                  (see rules_engine/schema.sql's gre_exceptions_uix). Not
                  required to appear in run_params --
                  build it however fits your data (a batch id, a
                  year+month pair, a specific date, or any other
                  column/combination) via rules_engine/db_ops.py::build_run_key(),
                  or pass your own string directly.
    run_params  : dict of named values substituted into rule_syntax's "{key}"/"$key"
                  tokens (see rules_engine/db_ops.py::_substitute_params()) AND
                  used, key-for-key, as the equality filters for the
                  auto-generated total-record count (see
                  _compute_total()/_build_total_query()) -- EVERY run_params
                  key is treated as a literal column name on this rule's
                  table for that count, so a run_params key only makes
                  sense here if it's ALSO a real column. Has no
                  reserved/required key -- entirely up to the rule author
                  what it contains. Two keys get an extra, optional
                  courtesy: if present, "run_type" and "batch_schedule"
                  are ALSO copied onto gre_exceptions.run_type/
                  batch_schedule (purely descriptive columns, see
                  rules_engine/schema.sql) -- this doesn't make them
                  reserved, it's just where those two particular values
                  land if you choose to use them.
    text_params : optional dict of named values substituted into
                  rule_syntax's "{key}"/"$key" tokens EXACTLY like
                  run_params -- the only difference is text_params is
                  NEVER treated as a total-count scoping column (see
                  run_params above). Use this for a substitution value
                  whose name doesn't match a real column on the table --
                  e.g. a run_params key like RUNTYPE used only inside
                  rule_syntax's own SQL text, where the actual runtime
                  column is named differently (run_ty) and is scoped via
                  extra_filters instead. Passing a value as run_params
                  when it isn't a real column breaks the auto-generated
                  total-record count with an "unresolved/unknown column"
                  error from the source database -- that's exactly the
                  case text_params exists for. Merged with run_params for
                  substitution purposes only (text_params wins on a key
                  collision); has no reserved/required key either.
    meta_db     : schema name the gre_ tables live in
    total_cache : optional dict, shared across every rule in one
                  run_rule_group() call, that memoizes _compute_total()'s
                  COUNT(*) result -- see _compute_total()'s docstring.
                  None (the default) disables caching, e.g. for direct
                  single-rule calls in tests.
    extra_filters : optional dict of ad-hoc equality filters (column ->
                  value) applied ON TOP of run_params -- see
                  rules_engine/db_ops.py::build_extra_filters_clause()'s
                  docstring for the full rationale and safety notes. Only
                  takes effect on a rule whose rule_syntax embeds the
                  literal marker "{extra_filters}" or "$extra_filters";
                  ignored entirely for a rule that doesn't. Also merged
                  into the equality filters used for the auto-generated
                  total-record count (_compute_total()), so the
                  failure_pct denominator reflects the same narrowed
                  scope rule_syntax itself was filtered by.

    Returns
    -------
    "SUCCESS" | "ERROR"  -- an EXECUTION outcome (did the rule run without
    crashing), never the PASS/FAIL/WARN data verdict, which lives in
    gre_results. This is what rules_engine/runner.py's on_failure logic
    acts on.

    Commit model
    ------------
    Every violating row commits independently, in GRE_EXCEPTION_CHUNK-sized
    batches (bulk_insert_or_skip). The gre_results attempt row is its own
    separate commit too. Nothing here is
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
        _write_result_safe(meta_conn, meta_db, run_id, rule, run_key, start, "ERROR", error_message=str(exc),
                            executed_sql=rule.get("rule_syntax"))
        return "ERROR"

    # ── STEP 0b: extra_filters splice + run_params substitution -- fail fast,
    # never mid-run. extra_filters is spliced in FIRST, via plain text
    # replace (not _substitute_params()'s escaping path -- the clause it
    # builds is already-escaped, structural SQL, not a single literal
    # value to be quoted) -- see build_extra_filters_clause()'s docstring.
    # A rule_syntax with no "{extra_filters}"/"$extra_filters" marker is
    # simply unaffected -- str.replace() on absent text is a no-op.
    try:
        extra_filters_sql = build_extra_filters_clause(extra_filters)
    except ValueError as exc:
        logger.error("Rule %s: extra_filters rejected: %s", rule.get("rule_id"), exc)
        _log_error(meta_conn, meta_db, run_id, rule, run_key, "PARAM_SUBSTITUTION_ERROR", str(exc))
        _write_result_safe(meta_conn, meta_db, run_id, rule, run_key, start, "ERROR", error_message=str(exc),
                            executed_sql=rule.get("rule_syntax"))
        return "ERROR"
    extra_filters_marker_present = "{extra_filters}" in rule["rule_syntax"] or "$extra_filters" in rule["rule_syntax"]
    templated = rule["rule_syntax"].replace("{extra_filters}", extra_filters_sql).replace(
        "$extra_filters", extra_filters_sql)
    # text_params merges on top of run_params for SUBSTITUTION only (wins
    # on a key collision) -- run_params itself, unmodified, is what STEP 2
    # below uses for the total-count scoping columns. This keeps a
    # text-only value (e.g. RUNTYPE, when the real column is run_ty) out
    # of the auto-generated denominator query entirely -- see
    # execute_rule()'s text_params docstring above.
    substitution_params = {**run_params, **(text_params or {})}
    try:
        query = _substitute_params(templated, substitution_params)
    except ValueError as exc:
        logger.error("Rule %s: run_params substitution failed: %s", rule.get("rule_id"), exc)
        _log_error(meta_conn, meta_db, run_id, rule, run_key, "PARAM_SUBSTITUTION_ERROR", str(exc))
        _write_result_safe(meta_conn, meta_db, run_id, rule, run_key, start, "ERROR", error_message=str(exc),
                            executed_sql=templated)
        return "ERROR"

    # ── STEP 1: ONE scan of rule_syntax -- TRUE failed count + all rows ──────
    # src_key_cols passed through so _scan_violations() can project each
    # retained row down to just the natural-key columns instead of every
    # column rule_syntax happened to SELECT -- see that function's
    # docstring's src_key_cols parameter for why this is always safe.
    try:
        failed, violating_rows = _scan_violations(db_conn, query, src_key_cols=split_src_key_cols(rule))
    except Exception as exc:
        logger.error("Rule %s: rule_syntax scan failed: %s", rule.get("rule_id"), exc, exc_info=True)
        _log_error(meta_conn, meta_db, run_id, rule, run_key, "SQL_RUNTIME", str(exc))
        _write_result_safe(meta_conn, meta_db, run_id, rule, run_key, start, "ERROR", error_message=str(exc),
                            executed_sql=query)
        return "ERROR"

    # ── STEP 2: total in-scope record count (memoized across the run_group) ──
    # extra_filters only narrows the total-count denominator when the rule
    # actually opted in (embedded the marker) -- a rule that never
    # references "{extra_filters}"/"$extra_filters" must be completely
    # unaffected by a caller passing extra_filters, same as an unused
    # run_params key. Without this guard, extra_filters columns that don't
    # even apply to this rule's table would still get spliced into the
    # denominator query and break it (or silently narrow scope the actual
    # scan never applied).
    total_params = {**run_params, **(extra_filters or {})} if extra_filters_marker_present else run_params
    extra_filters_only = extra_filters if extra_filters_marker_present else None
    try:
        total = _compute_total(db_conn, rule, total_params, total_cache=total_cache)
    except Exception as exc:
        # A run_params/--param key that doesn't name a real column on this
        # rule's table (the caller only ever meant it for rule_syntax TEXT
        # substitution, not scoping -- text_params exists to say that
        # explicitly, but a caller who didn't realize a --param key needed
        # to be a real column shouldn't have the WHOLE RULE fail over it)
        # breaks THIS query, since every run_params key is otherwise
        # assumed to be a scoping column (see _build_total_query()'s
        # docstring). Retry ONCE, dropping run_params from the denominator
        # entirely and keeping only extra_filters (if the rule opted in) --
        # this can never make the denominator MORE wrong than "unscoped by
        # run_params," and lets the rule still produce a real verdict
        # instead of erroring out over an unrelated column name mismatch.
        fallback_params = extra_filters_only or {}
        try:
            total = _compute_total(db_conn, rule, fallback_params, total_cache=total_cache)
            logger.warning(
                "Rule %s: total-record count failed when scoped by run_params %s (%s) -- "
                "retried WITHOUT run_params (extra_filters only: %s) and got total=%s. "
                "A run_params/--param key here doesn't name a real column on this rule's "
                "table -- pass it via text_params/--text-param instead to avoid this "
                "fallback and its extra query round trip, and to keep the denominator "
                "correctly scoped by whichever run_params keys ARE real columns.",
                rule.get("rule_id"), sorted(run_params), exc, fallback_params, total,
            )
        except Exception as exc2:
            logger.error("Rule %s: scope/count query failed even without run_params: %s",
                         rule.get("rule_id"), exc2, exc_info=True)
            _log_error(meta_conn, meta_db, run_id, rule, run_key, "SCOPE_QUERY_FAILURE", str(exc2))
            _write_result_safe(meta_conn, meta_db, run_id, rule, run_key, start, "ERROR", error_message=str(exc2),
                                executed_sql=query)
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

    failure_pct = round((failed / total * 100), 6) if total else 0.0

    # Build (never execute) the STRTOK/split_part join text that ties this
    # rule's gre_exceptions rows back to their live source records -- see
    # build_source_tieback_sql()'s docstring. Only meaningful when there's
    # actually something to tie back to (failed > 0); best-effort even
    # then -- a failure here must never take down an otherwise-successful
    # rule attempt, so it's logged and the result row is still written
    # with source_tieback_sql=NULL rather than erroring the whole rule out.
    source_tieback_sql = None
    if failed > 0:
        try:
            source_tieback_sql = build_source_tieback_sql(rule, run_key, meta_db)
        except Exception as exc:
            logger.warning(
                "Rule %s: source_tieback_sql generation failed (non-fatal): %s",
                rule.get("rule_id"), exc, exc_info=True,
            )
            source_tieback_sql = None

    if verdict["write_result"]:
        try:
            _write_result(
                meta_conn, meta_db, run_id, rule, run_key, start, verdict["status"],
                total_records=total, failed_records=failed, failure_pct=failure_pct,
                threshold_pct_used=verdict["threshold_pct_used"],
                threshold_count_used=verdict["threshold_count_used"],
                threshold_operator_used=verdict["threshold_operator_used"],
                source_tieback_sql=source_tieback_sql,
                executed_sql=query,
            )
        except Exception as exc:
            logger.error("Rule %s: gre_results write failed: %s", rule.get("rule_id"), exc, exc_info=True)
            _log_error(meta_conn, meta_db, run_id, rule, run_key, "RESULTS_WRITE_FAILURE", str(exc))
            _write_result_safe(meta_conn, meta_db, run_id, rule, run_key, start, "ERROR",
                                failed_records=failed, error_message=str(exc), executed_sql=query)
            return "ERROR"
    else:
        # verdict["write_result"] is False for the "nothing to score"
        # (total==0) and "no threshold, no full-universe breach" cases --
        # still worth ONE row per attempt for history (this used to be
        # unconditionally covered by gre_log's own always-write behavior;
        # see this function's docstring on why the two tables merged).
        # _write_result_safe(), not _write_result(): a write failure here
        # is bookkeeping-only noise for an attempt that had nothing to
        # report in the first place, not worth escalating to ERROR.
        _write_result_safe(
            meta_conn, meta_db, run_id, rule, run_key, start, verdict["status"],
            total_records=total, failed_records=failed, failure_pct=failure_pct,
            source_tieback_sql=source_tieback_sql, executed_sql=query,
        )

    logger.info(
        "rule_id=%s | total=%d failed=%d inserted=%d reactivated=%d deactivated=%d | "
        "verdict=%s (write_result=%s) | %.3fs",
        rule.get("rule_id"), total, failed, reconcile["inserted"], reconcile["reactivated"],
        reconcile["deactivated"], verdict["status"], verdict["write_result"], time.time() - start,
    )
    return "SUCCESS"
