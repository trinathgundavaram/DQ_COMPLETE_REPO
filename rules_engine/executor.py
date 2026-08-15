"""
rules_engine/executor.py
--------------------------
Single-rule execution: run one rule's negative SQL SELECT, write every
violating row to gre_exceptions through one shared path (never the rule's
own INSERT), then evaluate the rule-level threshold and upsert gre_results.

Everything here commits independently, per row and per rule -- there is no
shared transaction spanning rules, or even spanning the exception rows of
one rule. See execute_rule() for why that's safe under a crash/resume.

Built on shared/db_ops.py
---------------------------
The low-level DB helpers (execute_query/execute_dml/bulk_insert*), the
dialect guard, {key} run_params substitution (_substitute_params), and the
retry-wrapped source query all live in shared/db_ops.py -- used identically
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
     shared/db_ops.py's bulk_insert_or_skip() batches this into
     GRE_EXCEPTION_CHUNK-sized executemany() calls -- see _write_exceptions().
  2. Fetching every violating row with one unbounded fetchall(), then
     deriving failed_records from a COUNT(*) against the *destination*
     table after the write, ties the accuracy of failed_records to
     whatever exception-detail capture happens to write. _scan_violations()
     runs rule_sql ONCE, streamed via fetchmany(), producing both a true
     failed count (counts every row, uncapped) and a GRE_MAX_EXCEPTIONS-
     capped row list for detail capture from that same pass -- so a rule
     that matches 10 million rows still gets an exact failed_records/
     threshold verdict, with gre_exceptions detail capture bounded to a
     safe, configurable ceiling, instead of trying to hold every row in
     memory. See execute_rule()'s docstring for the trade-off this makes.
  3. Running rule_sql AGAIN just to count it (a separate COUNT(*)-wrapped
     query) means every rule scanned its own base data twice per attempt.
     _scan_violations() replaces that two-query design (formerly
     _count_failed() + _fetch_violating_rows()) with the one combined scan
     described above -- roughly halving read load against the source
     table/connection for every rule, with no rule-authoring or schema
     change at all.
"""

import logging
import os
import time
from datetime import datetime

from shared.db_ops import (
    execute_dml, bulk_insert_or_skip, _is_duplicate_key_error,
    _substitute_params, _run_source_query, check_dialect, DialectMismatchError,
    log_error, EXCEPTION_CHUNK,
)

logger = logging.getLogger(__name__)

# Cap on how many violating rows get a gre_exceptions detail row per rule
# execution attempt. 0 or negative = unlimited. failed_records itself is
# ALWAYS the true source-side count (_scan_violations), never capped --
# only the row-level detail capture is bounded. Mirrors DQ_MAX_EXCEPTIONS.
MAX_EXCEPTIONS = int(os.getenv("GRE_MAX_EXCEPTIONS", "10000"))

# Severity values (case-insensitive) that resolve a breach to WARN rather
# than FAIL -- same convention as core/executor.py's _SOFT_SEVERITIES, so a
# project can invent any severity vocabulary it wants without touching code.
_SOFT_SEVERITIES = {"WARN", "WARNING", "INFO", "NOTICE"}


def _log_error(meta_conn, meta_db: str, run_id: str, rule: dict, batch_id: str,
                error_type: str, message: str, detail: str = None) -> None:
    """Rule-dict convenience wrapper around shared/db_ops.py::log_error(), for gre_rules-keyed callers."""
    log_error(meta_conn, meta_db, run_id, rule.get("rule_id"), rule.get("rule_group"),
              batch_id, error_type, message, detail)


# ---------------------------------------------------------------------------
# Single-scan rule evaluation
# ---------------------------------------------------------------------------

# Reuses shared/db_ops.py's retry decorator indirectly via _run_source_query
# for the total-record count path; _scan_violations runs its own cursor
# loop directly (streaming fetchmany(), not a single fetchall()) so it is
# NOT wrapped in _source_retry -- a retry here would re-run a
# partially-streamed scan from the top, which is wasteful for a rule
# matching a large row count. A transient failure here is instead treated
# as a hard error for this attempt (see execute_rule()'s STEP 1).
def _scan_violations(db_conn, query: str) -> tuple:
    """
    Run rule_sql ONCE, streamed via fetchmany(), instead of the old
    two-query design (a separate COUNT(*)-wrapped query, then rule_sql
    run again in full to fetch detail rows) -- roughly halving read load
    against the source table/connection for every rule, since both old
    queries evaluated the identical predicate against the identical rows.

    Returns (failed, rows):
      failed : TRUE count of every row the query returns. This keeps
               counting past MAX_EXCEPTIONS (it just stops appending to
               `rows` once the cap is hit) -- failed_records/threshold
               math stays exact no matter how many rows rule_sql matches,
               same guarantee the old _count_failed() gave.
      rows   : up to MAX_EXCEPTIONS violating rows (0/negative = unlimited)
               for gre_exceptions detail capture -- same cap/shape the old
               _fetch_violating_rows() returned.

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
    exchange for cutting the common-case scan cost in half. Note this
    also means the scan can no longer stop early once the cap is hit (it
    has to keep reading, just not storing, to keep `failed` exact) --
    in practice a wash-or-better trade against the old design, since the
    old COUNT(*) query already had to evaluate the full matching set on
    the server regardless of any cap.
    """
    cursor = db_conn.cursor()
    cursor.execute(query)
    if cursor.description is None:
        cursor.close()
        return 0, []
    columns = [c[0].lower() for c in cursor.description]
    cap = MAX_EXCEPTIONS if MAX_EXCEPTIONS > 0 else float("inf")

    failed = 0
    rows = []
    cap_logged = False
    while True:
        batch = cursor.fetchmany(EXCEPTION_CHUNK)
        if not batch:
            break
        for r in batch:
            failed += 1
            if len(rows) < cap:
                rows.append(dict(zip(columns, r)))
            elif not cap_logged:
                logger.warning(
                    "gre_exceptions capture cap reached (%d rows) -- failed_records stays "
                    "exact (counted from this same scan); gre_exceptions only gets the "
                    "first %d rows from this attempt.",
                    MAX_EXCEPTIONS, MAX_EXCEPTIONS,
                )
                cap_logged = True
    cursor.close()
    return failed, rows


# ---------------------------------------------------------------------------
# Natural key
# ---------------------------------------------------------------------------

def build_natural_key(rule: dict, row: dict) -> str:
    """
    "col1=val1|col2=val2" from rule['natural_key_columns'] -- this engine's
    analog of dq_rules.primary_key_columns / utils/ids.py::build_pk_string.
    Every violating row must produce one of these; it's what makes
    gre_exceptions_uix (rule_id, batch_id, natural_key_value) meaningful.
    """
    cols = [c.strip() for c in (rule.get("natural_key_columns") or "").split(",") if c.strip()]
    if not cols:
        raise ValueError(
            f"rule_id={rule.get('rule_id')} has no natural_key_columns -- "
            "every rule must declare one to write idempotent exception rows."
        )
    # Explicit None -> 'NULL' (not just a missing-key default) so a
    # genuinely NULL key column is stable and human-readable in
    # gre_exceptions.natural_key_value, not the string "None".
    def _fmt(c):
        v = row.get(c, "NULL")
        return "NULL" if v is None else v
    return "|".join(f"{c}={_fmt(c)}" for c in cols)


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
                                   gre_results for this rule+batch at all
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

def _write_exceptions(meta_conn, meta_db: str, rule: dict, run_id: str, batch_id: str, rows: list) -> int:
    """
    Write every violating row to gre_exceptions via one shared, batched
    path. Rows are de-duplicated by natural key WITHIN this call first (a
    rule_sql that legitimately returns the same natural key twice in one
    pull would otherwise cost a wasted duplicate-key round trip per
    repeat), then written with bulk_insert_or_skip() -- one executemany()
    per GRE_EXCEPTION_CHUNK-sized chunk instead of one INSERT+commit per
    row, falling back to row-by-row only for a chunk that collides with a
    natural key already committed by an earlier attempt on this batch_id.
    Returns how many NEW rows were inserted this call (not the total on
    file).
    """
    if not rows:
        return 0

    sql = f"""
        INSERT INTO {meta_db}.gre_exceptions (
            run_id, rule_id, table_name, element_name, source_name,
            issue_desc, batch_id, natural_key_value
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    seen = set()
    params = []
    for row in rows:
        nk = build_natural_key(rule, row)
        if nk in seen:
            continue
        seen.add(nk)
        issue_desc = f"Rule '{rule.get('rule_name')}' violated (natural_key={nk})"
        params.append([
            run_id,
            rule.get("rule_id"),
            rule.get("table_name"),
            rule.get("element_name"),
            rule.get("source_connection"),
            issue_desc,
            batch_id,
            nk,
        ])

    return bulk_insert_or_skip(meta_conn, sql, params)


def _upsert_result(meta_conn, meta_db: str, row: dict) -> None:
    """
    Upsert one gre_results row for (rule_id, batch_id): INSERT, and on the
    unique-index duplicate-key error, UPDATE in place instead -- unlike
    gre_exceptions, this table is a summary row, not row-level history, so
    a rerun should overwrite it rather than accumulate duplicates.
    """
    insert_sql = f"""
        INSERT INTO {meta_db}.gre_results (
            rule_id, batch_id, run_id, total_records, failed_records,
            failure_pct, threshold_pct_used, threshold_count_used,
            threshold_operator_used, severity, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = [
        row["rule_id"], row["batch_id"], row["run_id"], row["total_records"],
        row["failed_records"], row["failure_pct"], row["threshold_pct_used"],
        row["threshold_count_used"], row["threshold_operator_used"],
        row["severity"], row["status"],
    ]
    try:
        execute_dml(meta_conn, insert_sql, params)
        return
    except Exception as exc:
        if not _is_duplicate_key_error(exc):
            raise

    update_sql = f"""
        UPDATE {meta_db}.gre_results
        SET run_id = ?, total_records = ?, failed_records = ?, failure_pct = ?,
            threshold_pct_used = ?, threshold_count_used = ?,
            threshold_operator_used = ?, severity = ?, status = ?,
            evaluated_at = CURRENT_TIMESTAMP
        WHERE rule_id = ? AND batch_id = ?
    """
    execute_dml(meta_conn, update_sql, [
        row["run_id"], row["total_records"], row["failed_records"], row["failure_pct"],
        row["threshold_pct_used"], row["threshold_count_used"], row["threshold_operator_used"],
        row["severity"], row["status"], row["rule_id"], row["batch_id"],
    ])


def _log_attempt(meta_conn, meta_db: str, run_id: str, rule: dict, batch_id: str,
                  status: str, rowcount: int, start_time: float, error_message: str = None) -> None:
    """Insert one gre_log row for this execution attempt. Never raises."""
    sql = f"""
        INSERT INTO {meta_db}.gre_log (
            run_id, rule_id, rule_group, batch_id, seq_no,
            start_time, end_time, status, rowcount, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        execute_dml(meta_conn, sql, [
            run_id, rule.get("rule_id"), rule.get("rule_group"), batch_id, rule.get("seq_no"),
            datetime.fromtimestamp(start_time), datetime.now(), status, rowcount, error_message,
        ])
    except Exception as exc:
        logger.error("Failed to write gre_log row for rule_id=%s: %s", rule.get("rule_id"), exc)


# ---------------------------------------------------------------------------
# Total in-scope record count
# ---------------------------------------------------------------------------

def _compute_total(db_conn, rule: dict, run_params: dict, total_cache: dict = None) -> int:
    """
    total_records = scope_sql result if the rule defines one, else an
    unfiltered COUNT(*) on table_name (whole table). A rule that needs its
    total scoped MUST define scope_sql -- there is no implicit default
    filter column anymore (see rules_engine/schema.sql's header for why:
    every project scopes its data differently, so a rule/config author
    writes the exact WHERE clause their project needs, using whichever
    {key} tokens run_params supplies, rather than the engine guessing a
    column name). scope_sql goes through the same _substitute_params()
    pass as rule_sql.

    total_cache: an optional dict shared across every rule in one
    run_rule_group() call (see runner.py), keyed by
    (source_connection, effective query text). Multiple rules in a group
    very often ask the identical question -- same table_name, or an
    intentionally shared scope_sql -- so caching by the actual resolved
    query (not just table_name) reuses the COUNT(*) result across every
    rule that would otherwise re-run the exact same scan, without needing
    to special-case scope_sql vs. the default path. A fresh cache per run
    means this never sees stale data across runs; within one run the
    source isn't expected to change mid-run anyway (the same assumption
    gre_exceptions' idempotency already relies on). Callers that don't
    pass a cache (e.g. direct execute_rule() calls in tests) get the old
    always-fresh-query behavior unchanged.
    """
    scope_sql = (rule.get("scope_sql") or "").strip()
    if scope_sql:
        query = _substitute_params(scope_sql, run_params)
    else:
        query = f"SELECT COUNT(*) AS total_count FROM {rule['table_name']}"

    cache_key = (rule.get("source_connection"), query)
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

def execute_rule(rule: dict, db_conn, meta_conn, run_id: str, run_params: dict, meta_db: str,
                  total_cache: dict = None) -> str:
    """
    Execute one rule end-to-end: check its declared dialect against the
    source connection, substitute run_params into rule_sql, run it, write
    every violating row to gre_exceptions, evaluate the rule-level
    threshold, upsert gre_results, and log the attempt.

    Parameters
    ----------
    rule        : one row from gre_rules (dict)
    db_conn     : SourceAdapter for rule['source_connection'] -- READS ONLY
    meta_conn   : SourceAdapter for the gre_ metadata store -- all writes go here
    run_id      : id for this run (assigned by rules_engine/runner.py)
    run_params  : dict of named values substituted into rule_sql/scope_sql's
                  "{key}" tokens -- see shared/db_ops.py::_substitute_params()
                  and build_run_params(). MUST contain a "batch_id" key
                  (build_run_params() guarantees this): batch_id is still
                  the value gre_exceptions/gre_log/gre_results/gre_audit key
                  their tracking/idempotency off, even though it's no
                  longer the only value a rule can reference.
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
    rule/batch picks up exactly where it left off because of the
    gre_exceptions_uix natural-key uniqueness.

    Big-dataset path
    -----------------
    rule_sql is scanned ONCE (STEP 1, _scan_violations) instead of once
    for a COUNT(*) and again for detail-row capture -- see that function's
    docstring for the exact trade-off this makes. failed_records comes
    from that same scan's true row count, exact no matter how many rows
    rule_sql actually matches, even though detail-row capture (also from
    that scan) is capped at MAX_EXCEPTIONS. Because count and capture now
    share one query, a failure during STEP 1 fails the whole rule (there's
    no longer an independently-obtained count to fall back on) -- whereas
    a failure specifically WRITING the already-fetched rows (STEP 3) is
    still logged but non-fatal, since `failed`/`total` are already known
    by then. total_cache (STEP 2) similarly avoids a redundant COUNT(*)
    scan when several rules in a group ask the same "how many rows are in
    this batch" question.
    """
    start = time.time()
    batch_id = run_params["batch_id"]

    # ── STEP 0: dialect guard -- fail fast, never mid-run ─────────────────────
    source_type = getattr(db_conn, "source_type", "unknown")
    try:
        check_dialect(rule, source_type)
    except DialectMismatchError as exc:
        logger.error("Rule %s: dialect check failed: %s", rule.get("rule_id"), exc)
        _log_error(meta_conn, meta_db, run_id, rule, batch_id, "DIALECT_MISMATCH", str(exc))
        _log_attempt(meta_conn, meta_db, run_id, rule, batch_id, "ERROR", 0, start, str(exc))
        return "ERROR"

    # ── STEP 0b: run_params substitution -- fail fast, never mid-run ─────────
    try:
        query = _substitute_params(rule["rule_sql"], run_params)
    except ValueError as exc:
        logger.error("Rule %s: run_params substitution failed: %s", rule.get("rule_id"), exc)
        _log_error(meta_conn, meta_db, run_id, rule, batch_id, "PARAM_SUBSTITUTION_ERROR", str(exc))
        _log_attempt(meta_conn, meta_db, run_id, rule, batch_id, "ERROR", 0, start, str(exc))
        return "ERROR"

    # ── STEP 1: ONE scan of rule_sql -- TRUE failed count + capped rows ──────
    try:
        failed, violating_rows = _scan_violations(db_conn, query)
    except Exception as exc:
        logger.error("Rule %s: rule_sql scan failed: %s", rule.get("rule_id"), exc, exc_info=True)
        _log_error(meta_conn, meta_db, run_id, rule, batch_id, "SQL_RUNTIME", str(exc))
        _log_attempt(meta_conn, meta_db, run_id, rule, batch_id, "ERROR", 0, start, str(exc))
        return "ERROR"

    # ── STEP 2: total in-scope record count (memoized across the run_group) ──
    try:
        total = _compute_total(db_conn, rule, run_params, total_cache=total_cache)
    except Exception as exc:
        logger.error("Rule %s: scope/count query failed: %s", rule.get("rule_id"), exc, exc_info=True)
        _log_error(meta_conn, meta_db, run_id, rule, batch_id, "SCOPE_QUERY_FAILURE", str(exc))
        _log_attempt(meta_conn, meta_db, run_id, rule, batch_id, "ERROR", 0, start, str(exc))
        return "ERROR"

    # ── STEP 3: write the already-fetched violating rows -- best effort ──────
    # A failure here is logged but never fails the rule: `failed`/`total`
    # above are already correct (from the STEP 1 scan) and independent of
    # whether the write itself succeeds.
    written = 0
    if violating_rows:
        try:
            written = _write_exceptions(meta_conn, meta_db, rule, run_id, batch_id, violating_rows)
        except Exception as exc:
            logger.warning(
                "Rule %s: exception-row write failed (failed_records is still accurate): %s",
                rule.get("rule_id"), exc, exc_info=True,
            )
            _log_error(meta_conn, meta_db, run_id, rule, batch_id, "WRITE_FAILURE", str(exc))

    verdict = evaluate_threshold(
        total, failed,
        threshold_pct=rule.get("threshold_pct"),
        threshold_count=rule.get("threshold_count"),
        threshold_operator=rule.get("threshold_operator", "OR"),
        severity=rule.get("severity"),
    )

    if verdict["write_result"]:
        failure_pct = round((failed / total * 100), 6) if total else 0.0
        try:
            _upsert_result(meta_conn, meta_db, {
                "rule_id": rule.get("rule_id"),
                "batch_id": batch_id,
                "run_id": run_id,
                "total_records": total,
                "failed_records": failed,
                "failure_pct": failure_pct,
                "threshold_pct_used": verdict["threshold_pct_used"],
                "threshold_count_used": verdict["threshold_count_used"],
                "threshold_operator_used": verdict["threshold_operator_used"],
                "severity": rule.get("severity"),
                "status": verdict["status"],
            })
        except Exception as exc:
            logger.error("Rule %s: gre_results upsert failed: %s", rule.get("rule_id"), exc, exc_info=True)
            _log_error(meta_conn, meta_db, run_id, rule, batch_id, "RESULTS_WRITE_FAILURE", str(exc))
            _log_attempt(meta_conn, meta_db, run_id, rule, batch_id, "ERROR", written, start, str(exc))
            return "ERROR"

    logger.info(
        "rule_id=%s | total=%d failed=%d written=%d | verdict=%s (write_result=%s) | %.3fs",
        rule.get("rule_id"), total, failed, written, verdict["status"], verdict["write_result"], time.time() - start,
    )
    # gre_log.rowcount is "violating rows written to gre_exceptions THIS
    # attempt" per the schema comment -- `written` (bulk_insert_or_skip's
    # actual insert count, excluding skipped duplicates), not `failed`
    # (the true total, which double-counts rows already on file from a
    # prior attempt on a rerun).
    _log_attempt(meta_conn, meta_db, run_id, rule, batch_id, "SUCCESS", written, start)
    return "SUCCESS"
