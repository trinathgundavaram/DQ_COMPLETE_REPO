"""
shared/db_ops.py
-----------------
Low-level DB helpers used by BOTH rules_engine/ and sampling/ -- the one
place either package goes for cursor/commit mechanics and error logging,
instead of each keeping its own copy.

What lives here vs. what doesn't
---------------------------------
Everything below is genuinely table-agnostic or writes to a table BOTH
packages write to (gre_errors). Anything that only makes sense for one
side -- rule-level threshold evaluation, the single-scan rule
optimization, gre_exceptions/gre_results writers -- lives in
rules_engine/executor.py instead, even though it's built on top of these
same primitives. See shared/README.md for the full split rationale.

Run-parameter substitution (_substitute_params / build_run_params) is the
one mechanism both packages use to let a project scope its data however
it needs to -- a month/year pair, a batch_id + run_type combination, a
region/contract column, or nothing at all -- without the engine hardcoding
any one project's notion of "scope" as a schema column. See that
function's docstring below for the mechanics.
"""

import logging
import os
import re

logger = logging.getLogger(__name__)

# ── Tunable constants ───────────────────────────────────────────────────────
MAX_RETRIES = int(os.getenv("GRE_QUERY_MAX_RETRIES", "3"))

# Chunk size for all executemany()-based bulk writes across both packages
# (rules_engine's gre_exceptions writer, sampling's gre_sample_selections /
# gre_sample_selection_attrs writers).
EXCEPTION_CHUNK = int(os.getenv("GRE_EXCEPTION_CHUNK", "500"))

# ── Optional tenacity retry ─────────────────────────────────────────────────
try:
    from tenacity import (
        retry,
        stop_after_attempt,
        wait_exponential,
        retry_if_exception_type,
    )

    def _source_retry(fn):
        return retry(
            stop=stop_after_attempt(MAX_RETRIES),
            wait=wait_exponential(multiplier=1, min=2, max=15),
            retry=retry_if_exception_type(Exception),
            reraise=True,
        )(fn)

except ImportError:
    logger.warning("tenacity not installed -- source query retries disabled.")

    def _source_retry(fn):
        return fn


# ---------------------------------------------------------------------------
# Low-level DB helpers
# ---------------------------------------------------------------------------

def execute_query(conn, query: str, params=None) -> list:
    """Run a SELECT and return rows as list-of-dicts (column names lowercased)."""
    cursor = conn.cursor()
    if params is not None:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    if cursor.description is None:
        cursor.close()
        return []
    columns = [c[0].lower() for c in cursor.description]
    rows = [dict(zip(columns, r)) for r in cursor.fetchall()]
    cursor.close()
    return rows


def execute_dml(conn, query: str, params=None):
    """Execute one DML statement and commit immediately -- every call is its own transaction."""
    cursor = conn.cursor()
    if params is not None:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    conn.commit()
    cursor.close()


_DUPLICATE_KEY_MARKERS = (
    "unique", "duplicate", "already exists", "constraint",
)


def _is_duplicate_key_error(exc: Exception) -> bool:
    """
    Best-effort, driver-agnostic detection of a unique-constraint violation.

    Different drivers (teradatasql, psycopg2, duckdb) raise different
    exception classes for this, so this checks the message text rather than
    a fixed set of classes -- crude, but it's the same "catch the
    duplicate-key error" mechanism dq_metrics_summary_uix relies on,
    generalised across dialects instead of hardcoded to one.
    """
    msg = str(exc).lower()
    return any(marker in msg for marker in _DUPLICATE_KEY_MARKERS)


def _insert_or_skip(conn, sql: str, params: list) -> bool:
    """
    INSERT one row; commit on success. On a duplicate-key error, roll back
    and treat it as "already committed by an earlier attempt" -- skip,
    don't overwrite. Any other error is re-raised.

    This -- not delete-then-insert -- is the idempotency mechanism, modeled
    on dq_metrics_summary_uix: a crash mid-write never leaves a half-deleted
    row, because nothing is ever deleted here.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        conn.commit()
        return True
    except Exception as exc:
        try:
            conn.commit()   # release the aborted statement so the connection stays usable
        except Exception:
            pass
        if _is_duplicate_key_error(exc):
            logger.debug("Duplicate key on insert -- already recorded, skipping. (%s)", exc)
            return False
        raise
    finally:
        cursor.close()


def bulk_insert(conn, sql: str, rows: list, chunk_size: int = None) -> None:
    """
    Plain chunked executemany() -- no duplicate-key handling. Use only for
    append-only writes where a unique-index collision is not expected (e.g.
    sampling/sampling.py's gre_sample_selections / gre_sample_selection_attrs,
    which have no unique index and always write under a fresh
    sample_run_id). One commit per chunk instead of one per row, cutting
    round trips by ~chunk_size on a large candidate/exception set.

    `rows` is a list of positional param sequences (list/tuple), matching
    DB-API cursor.executemany()'s convention -- the same shape
    core/executor.py::bulk_insert expects.
    """
    if not rows:
        return
    size = chunk_size or EXCEPTION_CHUNK
    cursor = conn.cursor()
    try:
        for i in range(0, len(rows), size):
            cursor.executemany(sql, rows[i:i + size])
            conn.commit()
    finally:
        cursor.close()


def bulk_insert_or_skip(conn, sql: str, rows: list, chunk_size: int = None) -> int:
    """
    Chunked executemany() with duplicate-key tolerance -- the bulk analog of
    _insert_or_skip(), used by rules_engine/executor.py for gre_exceptions
    where gre_exceptions_uix (rule_id, batch_id, natural_key_value) is what
    makes a rerun idempotent.

    Tries each chunk as one executemany() batch first (cheap: one round
    trip for up to `chunk_size` rows). If a chunk raises -- in practice
    almost always because one row in it collides with a natural key
    committed by an EARLIER attempt on this batch_id -- it falls back to
    row-by-row _insert_or_skip() for JUST that chunk, so one stale
    duplicate never costs the other rows in the same batch. Any non
    duplicate-key error still propagates (same contract as
    _insert_or_skip()).

    Returns the number of rows actually inserted this call (excludes
    skipped duplicates). Note: on a chunk that contains BOTH a new row and
    a duplicate, some drivers (e.g. DuckDB) apply rows before the one that
    fails rather than rolling the whole executemany() back, so that new row
    is already committed by the time the row-by-row fallback re-attempts
    it -- the fallback then sees it as "already there" and doesn't count
    it. Data-wise this is harmless (the row exists exactly once, which is
    all a unique index promises); the only effect is this return value can
    slightly undercount in that specific mixed-chunk case. Accepted
    trade-off for avoiding a much more expensive per-row existence check on
    every chunk.
    """
    if not rows:
        return 0
    size = chunk_size or EXCEPTION_CHUNK
    inserted = 0
    for i in range(0, len(rows), size):
        chunk = rows[i:i + size]
        cursor = conn.cursor()
        try:
            cursor.executemany(sql, chunk)
            conn.commit()
            inserted += len(chunk)
        except Exception as exc:
            try:
                conn.commit()   # release the aborted statement, mirrors _insert_or_skip
            except Exception:
                pass
            if not _is_duplicate_key_error(exc):
                raise
            logger.debug(
                "Duplicate key within a %d-row bulk chunk -- retrying that chunk row-by-row.",
                len(chunk),
            )
            for params in chunk:
                if _insert_or_skip(conn, sql, list(params)):
                    inserted += 1
        finally:
            cursor.close()
    return inserted


# ---------------------------------------------------------------------------
# Run-parameter token substitution (v2 scoping mechanism -- see
# rules_engine/schema.sql / sampling/schema.sql headers for why there's no
# filter_column system)
# ---------------------------------------------------------------------------
# v1 supported exactly one substitutable value ({batch_id}), passed as a
# scalar all the way down. Different projects scope their data differently
# -- a month/year pair, a batch_id + run_type combination, a region or
# contract column, or no filter at all -- so v2 generalizes this to an
# arbitrary dict of named values: a rule/config author embeds whichever
# "{key}" tokens their SQL needs, and the caller supplies a matching dict
# at run time. batch_id remains a required, always-present key (see
# build_run_params() below) since it's still the one value the
# idempotency/checkpoint schema (gre_exceptions_uix, gre_log, gre_audit,
# sample_run_id) keys off -- but it is no longer the ONLY value a rule can
# reference.

_TOKEN_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")


def _escape_sql_literal(value) -> str:
    """
    Escape a value for embedding inside a single-quoted SQL string literal
    -- doubles embedded single quotes, and treats None as an empty string.
    Shared by _substitute_params() below (rule_sql/scope_sql/exclusion_sql
    token substitution) and rules_engine/reporting.py's natural-key
    tie-back query builder, so there is exactly one escaping
    implementation, not two that could quietly drift apart.
    """
    return str(value if value is not None else "").replace("'", "''")


def _find_unresolved_tokens(sql: str) -> list:
    """
    After substitution, scan for any "{name}"-shaped token still present
    -- almost always a param the caller forgot to pass (or a typo in the
    rule/config's SQL), which would otherwise surface as a confusing SQL
    syntax error from the source database instead of a clear, specific,
    pre-execution one. Deliberately simple (a bare identifier in braces,
    not a full templating grammar) since that's the only shape this
    substitution mechanism ever produces or consumes.
    """
    return _TOKEN_RE.findall(sql)


def _substitute_params(sql: str, params: dict) -> str:
    """
    Replace every literal "{key}" token in `sql` with the escaped string
    value of params[key], for every key present in `params`. Rule/config
    authors are responsible for their own quoting (e.g.
    `WHERE batch_id = '{batch_id}'`) -- this only does the swap.

    Raises ValueError if, after substitution, any "{token}"-shaped text
    remains -- fail fast with a clear message naming the missing
    parameter(s), rather than letting an unsubstituted token reach the
    database as a syntax error. `sql` may be None/empty (returns it
    unchanged) so callers don't need to guard optional CLOB columns
    (scope_sql, exclusion_sql) themselves.
    """
    if not sql:
        return sql
    resolved = sql
    for key, value in (params or {}).items():
        resolved = resolved.replace("{%s}" % key, _escape_sql_literal(value))

    unresolved = _find_unresolved_tokens(resolved)
    if unresolved:
        raise ValueError(
            f"Unresolved parameter token(s) {sorted(set(unresolved))} in SQL -- "
            f"no matching key in the run_params passed to this run. "
            f"Params supplied: {sorted((params or {}).keys())}."
        )
    return resolved


def build_run_params(batch_id: str, extra_params: dict = None) -> dict:
    """
    Merge an optional caller-supplied `extra_params` dict with the
    required `batch_id` value, used identically by
    rules_engine/runner.py::run_rule_group() and
    sampling/sampling.py::run_sampling() -- one implementation instead of
    two copies that could drift.

    `batch_id` always wins on key collision: it's resolved from the
    dedicated, required `batch_id` argument both entry points already
    take (for tracking/idempotency), so a stray "batch_id" key inside
    extra_params can never silently override it.
    """
    return {**(extra_params or {}), "batch_id": batch_id}


# ---------------------------------------------------------------------------
# Retry-wrapped source query
# ---------------------------------------------------------------------------

@_source_retry
def _run_source_query(db_conn, sql: str) -> list:
    return execute_query(db_conn, sql)


# ---------------------------------------------------------------------------
# gre_errors -- shared error log
# ---------------------------------------------------------------------------

def log_error(meta_conn, meta_db: str, run_id, rule_id, rule_group, batch_id,
              error_type: str, message: str, detail: str = None) -> None:
    """
    Insert one gre_errors row from explicit scalar fields. Never raises --
    an errors-table failure must not mask the real error.

    This is the ONE shared gre_errors write path for the whole engine:
    rule execution (via rules_engine/executor.py's rule-dict convenience
    wrapper _log_error()) and sampling/sampling.py's sampling runs (which
    have no rule_id/rule dict to key off of -- see
    sampling.py::_log_sampling_error()) both go through this function
    instead of each keeping its own INSERT+try/except copy. gre_errors
    itself lives in shared/schema.sql for the same reason.
    """
    sql = f"""
        INSERT INTO {meta_db}.gre_errors (
            run_id, rule_id, rule_group, batch_id, error_type, error_message, error_detail
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    try:
        execute_dml(meta_conn, sql, [run_id, rule_id, rule_group, batch_id, error_type, message, detail])
    except Exception as exc:
        logger.error("Failed to write gre_errors row (run_id=%s rule_id=%s): %s", run_id, rule_id, exc)
