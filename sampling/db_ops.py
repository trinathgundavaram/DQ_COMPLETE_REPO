"""
sampling/db_ops.py
--------------------
Low-level DB helpers for sampling/ -- cursor/commit mechanics, run_params
substitution, run_id generation, and gre_sampling_errors logging.

This used to live in shared/db_ops.py, shared verbatim with rules_engine/
(which has its own copy, rules_engine/db_ops.py, writing to its own
gre_rule_errors instead). The two packages are now fully independent --
no common shared/ module, no shared gre_errors table -- see README.md's
"Package separation" for why. Most of what's below is genuinely
table-agnostic (execute_query/execute_dml, the bulk writers, run_params
substitution, generate_run_id) and is duplicated in rules_engine/db_ops.py
byte-for-byte apart from the gre_rule_errors/gre_sampling_errors split at
the bottom; a fix to one of those generic helpers needs to be applied to
both copies.

gre_sampling_errors' shape differs slightly from the old shared gre_errors
table it replaces: the old table had no rule_id/rule_group concept that
actually applied to a sampling run (sampling always passed rule_id=NULL
and overloaded the rule_group column to carry process_name, purely for
triage, since gre_errors was designed around rules_engine's vocabulary).
Now that this package has its own table, it just has an honest
process_name column instead -- see log_error()'s signature below.

Run-parameter substitution (_substitute_params) is the one mechanism this
package uses to let a project scope its data however it needs to -- a
month/year pair, a specific date, a region/contract column, or nothing at
all -- without the engine hardcoding any one project's notion of "scope"
as a schema column. `run_params` has zero reserved keys of its own; the
one identifier the tracking/idempotency schema (gre_sample_selections,
gre_sampling_audit) keys off -- `run_key` -- is a separate, explicit
parameter passed alongside run_params, not a required member of it. See
_substitute_params()'s docstring below for the substitution mechanics,
and build_run_key() for a convenience way to build a run_key out of
whatever column(s) a caller wants (a batch id, a year+month pair, a
specific date, or any other combination).

`run_key` (which logical run this is) and `run_id` (which specific
attempt at it this is) are different things -- see generate_run_id()'s
docstring below for the full distinction and the shape `run_id` takes.

Debug logging
-------------
execute_query()/execute_dml()/_chunked_executemany()/_run_source_query()
below all log at DEBUG: the SQL statement TEXT (one-lined via _one_line()
below, truncated) and row COUNTS (rows returned, rows affected, chunk
sizes). They NEVER log bind parameter VALUES or any fetched/written row
DATA -- this package runs scope_sql/exclusion_sql against source tables
that can carry real case/member identifiers, so only shape-level facts
(what ran, how many rows) go to the log, never content. Nothing here
calls logging.basicConfig() -- that's an opt-in caller decision, see
sampling/config.py::configure_logging().
"""

import logging
import os
import re
import secrets
from datetime import datetime

logger = logging.getLogger(__name__)


def _one_line(sql: str, max_len: int = 500) -> str:
    """Collapse a multi-line SQL string to one line for a single debug-log
    record, truncated so one giant generated statement (e.g. a long IN
    list) doesn't blow out log size. SQL TEXT ONLY -- never call this on
    anything that might contain fetched row data or bind param values."""
    if not sql:
        return ""
    one_line = " ".join(sql.split())
    if len(one_line) > max_len:
        return one_line[:max_len] + f"... [truncated, {len(one_line)} chars total]"
    return one_line

# ── Tunable constants ───────────────────────────────────────────────────────
MAX_RETRIES = int(os.getenv("GRE_QUERY_MAX_RETRIES", "3"))

# Chunk size for every executemany()-based bulk write in this package
# (gre_sample_selections / gre_sample_selection_attrs).
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
    logger.debug("execute_query: %s | params=%d", _one_line(query), len(params) if params else 0)
    cursor = conn.cursor()
    try:
        if params is not None:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        if cursor.description is None:
            logger.debug("execute_query: no result set (not a SELECT)")
            return []
        columns = [c[0].lower() for c in cursor.description]
        rows = [dict(zip(columns, r)) for r in cursor.fetchall()]
        logger.debug("execute_query: %d row(s) returned", len(rows))
        return rows
    finally:
        cursor.close()


def execute_dml(conn, query: str, params=None):
    """Execute one DML statement and commit immediately -- every call is its own transaction."""
    logger.debug("execute_dml: %s | params=%d", _one_line(query), len(params) if params else 0)
    cursor = conn.cursor()
    try:
        if params is not None:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        conn.commit()
        logger.debug("execute_dml: %s row(s) affected", getattr(cursor, "rowcount", "?"))
    finally:
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


def _chunked_executemany(conn, sql: str, rows: list, chunk_size: int = None) -> int:
    """
    Shared body for bulk_insert()/bulk_execute() below -- both are a plain
    chunked executemany() with one commit per chunk (not per row), and
    differ only in what they return (bulk_insert() has never had a
    meaningful count to give back; bulk_execute() returns len(rows)). One
    loop, two thin public wrappers, rather than the same chunking logic
    kept twice.
    """
    if not rows:
        return 0
    size = chunk_size or EXCEPTION_CHUNK
    logger.debug("_chunked_executemany: %s | %d row(s), chunk_size=%d", _one_line(sql), len(rows), size)
    cursor = conn.cursor()
    try:
        for i in range(0, len(rows), size):
            cursor.executemany(sql, rows[i:i + size])
            conn.commit()
    finally:
        cursor.close()
    return len(rows)


def bulk_insert(conn, sql: str, rows: list, chunk_size: int = None) -> None:
    """
    Plain chunked executemany() -- no duplicate-key handling. Use for
    append-only writes where a unique-index collision is not expected
    (gre_sample_selections / gre_sample_selection_attrs, which have no
    unique index and always write under a fresh sample_run_id). One
    commit per chunk instead of one per row, cutting round trips by
    ~chunk_size on a large candidate set.

    `rows` is a list of positional param sequences (list/tuple), matching
    DB-API cursor.executemany()'s convention.
    """
    _chunked_executemany(conn, sql, rows, chunk_size)


def bulk_execute(conn, sql: str, rows: list, chunk_size: int = None) -> int:
    """
    Chunked executemany() for UPDATE/DELETE-shaped DML -- the mutate-
    existing-rows analog of bulk_insert() (which is insert-only). Used by
    the etl_is_curr_ind reconciliation in sampling/sampling.py for
    gre_sample_selections/gre_sample_selection_attrs: flipping a batch of
    rows' current-indicator on a rerun, rather than one UPDATE+commit per
    row.

    No duplicate-key handling -- an UPDATE by primary/unique key has
    nothing to collide on. `rows` is a list of positional param
    sequences, DB-API executemany() convention, same as bulk_insert().

    Returns len(rows) (the number of UPDATE statements issued) -- not
    every DB-API driver exposes a uniform "rows actually matched" count,
    and the caller already knows which keys it targeted.
    """
    return _chunked_executemany(conn, sql, rows, chunk_size)


# ---------------------------------------------------------------------------
# Run-parameter token substitution (v2 scoping mechanism -- see
# sampling/schema.sql's header for why there's no filter_column system)
# ---------------------------------------------------------------------------
# v1 supported exactly one substitutable value ({batch_id}), passed as a
# scalar all the way down. Different projects scope their data differently
# -- a month/year pair, a specific date, a region or contract column, or no
# filter at all -- so v2 generalizes this to an arbitrary dict of named
# values: a sampling config author embeds whichever "{key}" (or "$key")
# tokens their SQL needs, and the caller supplies a matching dict at run
# time. There is no reserved/required key -- run_params is entirely up to
# the config author. The one value the idempotency/checkpoint schema
# (gre_sample_selections, gre_sampling_audit) keys off is `run_key`, a
# separate explicit parameter callers pass to sampling/sampling.py's entry
# points -- see build_run_key() below for a convenience way to build one
# out of a batch id, a year+month pair, a specific date, or any other
# column/combination.

# Two interchangeable token spellings, freely mixed in the same scope_sql/
# exclusion_sql: "{key}" (braces) and "$key" (bare, no braces --
# word-boundary terminated, so "$year" in "...$year_end..." does NOT
# consume "_end"). Group 1 catches the braced form, group 2 the dollar
# form -- exactly one of the two is non-None per match. Caution for
# postgres scope_sql/exclusion_sql specifically: Postgres's own
# dollar-quoted string literals ($tag$...$tag$) can collide with this if
# `tag` happens to match a run_params key -- prefer "{key}" braces in any
# SQL that also uses dollar-quoting.
_TOKEN_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}|\$([a-zA-Z_][a-zA-Z0-9_]*)\b")


def _escape_sql_literal(value) -> str:
    """
    Escape a value for embedding inside a single-quoted SQL string literal
    -- doubles embedded single quotes, and treats None as an empty string.
    Shared by _substitute_params() below (scope_sql/exclusion_sql token
    substitution).
    """
    return str(value if value is not None else "").replace("'", "''")


def _find_unresolved_tokens(sql: str) -> list:
    """
    After substitution, scan for any "{name}"- or "$name"-shaped token
    still present -- almost always a param the caller forgot to pass (or a
    typo in the config's SQL), which would otherwise surface as a
    confusing SQL syntax error from the source database instead of a
    clear, specific, pre-execution one. Deliberately simple (a bare
    identifier, braced or dollar-prefixed, not a full templating grammar)
    since that's the only shape this substitution mechanism ever produces
    or consumes.
    """
    return [braced or dollar for braced, dollar in _TOKEN_RE.findall(sql)]


def _substitute_params(sql: str, params: dict) -> str:
    """
    Replace every literal "{key}" or "$key" token in `sql` with the
    escaped string value of params[key], for every key present in
    `params` -- both spellings are recognized everywhere this mechanism is
    used (scope_sql, exclusion_sql), freely mixed in the same SQL. Config
    authors are responsible for their own quoting (e.g. `WHERE run_date =
    '{run_date}'` / `WHERE run_date = '$run_date'`) -- this only does the
    swap.

    Raises ValueError if, after substitution, any "{token}"/"$token"-shaped
    text remains -- fail fast with a clear message naming the missing
    parameter(s), rather than letting an unsubstituted token reach the
    database as a syntax error. `sql` may be None/empty (returns it
    unchanged) so callers don't need to guard optional CLOB columns
    (scope_sql, exclusion_sql) themselves.
    """
    if not sql:
        return sql
    params = params or {}

    def _replace(match: "re.Match") -> str:
        key = match.group(1) or match.group(2)
        if key not in params:
            return match.group(0)   # left as-is; caught by the unresolved-token check below
        return _escape_sql_literal(params[key])

    resolved = _TOKEN_RE.sub(_replace, sql)

    unresolved = _find_unresolved_tokens(resolved)
    if unresolved:
        raise ValueError(
            f"Unresolved parameter token(s) {sorted(set(unresolved))} in SQL -- "
            f"no matching key in the run_params passed to this run. "
            f"Params supplied: {sorted((params or {}).keys())}."
        )
    return resolved


def build_run_key(*parts, delimiter: str = "_") -> str:
    """
    Join one or more values into a single opaque tracking/idempotency key
    -- gre_sample_selections and gre_sampling_audit all key off this ONE
    value, however the caller chooses to construct it. There is no fixed
    shape: a plain batch id, a year+month pair, a specific date, a
    region, or any combination all work equally well.

        build_run_key("BATCH_2026_08_19")   -> "BATCH_2026_08_19"
        build_run_key(2026, 8)              -> "2026_8"
        build_run_key("2026-08-19")         -> "2026-08-19"
        build_run_key(region, year, month)  -> e.g. "NORTHEAST_2026_8"

    Purely a convenience formatter (str(...) + join) -- callers are free
    to build their own string directly instead (e.g. reuse an existing
    date/batch value) and skip this helper entirely. Raises ValueError if
    called with zero parts, since an empty run_key can't meaningfully
    track/dedupe anything.
    """
    if not parts:
        raise ValueError("build_run_key() needs at least one part to build a run_key from.")
    return delimiter.join(str(p) for p in parts)


def default_run_key() -> str:
    """
    Today's date (YYYY-MM-DD, local time) as a run_key -- what
    run_sampling()/run_sampling_for_process_name() below fall back to
    when a caller doesn't pass one at all. See rules_engine/db_ops.py's
    identical helper for the full rationale (this mirrors it exactly,
    same as generate_run_id()/build_run_key() above already do).
    """
    return datetime.now().date().isoformat()


def generate_run_id(*label_parts, timestamp: datetime = None) -> str:
    """
    Build one `run_id` (`sample_run_id`) -- the identifier
    `gre_sample_selections`, `gre_sample_selection_attrs`,
    `gre_sampling_errors`, and `gre_sampling_audit` all stamp on every row
    written by ONE execution (sampling/sampling.py::run_sampling()), as
    opposed to `run_key`, which identifies WHICH LOGICAL RUN a caller is
    tracking/re-running (see build_run_key() above) and can be shared
    across many `run_id`s over time (a rerun of the same `run_key` always
    mints a brand new `run_id`). If `run_key` answers "which batch/period
    is this," `run_id` answers "which specific attempt at it is this."

    Shape: "{label_parts joined by '::'}::{timestamp to the microsecond}::{6 hex chars}"

        generate_run_id("HealthSpring UM Sample", "2026-08-01")
            -> "HealthSpring UM Sample::2026-08-01::20260819T143022.183045::f9e8d7"

    Why this shape, piece by piece:
      - Label parts (e.g. a sample config's name + run_key) come first so
        a human scanning `gre_sampling_audit`/a log line can immediately
        tell WHAT ran and for WHICH run_key without decoding anything --
        no separate lookup needed for the common case of "which
        config/run_key does this row belong to."
      - '::' (not '_' or '-') separates the parts visually, chosen
        specifically because sample_name/run_key are free-form VARCHAR
        columns that very often already contain '_' or '-' themselves
        (e.g. run_key="BATCH_2026_08_19") -- '::' reads clearly as a
        segment boundary in a log line or a SQL result grid even when the
        labels around it don't. This is for human readability ONLY:
        nothing in this codebase parses a run_id back apart by delimiter
        (searched and confirmed -- every table just compares run_id for
        exact equality), so this is safe to pick for legibility without
        an ambiguous-parsing risk to worry about.
      - The timestamp is microsecond-precision and in a sortable
        YYYYMMDDTHHMMSS.ffffff shape (lexicographic sort == chronological
        sort), so `ORDER BY sample_run_id` on any of these tables already
        sorts runs oldest-to-newest without touching load_datetime/
        started_at, AND a human can read the exact moment an attempt
        started directly off the id, no join required.
      - The trailing 6 hex characters (`secrets.token_hex(3)`) exist
        SOLELY to make collision effectively impossible even if two calls
        land in the same microsecond. A run_id collision is a real
        correctness risk, not just a cosmetic one: every reconciliation
        query in this package (gre_sample_selections'/
        gre_sample_selection_attrs' etl_is_curr_ind flip) filters on
        "sample_run_id <> this_run_id" to tell "this attempt" from "an
        earlier one" -- two attempts sharing a run_id would each treat the
        other's rows as its own, silently corrupting that logic.

    `timestamp`: defaults to `datetime.now()`; pass an explicit value
    (e.g. a run's own `started_at`) so the run_id's embedded timestamp
    matches a timestamp already being persisted elsewhere for the same
    run, instead of a call to this function drifting by however many
    milliseconds elapsed since that earlier `datetime.now()`.

    Label parts that are `None` or empty string are skipped rather than
    producing an empty, doubled-up '::' segment.

    Length: the timestamp + suffix + delimiters overhead is a fixed 34
    characters ("::20260819T143022.183045::a1b2c3"), leaving comfortable
    room for label parts under gre_sample_selections.sample_run_id's
    VARCHAR(200). Logs a warning rather than silently truncating if a
    caller's label parts are unusually long enough to push the total over
    200 -- the resulting INSERT would fail loudly at the database instead
    in that case, which is preferable to a truncated, ambiguous run_id,
    but this warning surfaces the problem before that point.
    """
    ts = (timestamp or datetime.now()).strftime("%Y%m%dT%H%M%S.%f")
    suffix = secrets.token_hex(3)
    labels = [str(p) for p in label_parts if p not in (None, "")]
    run_id = "::".join([*labels, ts, suffix])
    if len(run_id) > 200:
        logger.warning(
            "generate_run_id(): generated run_id is %d characters, over the 200-char "
            "VARCHAR(200) sample_run_id column width used across gre_sample_* tables -- "
            "the write that uses this run_id may fail. Label parts: %r",
            len(run_id), labels,
        )
    return run_id


def count_prior_attempts(meta_conn, meta_db: str, run_key, sample_config_id) -> int:
    """
    How many prior attempts at this (run_key, sample_config_id) already
    exist, counted from `gre_sampling_audit`. Callers use `this count + 1`
    as a human-readable "attempt-N" label folded into `sample_run_id` (see
    sampling/sampling.py::run_sampling()), so a rerun of the same run_key
    is visibly attempt 2, 3, ... at a glance in `gre_sampling_audit`
    instead of requiring a human to compare two run_ids' timestamps to
    tell which came first.

    Queries gre_sampling_audit_config_run_key_ix's own indexed columns
    directly.

    This is a LABEL, not a uniqueness mechanism: run_id's own trailing hex
    suffix (see generate_run_id() above) is what actually guarantees no
    two attempts collide. Two callers racing to start the same run_key at
    the exact same instant could in principle compute the same attempt
    number (a COUNT-then-INSERT race, not wrapped in a transaction) -- an
    accepted cosmetic edge case, since it never affects correctness of the
    etl_is_curr_ind reconciliation, which keys off the (unique) run_id,
    not this number.
    """
    sql = f"SELECT COUNT(*) AS cnt FROM {meta_db}.gre_sampling_audit WHERE run_key = ? AND sample_config_id = ?"
    rows = execute_query(meta_conn, sql, [run_key, sample_config_id])
    return int(rows[0]["cnt"]) if rows else 0


# ---------------------------------------------------------------------------
# Retry-wrapped source query
# ---------------------------------------------------------------------------

@_source_retry
def _run_source_query(db_conn, sql: str) -> list:
    # source_type here identifies which adapter (teradata/postgres/file/s3)
    # this query is running against -- execute_query() below logs the SQL
    # text/rowcount itself, this just adds that dialect context.
    logger.debug("_run_source_query against source_type=%s", getattr(db_conn, "source_type", "?"))
    return execute_query(db_conn, sql)


# ---------------------------------------------------------------------------
# gre_sampling_errors -- this package's own error log
# ---------------------------------------------------------------------------
# Kept fully separate from rules_engine/db_ops.py's gre_rule_errors --
# sampling used to share one gre_errors table with rules_engine/, where a
# sampling-run error had rule_id=NULL and its process_name overloaded into
# the rule_group column purely for triage (rules_engine's own vocabulary,
# repurposed). Now that this package has its own table, there's an honest
# process_name column instead of a repurposed rule_group -- no more
# rule_id at all (a sampling run never had one).

def _deactivate_prior_errors(meta_conn, meta_db: str, run_key, current_run_id) -> None:
    """
    Soft-deactivate every gre_sampling_errors row for `run_key` left
    active from an EARLIER run_id, mirroring sampling/sampling.py::
    _deactivate_prior_sampling_runs() for gre_sample_selections /
    gre_sample_selection_attrs -- the same "always re-execute, deactivate
    stale, activate new" pattern applied here to gre_sampling_errors.
    Never deletes -- gre_sampling_errors keeps full history for audit;
    only active_ind flips. Never raises: a failure here must not mask the
    real error being logged.
    """
    try:
        execute_dml(
            meta_conn,
            f"""
            UPDATE {meta_db}.gre_sampling_errors
            SET active_ind = 'N', last_updated_datetime = CURRENT_TIMESTAMP
            WHERE run_key = ? AND run_id <> ? AND active_ind = 'Y'
            """,
            [run_key, current_run_id],
        )
    except Exception as exc:
        logger.error(
            "Failed to deactivate prior gre_sampling_errors rows for run_key=%s: %s",
            run_key, exc,
        )


def log_error(meta_conn, meta_db: str, run_id, process_name, run_key,
              error_type: str, message: str, detail: str = None) -> None:
    """
    Insert one gre_sampling_errors row, after deactivating any row left
    active for this run_key from an earlier run_id -- see
    _deactivate_prior_errors()'s docstring. This error is always inserted
    with active_ind='Y': it belongs to the run_id currently executing,
    which is by definition the newest one for this run_key. Never raises
    -- an errors-table failure must not mask the real error.

    This is the ONE gre_sampling_errors write path for this package --
    sampling/sampling.py's runs go through this via its own
    _log_sampling_error() wrapper rather than each call site keeping its
    own INSERT+try/except copy.
    """
    _deactivate_prior_errors(meta_conn, meta_db, run_key, run_id)

    sql = f"""
        INSERT INTO {meta_db}.gre_sampling_errors (
            run_id, process_name, run_key, error_type, error_message, error_detail, active_ind
        ) VALUES (?, ?, ?, ?, ?, ?, 'Y')
    """
    try:
        execute_dml(meta_conn, sql, [run_id, process_name, run_key, error_type, message, detail])
    except Exception as exc:
        logger.error("Failed to write gre_sampling_errors row (run_id=%s): %s", run_id, exc)
