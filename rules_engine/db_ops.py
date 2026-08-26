"""
rules_engine/db_ops.py
------------------------
Low-level DB helpers for rules_engine/ -- cursor/commit mechanics,
run_params substitution, run_id generation, and gre_rule_errors logging.

This used to live in shared/db_ops.py, shared verbatim with sampling/
(which has its own copy, sampling/db_ops.py, writing to its own
gre_sampling_errors instead). The two packages are now fully independent
-- no common shared/ module, no shared gre_errors table -- see README.md's
"Package separation" for why. Most of what's below is genuinely
table-agnostic (execute_query/execute_dml, the bulk writers, run_params
substitution, generate_run_id) and is duplicated in sampling/db_ops.py
byte-for-byte apart from the gre_rule_errors/gre_sampling_errors split at
the bottom; a fix to one of those generic helpers needs to be applied to
both copies.

Run-parameter substitution (_substitute_params) is the one mechanism this
package uses to let a project scope its data however it needs to -- a
month/year pair, a specific date, a region/contract column, or nothing at
all -- without the engine hardcoding any one project's notion of "scope"
as a schema column. `run_params` has zero reserved keys of its own; the
one identifier the tracking/idempotency schema (gre_exceptions,
gre_results, gre_rule_audit) keys off -- `run_key` -- is a separate,
explicit parameter passed alongside run_params, not a required member of
it. See _substitute_params()'s docstring below for the substitution
mechanics, and build_run_key() for a convenience way to build a run_key
out of whatever column(s) a caller wants (a batch id, a year+month pair,
a specific date, or any other combination).

`run_key` (which logical run this is) and `run_id` (which specific
attempt at it this is) are different things -- see generate_run_id()'s
docstring below for the full distinction and the shape `run_id` takes.

Debug logging
-------------
execute_query()/execute_dml()/_chunked_executemany()/bulk_insert_or_skip()/
_run_source_query() below all log at DEBUG: the SQL statement TEXT (one-
lined via _one_line() below, truncated) and row COUNTS (rows returned,
rows affected, chunk sizes). They NEVER log bind parameter VALUES or any
fetched/written row DATA -- this package runs rule_syntax/scope_sql
against source tables that can carry real case/member/claim identifiers,
so only shape-level facts (what ran, how many rows) go to the log, never
content. Nothing here calls logging.basicConfig() -- that's an opt-in
caller decision, see rules_engine/config.py::configure_logging().
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
# (gre_exceptions).
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
    Plain chunked executemany() -- no duplicate-key handling. One commit
    per chunk instead of one per row, cutting round trips by ~chunk_size
    on a large exception set.

    `rows` is a list of positional param sequences (list/tuple), matching
    DB-API cursor.executemany()'s convention.
    """
    _chunked_executemany(conn, sql, rows, chunk_size)


def bulk_insert_or_skip(conn, sql: str, rows: list, chunk_size: int = None) -> int:
    """
    Chunked executemany() with duplicate-key tolerance -- the bulk analog of
    _insert_or_skip(), used by rules_engine/executor.py for gre_exceptions
    where gre_exceptions_uix (rule_id, run_key, src_key_value) is what
    makes a rerun idempotent.

    Tries each chunk as one executemany() batch first (cheap: one round
    trip for up to `chunk_size` rows). If a chunk raises -- in practice
    almost always because one row in it collides with a src key
    committed by an EARLIER attempt on this run_key -- it falls back to
    row-by-row _insert_or_skip() for JUST that chunk, so one stale
    duplicate never costs the other rows in the same run. Any non
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
    logger.debug("bulk_insert_or_skip: %s | %d row(s), chunk_size=%d", _one_line(sql), len(rows), size)
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
    logger.debug(
        "bulk_insert_or_skip: %d of %d row(s) actually inserted (rest were duplicates)",
        inserted, len(rows),
    )
    return inserted


def bulk_execute(conn, sql: str, rows: list, chunk_size: int = None) -> int:
    """
    Chunked executemany() for UPDATE/DELETE-shaped DML -- the mutate-
    existing-rows analog of bulk_insert() (which is insert-only). Used by
    the etl_is_curr_ind reconciliation in rules_engine/executor.py::
    _write_exceptions(): flipping a batch of rows' current-indicator on a
    rerun, rather than one UPDATE+commit per row.

    No duplicate-key handling (unlike bulk_insert_or_skip()) -- an UPDATE
    by primary/unique key has nothing to collide on. `rows` is a list of
    positional param sequences, DB-API executemany() convention, same as
    bulk_insert()/bulk_insert_or_skip().

    Returns len(rows) (the number of UPDATE statements issued) -- not
    every DB-API driver exposes a uniform "rows actually matched" count,
    and the caller already knows which record_ids/keys it targeted.
    """
    return _chunked_executemany(conn, sql, rows, chunk_size)


# ---------------------------------------------------------------------------
# Run-parameter token substitution (v2 scoping mechanism -- see
# rules_engine/schema.sql's header for why there's no filter_column system)
# ---------------------------------------------------------------------------
# v1 supported exactly one substitutable value ({batch_id}), passed as a
# scalar all the way down. Different projects scope their data differently
# -- a month/year pair, a specific date, a region or contract column, or no
# filter at all -- so v2 generalizes this to an arbitrary dict of named
# values: a rule author embeds whichever "{key}" (or "$key") tokens their
# SQL needs, and the caller supplies a matching dict at run time. There is
# no reserved/required key -- run_params is entirely up to the rule author.
# The one value the idempotency/checkpoint schema (gre_exceptions,
# gre_results, gre_rule_audit) keys off is `run_key`, a separate explicit
# parameter callers pass to rules_engine/runner.py's entry points -- see
# build_run_key() below for a convenience way to build one out of a batch
# id, a year+month pair, a specific date, or any other column/combination.

# Two interchangeable token spellings, freely mixed in the same rule_syntax:
# "{key}" (braces) and "$key" (bare, no braces -- word-boundary terminated,
# so "$year" in "...$year_end..." does NOT consume "_end"). Group 1 catches
# the braced form, group 2 the dollar form -- exactly one of the two is
# non-None per match. Caution for postgres rule_syntax specifically:
# Postgres's own dollar-quoted string literals ($tag$...$tag$) can collide
# with this if `tag` happens to match a run_params key -- prefer "{key}"
# braces in any rule_syntax that also uses dollar-quoting.
_TOKEN_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}|\$([a-zA-Z_][a-zA-Z0-9_]*)\b")


def _escape_sql_literal(value) -> str:
    """
    Escape a value for embedding inside a single-quoted SQL string literal
    -- doubles embedded single quotes, and treats None as an empty string.
    Shared by _substitute_params() below (rule_syntax token substitution)
    and rules_engine/reporting.py's src-key tie-back query builder, so
    there is exactly one escaping implementation in this package.
    """
    return str(value if value is not None else "").replace("'", "''")


def _find_unresolved_tokens(sql: str) -> list:
    """
    After substitution, scan for any "{name}"- or "$name"-shaped token
    still present -- almost always a param the caller forgot to pass (or a
    typo in the rule's SQL), which would otherwise surface as a confusing
    SQL syntax error from the source database instead of a clear, specific,
    pre-execution one. Deliberately simple (a bare identifier, braced or
    dollar-prefixed, not a full templating grammar) since that's the only
    shape this substitution mechanism ever produces or consumes.
    """
    return [braced or dollar for braced, dollar in _TOKEN_RE.findall(sql)]


def _substitute_params(sql: str, params: dict) -> str:
    """
    Replace every literal "{key}" or "$key" token in `sql` with the
    escaped string value of params[key], for every key present in
    `params` -- both spellings are recognized everywhere this mechanism is
    used (rule_syntax, scope_sql, exclusion_sql), freely mixed in the same
    SQL. Rule authors are responsible for their own quoting (e.g. `WHERE
    run_date = '{run_date}'` / `WHERE run_date = '$run_date'`) -- this
    only does the swap.

    Raises ValueError if, after substitution, any "{token}"/"$token"-shaped
    text remains -- fail fast with a clear message naming the missing
    parameter(s), rather than letting an unsubstituted token reach the
    database as a syntax error. `sql` may be None/empty (returns it
    unchanged) so callers don't need to guard optional CLOB columns
    themselves.
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


_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def build_extra_filters_clause(extra_filters: dict) -> str:
    """
    Build a "AND col1 = 'val1' AND col2 = 'val2' ..." fragment from an
    extra_filters dict -- ONE OR MORE ad-hoc equality filters a caller
    wants applied at run time, on top of whatever run_params tokens the
    rule_syntax already substitutes. Unlike run_params (which only fills
    in "{key}"/"$key" placeholders the rule author explicitly wrote), this
    is for a column the rule_syntax DIDN'T anticipate at all -- e.g.
    filtering an already-authored rule by "run_ty='MNT'" without having
    to add a {run_ty} token to every rule that might ever need it.

    A rule opts into this by embedding the literal marker "{extra_filters}"
    (or "$extra_filters") anywhere in its rule_syntax -- typically right
    before the end of the WHERE clause, before any GROUP BY/ORDER BY. See
    rules_engine/executor.py::execute_rule()'s extra_filters parameter for
    where this gets spliced in. A rule that doesn't embed the marker
    simply never has extra_filters applied to it -- same "extra values are
    silently unused" philosophy as run_params' own unused keys.

    Returns "" (empty string, so the marker disappears cleanly with no
    dangling "AND") when extra_filters is empty/None. Every key is
    validated as a plain SQL identifier (letters/digits/underscore,
    not starting with a digit) and rejected with ValueError otherwise --
    unlike run_params' values (always treated as literal data and safely
    quoted/escaped), extra_filters' KEYS become literal column names
    spliced directly into the SQL text, so they can't be escaped the same
    way; this validation is what keeps that safe against a bad/malicious
    key rather than quoting a column name like a string ever could.
    Values ARE escaped via _escape_sql_literal(), same as every other
    literal this package ever substitutes.
    """
    if not extra_filters:
        return ""
    bad_keys = [key for key in extra_filters if not _IDENTIFIER_RE.match(key)]
    if bad_keys:
        raise ValueError(
            f"extra_filters key(s) {sorted(bad_keys)} are not valid SQL identifiers -- "
            f"refusing to splice them into a query as column names."
        )
    clauses = [
        f"{key} = '{_escape_sql_literal(value)}'"
        for key, value in sorted(extra_filters.items())
    ]
    return "AND " + " AND ".join(clauses)


def build_run_key(*parts, delimiter: str = "_") -> str:
    """
    Join one or more values into a single opaque tracking/idempotency key
    -- gre_exceptions, gre_results, and gre_rule_audit all key off
    this ONE value, however the caller chooses to construct it. There is
    no fixed shape: a plain batch id, a year+month pair, a specific date,
    a region, or any combination all work equally well.

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
    run_rule_group()/run_all_active_groups()/run_by_process_name() below
    fall back to when a caller doesn't pass one at all. Every batch id,
    year+month pair, or any other run_key shape a caller explicitly
    builds (build_run_key() above, or a plain string) always wins; this
    is purely the "nothing was passed" default, so an unattended/scheduled
    caller that runs once a day gets a sensible, idempotent run_key for
    free -- reruns on the SAME calendar day naturally land on the same
    run_key and get treated as reruns of the same run (see
    rules_engine/executor.py::_write_result()'s active_ind reconciliation),
    while the next day's run gets a fresh one automatically.
    """
    return datetime.now().date().isoformat()


def generate_run_id(*label_parts, timestamp: datetime = None) -> str:
    """
    Build one `run_id` -- the identifier `gre_exceptions`,
    `gre_results`, `gre_rule_errors`, and `gre_rule_audit` all stamp on
    every row written by ONE execution
    (rules_engine/runner.py::run_rule_group()), as opposed to `run_key`,
    which identifies WHICH LOGICAL RUN a caller is tracking/re-running
    (see build_run_key() above) and can be shared across many `run_id`s
    over time (a rerun of the same `run_key` always mints a brand new
    `run_id`). If `run_key` answers "which batch/period is this," `run_id`
    answers "which specific attempt at it is this."

    Shape: "{label_parts joined by '::'}::{timestamp to the microsecond}::{6 hex chars}"

        generate_run_id("claims_dq", "BATCH_2026_08_19")
            -> "claims_dq::BATCH_2026_08_19::20260819T143022.183045::a1b2c3"

    Why this shape, piece by piece:
      - Label parts (e.g. rule_group + run_key) come first so a human
        scanning `gre_results`/`gre_rule_audit`/a log line can immediately
        tell WHAT ran and for WHICH run_key without decoding anything --
        no separate lookup needed for the common case of "which
        rule_group/run_key does this row belong to."
      - '::' (not '_' or '-') separates the parts visually, chosen
        specifically because rule_group/run_key are free-form VARCHAR
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
        sort), so `ORDER BY run_id` on any of these tables already sorts
        runs oldest-to-newest without touching load_datetime/started_at,
        AND a human can read the exact moment an attempt started directly
        off the id, no join required.
      - The trailing 6 hex characters (`secrets.token_hex(3)`) exist
        SOLELY to make collision effectively impossible even if two calls
        land in the same microsecond. A run_id collision is a real
        correctness risk, not just a cosmetic one: every reconciliation
        query in this package (gre_exceptions' etl_is_curr_ind flip,
        gre_results/gre_rule_errors' active_ind flip) filters on "run_id <>
        this_run_id" to tell "this attempt" from "an earlier one" -- two
        attempts sharing a run_id would each treat the other's rows as
        its own, silently corrupting that logic.

    `timestamp`: defaults to `datetime.now()`; pass an explicit value
    (e.g. a run's own `started_at`) so the run_id's embedded timestamp
    matches a timestamp already being persisted elsewhere for the same
    run, instead of a call to this function drifting by however many
    milliseconds elapsed since that earlier `datetime.now()`.

    Label parts that are `None` or empty string are skipped rather than
    producing an empty, doubled-up '::' segment -- lets a caller pass
    something optional (e.g. a `rule_variant` that might be `None`)
    without special-casing it themselves.

    Length: the timestamp + suffix + delimiters overhead is a fixed 34
    characters ("::20260819T143022.183045::a1b2c3"), leaving comfortable
    room for label parts under gre_*.run_id's VARCHAR(200) (rule_group and
    run_key are each VARCHAR(100) at the source -- ordinary values leave
    this well under the limit). Logs a warning rather than silently
    truncating if a caller's label parts are unusually long enough to
    push the total over 200 -- the resulting INSERT would fail loudly at
    the database instead in that case, which is preferable to a
    truncated, ambiguous run_id, but this warning surfaces the problem
    before that point.
    """
    ts = (timestamp or datetime.now()).strftime("%Y%m%dT%H%M%S.%f")
    suffix = secrets.token_hex(3)
    labels = [str(p) for p in label_parts if p not in (None, "")]
    run_id = "::".join([*labels, ts, suffix])
    if len(run_id) > 200:
        logger.warning(
            "generate_run_id(): generated run_id is %d characters, over the 200-char "
            "VARCHAR(200) run_id column width used across gre_* tables -- "
            "the write that uses this run_id may fail. Label parts: %r",
            len(run_id), labels,
        )
    return run_id


def count_prior_attempts(meta_conn, meta_db: str, run_key, rule_group: str) -> int:
    """
    How many prior attempts at this (run_key, rule_group) already exist,
    counted from `gre_rule_audit`. Callers use `this count + 1` as a
    human-readable "attempt-N" label folded into `run_id` (see
    rules_engine/runner.py::run_rule_group()), so a rerun of the same
    run_key is visibly attempt 2, 3, ... at a glance in
    `gre_results`/`gre_rule_audit` instead of requiring a human to compare two
    run_ids' timestamps to tell which came first.

    Queries gre_rule_audit_group_run_key_ix's own indexed columns
    directly.

    This is a LABEL, not a uniqueness mechanism: run_id's own trailing hex
    suffix (see generate_run_id() above) is what actually guarantees no
    two attempts collide. Two callers racing to start the same run_key at
    the exact same instant could in principle compute the same attempt
    number (a COUNT-then-INSERT race, not wrapped in a transaction) -- an
    accepted cosmetic edge case, since it never affects correctness of the
    active_ind reconciliation, which keys off the (unique) run_id, not
    this number.
    """
    sql = f"SELECT COUNT(*) AS cnt FROM {meta_db}.gre_rule_audit WHERE run_key = ? AND rule_group = ?"
    rows = execute_query(meta_conn, sql, [run_key, rule_group])
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
# gre_rule_errors -- this package's own error log
# ---------------------------------------------------------------------------
# Kept fully separate from sampling/db_ops.py's gre_sampling_errors --
# rules_engine used to share one gre_errors table with sampling/, where
# rule_id was NULL for a sampling-run error row (see sampling/db_ops.py's
# module docstring for the mirror-image history). Now that the two
# packages don't share a table, rule_id here is always populated (every
# rules_engine error is tied to a specific rule) -- no more "rule_id IS
# NULL" branch to worry about.

def _deactivate_prior_errors(meta_conn, meta_db: str, rule_id, run_key, current_run_id) -> None:
    """
    Soft-deactivate every gre_rule_errors row for (rule_id, run_key) left
    active from an EARLIER run_id, mirroring rules_engine/executor.py::
    _deactivate_prior_results() for gre_results -- the same "always
    re-execute, deactivate stale, activate new" pattern applied here to
    gre_rule_errors. Never deletes -- gre_rule_errors keeps full history
    for audit; only active_ind flips. Never raises: a failure here must
    not mask the real error being logged.
    """
    try:
        execute_dml(
            meta_conn,
            f"""
            UPDATE {meta_db}.gre_rule_errors
            SET active_ind = 'N', last_updated_datetime = CURRENT_TIMESTAMP
            WHERE rule_id = ? AND run_key = ? AND run_id <> ? AND active_ind = 'Y'
            """,
            [rule_id, run_key, current_run_id],
        )
    except Exception as exc:
        logger.error(
            "Failed to deactivate prior gre_rule_errors rows for rule_id=%s run_key=%s: %s",
            rule_id, run_key, exc,
        )


def log_error(meta_conn, meta_db: str, run_id, rule_id, rule_group, run_key,
              error_type: str, message: str, detail: str = None) -> None:
    """
    Insert one gre_rule_errors row, after deactivating any row left active
    for this (rule_id, run_key) from an earlier run_id -- see
    _deactivate_prior_errors()'s docstring. This error is always inserted
    with active_ind='Y': it belongs to the run_id currently executing,
    which is by definition the newest one for this run_key. Never raises
    -- an errors-table failure must not mask the real error.

    This is the ONE gre_rule_errors write path for this package --
    rule execution goes through this via rules_engine/executor.py's
    rule-dict convenience wrapper _log_error() rather than each call site
    keeping its own INSERT+try/except copy.
    """
    _deactivate_prior_errors(meta_conn, meta_db, rule_id, run_key, run_id)

    sql = f"""
        INSERT INTO {meta_db}.gre_rule_errors (
            run_id, rule_id, rule_group, run_key, error_type, error_message, error_detail, active_ind
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'Y')
    """
    try:
        execute_dml(meta_conn, sql, [run_id, rule_id, rule_group, run_key, error_type, message, detail])
    except Exception as exc:
        logger.error("Failed to write gre_rule_errors row (run_id=%s rule_id=%s): %s", run_id, rule_id, exc)
