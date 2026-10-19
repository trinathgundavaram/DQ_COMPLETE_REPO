import logging
import os
import re
import secrets
from datetime import datetime

logger = logging.getLogger(__name__)


def _one_line(sql: str, max_len: int = 500) -> str:
    if not sql:
        return ""
    one_line = " ".join(sql.split())
    if len(one_line) > max_len:
        return one_line[:max_len] + f"... [truncated, {len(one_line)} chars total]"
    return one_line


MAX_RETRIES = int(os.getenv("GRE_QUERY_MAX_RETRIES", "3"))


EXCEPTION_CHUNK = int(os.getenv("GRE_EXCEPTION_CHUNK", "500"))


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


def execute_query(conn, query: str, params=None, log_params: bool = False) -> list:
    if log_params:
        logger.debug("execute_query: %s | params=%r", _one_line(query), params or [])
    else:
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


def execute_dml(conn, query: str, params=None, log_params: bool = False):
    if log_params:
        logger.debug("execute_dml: %s | params=%r", _one_line(query), params or [])
    else:
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
    msg = str(exc).lower()
    return any(marker in msg for marker in _DUPLICATE_KEY_MARKERS)


def _insert_or_skip(conn, sql: str, params: list) -> bool:
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        conn.commit()
        return True
    except Exception as exc:
        try:
            conn.commit()
        except Exception:
            pass
        if _is_duplicate_key_error(exc):
            logger.debug("Duplicate key on insert -- already recorded, skipping. (%s)", exc)
            return False
        raise
    finally:
        cursor.close()


def _chunked_executemany(conn, sql: str, rows: list, chunk_size: int = None) -> int:
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
    _chunked_executemany(conn, sql, rows, chunk_size)


def bulk_insert_or_skip(conn, sql: str, rows: list, chunk_size: int = None) -> int:
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
                conn.commit()
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
    return _chunked_executemany(conn, sql, rows, chunk_size)


_TOKEN_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}|\$([a-zA-Z_][a-zA-Z0-9_]*)\b")


def _escape_sql_literal(value) -> str:
    return str(value if value is not None else "").replace("'", "''")


def _find_unresolved_tokens(sql: str) -> list:
    return [braced or dollar for braced, dollar in _TOKEN_RE.findall(sql)]


def _substitute_params(sql: str, params: dict) -> str:
    if not sql:
        return sql
    params = params or {}

    def _replace(match: "re.Match") -> str:
        key = match.group(1) or match.group(2)
        if key not in params:
            return match.group(0)
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
    if not parts:
        raise ValueError("build_run_key() needs at least one part to build a run_key from.")
    return delimiter.join(str(p) for p in parts)


def default_run_key() -> str:
    return datetime.now().date().isoformat()


def generate_run_id(*label_parts, timestamp: datetime = None) -> str:
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
    sql = f"SELECT COUNT(*) AS cnt FROM {meta_db}.gre_rule_audit WHERE run_key = ? AND rule_group = ?"
    rows = execute_query(meta_conn, sql, [run_key, rule_group], log_params=True)
    return int(rows[0]["cnt"]) if rows else 0


@_source_retry
def _run_source_query(db_conn, sql: str) -> list:


    logger.debug("_run_source_query against source_type=%s", getattr(db_conn, "source_type", "?"))
    return execute_query(db_conn, sql)


def log_error(meta_conn, meta_db: str, run_id, rule_id, rule_group, run_key,
              error_type: str, message: str, detail: str = None, rule_variant=None) -> None:
    sql = f"""
        INSERT INTO {meta_db}.gre_rule_errors (
            run_id, rule_id, rule_group, rule_variant, run_key, error_type,
            error_message, error_detail, active_ind
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'Y')
    """
    try:
        execute_dml(meta_conn, sql,
                    [run_id, rule_id, rule_group, rule_variant, run_key, error_type, message, detail],
                    log_params=True)
    except Exception as exc:
        logger.error("Failed to write gre_rule_errors row (run_id=%s rule_id=%s): %s", run_id, rule_id, exc)
