"""
core/profiler.py
----------------
Automated per-column statistical profiling.

Profile data is stored in dq_column_profile and controlled by dq_profile_config.
Only tables with an active config row are profiled (opt-in by table).

Enabling profiling for a table
--------------------------------
Insert a row into dq_profile_config:

    INSERT INTO {meta_db}.dq_profile_config
        (config_id, project_name, process_name, table_name,
         enabled, columns_include, columns_exclude, top_n_values, run_frequency)
    VALUES
        (1, 'CLAIMS', 'MEMBER', 'CLAIMS_DB.SCHEMA.MEMBER_TBL',
         1, NULL, 'LOAD_TS,AUDIT_USER', 10, 'ALWAYS');

    -- columns_include: CSV of columns to profile; NULL = all columns
    -- columns_exclude: CSV of columns to skip (applied after include)
    -- run_frequency:   ALWAYS | DAILY | WEEKLY | MANUAL

Per-column statistics computed
-------------------------------
total_rows     — row count in scope (respects filter_sql if set)
null_count     — rows where column IS NULL
null_pct       — null_count / total_rows * 100
distinct_count — COUNT(DISTINCT col)
distinct_pct   — distinct_count / total_rows * 100
min_value      — MIN(col) as string
max_value      — MAX(col) as string
mean_value     — AVG(CAST(col AS FLOAT)) — NULL for non-numeric columns
stddev_value   — STDDEV_SAMP / STDEV — NULL for non-numeric columns
top_values     — JSON array [{value, count}] of top-N values by frequency
                 Only computed when distinct_count ≤ top_n_threshold (10 000)

Cross-DB compatibility
----------------------
Column discovery : SELECT * WHERE 1=0 + cursor.description (works on all 5 DBs)
Stats query      : ANSI SQL (null count, distinct, min, max via COUNT / MIN / MAX)
Numeric stats    : CAST(col AS FLOAT); errors silently suppressed for text columns
Top-N            : Teradata uses QUALIFY RANK(); SQL Server uses TOP N;
                   PostgreSQL / Databricks / DuckDB use LIMIT N
"""

import json
import logging
from datetime import date, datetime
from typing import List, Optional

logger = logging.getLogger(__name__)

# Columns with distinct_count above this threshold won't get top-N (perf guard)
_TOP_N_CARDINALITY_LIMIT = 10_000


# ---------------------------------------------------------------------------
# Engine entry point
# ---------------------------------------------------------------------------

def profile_tables_for_run(
    cf,           # ConnectionFactory
    td_conn,      # Teradata metadata connection (main thread)
    rules: list,
    run: dict,
    meta_db: str,
):
    """
    Profile every unique (source_system, table) combination that has an active
    dq_profile_config entry.  Called by engine.py after rule execution.

    Parameters
    ----------
    cf       : ConnectionFactory — provides source DB connections
    td_conn  : metadata connection for reading config + writing profile rows
    rules    : loaded rule dicts (used to discover unique tables per project)
    run      : run context dict
    meta_db  : metadata schema name
    """
    from core.executor import execute_query, bulk_insert

    # Build unique (source_system, table_name) pairs from this run's rules
    seen    = set()
    targets = []
    for rule in rules:
        src  = (rule.get("source_system") or "").lower()
        tbl  = (rule.get("src_tbl_nm") or "").strip()
        key  = (src, tbl)
        if key not in seen and src and tbl:
            seen.add(key)
            targets.append({"source_system": src, "table_name": tbl, "rule": rule})

    if not targets:
        return

    # Load all profile configs for this project/process in one query.
    # Fix: dq_profile_config rows may use NULL project_name/process_name as
    # wildcards (see _load_profile_configs), so more than one active row can
    # match the same table_name (e.g. a global default plus a project-
    # specific override). Resolve to the most specific row per table_name —
    # exact project+process > project-only wildcard > global wildcard —
    # mirroring the same pattern used in core/reporting.py::_load_routes().
    cfg_rows = _load_profile_configs(td_conn, run, meta_db, execute_query)
    cfg_by_table = _resolve_most_specific_configs(cfg_rows)

    profiled = 0
    for target in targets:
        tbl  = target["table_name"]
        rule = target["rule"]
        cfg  = cfg_by_table.get(tbl)

        if cfg is None or not cfg.get("enabled", 1):
            continue
        if not _should_run_now(cfg):
            logger.debug("Profiling skipped for %s (frequency=%s, last_profiled=%s).",
                         tbl, cfg.get("run_frequency"), cfg.get("last_profiled"))
            continue

        src    = target["source_system"]
        db_con = cf.get(src)
        if db_con is None:
            logger.warning("Profiler: no connection '%s' — skipping table %s.", src, tbl)
            continue

        try:
            source_type = getattr(db_con, "source_type", "teradata")
            _profile_one_table(
                db_con, td_conn, tbl, rule, run, meta_db, cfg,
                source_type, execute_query, bulk_insert,
            )
            _update_last_profiled(td_conn, cfg.get("config_id"), meta_db, execute_query)
            profiled += 1
        except Exception as exc:
            logger.error("Profiling failed for table %s: %s", tbl, exc, exc_info=True)

    if profiled:
        logger.info("Profiled %d table(s).", profiled)


# ---------------------------------------------------------------------------
# Core profiling logic
# ---------------------------------------------------------------------------

def _profile_one_table(
    db_conn, td_conn, table: str, rule: dict, run: dict,
    meta_db: str, cfg: dict, source_type: str, execute_query, bulk_insert,
):
    """Profile all configured columns for one table and persist results."""
    top_n = int(cfg.get("top_n_values") or 10)

    # Discover columns
    all_cols = _discover_columns(db_conn, table)
    cols     = _filter_columns(all_cols, cfg)

    if not cols:
        logger.warning("No columns to profile for table %s after include/exclude filter.", table)
        return

    profile_date = date.today()
    run_date_str = str(profile_date)
    rows_to_insert = []

    for col in cols:
        stats = _profile_column(db_conn, source_type, table, col, top_n)
        if stats is None:
            continue

        rows_to_insert.append((
            run["run_id"],
            table,
            col,
            stats["total_rows"],
            stats["null_count"],
            stats["null_pct"],
            stats["distinct_count"],
            stats["distinct_pct"],
            stats["min_value"],
            stats["max_value"],
            stats["mean_value"],
            stats["stddev_value"],
            json.dumps(stats["top_values"], default=str),
            profile_date,
            source_type,
        ))

    if not rows_to_insert:
        return

    # project_name/process_name are NOT stored — derivable via run_id ->
    # dq_run_control (see ddl.sql v7).
    sql = f"""
        INSERT INTO {meta_db}.dq_column_profile (
            run_id, table_name, column_name,
            total_rows, null_count, null_pct, distinct_count, distinct_pct,
            min_value, max_value, mean_value, stddev_value,
            top_values, profile_date, source_type, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """
    bulk_insert(td_conn, sql, rows_to_insert)
    logger.info(
        "Profiled %d column(s) for table %s (run %s).",
        len(rows_to_insert), table, run["run_id"],
    )


def _profile_column(
    db_conn,
    source_type: str,
    table: str,
    col: str,
    top_n: int,
) -> Optional[dict]:
    """
    Compute statistics for one column.  Returns None if the stats query fails.

    Perf fix: numeric columns previously required two full-table-scan
    queries (basic stats, then a separate numeric-stats query). They're now
    merged into a single query — AVG/STDDEV(CAST(col AS FLOAT)) fails for
    non-numeric columns and aborts the whole statement (same failure
    semantics the original two-query split already relied on), so on
    failure we transparently fall back to the original basic-stats-only
    query. Net effect: 1 scan instead of 2 for numeric columns, no change
    in query count or behavior for non-numeric columns.
    """
    from core.executor import execute_query

    stddev_fn = "STDEV" if source_type in ("sqlserver", "mssql") else "STDDEV_SAMP"

    # ── Attempt 1: combined basic + numeric stats in one scan ────────────────
    r = None
    mean_val = stddev_val = None
    try:
        combined_sql = f"""
            SELECT
                COUNT(*)              AS total_rows,
                COUNT({col})          AS non_null_count,
                COUNT(DISTINCT {col}) AS distinct_count,
                MIN({col})            AS min_val,
                MAX({col})            AS max_val,
                AVG(CAST({col} AS FLOAT))         AS mean_val,
                {stddev_fn}(CAST({col} AS FLOAT)) AS stddev_val
            FROM {table}
        """
        rows = execute_query(db_conn, combined_sql)
        if rows:
            r  = rows[0]
            mv = r.get("mean_val")
            sv = r.get("stddev_val")
            mean_val   = float(mv) if mv is not None else None
            stddev_val = float(sv) if sv is not None else None
    except Exception:
        r = None   # non-numeric column (CAST failed) or other error — fall back below

    # ── Fallback: basic stats only, no numeric CAST ───────────────────────────
    # MIN/MAX are fetched without CAST so the driver returns native Python types;
    # we convert to str in Python.  This is the most portable approach.
    if r is None:
        try:
            basic_sql = f"""
                SELECT
                    COUNT(*)              AS total_rows,
                    COUNT({col})          AS non_null_count,
                    COUNT(DISTINCT {col}) AS distinct_count,
                    MIN({col})            AS min_val,
                    MAX({col})            AS max_val
                FROM {table}
            """
            rows = execute_query(db_conn, basic_sql)
            if not rows:
                return None
            r = rows[0]
        except Exception as exc:
            logger.debug("Basic stats failed for %s.%s: %s", table, col, exc)
            return None
        mean_val = stddev_val = None   # confirmed non-numeric — no numeric stats

    total        = int(r.get("total_rows") or 0)
    non_null     = int(r.get("non_null_count") or 0)
    null_count   = total - non_null
    distinct     = int(r.get("distinct_count") or 0)

    # ── Top-N values by frequency (low-cardinality only) ──────────────────────
    top_values = []
    if 0 < distinct <= _TOP_N_CARDINALITY_LIMIT:
        try:
            top_values = _get_top_n(db_conn, source_type, table, col, top_n)
        except Exception as exc:
            logger.debug("Top-N failed for %s.%s: %s", table, col, exc)

    return {
        "total_rows":    total,
        "null_count":    null_count,
        "null_pct":      round(null_count / total * 100, 4) if total else 0.0,
        "distinct_count": distinct,
        "distinct_pct":  round(distinct / total * 100, 4)  if total else 0.0,
        "min_value":     str(r.get("min_val") or ""),
        "max_value":     str(r.get("max_val") or ""),
        "mean_value":    mean_val,
        "stddev_value":  stddev_val,
        "top_values":    top_values,
    }


def _get_top_n(db_conn, source_type: str, table: str, col: str, n: int) -> list:
    """Return top-N values by frequency as [{value, count}] list."""
    from core.executor import execute_query

    st = (source_type or "").lower()

    # CAST to VARCHAR for display; Databricks uses STRING
    cast = f"CAST({col} AS STRING)" if st == "databricks" else f"CAST({col} AS VARCHAR(500))"

    if st == "teradata":
        # Teradata: use QUALIFY + RANK() to limit rows (subquery ORDER BY restriction)
        sql = f"""
            SELECT val, cnt
            FROM (
                SELECT {cast} AS val, COUNT(*) AS cnt
                FROM {table}
                WHERE {col} IS NOT NULL
                GROUP BY {col}
            ) _topn
            QUALIFY RANK() OVER (ORDER BY cnt DESC) <= {n}
        """
    elif st in ("sqlserver", "mssql"):
        sql = f"""
            SELECT TOP {n} {cast} AS val, COUNT(*) AS cnt
            FROM {table}
            WHERE {col} IS NOT NULL
            GROUP BY {col}
            ORDER BY cnt DESC
        """
    else:
        # PostgreSQL, Aurora, Databricks, DuckDB/file
        sql = f"""
            SELECT {cast} AS val, COUNT(*) AS cnt
            FROM {table}
            WHERE {col} IS NOT NULL
            GROUP BY {col}
            ORDER BY cnt DESC
            LIMIT {n}
        """

    rows = execute_query(db_conn, sql)
    return [{"value": str(r.get("val") or ""), "count": int(r.get("cnt") or 0)}
            for r in rows]


# ---------------------------------------------------------------------------
# Column discovery and filtering
# ---------------------------------------------------------------------------

def _discover_columns(db_conn, table: str) -> List[str]:
    """
    Return column names by running SELECT * WHERE 1=0 + cursor.description.
    Works identically across all 5 source types (Teradata, PG, Databricks,
    SQL Server, DuckDB).
    """
    try:
        cursor = db_conn.cursor()
        cursor.execute(f"SELECT * FROM {table} WHERE 1=0")
        if cursor.description:
            cols = [c[0].lower() for c in cursor.description]
        else:
            cols = []
        cursor.close()
        return cols
    except Exception as exc:
        logger.warning("Column discovery failed for %s: %s", table, exc)
        return []


def _filter_columns(all_cols: List[str], cfg: dict) -> List[str]:
    """Apply include/exclude lists from dq_profile_config."""
    include_raw = (cfg.get("columns_include") or "").strip()
    exclude_raw = (cfg.get("columns_exclude") or "").strip()

    include_set = {c.strip().lower() for c in include_raw.split(",") if c.strip()} if include_raw else set()
    exclude_set = {c.strip().lower() for c in exclude_raw.split(",") if c.strip()} if exclude_raw else set()

    if include_set:
        cols = [c for c in all_cols if c in include_set]
    else:
        cols = list(all_cols)

    if exclude_set:
        cols = [c for c in cols if c not in exclude_set]

    return cols


# ---------------------------------------------------------------------------
# Config loading and frequency control
# ---------------------------------------------------------------------------

def _resolve_most_specific_configs(cfg_rows: list) -> dict:
    """
    Collapse dq_profile_config rows to one per table_name, keeping the most
    specific match when a global/project wildcard row and a more specific
    row both target the same table (specificity = non-null project_name +
    non-null process_name on that row, same scoring as _load_routes()).
    """
    def specificity(r):
        return (r.get("project_name") is not None) + (r.get("process_name") is not None)

    best: dict = {}
    for r in cfg_rows:
        tbl = r.get("table_name")
        if tbl not in best or specificity(r) > specificity(best[tbl]):
            best[tbl] = r
    return best


def _load_profile_configs(td_conn, run: dict, meta_db: str, execute_query) -> list:
    """
    Load dq_profile_config rows matching this project/process.
    Rows with NULL project_name or process_name match any value (wildcard).
    """
    project = run.get("project_name", "")
    process = run.get("process_name", "")
    try:
        return execute_query(
            td_conn,
            f"""
            SELECT *
            FROM {meta_db}.dq_profile_config
            WHERE enabled = 1
              AND (project_name IS NULL OR project_name = ?)
              AND (process_name IS NULL OR process_name = ?)
            """,
            [project, process],
        )
    except Exception as exc:
        logger.warning("Could not load dq_profile_config: %s", exc)
        return []


def _should_run_now(cfg: dict) -> bool:
    """
    Return True if profiling should execute based on run_frequency and last_profiled.
    """
    freq         = (cfg.get("run_frequency") or "ALWAYS").upper()
    last_profiled = cfg.get("last_profiled")

    if freq == "MANUAL":
        return False   # only run when explicitly triggered
    if freq == "ALWAYS" or last_profiled is None:
        return True

    # Convert last_profiled to date for comparison
    if isinstance(last_profiled, (datetime, date)):
        last_date = last_profiled.date() if isinstance(last_profiled, datetime) else last_profiled
    else:
        try:
            last_date = datetime.fromisoformat(str(last_profiled)).date()
        except Exception:
            return True   # can't parse — run to be safe

    today = date.today()
    if freq == "DAILY":
        return (today - last_date).days >= 1
    if freq == "WEEKLY":
        return (today - last_date).days >= 7
    return True


def _update_last_profiled(td_conn, config_id, meta_db: str, execute_query):
    """Stamp last_profiled on the config row after a successful profile run."""
    from core.executor import execute_dml
    try:
        execute_dml(
            td_conn,
            f"""
            UPDATE {meta_db}.dq_profile_config
            SET last_profiled = CURRENT_TIMESTAMP
            WHERE config_id = ?
            """,
            [config_id],
        )
    except Exception as exc:
        logger.warning("Could not update last_profiled for config_id=%s: %s", config_id, exc)
