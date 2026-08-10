"""
utils/db_helpers.py
---------------------
Table/database name resolution, and the dq_scope dimension resolver,
shared by every connector/rule.

resolve_db_name(db_pattern) — replaces an {ENV} token in a DB name pattern
    with the current environment's suffix (DEV/QA/UAT/PROD). Patterns with
    no {ENV} token are returned unchanged, so rules can reference literal
    DB names without an environment placeholder.

resolve_table(rule) — builds the fully-qualified table (or DuckDB view)
    name to embed in a rule's SQL:
      - Flat-file sources (.csv/.xlsx/.xls/.tsv/.parquet) return just the
        filename stem, since FileAdapter registers that stem as a DuckDB
        view name (e.g. "claims_2024.csv" -> "claims_2024").
      - Database sources combine src_db_name (with {ENV} resolved) +
        optional src_schema + src_tbl_nm, or return src_tbl_nm as-is if no
        src_db_name is set (already fully qualified).

find_scope_id() / get_scope_id() — dq_scope is the single dimension table
    every other project-scoped table (dq_rules, dq_run_control,
    dq_metrics_summary, dq_sampling_config) references by scope_id instead
    of each repeating its own project_name/process_name pair. find_scope_id
    is a read-only lookup (returns None if the scope doesn't exist yet —
    correct for filtering, since "no such scope" and "scope exists but has
    no matching rows" both mean zero results). get_scope_id is get-or-
    create, used only where a row must be persisted against some scope
    that may not have been seen before (dq_run_control, dq_metrics_summary)
    — race-safe via the same duplicate-key-then-reselect fallback pattern
    rules_engine/metrics.py::_upsert_metrics already uses for concurrent runs.
"""

import logging
from pathlib import Path

from config.env_config import get_config

logger = logging.getLogger(__name__)

# Flat-file extensions recognised by FileAdapter
_FILE_EXTENSIONS = {".csv", ".xlsx", ".xls", ".tsv", ".parquet"}


def resolve_db_name(db_pattern: str) -> str:
    """
    Examples (DEV env):
        "CMSUNIV_FILELAND{ENV}_T"  ->  "CMSUNIV_FILELAND_DEV_T"
        "PROD_CLAIMS_DB"           ->  "PROD_CLAIMS_DB"   (no token — unchanged)
    """
    if not db_pattern:
        raise ValueError("db_pattern is empty — cannot resolve DB name.")

    if "{ENV}" not in db_pattern:
        return db_pattern

    cfg = get_config()
    return db_pattern.replace("{ENV}", cfg["ENV_TOKEN"])


def resolve_table(rule: dict) -> str:
    """
    Return either a DuckDB view name (file/S3 sources) or a fully-qualified
    DB.SCHEMA.TABLE name (database sources) for the rule's src_tbl_nm.
    """
    src_tbl_nm  = (rule.get("src_tbl_nm") or "").strip()
    src_db_name = (rule.get("src_db_name") or "").strip()
    src_schema  = (rule.get("src_schema") or "").strip()

    if not src_tbl_nm:
        raise ValueError(
            f"rule_id={rule.get('rule_id')}: src_tbl_nm is empty — cannot resolve table."
        )

    # File source: return DuckDB view name (stem only)
    if Path(src_tbl_nm).suffix.lower() in _FILE_EXTENSIONS:
        return Path(src_tbl_nm).stem

    # Database source
    if not src_db_name:
        return src_tbl_nm   # already fully qualified or no DB prefix needed

    db = resolve_db_name(src_db_name)
    if src_schema:
        return f"{db}.{src_schema}.{src_tbl_nm}"
    return f"{db}.{src_tbl_nm}"


def find_scope_id(td, project_name: str, process_name, meta_db: str):
    """
    Look up an existing dq_scope row for (project_name, process_name).

    Returns the scope_id, or None if no such scope has been created yet
    (correct for read-side filters — a scope that doesn't exist matches
    zero rows, same as one that exists but is empty).
    """
    from rules_engine.executor import execute_query

    if process_name is None:
        sql = f"""
            SELECT scope_id FROM {meta_db}.dq_scope
            WHERE project_name = ? AND process_name IS NULL
        """
        params = [project_name]
    else:
        sql = f"""
            SELECT scope_id FROM {meta_db}.dq_scope
            WHERE project_name = ? AND process_name = ?
        """
        params = [project_name, process_name]

    rows = execute_query(td, sql, params)
    return rows[0]["scope_id"] if rows else None


def get_scope_id(td, project_name: str, process_name, meta_db: str) -> int:
    """
    Get-or-create the dq_scope row for (project_name, process_name).

    Used only where a row must be persisted against a scope that may not
    have been seen before (dq_run_control at run start, dq_metrics_summary
    on upsert). Two threads racing to create the same new scope is handled
    the same way rules_engine/metrics.py::_upsert_metrics handles a concurrent-run
    MERGE race: attempt the INSERT, and on a duplicate-key error just
    re-select the row the other thread already created.
    """
    from rules_engine.executor import execute_dml

    existing = find_scope_id(td, project_name, process_name, meta_db)
    if existing is not None:
        return existing

    try:
        execute_dml(
            td,
            f"INSERT INTO {meta_db}.dq_scope (project_name, process_name) VALUES (?, ?)",
            [project_name, process_name],
        )
    except Exception as exc:
        err = str(exc).lower()
        if not any(k in err for k in ("unique", "duplicate", "2801", "primary index")):
            raise
        logger.debug(
            "dq_scope unique-index race for (%s, %s) — another writer created it first.",
            project_name, process_name,
        )

    scope_id = find_scope_id(td, project_name, process_name, meta_db)
    if scope_id is None:
        # Only reachable if the INSERT silently failed for a reason other
        # than a duplicate key — surface it rather than returning None to
        # a caller that needs a valid FK.
        raise RuntimeError(
            f"Could not resolve or create dq_scope for "
            f"(project_name={project_name!r}, process_name={process_name!r})."
        )
    return scope_id
