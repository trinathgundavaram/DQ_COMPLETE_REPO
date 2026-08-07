"""
utils/db_helpers.py
---------------------
Table and database name resolution shared by every connector/rule.

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
"""

from pathlib import Path

from config.env_config import get_config

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
