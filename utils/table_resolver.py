"""
utils/table_resolver.py
------------------------
Resolve the fully-qualified table (or DuckDB view) name from a rule dict.

File source detection
---------------------
When src_tbl_nm has a recognised flat-file extension (.csv, .xlsx, .xls,
.tsv, .parquet) the function returns ONLY the filename stem — no DB/schema
prefix — because the FileAdapter registers that stem as a DuckDB view name.

    "claims_2024.csv"  →  "claims_2024"
    "members.xlsx"     →  "members"

Database source
---------------
    src_db_name present  →  resolve {ENV} token, combine with optional
                             src_schema + src_tbl_nm.
    src_db_name absent   →  return src_tbl_nm as-is (already qualified).
"""

from pathlib import Path
from utils.db_resolver import resolve_db_name

# Flat-file extensions recognised by FileAdapter
_FILE_EXTENSIONS = {".csv", ".xlsx", ".xls", ".tsv", ".parquet"}


def resolve_table(rule: dict) -> str:
    """
    Build the table name to embed in SQL queries.

    Parameters
    ----------
    rule : dict row from dq_rules

    Returns
    -------
    str  — either a DuckDB view name (file sources) or a fully-qualified
            DB.SCHEMA.TABLE name (database sources).
    """
    src_tbl_nm  = (rule.get("src_tbl_nm") or "").strip()
    src_db_name = (rule.get("src_db_name") or "").strip()
    src_schema  = (rule.get("src_schema") or "").strip()

    if not src_tbl_nm:
        raise ValueError(
            f"rule_id={rule.get('rule_id')}: src_tbl_nm is empty — "
            "cannot resolve table."
        )

    # ── File source: return DuckDB view name (stem only) ─────────────────
    if Path(src_tbl_nm).suffix.lower() in _FILE_EXTENSIONS:
        return Path(src_tbl_nm).stem

    # ── Database source ───────────────────────────────────────────────────
    if not src_db_name:
        # Already fully qualified or no DB prefix needed
        return src_tbl_nm

    db = resolve_db_name(src_db_name)

    if src_schema:
        return f"{db}.{src_schema}.{src_tbl_nm}"
    return f"{db}.{src_tbl_nm}"
