"""
utils/ids.py
--------------
Identifier/key builders used across a run.

generate_run_id(...)          — deterministic, human-readable run_id.
build_json_pk(rule, row)      — JSON key->value map for an exception row.
build_pk_string(rule, row)    — pipe-delimited "key=value" string, same data.

Both key builders read rule['primary_key_columns'] — the entity key
column(s) declared on the rule (Section 4's "key_columns" concept) — so a
two-table join rule and a single-table rule both produce comparable,
storable exception rows without forcing every rule to share one output
schema.
"""

import json
from datetime import datetime


def generate_run_id(
    project: str,
    process: str,
    run_type: str,
    run_mode: str,
    batch_id: str = None,
    start_date=None,
    end_date=None,
) -> str:
    """
    Format:
      BATCH : {PROJECT}_{PROCESS}_{RUN_TYPE}_BATCH_{batch_id}_{YYYYMMDD_HHMMSS}
      DATE  : {PROJECT}_{PROCESS}_{RUN_TYPE}_DATE_{start_date}_{end_date}_{YYYYMMDD_HHMMSS}
      FULL  : {PROJECT}_{PROCESS}_{RUN_TYPE}_FULL_{YYYYMMDD_HHMMSS}
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")   # seconds prevent same-minute collision

    if run_mode == "BATCH" and batch_id:
        dataset = batch_id
    elif run_mode == "DATE" and start_date and end_date:
        dataset = f"{start_date}_{end_date}"
    else:
        dataset = "FULL"

    return f"{project}_{process}_{run_type}_{run_mode}_{dataset}_{ts}"


def build_json_pk(rule: dict, row: dict) -> str:
    """'{"member_id": "123", "claim_id": "CLM001"}' from primary_key_columns."""
    cols = _parse_pk_columns(rule)
    return json.dumps({col: str(row.get(col, "NULL")) for col in cols})


def build_pk_string(rule: dict, row: dict) -> str:
    """"member_id=123|claim_id=CLM001" — same key columns, human-readable."""
    cols = _parse_pk_columns(rule)
    return "|".join(f"{c}={row.get(c, 'NULL')}" for c in cols)


def _parse_pk_columns(rule: dict) -> list:
    raw = rule.get("primary_key_columns") or ""
    return [c.strip() for c in raw.split(",") if c.strip()]
