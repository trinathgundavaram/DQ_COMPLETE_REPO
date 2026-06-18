import json


def build_json_pk(rule: dict, row: dict) -> str:
    """
    Build a JSON string of primary key column → value pairs.

    Example:
        rule['primary_key_columns'] = "member_id, claim_id"
        row = {'member_id': 123, 'claim_id': 'CLM001', ...}
        → '{"member_id": "123", "claim_id": "CLM001"}'
    """
    cols = _parse_pk_columns(rule)
    return json.dumps({col: str(row.get(col, "NULL")) for col in cols})


def build_pk_string(rule: dict, row: dict) -> str:
    """
    Build a pipe-delimited key=value string for quick human reading.

    Example:
        → "member_id=123|claim_id=CLM001"
    """
    cols = _parse_pk_columns(rule)
    return "|".join(f"{c}={row.get(c, 'NULL')}" for c in cols)


def _parse_pk_columns(rule: dict) -> list:
    raw = rule.get("primary_key_columns") or ""
    return [c.strip() for c in raw.split(",") if c.strip()]
