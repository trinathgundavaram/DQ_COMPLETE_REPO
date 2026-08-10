"""
utils/metadata_writers.py
----------------------------
Writer for the "something went wrong with the ENGINE" table:

    log_message(...) -> dq_run_logs — general structured run log, and the
        one place a rule/engine-level problem worth triaging gets recorded
        (pass issue_type to mark a row as a triageable issue rather than a
        plain informational log line -- see the module note below).

Writes to `td` (the METADATA connection) — never the source connection
being validated — and uses parameterised `?` placeholders (no manual SQL
escaping). meta_db is resolved lazily per call, not cached at import time,
so DQ_ENV/DQ_META_DB changes after import are respected.

Deliberately separate from dq_exceptions (utils/ids.py builds the keys
for those) — a data finding and an engine problem are never the same row,
see rules_engine/executor.py and DESIGN.md.

dq_run_logs also used to be split into two tables: a general log
(dq_run_logs) and a curated "issues needing triage" table (dq_rule_issues,
written by a since-removed log_issue() function). They were merged because
every real call site wrote near-identical information to both, back to
back, on every single error path (log_issue(...) immediately followed by
log_message(...) with the same rule/exception context) -- and
dq_rule_issues itself was read back in exactly one place (a plain
COUNT(*) for the run summary's issue_count). issue_type/table_name below
are what dq_rule_issues used to carry; a row with issue_type set is
exactly the set of rows dq_rule_issues used to hold. _count_issues() in
rules_engine/engine.py now filters dq_run_logs WHERE issue_type IS NOT NULL
to reproduce that same count.
"""

import logging

from config.env_config import get_meta_db

logger = logging.getLogger(__name__)


def log_message(
    td,
    run_id: str,
    level: str,
    message: str,
    rule_id=None,
    rule_code=None,
    error_code: str = None,
    error_detail: str = None,
    issue_type: str = None,
    table_name: str = None,
    meta_db: str = None,
):
    """
    Insert a structured log entry into dq_run_logs. Never raises.

    issue_type/table_name are optional: set issue_type (e.g.
    'DIALECT_MISMATCH', 'SQL_SYNTAX', 'CONFIG_ERROR') to mark this row as a
    triageable rule/engine issue rather than a plain informational log
    line -- see rules_engine/engine.py::_count_issues(), which counts
    exactly these rows for a run's issue_count. table_name is the rule's
    source table, when known and relevant to the issue.
    """
    meta_db = meta_db or get_meta_db()

    sql = f"""
        INSERT INTO {meta_db}.dq_run_logs
            (run_id, rule_id, rule_code, table_name, log_level, message,
             error_code, error_detail, issue_type, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """
    try:
        cursor = td.cursor()
        cursor.execute(sql, [
            run_id, rule_id, rule_code or None, table_name or None, level, message,
            error_code or None, error_detail or None, issue_type or None,
        ])
        td.commit()
        cursor.close()
    except Exception as exc:
        logger.error("Failed to insert run log: %s", exc)

    py_level = getattr(logging, level.upper(), logging.INFO)
    logger.log(py_level, "[%s] run=%s rule=%s | %s", level, run_id, rule_code, message)
