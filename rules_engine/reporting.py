"""
rules_engine/reporting.py
--------------------
Everything that tells a human what a run found: live notifications and the
static per-run audit report. Both read the same result tables
(dq_rule_execution, dq_exceptions, dq_run_control) and both exist to answer
"what happened," just on different timescales — kept in one file since
they're the same "reporting" concern, not two.

Notifications — notify_run_completion()
------------------------------------------
Every finding is one of exactly two classes, NEVER sent on the same channel:

  DATA_VIOLATION — a case has a data problem. Some recipients see every
                    one; others only the subset they can act on directly
                    (dq_rules.business_correctable = 1).
  ENGINE_FAILURE — the ENGINE failed to evaluate a rule (SQL error, dialect
                    mismatch, missing table, connection timeout) — never a
                    data finding, never forwarded to a compliance/business
                    audience where it could be mistaken for "checked and
                    found nothing."

Routing is entirely data-driven via dq_notification_routes (project_name,
process_name, finding_class, audience, channel_type, destination,
business_correctable_only). This module does not know or care what any
project calls its audiences ("ROAR", "ops", "on-call") — audience is a
human-readable label carried through purely for the humans managing the
routing table; the code only branches on finding_class and
business_correctable_only. No routes configured -> falls back to the
single global channel (utils.alert.send_alert()); the message body always
states its finding class so even the fallback case is unambiguous.

Static audit report — generate_report()
-------------------------------------------
A self-contained, timestamped HTML snapshot of one run — "the artifact
you'd hand an auditor or regulator a year later, proving what was flagged
and when, that doesn't change if someone edits a dashboard filter
afterward." Content-addressed (filename embeds a SHA-256 of its own
content) and never regenerated in place — a later disposition shows up as
a new dq_exception_dispositions row and a new report run, never an edit
to this file. Generated automatically at end-of-run when DQ_AUTO_AUDIT_REPORT=true,
or on demand via `python -m rules_engine.reporting --run-id <run_id>`.

Public API
----------
notify_run_completion(td, run, meta_db, total_rules, data_issue_rules,
                       engine_issue_rules, suppressed_rules, final_status)
generate_report(td, meta_db, run_id, out_dir) -> str (path written)
"""

import argparse
import hashlib
import html
import logging
import os
from datetime import datetime

from utils.alert import send_alert, send_alert_to

logger = logging.getLogger(__name__)


# =============================================================================
# Notifications
# =============================================================================

def notify_run_completion(
    td, run: dict, meta_db: str, total_rules: int,
    data_issue_rules: int, engine_issue_rules: int,
    suppressed_rules: int, final_status: str,
):
    """Send the two notification classes for a completed run — never merged."""
    if data_issue_rules > 0:
        _notify_data_violations(td, run, meta_db, data_issue_rules)

    if engine_issue_rules > 0:
        _notify_engine_failure(td, run, meta_db, engine_issue_rules)

    if data_issue_rules == 0 and engine_issue_rules == 0:
        send_alert(
            f"[RUN OK] {run['project_name']}/{run['process_name']} ({run['run_type']}) "
            f"completed clean — {total_rules} rules, no findings.\nRun ID: {run['run_id']}",
            "INFO",
        )


def _load_routes(td, project: str, process: str, finding_class: str, meta_db: str) -> list:
    """
    Active dq_notification_routes rows for this finding_class that apply to
    (project, process). Most specific match wins per audience label: exact
    project+process > project-only wildcard > global wildcard.
    """
    from rules_engine.executor import execute_query

    try:
        rows = execute_query(td, f"""
            SELECT route_id, project_name, process_name, finding_class, audience,
                   channel_type, destination, business_correctable_only
            FROM {meta_db}.dq_notification_routes
            WHERE finding_class = ?
              AND active_flag = 1
              AND (project_name IS NULL OR project_name = ?)
              AND (process_name IS NULL OR process_name = ?)
        """, [finding_class, project, process])
    except Exception as exc:
        logger.warning("Could not load dq_notification_routes (%s) — using fallback channel: %s",
                       finding_class, exc)
        return []

    def specificity(r):
        return (r["project_name"] is not None) + (r["process_name"] is not None)

    best = {}
    for r in rows:
        key = (r["audience"], bool(r["business_correctable_only"]))
        if key not in best or specificity(r) > specificity(best[key]):
            best[key] = r
    return list(best.values())


def _dispatch(routes: list, message: str, level: str, fallback_message: str = None):
    """Send to every route, or the global fallback channel if there are none."""
    if routes:
        for r in routes:
            send_alert_to(message, level, r["channel_type"], r["destination"])
    else:
        send_alert(fallback_message or message, level)


def _business_correctable_summary(td, run: dict, meta_db: str) -> dict:
    """Count this run's exceptions whose rule is flagged business_correctable=1."""
    from rules_engine.executor import execute_query

    try:
        rows = execute_query(td, f"""
            SELECT e.rule_code, COUNT(*) AS exception_count
            FROM {meta_db}.dq_exceptions e
            JOIN {meta_db}.dq_rules r ON r.rule_id = e.rule_id
            WHERE e.run_id = ? AND r.business_correctable = 1
            GROUP BY e.rule_code
            ORDER BY exception_count DESC
        """, [run["run_id"]])
    except Exception as exc:
        logger.warning("business_correctable summary query failed: %s", exc)
        return {"rows": [], "total": 0}
    return {"rows": rows, "total": sum(int(r["exception_count"]) for r in rows)}


def _notify_data_violations(td, run: dict, meta_db: str, data_issue_rules: int):
    project, process, run_id = run["project_name"], run["process_name"], run["run_id"]

    routes = _load_routes(td, project, process, "DATA_VIOLATION", meta_db)
    full_routes   = [r for r in routes if not r["business_correctable_only"]]
    subset_routes = [r for r in routes if r["business_correctable_only"]]

    full_msg = (
        f"[DATA VIOLATION] {project}/{process} ({run['run_type']}) found data issues\n\n"
        f"Run ID: {run_id}\nRules with data findings: {data_issue_rules}\n"
        f"Full list: DQ dashboard, filter run_id={run_id}."
    )
    _dispatch(full_routes, full_msg, "WARN", fallback_message=full_msg)

    if subset_routes:
        subset = _business_correctable_summary(td, run, meta_db)
        if subset["total"] > 0:
            lines = "\n".join(f"  {r['rule_code']}: {r['exception_count']}" for r in subset["rows"])
            subset_msg = f"[DATA VIOLATION - ACTIONABLE SUBSET] {project}/{process}\n\nRun ID: {run_id}\n{lines}\n"
            _dispatch(subset_routes, subset_msg, "WARN")


def _notify_engine_failure(td, run: dict, meta_db: str, engine_issue_rules: int):
    project, process, run_id = run["project_name"], run["process_name"], run["run_id"]

    from rules_engine.executor import execute_query
    try:
        details = execute_query(td, f"""
            SELECT rule_code, status, COUNT(*) AS n
            FROM {meta_db}.dq_rule_execution
            WHERE run_id = ? AND status IN ('ERROR', 'SKIP')
            GROUP BY rule_code, status
            ORDER BY status, rule_code
        """, [run_id])
    except Exception:
        details = []
    detail_lines = "\n".join(f"  {d['rule_code']} — {d['status']}" for d in details) or "  (see dq_rule_issues)"

    msg = (
        f"[ENGINE FAILURE] {project}/{process} ({run['run_type']}) — rule/engine errors, "
        f"needs rule-owner attention\n\nRun ID: {run_id}\n"
        f"Rules that could NOT be evaluated (SQL error, dialect mismatch, missing table, "
        f"connection failure): {engine_issue_rules}\n\n{detail_lines}\n\n"
        f"This is NOT a data-quality finding — do not forward to a compliance/business audience."
    )
    routes = _load_routes(td, project, process, "ENGINE_FAILURE", meta_db)
    _dispatch(routes, msg, "ERROR", fallback_message=msg)


# =============================================================================
# Static audit report
# =============================================================================

_REPORT_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>DQ Audit Report — {run_id}</title>
<style>
  body {{ font-family: -apple-system, Segoe UI, Arial, sans-serif; margin: 2rem; color: #1a1a1a; }}
  h1 {{ font-size: 1.4rem; }}
  .meta {{ color: #555; margin-bottom: 1.5rem; }}
  .immutable-notice {{ background: #fff3cd; border: 1px solid #ffe69c; padding: 0.75rem 1rem;
                        border-radius: 4px; margin-bottom: 1.5rem; font-size: 0.9rem; }}
  table {{ border-collapse: collapse; width: 100%; margin-bottom: 2rem; font-size: 0.85rem; }}
  th, td {{ border: 1px solid #ddd; padding: 6px 8px; text-align: left; }}
  th {{ background: #f3f3f3; position: sticky; top: 0; }}
  tr:nth-child(even) {{ background: #fafafa; }}
  .status-FAIL, .status-ERROR {{ color: #b30000; font-weight: 600; }}
  .status-WARN {{ color: #b36b00; }}
  .status-PASS {{ color: #157a15; }}
  .status-SKIP {{ color: #666; }}
  .footer {{ color: #888; font-size: 0.75rem; margin-top: 3rem; border-top: 1px solid #eee; padding-top: 1rem; }}
</style>
</head>
<body>
<h1>Data Quality Audit Report</h1>
<div class="meta">
  Run ID: <strong>{run_id}</strong><br>
  Project / Process: {project} / {process}<br>
  Run type / mode: {run_type} / {run_mode}<br>
  Report generated: {generated_at} UTC<br>
  Content hash (SHA-256): <code>{content_hash}</code>
</div>
<div class="immutable-notice">
  This is a point-in-time, immutable snapshot. Findings shown here reflect
  dq_exceptions and dq_rule_execution as of the generation timestamp above.
  Any later case disposition (waived/resolved/corrected) is recorded as a
  NEW row in dq_exception_dispositions and will NOT alter this file — see the
  live dashboard for current disposition status.
</div>

<h2>Rule Execution Summary ({rule_count} rules)</h2>
{rule_table}

<h2>Data Exceptions ({exception_count} rows)</h2>
{exception_table}

<div class="footer">
  Generated by rules_engine/reporting.py. This file's name embeds a SHA-256 hash
  of its own content — any alteration after generation is detectable.
</div>
</body>
</html>
"""


def generate_report(td, meta_db: str, run_id: str, out_dir: str) -> str:
    """Build and write the static audit report for one run_id. Returns the path written."""
    from rules_engine.executor import execute_query

    run_rows = execute_query(td, f"""
        SELECT rc.*, s.project_name, s.process_name
        FROM {meta_db}.dq_run_control rc
        JOIN {meta_db}.dq_scope s ON s.scope_id = rc.scope_id
        WHERE rc.run_id = ?
    """, [run_id])
    if not run_rows:
        raise ValueError(f"No dq_run_control row found for run_id={run_id}")
    run_row = run_rows[0]

    rule_rows = execute_query(td, f"""
        SELECT rule_code, status, total_records, failed_records, failure_pct,
               severity, execution_time, run_timestamp
        FROM {meta_db}.dq_rule_execution WHERE run_id = ? ORDER BY rule_code
    """, [run_id])
    exception_rows = execute_query(td, f"""
        SELECT e.rule_code, e.table_name, e.primary_key_str, e.created_at
        FROM {meta_db}.dq_exceptions e WHERE e.run_id = ? ORDER BY e.rule_code, e.created_at
    """, [run_id])

    rule_table = _rows_to_html_table(
        rule_rows,
        ["rule_code", "status", "total_records", "failed_records", "failure_pct",
         "severity", "execution_time", "run_timestamp"],
        status_col="status",
    )
    exception_table = _rows_to_html_table(
        exception_rows, ["rule_code", "table_name", "primary_key_str", "created_at"],
    )

    body = _REPORT_TEMPLATE.format(
        run_id=html.escape(run_id),
        project=html.escape(str(run_row.get("project_name", ""))),
        process=html.escape(str(run_row.get("process_name", ""))),
        run_type=html.escape(str(run_row.get("run_type", ""))),
        run_mode=html.escape(str(run_row.get("run_mode", ""))),
        generated_at=datetime.utcnow().isoformat(timespec="seconds"),
        content_hash="{content_hash}",   # filled in after hashing below
        rule_count=len(rule_rows),
        exception_count=len(exception_rows),
        rule_table=rule_table,
        exception_table=exception_table,
    )

    content_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()[:16]
    body = body.replace("{content_hash}", content_hash)

    os.makedirs(out_dir, exist_ok=True)
    safe_run_id = "".join(c if c.isalnum() or c in "_-" else "_" for c in run_id)
    path = os.path.join(out_dir, f"audit_report_{safe_run_id}_{content_hash}.html")

    if os.path.exists(path):
        logger.info("Audit report already exists (identical content) — not rewriting: %s", path)
        return path

    with open(path, "w", encoding="utf-8") as f:
        f.write(body)
    logger.info("Audit report written: %s", path)
    return path


def _rows_to_html_table(rows: list, columns: list, status_col: str = None) -> str:
    if not rows:
        return "<p><em>(no rows)</em></p>"
    head = "".join(f"<th>{html.escape(c)}</th>" for c in columns)
    body_rows = []
    for r in rows:
        cells = []
        for c in columns:
            val = html.escape(str(r.get(c, "")))
            cls = f' class="status-{html.escape(str(r.get(c, "")))}"' if status_col and c == status_col else ""
            cells.append(f"<td{cls}>{val}</td>")
        body_rows.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"


def _cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--out-dir", default=os.getenv("DQ_AUDIT_REPORT_DIR", "./dq_audit_reports"))
    args = parser.parse_args()

    from config.env_config import get_meta_db
    from db.connection_factory import ConnectionFactory

    meta_db = get_meta_db()
    cf = ConnectionFactory()
    cf.load()
    td = cf.get(os.getenv("DQ_META_CONNECTION", "teradata"))
    try:
        print(generate_report(td, meta_db, args.run_id, args.out_dir))
    finally:
        cf.close_all()


if __name__ == "__main__":
    _cli()
