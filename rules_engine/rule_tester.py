"""
rules_engine/rule_tester.py
-------------------
Single-rule test harness — issue #18.

Lets developers test a DQ rule interactively without writing anything to the
metadata database (dq_rule_execution, dq_exceptions, dq_run_logs, etc.).

Usage
-----
From the command line:

    python -m rules_engine.rule_tester --rule-code MY_RULE_001

From Python:

    from rules_engine.rule_tester import test_rule
    result = test_rule(rule_code="MY_RULE_001", run_mode="FULL")
    print(result)

Environment variables follow the same DQ_* conventions as the main framework.
"""

import argparse
import json
import logging
import os
import sys
import time
from typing import Optional

from config.env_config import get_meta_db
from db.connection_factory import ConnectionFactory
from rules_engine.executor import (
    execute_query,
    validate_sql,
    evaluate_rule,
    _count_total,
    _count_failed,
    _run_table_check,
    _fetch_failed_rows,
    _check_column_exists,
)
from rules_engine.rule_sql import build_query, build_count_query
from utils.db_helpers import resolve_table

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

META_CONNECTION = os.getenv("DQ_META_CONNECTION", "teradata")

# Stub run context used for tester queries — never written to the DB
_DRY_RUN_STUB = {
    "run_id":       "RULE_TESTER",
    "project_name": "TEST",
    "process_name": "TEST",
    "run_type":     "TEST",
    "run_mode":     "FULL",
    "batch_id":     "",
    "dataset_id":   "TEST",
    "start_date":   "1900-01-01",
    "end_date":     "2099-12-31",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def test_rule(
    rule_code: str,
    run_mode: str = "FULL",
    batch_id: str = None,
    start_date: str = None,
    end_date: str = None,
    show_rows: int = 20,
) -> dict:
    """
    Execute a single DQ rule in read-only / test mode.

    No rows are written to any metadata table.  Results are returned as a
    plain dict and printed to stdout.

    Parameters
    ----------
    rule_code  : dq_rules.rule_code to test
    run_mode   : FULL | BATCH | DATE  (controls filter)
    batch_id   : used when run_mode=BATCH
    start_date : YYYY-MM-DD inclusive start (DATE mode)
    end_date   : YYYY-MM-DD inclusive end   (DATE mode)
    show_rows  : max failed-row preview to print (0 = skip)

    Returns
    -------
    dict with keys: rule_code, status, total, failed, passed,
                    failure_pct, elapsed_s, query, failed_rows_preview
    """
    meta_db = get_meta_db()
    cf      = ConnectionFactory()
    cf.load()
    td      = cf.get(META_CONNECTION)

    if td is None:
        raise RuntimeError(
            f"Metadata connection '{META_CONNECTION}' unavailable. "
            "Set DQ_META_CONNECTION and its credentials."
        )

    # ── Load rule ─────────────────────────────────────────────────────────────
    rule = _fetch_rule(td, rule_code, meta_db)
    if rule is None:
        raise ValueError(
            f"Rule '{rule_code}' not found in {meta_db}.dq_rules."
        )

    print(f"\n{'='*60}")
    print(f"  DQ RULE TESTER — {rule_code}")
    print(f"{'='*60}")
    print(f"  Description : {rule.get('rule_description') or '(none)'}")
    print(f"  Table       : {rule.get('src_tbl_nm')}")
    print(f"  Run mode    : {run_mode}")
    print(f"  Threshold % : {rule.get('threshold_pct')}")
    print(f"  Threshold # : {rule.get('threshold_count')}")
    print(f"  Severity    : {rule.get('severity')}")
    print(f"{'='*60}\n")

    run_stub = dict(_DRY_RUN_STUB)
    run_stub.update({
        "run_mode":   run_mode,
        "batch_id":   batch_id   or "",
        "start_date": start_date or "1900-01-01",
        "end_date":   end_date   or "2099-12-31",
    })

    # ── Source connection ──────────────────────────────────────────────────────
    source_system = (rule.get("source_system") or META_CONNECTION).lower()
    db_conn       = cf.get(source_system)

    if db_conn is None:
        raise RuntimeError(
            f"Source connection '{source_system}' unavailable. "
            "Check config/connections.yaml and its DQ_<NAME>_* secrets."
        )

    # ── Source preparation (FileAdapter) ──────────────────────────────────────
    if hasattr(db_conn, "prepare"):
        logger.info("Preparing file source...")
        db_conn.prepare(rule)

    source_type = getattr(db_conn, "source_type", "teradata")

    # ── Build SQL + determine level ────────────────────────────────────────────
    try:
        query, level = build_query(rule, run_stub, source_type)
    except ValueError as exc:
        print(f"  SQL build error: {exc}\n")
        return {"rule_code": rule_code, "status": "ERROR", "error": str(exc)}

    # ── SCHEMA: COLUMN_EXISTS ──────────────────────────────────────────────────
    if level == "SCHEMA":
        print(f"Check type : COLUMN_EXISTS (schema check — no SQL query)\n")
        start = time.time()
        try:
            exists  = _check_column_exists(db_conn, source_type, rule)
            elapsed = round(time.time() - start, 4)
            status  = evaluate_rule(
                total=1, failed=0 if exists else 1,
                threshold_pct=rule.get("threshold_pct"),
                threshold_count=rule.get("threshold_count"),
                severity=rule.get("severity"),
                require_rows=False,
                threshold_operator=rule.get("threshold_operator", "OR"),
            )
            print(f"{'─'*40}")
            print(f"  STATUS      : {status}")
            print(f"  Column found: {exists}")
            print(f"  Elapsed     : {elapsed}s")
            print(f"{'─'*40}\n")
            print("NOTE: No data was written to any DQ metadata table.")
            return {
                "rule_code":   rule_code,
                "status":      status,
                "level":       "SCHEMA",
                "column_exists": exists,
                "elapsed_s":   elapsed,
            }
        except Exception as exc:
            print(f"  COLUMN_EXISTS error: {exc}")
            return {"rule_code": rule_code, "status": "ERROR", "error": str(exc)}

    print(f"Level : {level}")
    print("Rule SQL:")
    print(f"  {query.strip()}\n")

    # ── SQL validation ─────────────────────────────────────────────────────────
    print("Validating SQL syntax...")
    try:
        validate_sql(db_conn, query)
        print("  SQL: OK\n")
    except Exception as exc:
        print(f"  SQL: INVALID — {exc}\n")
        return {
            "rule_code": rule_code,
            "status":    "ERROR",
            "error":     f"SQL syntax invalid: {exc}",
        }

    # ── Execute ────────────────────────────────────────────────────────────────
    start = time.time()

    if level == "TABLE":
        print("Running TABLE-level check...")
        row_count   = _run_table_check(db_conn, query)
        failed      = 1 if row_count > 0 else 0
        total       = 1
        passed      = 1 - failed
        failure_pct = float(failed * 100)
        pass_pct    = float(passed * 100)
        print(f"  Violation rows returned: {row_count} ({'FAIL' if row_count else 'PASS'})\n")
    else:
        count_query = build_count_query(rule, run_stub)
        print("Counting total records...")
        total = _count_total(db_conn, count_query)
        print(f"  Total in scope : {total:,}\n")

        print("Counting failed records...")
        failed = _count_failed(db_conn, query)
        print(f"  Failed records : {failed:,}\n")

        passed      = max(total - failed, 0)
        failure_pct = round((failed / total * 100), 6) if total else 0.0
        pass_pct    = round(100.0 - failure_pct, 6)

    elapsed = round(time.time() - start, 4)

    status = evaluate_rule(
        total, failed,
        rule.get("threshold_pct"),
        rule.get("threshold_count"),
        rule.get("severity"),
        require_rows=bool(rule.get("require_rows", 0)) if level == "ROW" else False,
        threshold_operator=rule.get("threshold_operator", "OR"),
    )

    # ── Summary ────────────────────────────────────────────────────────────────
    print(f"{'─'*40}")
    print(f"  STATUS      : {status}")
    print(f"  Total       : {total:,}")
    print(f"  Failed      : {failed:,}  ({failure_pct:.4f}%)")
    print(f"  Passed      : {passed:,}  ({pass_pct:.4f}%)")
    print(f"  Elapsed     : {elapsed}s")
    print(f"{'─'*40}\n")

    # ── Failed-row preview (ROW level only) ───────────────────────────────────
    failed_preview = []
    if level == "ROW" and failed > 0 and show_rows > 0:
        print(f"Failed row preview (first {show_rows}):")
        try:
            rows = _fetch_failed_rows(db_conn, query)
            failed_preview = rows[:show_rows]
            for i, r in enumerate(failed_preview, 1):
                print(f"  [{i:4d}] {json.dumps(r, default=str)}")
        except Exception as exc:
            print(f"  Could not fetch rows: {exc}")
        print()

    result = {
        "rule_code":           rule_code,
        "level":               level,
        "status":              status,
        "total":               total,
        "failed":              failed,
        "passed":              passed,
        "failure_pct":         failure_pct,
        "pass_pct":            pass_pct,
        "elapsed_s":           elapsed,
        "query":               query,
        "failed_rows_preview": failed_preview,
    }

    print("NOTE: No data was written to any DQ metadata table.")
    return result


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _fetch_rule(td, rule_code: str, meta_db: str) -> Optional[dict]:
    """Load a single rule row from dq_rules by rule_code."""
    # rule_code is CLI-supplied (see _cli() below) — bound as a param
    # rather than interpolated.
    rows = execute_query(
        td,
        f"SELECT * FROM {meta_db}.dq_rules WHERE rule_code = ?",
        [rule_code],
    )
    return rows[0] if rows else None


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _cli():
    parser = argparse.ArgumentParser(
        description="Test a single DQ rule without writing to metadata tables.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m rules_engine.rule_tester --rule-code MY_RULE_001
  python -m rules_engine.rule_tester --rule-code MY_RULE_001 --run-mode DATE --start 2024-01-01 --end 2024-01-31
  python -m rules_engine.rule_tester --rule-code MY_RULE_001 --run-mode BATCH --batch-id BATCH_2024
  python -m rules_engine.rule_tester --rule-code MY_RULE_001 --show-rows 50
        """,
    )
    parser.add_argument("--rule-code", required=True,
                        help="rule_code to test (from dq_rules)")
    parser.add_argument("--run-mode",  default="FULL",
                        choices=["FULL", "BATCH", "DATE"],
                        help="FULL (default), BATCH, or DATE")
    parser.add_argument("--batch-id",  default=None,
                        help="Batch identifier (BATCH mode)")
    parser.add_argument("--start",     default=None, dest="start_date",
                        metavar="YYYY-MM-DD",
                        help="Inclusive start date (DATE mode)")
    parser.add_argument("--end",       default=None, dest="end_date",
                        metavar="YYYY-MM-DD",
                        help="Inclusive end date (DATE mode)")
    parser.add_argument("--show-rows", type=int, default=20,
                        metavar="N",
                        help="Number of failed rows to preview (default 20, 0 = skip)")

    args = parser.parse_args()

    try:
        result = test_rule(
            rule_code  = args.rule_code,
            run_mode   = args.run_mode,
            batch_id   = args.batch_id,
            start_date = args.start_date,
            end_date   = args.end_date,
            show_rows  = args.show_rows,
        )
        sys.exit(0 if result.get("status") in ("PASS",) else 1)
    except Exception as exc:
        logger.error("rule_tester failed: %s", exc, exc_info=True)
        sys.exit(2)


if __name__ == "__main__":
    _cli()
