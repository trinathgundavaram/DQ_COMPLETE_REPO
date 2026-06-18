"""
main.py
-------
Command-line entry point for the Data Quality Framework.

Usage
-----
    python main.py --project CLAIMS --process MEMBER --run-type MONTHLY --run-mode BATCH --batch-id BATCH_2024

    python main.py --project CLAIMS --process MEMBER --run-type DAILY \
                   --run-mode DATE --start 2024-01-01 --end 2024-01-31

    python main.py --project CLAIMS --process MEMBER --run-type MONTHLY \
                   --run-mode FULL --dry-run

Environment variables (set before running)
-------------------------------------------
    DQ_ENV                  DEV | QA | UAT | PROD  (default DEV)
    DQ_META_DB              Override metadata DB name  (optional)
    DQ_META_CONNECTION      Metadata connection name  (default "teradata")
    DQ_CONNECTION_NAMES     Comma-separated source connection names
    DQ_LOG_LEVEL            DEBUG | INFO | WARNING | ERROR  (default INFO)
    DQ_MAX_WORKERS          Thread-pool size  (default 5)
    DQ_STALE_RUN_HOURS      Hours before a RUNNING run is considered stale  (default 4)
    DQ_PREVALIDATE_ABORT    true | false — abort run if pre-validation fails  (default false)

    Per source connection  (prefix = DQ_<NAME>_):
        DQ_<NAME>_TYPE, DQ_<NAME>_HOST, DQ_<NAME>_USER, DQ_<NAME>_PASSWORD, ...
        See db/adapters/ for full list of per-driver env vars.

Exit codes
----------
    0  — run completed (COMPLETED or COMPLETED_WITH_ISSUES)
    1  — run failed (FAILED) or unhandled exception
    2  — argument error
"""

import argparse
import json
import logging
import sys

logger = logging.getLogger(__name__)


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        prog="dq_engine",
        description="Run the Data Quality framework for a given project/process.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --project CLAIMS --process MEMBER --run-type MONTHLY --run-mode FULL
  python main.py --project CLAIMS --process MEMBER --run-type DAILY --run-mode BATCH --batch-id B001
  python main.py --project CLAIMS --process MEMBER --run-type DAILY --run-mode DATE --start 2024-01-01 --end 2024-01-31
  python main.py --project CLAIMS --process MEMBER --run-type MONTHLY --run-mode FULL --dry-run
        """,
    )

    parser.add_argument("--project",   required=True,
                        help="project_name (matches dq_rules.project_name)")
    parser.add_argument("--process",   required=True,
                        help="process_name (matches dq_rules.process_name)")
    parser.add_argument("--run-type",  required=True, dest="run_type",
                        choices=["DAILY", "WEEKLY", "MONTHLY", "ADHOC", "TEST"],
                        help="Run cadence/type")
    parser.add_argument("--run-mode",  required=True, dest="run_mode",
                        choices=["FULL", "BATCH", "DATE"],
                        help="Scope of data to validate")
    parser.add_argument("--batch-id",  default=None, dest="batch_id",
                        help="Batch identifier (required when --run-mode BATCH)")
    parser.add_argument("--start",     default=None, dest="start_date",
                        metavar="YYYY-MM-DD",
                        help="Inclusive start date (required when --run-mode DATE)")
    parser.add_argument("--end",       default=None, dest="end_date",
                        metavar="YYYY-MM-DD",
                        help="Inclusive end date (required when --run-mode DATE)")
    parser.add_argument("--dry-run",   action="store_true", dest="dry_run",
                        help="Validate rules without writing any DB results")
    parser.add_argument("--json-summary", action="store_true", dest="json_summary",
                        help="Print the run summary as JSON to stdout on completion")

    args = parser.parse_args(argv)

    # Cross-argument validation
    if args.run_mode == "BATCH" and not args.batch_id:
        parser.error("--batch-id is required when --run-mode is BATCH")
    if args.run_mode == "DATE" and not (args.start_date and args.end_date):
        parser.error("--start and --end are required when --run-mode is DATE")

    return args


def main(argv=None):
    args = _parse_args(argv)

    # Import here so DQ_LOG_LEVEL (set before invocation) takes effect first
    from core.engine import run_engine

    try:
        result = run_engine(
            project    = args.project,
            process    = args.process,
            run_type   = args.run_type,
            run_mode   = args.run_mode,
            batch_id   = args.batch_id,
            start_date = args.start_date,
            end_date   = args.end_date,
            dry_run    = args.dry_run,
        )

        if args.json_summary:
            print(json.dumps(result, indent=2, default=str))
        else:
            if args.dry_run:
                print(f"\nDRY RUN complete — {result.get('passed', 0)}/{result.get('total', 0)} rules OK.")
                failed = result.get("failed_rules", [])
                if failed:
                    print(f"Failed rules ({len(failed)}):")
                    for code, reason in failed:
                        print(f"  {code}: {reason}")
            else:
                status = result.get("status", "UNKNOWN")
                run_id = result.get("run_id", "")
                print(f"\nRun complete: {run_id}")
                print(f"Status      : {status}")
                print(f"Total rules : {result.get('total_rules', 0)}")
                print(f"Failed/Skip : {result.get('failed_rules', 0)}")
                print(f"Issues      : {result.get('issue_count', 0)}")

        # Exit 1 only on hard FAILED
        status = result.get("status", "")
        sys.exit(1 if status == "FAILED" else 0)

    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
        sys.exit(1)
    except Exception as exc:
        logger.error("Unhandled error: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
