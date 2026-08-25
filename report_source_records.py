#!/usr/bin/env python3
"""
report_source_records.py
--------------------------
CLI wrapper around rules_engine.reporting.get_source_records_for_rule() /
get_source_records_for_process() -- ties gre_exceptions rows back to the
actual SOURCE record they came from (the "AUTHZN_CLM_NUM=... -- what's the
real claim row behind this?" question) and writes the result as a CSV,
one row per source record PER RULE it failed (a record failing 3 rules
appears 3 times, each tagged with which rule -- see reporting.py's
docstrings for why this is a deliberate per-rule tie-back, not a merged
one).

Usage
-----
    # Every rule that has current exceptions for one process this run_key:
    python report_source_records.py --process-name ODAG3 --run-key TEST1

    # Narrowed to one project too:
    python report_source_records.py --process-name ODAG3 --run-key TEST1 \\
        --project-name HEALTHSPRING_UM

    # Narrowed to one rule by name (gre_exceptions.rule_nm):
    python report_source_records.py --process-name ODAG3 --run-key TEST1 \\
        --rule-nm ODAG3V22R16

    # Or by numeric rule_id directly (skips process/rule_nm discovery):
    python report_source_records.py --rule-id 481 --run-key TEST1

    # Write to a file instead of stdout:
    python report_source_records.py --process-name ODAG3 --run-key TEST1 \\
        --output odag3_test1_tieback.csv

Every output row carries the source table's own columns PLUS this
finding's context under underscore-prefixed keys (_rule_id, _rule_nm,
_process_name, _project_name, _record_id, _src_key_value, _issue_desc,
_exception_flag) -- see rules_engine/reporting.py::
get_source_records_for_rule()'s docstring for the full contract,
including the "reflects the source table's CURRENT state, not the state
at run time" trade-off.

This is a thin convenience layer -- rules_engine/reporting.py's
get_source_records_for_rule()/get_source_records_for_process() are the
actual library functions; import and call those directly instead of this
script for anything more involved than a one-off local/scheduled export
(further filtering, a different output format, feeding a dashboard, ...).
"""
import argparse
import csv
import sys

from db.connection_factory import build_and_load_connection_factory


def _write_csv(records: list, output_path: str = None) -> None:
    if not records:
        print("No matching records -- nothing to write.", file=sys.stderr)
        return

    # Column order: this finding's context first (the underscore-prefixed
    # keys, in a fixed readable order), then every source column, in the
    # order the source query returned them for the first record -- rather
    # than an alphabetical dict-key dump that would bury _rule_id/_rule_nm
    # at the end.
    context_cols = ["_rule_id", "_rule_nm", "_process_name", "_project_name",
                    "_record_id", "_src_key_value", "_issue_desc", "_exception_flag"]
    source_cols = [c for c in records[0].keys() if c not in context_cols]
    fieldnames = context_cols + source_cols

    handle = open(output_path, "w", newline="") if output_path else sys.stdout
    try:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in records:
            writer.writerow(row)
    finally:
        if output_path:
            handle.close()

    if output_path:
        print(f"Wrote {len(records)} row(s) to {output_path}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Tie gre_exceptions rows back to their source records and export as CSV.",
    )
    parser.add_argument("--run-key", required=True,
                        help="run_key to scope the tie-back to (matches gre_exceptions.run_key).")
    parser.add_argument("--output", default=None,
                        help="CSV file to write. Omit to print to stdout.")

    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--rule-id", type=int, default=None,
                       help="Tie back exceptions for exactly one rule_id.")
    scope.add_argument("--process-name", default=None,
                       help="Tie back exceptions for every rule_id that failed under this "
                            "process_name this run_key (gre_exceptions.process_name).")

    parser.add_argument("--project-name", default=None,
                        help="With --process-name: narrow further to one project_name.")
    parser.add_argument("--rule-nm", default=None,
                        help="With --process-name: narrow further to one rule_nm "
                             "(e.g. ODAG3V22R16) instead of every rule in the process.")

    args = parser.parse_args()

    if args.rule_id is not None and (args.project_name or args.rule_nm):
        parser.error("--project-name/--rule-nm only apply with --process-name, not --rule-id.")

    from shared import config as gre_config
    from rules_engine.reporting import get_source_records_for_rule, get_source_records_for_process

    cf = build_and_load_connection_factory()
    meta_db = gre_config.get_meta_db()
    meta_conn = cf.get(gre_config.get_meta_connection_name())
    if meta_conn is None:
        print("ERROR: metadata connection unavailable.", file=sys.stderr)
        return 1

    if args.rule_id is not None:
        print(f"Tying back rule_id={args.rule_id} run_key={args.run_key!r} ...", file=sys.stderr)
        records = get_source_records_for_rule(cf, meta_conn, meta_db, args.rule_id, args.run_key)
    else:
        print(f"Tying back process_name={args.process_name!r} run_key={args.run_key!r} "
              f"project_name={args.project_name!r} rule_nm={args.rule_nm!r} ...", file=sys.stderr)
        records = get_source_records_for_process(
            cf, meta_conn, meta_db, args.process_name, args.run_key,
            project_name=args.project_name, rule_nm=args.rule_nm,
        )

    _write_csv(records, args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
