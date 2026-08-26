#!/usr/bin/env python3
"""
run_by_process.py
------------------
CLI wrapper around rules_engine.runner.run_by_process_name() and
sampling.sampling.run_sampling_for_process_name() -- runs every active
rule_group (or sampling config) scoped to one process_name, without
having to write a one-off script each time.

Usage
-----
    # Every active rule_group whose gre_rules.process_name matches:
    python run_by_process.py rules --process-name UNIVERSE_VALIDATION

    # Narrowed to one project too:
    python run_by_process.py rules --process-name UNIVERSE_VALIDATION \\
        --project-name HEALTHSPRING_UM

    # A specific run_key (defaults to today's date, YYYY-MM-DD, if omitted):
    python run_by_process.py rules --process-name UNIVERSE_VALIDATION \\
        --run-key BATCH_2026_08_19

    # Same idea for sampling (gre_sampling_config.process_name):
    python run_by_process.py sampling --process-name WEEKLY_REVIEW_SAMPLE

    # Detailed debug logging (SQL text + row counts, never data/params --
    # see rules_engine/db_ops.py's or sampling/db_ops.py's module docstring
    # for exactly what's logged) is ON BY DEFAULT -- nothing to pass. To
    # quiet it down instead:
    python run_by_process.py rules --process-name UNIVERSE_VALIDATION \
        --log-level INFO

Exit code is 0 if every group/config completed successfully, 1 otherwise
(a rule_group/config that errored, or an unresolved process_name).

This is a thin convenience layer -- rules_engine/runner.py's
run_by_process_name() and sampling/sampling.py's run_sampling_for_process_name()
are the actual library functions; import and call those directly instead
of this script for anything more involved than a one-off local/scheduled
run (custom run_params, catching exceptions your own way, etc.).
"""
import argparse
import sys

from db.connection_factory import build_and_load_connection_factory


def _run_rules(args):
    from rules_engine.config import configure_logging
    from rules_engine.db_ops import default_run_key
    from rules_engine.runner import run_by_process_name

    configure_logging(args.log_level)
    cf = build_and_load_connection_factory()
    # run_by_process_name() itself defaults run_key to today's date (and
    # logs that it did) when None is passed -- default_run_key() here is
    # ONLY so this print line can show the actual value about to be used,
    # not a second, independent default computation.
    run_key = args.run_key or default_run_key()
    print(f"Running rules_engine for process_name={args.process_name!r} "
          f"project_name={args.project_name!r} run_key={run_key!r} ...")

    try:
        outcome = run_by_process_name(
            args.process_name, run_key, cf,
            project_name=args.project_name,
        )
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    exit_code = 0
    for rule_group, summary in outcome["rule_groups"].items():
        print(f"  {rule_group}: {summary['status']} "
              f"succeeded={summary['succeeded']} errored={summary['errored']}")
        if summary["status"] != "COMPLETED":
            exit_code = 1
    return exit_code


def _run_sampling(args):
    from sampling.config import configure_logging
    from sampling.db_ops import default_run_key
    from sampling.sampling import run_sampling_for_process_name

    configure_logging(args.log_level)
    cf = build_and_load_connection_factory()
    run_key = args.run_key or default_run_key()
    print(f"Running sampling for process_name={args.process_name!r} "
          f"project_name={args.project_name!r} run_key={run_key!r} ...")

    try:
        outcome = run_sampling_for_process_name(
            args.process_name, run_key, cf,
            project_name=args.project_name,
        )
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    exit_code = 0
    for config_id, summary in outcome["sampling_configs"].items():
        print(f"  config_id={config_id}: {summary['status']} "
              f"selected={summary['selected']}/{summary['target_volume']}")
        if summary["status"] != "COMPLETED":
            exit_code = 1
    return exit_code


def main():
    parser = argparse.ArgumentParser(
        description="Run the GRE rules engine or sampling engine, scoped to one process_name.",
    )
    parser.add_argument("--log-level", default=None,
                        help="Logging level (e.g. DEBUG, INFO, WARNING) -- SQL statement text and "
                             "row counts only, never result data or bind parameter values, written "
                             "to a file under logs/ (GRE_LOG_DIR), not the console. Defaults to the "
                             "GRE_LOG_LEVEL env var, or DEBUG, if omitted -- detailed logging is ON "
                             "by default; use --log-level INFO (or WARNING) to quiet it down.")
    subparsers = parser.add_subparsers(dest="engine", required=True)

    rules_parser = subparsers.add_parser(
        "rules", help="Run every active rule_group for a process_name (rules_engine).",
    )
    rules_parser.add_argument("--process-name", required=True,
                              help="gre_rules.process_name to run every active rule_group for.")
    rules_parser.add_argument("--project-name", default=None,
                              help="Optionally narrow further to one project_name.")
    rules_parser.add_argument("--run-key", default=None,
                              help="Tracking/idempotency identifier for this run. "
                                   "Defaults to today's date (YYYY-MM-DD) if omitted.")
    rules_parser.set_defaults(func=_run_rules)

    sampling_parser = subparsers.add_parser(
        "sampling", help="Run every active sampling config for a process_name (sampling).",
    )
    sampling_parser.add_argument("--process-name", required=True,
                                 help="gre_sampling_config.process_name to run every active config for.")
    sampling_parser.add_argument("--project-name", default=None,
                                 help="Optionally narrow further to one project_name.")
    sampling_parser.add_argument("--run-key", default=None,
                                 help="Tracking/idempotency identifier for this run. "
                                      "Defaults to today's date (YYYY-MM-DD) if omitted.")
    sampling_parser.set_defaults(func=_run_sampling)

    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
