import argparse
import os
import sys


_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PARENT_DIR = os.path.dirname(_THIS_DIR)
for _p in (_THIS_DIR, _PARENT_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from db.connection_factory import build_and_load_connection_factory


def _parse_params(pairs) -> dict:
    if pairs is None:
        return {}
    if isinstance(pairs, dict):
        return dict(pairs)
    params = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"--param must be KEY=VALUE, got: {pair!r}")
        key, value = pair.split("=", 1)
        params[key] = value
    return params


def run_rules(
    project_name: str = None,
    process_name: str = None,
    rule_group: str = None,
    rule_variant: str = None,
    run_key: str = None,
    run_params=None,
    extra_filters=None,
    text_params=None,
    log_level: str = "ERROR",
    cf=None,
):
    from rules_engine.config import configure_logging
    from rules_engine.db_ops import default_run_key
    from rules_engine.runner import run_by_scope

    run_params = _parse_params(run_params)
    extra_filters = _parse_params(extra_filters)
    text_params = _parse_params(text_params)

    if not (project_name or process_name or rule_group):
        print("ERROR: pass at least one of project_name, process_name, rule_group.",
              file=sys.stderr)
        return None, 1

    configure_logging(log_level)
    owns_cf = cf is None
    if owns_cf:
        cf = build_and_load_connection_factory()


    resolved_run_key = run_key or default_run_key()
    print(f"Running rules_engine for project_name={project_name!r} "
          f"process_name={process_name!r} rule_group={rule_group!r} "
          f"rule_variant={rule_variant!r} run_key={resolved_run_key!r} "
          f"run_params={run_params!r} extra_filters={extra_filters!r} "
          f"text_params={text_params!r} ...")
    if not rule_variant:
        print("  (no rule_variant passed -- every active rule in scope runs, "
              "regardless of its own rule_variant)")

    try:
        outcome = run_by_scope(
            run_key=resolved_run_key, cf=cf,
            project_name=project_name,
            process_name=process_name,
            rule_group=rule_group,
            rule_variant=rule_variant,
            run_params=run_params,
            extra_filters=extra_filters,
            text_params=text_params,
        )
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return None, 1
    finally:
        if owns_cf:
            cf.close_all()

    exit_code = 0
    for group_name, summary in outcome["rule_groups"].items():
        print(f"  {group_name}: {summary['status']} "
              f"succeeded={summary['succeeded']} errored={summary['errored']}")
        if summary["status"] != "COMPLETED":
            exit_code = 1
    return outcome, exit_code


def main():
    parser = argparse.ArgumentParser(
        description="Run the GRE rules engine, scoped to one project/process/rule_group "
                    "(rules-only copy of run_by_process.py -- see this folder's AIRFLOW_DEPLOYMENT.md).",
    )
    parser.add_argument("--log-level", default="ERROR",
                        help="Logging level (e.g. DEBUG, INFO, WARNING, ERROR). Defaults to "
                             "ERROR in this package (errors only) -- pass e.g. --log-level DEBUG "
                             "for the detailed SQL/row-count logging rules_engine/ supports.")
    parser.add_argument("--project-name", default=None, help="gre_rules.project_name.")
    parser.add_argument("--process-name", default=None, help="gre_rules.process_name.")
    parser.add_argument("--rule-group", default=None, help="Run exactly this one gre_rules.rule_group.")
    parser.add_argument("--rule-variant", default=None,
                        help="Narrow further to this rule_variant (plus universal/NULL rules). "
                             "Omitting runs every active rule in scope regardless of rule_variant.")
    parser.add_argument("--run-key", default=None,
                        help="Tracking/idempotency identifier. Defaults to today's date (YYYY-MM-DD).")
    parser.add_argument("--param", action="append", default=[], metavar="KEY=VALUE",
                        help="run_params entry, repeatable (e.g. --param year=2026).")
    parser.add_argument("--filter", action="append", default=[], metavar="KEY=VALUE",
                        help="Ad-hoc extra_filters entry, repeatable.")
    parser.add_argument("--text-param", action="append", default=[], metavar="KEY=VALUE",
                        help="text_params entry (never folded into the total-count WHERE), repeatable.")
    args = parser.parse_args()

    try:
        run_params = _parse_params(args.param)
        extra_filters = _parse_params(args.filter)
        text_params = _parse_params(args.text_param)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    _, exit_code = run_rules(
        project_name=args.project_name,
        process_name=args.process_name,
        rule_group=args.rule_group,
        rule_variant=args.rule_variant,
        run_key=args.run_key,
        run_params=run_params,
        extra_filters=extra_filters,
        text_params=text_params,
        log_level=args.log_level,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
