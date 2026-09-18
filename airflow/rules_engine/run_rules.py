#!/usr/bin/env python3
"""
run_rules.py
------------
Rules-only wrapper around rules_engine.runner.run_by_scope().

This is a trimmed copy of the original repo's run_by_process.py, kept
inside this rules_engine/ folder specifically so it carries NO
sampling-engine code at all -- everything under this folder (aside from
the sibling gre_rules_dag.py one level up) packages the rules framework
plus its Airflow bridge, in one place. See AIRFLOW_DEPLOYMENT.md in this
same folder. Behavior for the "rules" path is otherwise identical to
run_by_process.py's "rules" subcommand: same env vars, same
run_by_scope() call, same output shape.

Lives alongside rules_engine/'s own files (config.py, runner.py, ...) and
db/ (this folder's own trimmed connection_factory.py) rather than
one level up, so the whole framework + its Airflow glue is one
self-contained folder; only gre_rules_dag.py stays outside it, since
Airflow needs an actual DAG file to discover. See the sys.path bootstrap
below for how imports resolve given that nesting.

Two ways to use it
-------------------
1. As a library function -- this is what gre_rules_task.py (the Airflow
   bridge module, a sibling of this file) calls. The bridge sets every
   env var run_by_scope()/db/connection_factory.py need BEFORE calling
   run_rules(), then calls it directly in-process -- no subprocess, no
   CLI parsing:

       from run_rules import run_rules

       outcome, exit_code = run_rules(
           project_name="HEALTHSPRING_UM",
           process_name="UNIVERSE_VALIDATION",
           run_key="2026-09-18",
           run_params={"year": "2026", "month": "9"},
       )
       if exit_code != 0:
           raise RuntimeError("GRE rules run failed")

2. As a CLI, unchanged from run_by_process.py's "rules" subcommand, for
   local/manual runs against this folder's copy of the framework:

       python run_rules.py --process-name UNIVERSE_VALIDATION \\
           --run-key 2026-09-18 --param year=2026 --param month=9

Env vars
--------
Reads exactly what rules_engine/config.py and db/connection_factory.py
already read (GRE_ENVIRONMENT, GRE_META_DB, GRE_META_CONNECTION,
GRE_LOG_LEVEL, GRE_LOG_DIR, TERADATA_HOST/USER/PASSWORD/LOGMECH,
POSTGRES_HOST/PORT/DATABASE/USER/PASSWORD/SSLMODE, ...) -- nothing new is
invented here. In Airflow, gre_rules_task.py populates these from an
Airflow Connection (credentials) and Airflow Variable (everything else)
before calling run_rules(); locally, source a dev.env-style file into
the shell (or use rules_engine/config.py's own dev.env auto-load) before
invoking the CLI.
"""
import argparse
import os
import sys

# --- sys.path bootstrap ----------------------------------------------------
# This file lives INSIDE rules_engine/ alongside db/ (this folder's own
# connection_factory.py), so "from db.connection_factory import ..."
# below needs rules_engine/ itself on sys.path (db is a direct child of
# it). But rules_engine/config.py, rules_engine/db_ops.py, and
# rules_engine/runner.py -- imported further down as "rules_engine.X" --
# need rules_engine/'s PARENT (airflow/) on sys.path instead, since
# "rules_engine" has to resolve as a top-level package. Both are added
# here, unconditionally, so this file works the same whether it's run
# directly (`python run_rules.py`), imported by gre_rules_task.py, or
# imported from anywhere else -- never relying on the caller to have set
# sys.path up correctly first.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))       # .../airflow/rules_engine
_PARENT_DIR = os.path.dirname(_THIS_DIR)                     # .../airflow
for _p in (_THIS_DIR, _PARENT_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from db.connection_factory import build_and_load_connection_factory


def _parse_params(pairs) -> dict:
    """
    ["year=2026", "month=8"] -> {"year": "2026", "month": "8"}. Splits on
    the FIRST "=" only, so a value that itself contains "=" is preserved
    intact. Raises ValueError with the offending pair on anything with no
    "=" at all. Accepts either a list of "KEY=VALUE" strings (CLI usage)
    or a dict already (library usage passes dicts straight through).
    """
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
    """
    Library entry point -- the function the Airflow bridge module calls
    directly (no subprocess). Mirrors run_by_process.py's _run_rules()
    exactly, minus the argparse.Namespace plumbing and print()s replaced
    with return values a caller can act on programmatically.

    Every one of project_name/process_name/rule_group/rule_variant/
    run_params/extra_filters/text_params means exactly what it means in
    rules_engine.runner.run_by_scope() -- see that function's docstring
    for the full scoping rules (nothing here changes them).

    Parameters
    ----------
    log_level : defaults to "ERROR" in this package (rules_engine/config.py's
         own default, unchanged, is "DEBUG" -- detailed SQL/row-count
         logging). Pass None explicitly to fall back to the GRE_LOG_LEVEL
         env var / rules_engine's own DEBUG default instead, or pass any
         other level ("INFO", "WARNING", ...) to opt back into more detail.
    cf : an already-built ConnectionFactory, or None to build+load one
         here via db.connection_factory.build_and_load_connection_factory()
         (reads TERADATA_HOST/USER/PASSWORD/..., POSTGRES_* from the
         process environment -- the Airflow bridge module sets these from
         an Airflow Connection before calling run_rules()).

    Returns
    -------
    (outcome, exit_code) -- outcome is run_by_scope()'s own
    {"rule_groups": {...}} dict (or None if a ValueError short-circuited
    before it could run); exit_code is 0 if every rule_group in scope
    COMPLETED, 1 otherwise (including the "nothing to scope" and
    "unresolved scope" ValueError cases).
    """
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

    # run_by_scope() itself defaults run_key to today's date (and logs
    # that it did) when None is passed -- default_run_key() here is ONLY
    # so this print line can show the actual value about to be used, not
    # a second, independent default computation.
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


# ---------------------------------------------------------------------------
# CLI (unchanged behavior from run_by_process.py's "rules" subcommand)
# ---------------------------------------------------------------------------

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
