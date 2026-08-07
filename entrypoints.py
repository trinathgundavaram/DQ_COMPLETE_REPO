"""
entrypoints.py
-----------------
Trigger/orchestration-specific wrappers — one function or class per
execution context, all calling rules_engine.engine.run_engine() (the same function
rules_engine/main.py's CLI calls). Nothing in rules_engine/, db/, sampling/, or utils/ imports
anything from this file — the dependency only goes one way. That's what
"only the trigger differs" means in code: delete any one function below
and the engine still works everywhere else. Adding a 4th execution context
means adding one more function here, not a new file.

  lambda_handler(event, context)          — AWS Lambda
  glue_main()                              — AWS Glue (Python Shell or ETL-as-wrapper)
  run_dq_engine(...) / DataQualityEngineOperator — Airflow (plain callable + optional operator)
  cron_run_once(argv) / cron_run_scheduler(path) — local Python server (crontab or APScheduler)

This file is where the two frameworks in this repo (rules_engine/ = DQ rules
engine, sampling/ = stratified sampling) meet, but only at the
orchestration level: cron_run_scheduler() can optionally chain a
sampling.engine.run_stratified_sampling() call after a rules-engine run
finishes (see _run_cron_sampling()), for deployments that want both on the
same schedule. Neither framework imports the other; this file is the only
place a single call chain touches both.

Credentials/connections are always configured via the same DQ_* env vars
regardless of which function below is the trigger.
"""

import argparse
import json
import logging
import os
import sys
from datetime import date, timedelta

logger = logging.getLogger(__name__)


# =============================================================================
# AWS Lambda
# =============================================================================

def lambda_handler(event, context):
    """
    Deploy this repo + `entrypoints.lambda_handler` as the Lambda handler.
    Trigger via EventBridge Scheduler / Step Functions with an input event:
        {"project": "MY_PROJECT", "process": "MY_PROCESS", "run_type": "WEEKLY",
         "run_mode": "DATE", "start_date": "2026-08-01", "end_date": "2026-08-07"}
    `event` may be the payload directly, or wrapped under "input"/"detail"
    (Step Functions / EventBridge rule conventions) — both are handled.
    """
    from rules_engine.engine import run_engine

    payload = event.get("detail", event.get("input", event)) if isinstance(event, dict) else event
    if isinstance(payload, str):
        payload = json.loads(payload)

    missing = [k for k in ("project", "process", "run_type", "run_mode") if k not in payload]
    if missing:
        raise ValueError(f"Lambda event missing required field(s): {missing}")

    result = run_engine(
        project=payload["project"], process=payload["process"],
        run_type=payload["run_type"], run_mode=payload["run_mode"],
        batch_id=payload.get("batch_id"), start_date=payload.get("start_date"),
        end_date=payload.get("end_date"), dry_run=payload.get("dry_run", False),
    )
    logger.info("Lambda DQ run complete: %s", result.get("run_id"))
    return {"statusCode": 200 if result.get("status") != "FAILED" else 500,
            "body": json.dumps(result, default=str)}


# =============================================================================
# AWS Glue (Python Shell, or ETL job used purely as a Python-callable wrapper —
# the engine itself never uses Spark; it's a regular DB-API process)
# =============================================================================

def glue_main():
    """
    Job arguments (Glue job definition):
        --project MY_PROJECT --process MY_PROCESS --run-type WEEKLY
        --run-mode DATE --start-date 2026-08-01 --end-date 2026-08-07
    Falls back to plain --key value parsing when the awsglue library isn't
    installed (e.g. local testing), so this function works outside Glue too.
    """
    try:
        from awsglue.utils import getResolvedOptions
        args = getResolvedOptions(sys.argv, ["project", "process", "run-type", "run-mode"])
        optional = args
    except ImportError:
        args = optional = _parse_plain_args(sys.argv[1:])

    from rules_engine.engine import run_engine

    result = run_engine(
        project=args["project"], process=args["process"],
        run_type=args["run-type"], run_mode=args["run-mode"],
        batch_id=optional.get("batch-id"), start_date=optional.get("start-date"),
        end_date=optional.get("end-date"),
        dry_run=str(optional.get("dry-run", "false")).lower() == "true",
    )
    logger.info("Glue DQ run complete: %s (%s)", result.get("run_id"), result.get("status"))
    if result.get("status") == "FAILED":
        sys.exit(1)


def _parse_plain_args(argv) -> dict:
    """Minimal --key value parser mirroring AWS Glue's getResolvedOptions shape."""
    out, i = {}, 0
    while i < len(argv):
        if argv[i].startswith("--"):
            key = argv[i][2:]
            val = argv[i + 1] if i + 1 < len(argv) and not argv[i + 1].startswith("--") else "true"
            out[key] = val
            i += 2
        else:
            i += 1
    return out


# =============================================================================
# Airflow
# =============================================================================

def run_dq_engine(
    project: str, process: str, run_type: str, run_mode: str,
    batch_id: str = None, start_date: str = None, end_date: str = None,
    dry_run: bool = False,
    **_airflow_context,   # absorbs Airflow's injected context kwargs when called via op_kwargs
) -> dict:
    """
    Plain callable for PythonOperator / TaskFlow @task — no Airflow-version
    coupling. Raises on FAILED so the Airflow task fails too.

        from entrypoints import run_dq_engine
        validate = PythonOperator(task_id="validate", python_callable=run_dq_engine,
            op_kwargs={"project": "MY_PROJECT", "process": "MY_PROCESS",
                       "run_type": "WEEKLY", "run_mode": "DATE",
                       "start_date": "{{ ds }}", "end_date": "{{ ds }}"})
    """
    from rules_engine.engine import run_engine

    result = run_engine(project=project, process=process, run_type=run_type, run_mode=run_mode,
                        batch_id=batch_id, start_date=start_date, end_date=end_date, dry_run=dry_run)
    logger.info("Airflow DQ run complete: %s (%s)", result.get("run_id"), result.get("status"))
    if result.get("status") == "FAILED":
        raise RuntimeError(f"DQ engine run failed: {result.get('run_id')}")
    return result


try:
    from airflow.models.baseoperator import BaseOperator

    class DataQualityEngineOperator(BaseOperator):
        """
        Thin operator wrapping run_dq_engine() with templated fields, for
        teams that prefer a first-class operator over PythonOperator:

            from entrypoints import DataQualityEngineOperator
            validate = DataQualityEngineOperator(task_id="validate",
                project="MY_PROJECT", process="MY_PROCESS",
                run_type="WEEKLY", run_mode="DATE",
                start_date="{{ ds }}", end_date="{{ ds }}")
        """

        template_fields = ("project", "process", "run_type", "run_mode",
                            "batch_id", "start_date", "end_date")

        def __init__(self, project: str, process: str, run_type: str, run_mode: str,
                     batch_id: str = None, start_date: str = None, end_date: str = None,
                     dry_run: bool = False, **kwargs):
            super().__init__(**kwargs)
            self.project, self.process = project, process
            self.run_type, self.run_mode = run_type, run_mode
            self.batch_id, self.start_date, self.end_date = batch_id, start_date, end_date
            self.dry_run = dry_run

        def execute(self, context):
            return run_dq_engine(project=self.project, process=self.process,
                                 run_type=self.run_type, run_mode=self.run_mode,
                                 batch_id=self.batch_id, start_date=self.start_date,
                                 end_date=self.end_date, dry_run=self.dry_run)

except ImportError:
    # apache-airflow not installed in this environment — run_dq_engine()
    # above still works standalone for PythonOperator/TaskFlow use.
    DataQualityEngineOperator = None
    logger.debug("apache-airflow not installed — DataQualityEngineOperator unavailable.")


# =============================================================================
# Local Python server (cron or APScheduler) — useful for running close to an
# on-prem source database without a network hop
# =============================================================================

def cron_run_once(argv=None):
    """
    Single-shot mode for a plain crontab entry — identical to running
    rules_engine/main.py directly:
        python -m entrypoints --once --project ... (via the CLI below)
    """
    from rules_engine.main import main as cli_main
    cli_main(argv)


def cron_run_scheduler(config_path: str):
    """
    Long-lived APScheduler mode — keeps a process alive and fires
    run_engine() on a cron expression per job in a JSON config file
    (DQ_CRON_SCHEDULE_FILE, default schedule.json):

        [{"project": "MY_PROJECT", "process": "MY_PROCESS", "run_type": "WEEKLY",
          "run_mode": "DATE", "cron": "0 8 * * FRI", "lookback_days": 7,
          "sampling_config_name": "WEEKLY_REVIEW_SAMPLE"}]

    "sampling_config_name" is optional — set it to also fire the
    Sampling Framework (sampling/engine.py) as a follow-on step after the
    rules-engine run. The two frameworks stay decoupled even here: this
    function just calls both in sequence and hands the rules-engine run_id
    through for cross-referencing, the same way any other caller of
    sampling.engine.run_stratified_sampling() would.
    """
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        logger.error("apscheduler is required for scheduler mode. Install with: pip install apscheduler")
        sys.exit(1)

    with open(config_path) as f:
        jobs = json.load(f)

    scheduler = BlockingScheduler()
    for job in jobs:
        scheduler.add_job(_run_cron_job, CronTrigger.from_crontab(job["cron"]), args=[job],
                          id=f"{job['project']}_{job['process']}", replace_existing=True)
        logger.info("Scheduled %s/%s @ '%s'", job["project"], job["process"], job["cron"])

    logger.info("Scheduler started — %d job(s) registered.", len(jobs))
    scheduler.start()


def _run_cron_job(job: dict):
    from rules_engine.engine import run_engine

    end_date = date.today()
    start_date = end_date - timedelta(days=int(job.get("lookback_days", 7)) - 1)
    is_date_mode = job.get("run_mode", "DATE") == "DATE"

    result = run_engine(
        project=job["project"], process=job["process"], run_type=job["run_type"],
        run_mode=job.get("run_mode", "DATE"), batch_id=job.get("batch_id"),
        start_date=str(start_date) if is_date_mode else None,
        end_date=str(end_date) if is_date_mode else None,
    )
    logger.info("cron job: %s/%s complete — %s", job["project"], job["process"], result.get("status"))

    if job.get("sampling_config_name"):
        # Pass the same resolved date window used for the rules-engine run
        # through explicitly, rather than reading job["_resolved_start_date"]
        # / job["_resolved_end_date"], which are never set anywhere and
        # would silently cause sampling to always run unscoped (full table
        # pull) instead of matching the just-completed run's date window.
        #
        # Isolated in its own try/except: the rules-engine run above already
        # succeeded by this point, and a failure in this optional follow-on
        # step (bad sampling config, source outage, etc.) must not look like
        # an unhandled crash of the whole cron job with no context -- log it
        # with the job identity and alert, same as every other failure path
        # in this codebase, rather than letting it propagate to APScheduler's
        # generic job-exception handler.
        try:
            _run_cron_sampling(
                job, result,
                start_date=str(start_date) if is_date_mode else None,
                end_date=str(end_date) if is_date_mode else None,
            )
        except Exception as exc:
            logger.error(
                "Sampling follow-on failed for %s/%s (sampling_config_name=%s): %s",
                job["project"], job["process"], job.get("sampling_config_name"),
                exc, exc_info=True,
            )
            try:
                from utils.alert import send_alert
                send_alert(
                    f"[SAMPLING] Follow-on sampling failed after a successful DQ run\n\n"
                    f"Project: {job['project']}\nProcess: {job['process']}\n"
                    f"Sampling config: {job.get('sampling_config_name')}\n"
                    f"DQ run_id: {result.get('run_id')}\n\nError:\n{exc}",
                    "ERROR",
                )
            except Exception as alert_exc:
                logger.error("Could not send sampling-failure alert: %s", alert_exc, exc_info=True)


def _run_cron_sampling(job: dict, engine_result: dict, start_date=None, end_date=None):
    """Fire the Sampling Framework as a follow-on step after a rules-engine
    cron job. This is the only place in entrypoints.py that touches both
    frameworks — it's deployment glue, not a dependency between them."""
    from config.env_config import get_meta_db
    from rules_engine.executor import execute_query
    from sampling.engine import run_stratified_sampling
    from db.connection_factory import ConnectionFactory

    meta_db = get_meta_db()
    cf = ConnectionFactory()
    cf.load()
    td = cf.get(os.getenv("DQ_META_CONNECTION", "teradata"))
    try:
        rows = execute_query(td, f"""
            SELECT * FROM {meta_db}.dq_sampling_config
            WHERE sample_name = ? AND active_flag = 1
        """, [job['sampling_config_name']])
        if not rows:
            logger.warning("No active dq_sampling_config row named '%s'.", job["sampling_config_name"])
            return
        run_stratified_sampling(cf, td, rows[0], {
            "run_id": engine_result.get("run_id", "SAMPLING_RUN"),
            "start_date": start_date, "end_date": end_date,
        }, meta_db)
    finally:
        cf.close_all()


def _cli():
    """`python entrypoints.py [--schedule] [--config schedule.json] [main.py args...]`"""
    parser = argparse.ArgumentParser(prog="entrypoints")
    parser.add_argument("--schedule", action="store_true",
                        help="Run as a long-lived APScheduler process instead of a single shot.")
    parser.add_argument("--config", default=os.getenv("DQ_CRON_SCHEDULE_FILE", "schedule.json"))
    args, remaining = parser.parse_known_args()

    logging.basicConfig(level=os.getenv("DQ_LOG_LEVEL", "INFO"),
                        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")

    if args.schedule:
        cron_run_scheduler(args.config)
    else:
        cron_run_once(remaining)


if __name__ == "__main__":
    _cli()
