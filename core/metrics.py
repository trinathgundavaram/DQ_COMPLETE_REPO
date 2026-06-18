"""
metrics.py
----------
Post-run metrics calculation.

Called once after all rules finish executing. Responsibilities:

1. Aggregate the current run's dq_rule_execution rows into summary stats.
2. MERGE (upsert) those stats into dq_metrics_summary keyed by
   (project_name, process_name, run_type, run_month).
3. Compare current dq_score against the historical baseline for the
   SAME run_type (rolling average of the last N completed runs).
4. Compare dq_score across ALL run_types for the project/process so
   you can see whether DAILY / WEEKLY / MONTHLY diverge.
5. Return a structured comparison dict and emit alerts when the score
   drops below a configurable deviation threshold.
"""

import logging
from datetime import date

from core.executor import execute_query, execute_dml
from utils.alert import send_alert
from utils.logger import log_message

logger = logging.getLogger(__name__)

# ── tuneable constants ────────────────────────────────────────────────────────
# How many historical runs to use when computing the baseline average.
BASELINE_LOOKBACK = 10

# Alert when current dq_score drops more than this many percentage points
# below the historical baseline for the same run_type.
DEFAULT_DEVIATION_THRESHOLD_PCT = 5.0

# Per-run_type deviation overrides  { run_type: threshold_pct }
# A lower number = stricter alerting.
RUN_TYPE_DEVIATION_THRESHOLDS: dict = {
    "DAILY":   5.0,
    "WEEKLY":  4.0,
    "MONTHLY": 3.0,
}
# ─────────────────────────────────────────────────────────────────────────────


def calculate_metrics(td, run: dict, meta_db: str) -> dict:
    """
    Entry point called from engine.py after all rules have finished.

    Returns
    -------
    dict with keys:
        current          – aggregated stats for this run
        baseline         – historical average for same run_type (or None)
        deviation_pct    – how far current dq_score is below baseline
        breached         – True if deviation exceeds threshold
        cross_run_types  – list of {run_type, avg_score, min_score, max_score}
    """
    run_id   = run["run_id"]
    project  = run["project_name"]
    process  = run["process_name"]
    run_type = run["run_type"]

    # ── Step 1: aggregate current run ────────────────────────────────────────
    current = _aggregate_current_run(td, run_id, meta_db)
    if current is None:
        logger.warning("No execution records found for run_id=%s — skipping metrics.", run_id)
        return {}

    logger.info(
        "Metrics for run_id=%s | rules=%d failed=%d score=%.2f%%",
        run_id,
        current["total_rules"],
        current["failed_rules"],
        current["dq_score"],
    )

    # ── Step 2: upsert into dq_metrics_summary ───────────────────────────────
    _upsert_metrics(td, run, current, meta_db)

    # ── Step 3: baseline comparison for same run_type ────────────────────────
    baseline      = _get_baseline(td, project, process, run_type, meta_db)
    deviation_pct = 0.0
    breached      = False

    if baseline is not None:
        deviation_pct = round(baseline["avg_dq_score"] - current["dq_score"], 4)
        threshold     = RUN_TYPE_DEVIATION_THRESHOLDS.get(
            run_type.upper(), DEFAULT_DEVIATION_THRESHOLD_PCT
        )
        breached = deviation_pct > threshold

        logger.info(
            "run_type=%s | current_score=%.2f | baseline_avg=%.2f | deviation=%.2f | threshold=%.2f | breached=%s",
            run_type,
            current["dq_score"],
            baseline["avg_dq_score"],
            deviation_pct,
            threshold,
            breached,
        )

        log_message(
            td, run_id, "WARN" if breached else "INFO",
            f"DQ score {current['dq_score']:.2f}% vs {run_type} baseline "
            f"{baseline['avg_dq_score']:.2f}% (deviation {deviation_pct:.2f}pp)",
            meta_db=meta_db,
        )

        if breached:
            send_alert(
                f"⚠️ DQ SCORE BELOW BASELINE\n\n"
                f"Run ID      : {run_id}\n"
                f"Project     : {project}\n"
                f"Process     : {process}\n"
                f"Run Type    : {run_type}\n\n"
                f"Current Score  : {current['dq_score']:.2f}%\n"
                f"Baseline Avg   : {baseline['avg_dq_score']:.2f}%  "
                f"(last {BASELINE_LOOKBACK} {run_type} runs)\n"
                f"Deviation      : -{deviation_pct:.2f} pp  "
                f"(threshold {threshold} pp)\n\n"
                f"Failed Rules   : {current['failed_rules']} / {current['total_rules']}\n"
                f"Failed Records : {current['failed_records']:,}",
                "WARN",
            )

    # ── Step 4: cross-run_type comparison ────────────────────────────────────
    cross = _compare_run_types(td, project, process, meta_db)

    result = {
        "current":         current,
        "baseline":        baseline,
        "deviation_pct":   deviation_pct,
        "breached":        breached,
        "cross_run_types": cross,
    }

    _log_cross_comparison(run_id, run_type, cross, td, meta_db)
    return result


# ── private helpers ───────────────────────────────────────────────────────────

def _aggregate_current_run(td, run_id: str, meta_db: str):
    """Sum up execution records for this run_id."""
    sql = f"""
        SELECT
            COUNT(*)                              AS total_rules,
            SUM(CASE WHEN status IN ('FAIL','WARN') THEN 1 ELSE 0 END)
                                                  AS failed_rules,
            SUM(CASE WHEN status = 'PASS'         THEN 1 ELSE 0 END)
                                                  AS passed_rules,
            COALESCE(SUM(total_records),  0)      AS total_records,
            COALESCE(SUM(failed_records), 0)      AS failed_records,
            COALESCE(AVG(failure_pct),    0)      AS avg_failure_pct
        FROM {meta_db}.dq_rule_execution
        WHERE run_id = '{run_id}'
    """
    rows = execute_query(td, sql)
    if not rows:
        return None

    r = rows[0]
    total_rules = int(r.get("total_rules") or 0)
    if total_rules == 0:
        return None

    failed_rules = int(r.get("failed_rules") or 0)
    passed_rules = int(r.get("passed_rules") or 0)
    dq_score     = round((passed_rules / total_rules) * 100, 4) if total_rules else 0.0

    return {
        "total_rules":    total_rules,
        "failed_rules":   failed_rules,
        "passed_rules":   passed_rules,
        "total_records":  int(r.get("total_records")  or 0),
        "failed_records": int(r.get("failed_records") or 0),
        "avg_failure_pct": round(float(r.get("avg_failure_pct") or 0), 4),
        "dq_score":       dq_score,
    }


def _upsert_metrics(td, run: dict, current: dict, meta_db: str):
    """
    Merge current run stats into dq_metrics_summary.

    Key = (project_name, process_name, run_type, batch_id, dataset_id, run_month).
    If a matching row exists (same project/process/run_type/run_month/batch)
    it is updated; otherwise a new row is inserted.
    Teradata MERGE handles the upsert atomically.
    """
    today     = date.today()
    run_month = date(today.year, today.month, 1).strftime("%Y-%m-%d")

    project    = run["project_name"]
    process    = run.get("process_name") or ""
    run_type   = run["run_type"]
    batch_id   = run.get("batch_id")   or ""
    dataset_id = run.get("dataset_id") or ""

    total_rules    = current["total_rules"]
    failed_rules   = current["failed_rules"]
    passed_rules   = current["passed_rules"]
    total_records  = current["total_records"]
    failed_records = current["failed_records"]
    avg_fail_pct   = current["avg_failure_pct"]
    dq_score       = current["dq_score"]

    merge_sql = f"""
        MERGE INTO {meta_db}.dq_metrics_summary AS tgt
        USING (
            SELECT
                '{project}'    AS project_name,
                '{process}'    AS process_name,
                '{run_type}'   AS run_type,
                '{batch_id}'   AS batch_id,
                '{dataset_id}' AS dataset_id,
                DATE '{run_month}' AS run_month
        ) AS src
        ON  tgt.project_name = src.project_name
        AND tgt.process_name = src.process_name
        AND tgt.run_type     = src.run_type
        AND tgt.batch_id     = src.batch_id
        AND tgt.dataset_id   = src.dataset_id
        AND tgt.run_month    = src.run_month
        WHEN MATCHED THEN UPDATE SET
            total_runs       = tgt.total_runs + 1,
            total_rules      = tgt.total_rules      + {total_rules},
            failed_rules     = tgt.failed_rules     + {failed_rules},
            passed_rules     = tgt.passed_rules     + {passed_rules},
            total_records    = tgt.total_records    + {total_records},
            failed_records   = tgt.failed_records   + {failed_records},
            avg_failure_pct  = (tgt.avg_failure_pct * tgt.total_runs + {avg_fail_pct}) / (tgt.total_runs + 1),
            dq_score         = (tgt.dq_score        * tgt.total_runs + {dq_score})     / (tgt.total_runs + 1),
            created_at       = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN INSERT (
            project_name, process_name, run_type, batch_id, dataset_id,
            run_month, total_runs, total_rules, failed_rules, passed_rules,
            total_records, failed_records, avg_failure_pct, dq_score, created_at
        ) VALUES (
            '{project}', '{process}', '{run_type}', '{batch_id}', '{dataset_id}',
            DATE '{run_month}', 1, {total_rules}, {failed_rules}, {passed_rules},
            {total_records}, {failed_records}, {avg_fail_pct}, {dq_score},
            CURRENT_TIMESTAMP
        )
    """
    execute_dml(td, merge_sql)
    logger.info("dq_metrics_summary upserted for %s / %s / %s.", project, process, run_type)


def _get_baseline(td, project: str, process: str, run_type: str, meta_db: str):
    """
    Return the rolling average dq_score over the last BASELINE_LOOKBACK
    completed runs for this project / process / run_type.
    Returns None if no historical data exists yet.
    """
    sql = f"""
        SELECT
            AVG(dq_score)         AS avg_dq_score,
            MIN(dq_score)         AS min_dq_score,
            MAX(dq_score)         AS max_dq_score,
            AVG(avg_failure_pct)  AS avg_failure_pct,
            SUM(total_runs)       AS total_historical_runs
        FROM (
            SELECT dq_score, avg_failure_pct, total_runs, run_month
            FROM {meta_db}.dq_metrics_summary
            WHERE project_name = '{project}'
              AND process_name = '{process}'
              AND run_type     = '{run_type}'
            QUALIFY ROW_NUMBER() OVER (ORDER BY run_month DESC) <= {BASELINE_LOOKBACK}
        ) sub
    """
    rows = execute_query(td, sql)
    if not rows or rows[0].get("avg_dq_score") is None:
        return None

    r = rows[0]
    return {
        "avg_dq_score":          round(float(r["avg_dq_score"]),         4),
        "min_dq_score":          round(float(r["min_dq_score"]),         4),
        "max_dq_score":          round(float(r["max_dq_score"]),         4),
        "avg_failure_pct":       round(float(r["avg_failure_pct"]),      4),
        "total_historical_runs": int(r["total_historical_runs"] or 0),
    }


def _compare_run_types(td, project: str, process: str, meta_db: str) -> list:
    """
    Return latest average dq_score per run_type for this project/process.
    Lets you compare how DAILY vs WEEKLY vs MONTHLY runs perform.
    """
    sql = f"""
        SELECT
            run_type,
            AVG(dq_score)         AS avg_score,
            MIN(dq_score)         AS min_score,
            MAX(dq_score)         AS max_score,
            AVG(avg_failure_pct)  AS avg_failure_pct,
            SUM(total_runs)       AS total_runs,
            MAX(run_month)        AS latest_run_month
        FROM {meta_db}.dq_metrics_summary
        WHERE project_name = '{project}'
          AND process_name = '{process}'
        GROUP BY run_type
        ORDER BY run_type
    """
    rows = execute_query(td, sql)
    return [
        {
            "run_type":        r["run_type"],
            "avg_score":       round(float(r["avg_score"]       or 0), 4),
            "min_score":       round(float(r["min_score"]       or 0), 4),
            "max_score":       round(float(r["max_score"]       or 0), 4),
            "avg_failure_pct": round(float(r["avg_failure_pct"] or 0), 4),
            "total_runs":      int(r["total_runs"] or 0),
            "latest_run_month": str(r.get("latest_run_month") or ""),
        }
        for r in rows
    ]


def _log_cross_comparison(run_id: str, current_run_type: str,
                           cross: list, td, meta_db: str):
    if not cross:
        return

    lines = ["Run-type DQ score comparison:"]
    for c in cross:
        marker = " ← current" if c["run_type"] == current_run_type else ""
        lines.append(
            f"  {c['run_type']:10s}  avg={c['avg_score']:.2f}%  "
            f"min={c['min_score']:.2f}%  max={c['max_score']:.2f}%  "
            f"runs={c['total_runs']}{marker}"
        )

    summary = "\n".join(lines)
    logger.info("\n%s", summary)
    log_message(td, run_id, "INFO", summary, meta_db=meta_db)
