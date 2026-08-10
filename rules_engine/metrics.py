"""
rules_engine/metrics.py
---------------
Everything that happens to dq_metrics_summary / dq_anomaly_log after a run
finishes — computing this run's numbers, comparing them to history, and
deciding whether the history itself says something is wrong. Kept in one
file because anomaly detection is directly downstream of the metrics this
module just wrote: it reads the same dq_metrics_summary rows one function
later in the same engine.py call sequence.

calculate_metrics()  — aggregate this run's dq_rule_execution rows, upsert
    into dq_metrics_summary (a duplicate-key error from two concurrent runs
    falls back to a pure UPDATE so totals still accumulate correctly), and
    compare against a rolling baseline.

detect_and_log()  — statistical anomaly detection (z-score / IQR) on the
    metrics history calculate_metrics() just extended. A z-score of 4.2 is
    far more actionable than "score dropped 5%" — it says exactly how
    unusual this run is relative to its own history. Config is per
    project/process/run_type via dq_anomaly_config (NULL = wildcard, most-
    specific match wins); falls back to built-in defaults with no config.
"""

import logging
import statistics
from datetime import date
from typing import List, Optional, Tuple

from rules_engine.executor import execute_query, execute_dml, bulk_insert
from utils.alert import send_alert
from utils.metadata_writers import log_message

logger = logging.getLogger(__name__)

# Anomaly-detection built-in defaults — used when no dq_anomaly_config row matches
_DEFAULT_METHOD           = "ZSCORE"
_DEFAULT_ZSCORE_THRESHOLD = 3.0
_DEFAULT_IQR_MULTIPLIER   = 1.5
_DEFAULT_MIN_HISTORY      = 10
_DEFAULT_ALERT            = True

# ── Tunable constants ─────────────────────────────────────────────────────────
BASELINE_LOOKBACK = 10

DEFAULT_DEVIATION_THRESHOLD_PCT = 5.0

RUN_TYPE_DEVIATION_THRESHOLDS: dict = {
    "DAILY":   5.0,
    "WEEKLY":  4.0,
    "MONTHLY": 3.0,
}


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
    scope_id = run["scope_id"]
    project  = run["project_name"]
    process  = run["process_name"]
    run_type = run["run_type"]
    batch_id   = run.get("batch_id")   or ""
    dataset_id = run.get("dataset_id") or ""
    # Same bucket key _upsert_metrics() below MERGEs this run's numbers
    # into -- needed so _get_baseline() can exclude exactly that bucket
    # (see its docstring for why: dq_metrics_summary rows are rolling
    # aggregates across many runs, not one row per run, so comparing
    # "current" against a baseline that still includes current's own
    # just-written bucket understates real deviations).
    run_month = date(date.today().year, date.today().month, 1)

    current = _aggregate_current_run(td, run_id, meta_db)
    if current is None:
        logger.warning("No execution records for run_id=%s — skipping metrics.", run_id)
        return {}

    logger.info(
        "Metrics for run_id=%s | rules=%d failed=%d score=%.2f%%",
        run_id, current["total_rules"], current["failed_rules"], current["dq_score"],
    )

    _upsert_metrics(td, run, current, meta_db)

    baseline      = _get_baseline(td, scope_id, run_type, batch_id, dataset_id, run_month, meta_db)
    deviation_pct = 0.0
    breached      = False

    if baseline is not None:
        deviation_pct = round(baseline["avg_dq_score"] - current["dq_score"], 4)
        threshold     = RUN_TYPE_DEVIATION_THRESHOLDS.get(
            run_type.upper(), DEFAULT_DEVIATION_THRESHOLD_PCT
        )
        breached = deviation_pct > threshold

        logger.info(
            "run_type=%s | current=%.2f | baseline=%.2f | deviation=%.2f | "
            "threshold=%.2f | breached=%s",
            run_type, current["dq_score"], baseline["avg_dq_score"],
            deviation_pct, threshold, breached,
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

    cross  = _compare_run_types(td, scope_id, meta_db)
    result = {
        "current":         current,
        "baseline":        baseline,
        "deviation_pct":   deviation_pct,
        "breached":        breached,
        "cross_run_types": cross,
    }
    _log_cross_comparison(run_id, run_type, cross, td, meta_db)
    return result


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _aggregate_current_run(td, run_id: str, meta_db: str):
    """Sum execution records for this run_id (SKIP rows included in denominator)."""
    sql = f"""
        SELECT
            COUNT(*)                                                        AS total_rules,
            SUM(CASE WHEN status IN ('FAIL','WARN','ERROR','SKIP') THEN 1 ELSE 0 END)
                                                                            AS failed_rules,
            SUM(CASE WHEN status = 'PASS'                          THEN 1 ELSE 0 END)
                                                                            AS passed_rules,
            COALESCE(SUM(total_records),  0)                                AS total_records,
            COALESCE(SUM(failed_records), 0)                                AS failed_records,
            COALESCE(AVG(failure_pct),    0)                                AS avg_failure_pct
        FROM {meta_db}.dq_rule_execution
        WHERE run_id = ?
    """
    rows = execute_query(td, sql, [run_id])
    if not rows:
        return None

    r           = rows[0]
    total_rules = int(r.get("total_rules") or 0)
    if total_rules == 0:
        return None

    failed_rules = int(r.get("failed_rules") or 0)
    passed_rules = int(r.get("passed_rules") or 0)
    dq_score     = round((passed_rules / total_rules) * 100, 4) if total_rules else 0.0

    return {
        "total_rules":     total_rules,
        "failed_rules":    failed_rules,
        "passed_rules":    passed_rules,
        "total_records":   int(r.get("total_records")   or 0),
        "failed_records":  int(r.get("failed_records")  or 0),
        "avg_failure_pct": round(float(r.get("avg_failure_pct") or 0), 4),
        "dq_score":        dq_score,
    }


def _upsert_metrics(td, run: dict, current: dict, meta_db: str):
    """
    Merge current run stats into dq_metrics_summary.

    On unique-index violation (concurrent run race), falls back to
    UPDATE-only so both runs accumulate correctly.
    """
    today     = date.today()
    run_month = date(today.year, today.month, 1).strftime("%Y-%m-%d")

    scope_id   = run["scope_id"]
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

    # scope_id/run_type/batch_id/dataset_id trace back to CLI/Lambda-event
    # input (see utils/ids.py::generate_run_id and run["scope_id"] resolution
    # in engine.py) — bound as params rather than interpolated so a stray
    # quote can never reach the SQL text.
    merge_sql = f"""
        MERGE INTO {meta_db}.dq_metrics_summary AS tgt
        USING (
            SELECT
                ? AS scope_id,
                ? AS run_type,
                ? AS batch_id,
                ? AS dataset_id,
                CAST(? AS DATE) AS run_month
        ) AS src
        ON  tgt.scope_id     = src.scope_id
        AND tgt.run_type     = src.run_type
        AND tgt.batch_id     = src.batch_id
        AND tgt.dataset_id   = src.dataset_id
        AND tgt.run_month    = src.run_month
        WHEN MATCHED THEN UPDATE SET
            total_runs      = tgt.total_runs + 1,
            total_rules     = tgt.total_rules     + ?,
            failed_rules    = tgt.failed_rules    + ?,
            passed_rules    = tgt.passed_rules    + ?,
            total_records   = tgt.total_records   + ?,
            failed_records  = tgt.failed_records  + ?,
            avg_failure_pct = (tgt.avg_failure_pct * tgt.total_runs + ?)
                              / (tgt.total_runs + 1),
            dq_score        = (tgt.dq_score        * tgt.total_runs + ?)
                              / (tgt.total_runs + 1),
            created_at      = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN INSERT (
            scope_id, run_type, batch_id, dataset_id,
            run_month, total_runs, total_rules, failed_rules, passed_rules,
            total_records, failed_records, avg_failure_pct, dq_score, created_at
        ) VALUES (
            ?, ?, ?, ?,
            CAST(? AS DATE), 1, ?, ?, ?,
            ?, ?, ?, ?,
            CURRENT_TIMESTAMP
        )
    """
    merge_params = [
        scope_id, run_type, batch_id, dataset_id, run_month,
        total_rules, failed_rules, passed_rules, total_records, failed_records,
        avg_fail_pct, dq_score,
        scope_id, run_type, batch_id, dataset_id, run_month,
        total_rules, failed_rules, passed_rules,
        total_records, failed_records, avg_fail_pct, dq_score,
    ]

    try:
        execute_dml(td, merge_sql, merge_params)
        logger.info("dq_metrics_summary upserted for scope_id=%s/%s.", scope_id, run_type)

    except Exception as merge_err:
        # Unique-index violation — another concurrent run inserted first.
        # Teradata error 2801 = "Duplicate unique prime key" / similar unique errors.
        err_str = str(merge_err).lower()
        if any(k in err_str for k in ("unique", "duplicate", "2801", "primary index")):
            logger.warning(
                "MERGE unique-index conflict for scope_id=%s/%s — falling back to UPDATE.",
                scope_id, run_type,
            )
            _fallback_update(td, run, current, run_month, meta_db)
        else:
            raise


def _fallback_update(td, run: dict, current: dict, run_month: str, meta_db: str):
    """
    Pure UPDATE path when MERGE hits a unique-index race (fix #3).
    Uses the same weighted cumulative average formula as the MERGE.
    """
    scope_id   = run["scope_id"]
    run_type   = run["run_type"]
    batch_id   = run.get("batch_id")   or ""
    dataset_id = run.get("dataset_id") or ""

    sql = f"""
        UPDATE {meta_db}.dq_metrics_summary SET
            total_runs      = total_runs + 1,
            total_rules     = total_rules     + ?,
            failed_rules    = failed_rules    + ?,
            passed_rules    = passed_rules    + ?,
            total_records   = total_records   + ?,
            failed_records  = failed_records  + ?,
            avg_failure_pct = (avg_failure_pct * total_runs + ?)
                              / (total_runs + 1),
            dq_score        = (dq_score        * total_runs + ?)
                              / (total_runs + 1),
            created_at      = CURRENT_TIMESTAMP
        WHERE scope_id     = ?
          AND run_type     = ?
          AND batch_id     = ?
          AND dataset_id   = ?
          AND run_month    = CAST(? AS DATE)
    """
    params = [
        current['total_rules'], current['failed_rules'], current['passed_rules'],
        current['total_records'], current['failed_records'],
        current['avg_failure_pct'], current['dq_score'],
        scope_id, run_type, batch_id, dataset_id, run_month,
    ]
    execute_dml(td, sql, params)
    logger.info("Fallback UPDATE applied for scope_id=%s/%s.", scope_id, run_type)


def _get_baseline(td, scope_id: int, run_type: str, batch_id: str,
                   dataset_id: str, run_month, meta_db: str):
    """
    Rolling average dq_score over the last BASELINE_LOOKBACK buckets,
    EXCLUDING the (batch_id, dataset_id, run_month) bucket this run just
    wrote to via _upsert_metrics()'s MERGE.

    dq_metrics_summary rows are rolling aggregates across every run that
    has ever matched a given (scope_id, run_type, batch_id, dataset_id,
    run_month) key -- not one row per individual engine run. Without this
    exclusion, "baseline" would include the exact bucket this run's own
    numbers were just merged into, silently pulling the average toward the
    current value and understating how far off a genuinely bad run is.
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
            WHERE scope_id      = ?
              AND run_type     = ?
              AND NOT (batch_id = ? AND dataset_id = ? AND run_month = CAST(? AS DATE))
            QUALIFY ROW_NUMBER() OVER (ORDER BY run_month DESC) <= {BASELINE_LOOKBACK}
        ) sub
    """
    rows = execute_query(td, sql, [scope_id, run_type, batch_id, dataset_id, run_month])
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


def _compare_run_types(td, scope_id: int, meta_db: str) -> list:
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
        WHERE scope_id = ?
        GROUP BY run_type
        ORDER BY run_type
    """
    rows = execute_query(td, sql, [scope_id])
    return [
        {
            "run_type":         r["run_type"],
            "avg_score":        round(float(r["avg_score"]       or 0), 4),
            "min_score":        round(float(r["min_score"]       or 0), 4),
            "max_score":        round(float(r["max_score"]       or 0), 4),
            "avg_failure_pct":  round(float(r["avg_failure_pct"] or 0), 4),
            "total_runs":       int(r["total_runs"] or 0),
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
        marker = " <- current" if c["run_type"] == current_run_type else ""
        lines.append(
            f"  {c['run_type']:10s}  avg={c['avg_score']:.2f}%  "
            f"min={c['min_score']:.2f}%  max={c['max_score']:.2f}%  "
            f"runs={c['total_runs']}{marker}"
        )
    summary = "\n".join(lines)
    logger.info("\n%s", summary)
    log_message(td, run_id, "INFO", summary, meta_db=meta_db)



# =============================================================================
# Anomaly detection (z-score / IQR on metrics history)
# =============================================================================

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def detect_and_log(td_conn, run: dict, meta_db: str) -> List[dict]:
    """
    Run anomaly detection on the just-completed run's metrics and persist
    results to dq_anomaly_log.

    Called by engine.py after calculate_metrics() completes.

    Returns a list of anomaly dicts (empty if none detected or insufficient
    history).
    """
    scope_id = run.get("scope_id")
    project  = run.get("project_name", "")
    process  = run.get("process_name", "")
    run_type = run.get("run_type", "")
    run_id   = run.get("run_id", "")
    batch_id   = run.get("batch_id")   or ""
    dataset_id = run.get("dataset_id") or ""
    # Same bucket key calculate_metrics()'s _upsert_metrics() call just
    # MERGEd this run's numbers into -- see _load_current_metrics()/
    # _load_history() docstrings for why both need it.
    run_month = date(date.today().year, date.today().month, 1)

    # Load detection config (most-specific match wins) — dq_anomaly_config
    # is a low-cardinality wildcard table, still matched on raw project/
    # process, not scope_id; see ddl_shared.sql's header (design note 4)
    # for why it's deliberately not normalized to scope_id.
    cfg = _load_config(td_conn, project, process, run_type, meta_db, execute_query)

    method    = cfg["method"].upper()
    zthresh   = cfg["zscore_threshold"]
    iqr_mult  = cfg["iqr_multiplier"]
    min_hist  = cfg["min_history_runs"]
    do_alert  = cfg["alert_on_anomaly"]

    # Load current run's metrics
    current = _load_current_metrics(td_conn, scope_id, run_type, batch_id,
                                     dataset_id, run_month, meta_db, execute_query)
    if current is None:
        logger.debug("No metrics row found for run %s — skipping anomaly detection.", run_id)
        return []

    # Load historical metrics, excluding the exact bucket "current" came from
    history = _load_history(td_conn, scope_id, run_type, batch_id,
                            dataset_id, run_month, meta_db, execute_query)

    if len(history) < min_hist:
        logger.info(
            "Anomaly detection skipped for run %s — only %d historical run(s), "
            "minimum required: %d.",
            run_id, len(history), min_hist,
        )
        return []

    metrics_to_check = [
        ("dq_score",          current.get("dq_score"),          [h.get("dq_score")          for h in history], True),
        ("avg_failure_pct",   current.get("avg_failure_pct"),   [h.get("avg_failure_pct")   for h in history], False),
        ("failed_rule_pct",   _failed_rule_pct(current),        [_failed_rule_pct(h)         for h in history], False),
    ]

    anomalies  = []
    insert_rows = []

    for metric_name, current_val, hist_vals, higher_is_better in metrics_to_check:
        if current_val is None:
            continue

        # Math lives in evaluate_metric_drift() (below) so other frameworks
        # -- currently sampling/anomaly.py's candidate-pool volume check --
        # can reuse the exact same z-score/IQR statistics as a plain
        # library call. This loop's job is just logging/persistence.
        detections = evaluate_metric_drift(
            current_val, hist_vals, method=method,
            zscore_threshold=zthresh, iqr_multiplier=iqr_mult, min_history=min_hist,
        )

        for det in detections:
            if det["method"] == "ZSCORE" and det["is_anomaly"]:
                direction = "below" if det["z_score"] < 0 else "above"
                logger.warning(
                    "ANOMALY [z-score] %s: %s | current=%.4f μ=%.4f σ=%.4f z=%.2f (%s by %.1fσ)",
                    run_id, metric_name, current_val, det["historical_mean"],
                    det["historical_std"], det["z_score"], direction, abs(det["z_score"]),
                )
            if det["method"] == "IQR" and det["is_anomaly"]:
                logger.warning(
                    "ANOMALY [IQR] %s: %s | current=%.4f bounds=[%.4f, %.4f]",
                    run_id, metric_name, current_val, det["iqr_lower"], det["iqr_upper"],
                )

            if det["is_anomaly"]:
                anomalies.append({
                    "metric_name":   metric_name,
                    "current_value": current_val,
                    "method":        det["method"],
                    "severity":      det["severity"],
                })

            insert_rows.append((
                run_id,
                metric_name,
                current_val,
                det.get("historical_mean"),
                det.get("historical_std"),
                det.get("z_score"),
                det.get("iqr_lower"),
                det.get("iqr_upper"),
                1 if det["is_anomaly"] else 0,
                det["method"],
                det["severity"],
            ))

    # Persist to dq_anomaly_log
    if insert_rows:
        try:
            # project_name/process_name/run_type are NOT stored — derivable via
            # run_id -> dq_run_control (they're set once at run start).
            sql = f"""
                INSERT INTO {meta_db}.dq_anomaly_log (
                    run_id,
                    metric_name, current_value, historical_mean, historical_std,
                    z_score, iqr_lower_bound, iqr_upper_bound,
                    is_anomaly, detection_method, severity, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """
            bulk_insert(td_conn, sql, insert_rows)
        except Exception as exc:
            logger.error("Failed to write dq_anomaly_log: %s", exc)

    # Send alert if anomalies found and configured
    if anomalies and do_alert:
        _send_anomaly_alert(run, anomalies)

    return anomalies


# ---------------------------------------------------------------------------
# Public math API — reusable by other frameworks as a plain library call
# ---------------------------------------------------------------------------

def evaluate_metric_drift(
    current_val: float,
    history: list,
    method: str = "BOTH",
    zscore_threshold: float = 3.0,
    iqr_multiplier: float = 1.5,
    min_history: int = 1,
) -> List[dict]:
    """
    Compare one current numeric value against a list of historical values
    using z-score and/or IQR (Tukey fence) statistics. Pure function — no
    DB access, no logging, no project/run context — so it's safe to reuse
    from anywhere, not just detect_and_log() above.

    This is the sanctioned reuse point for "sampling/ may use rules_engine/ as a
    library, rules_engine/ never imports sampling/" (see DESIGN.md): sampling/
    anomaly.py's candidate-pool volume drift check calls this directly
    rather than reimplementing the same statistics. detect_and_log() also
    calls it now, so there's exactly one implementation of this math.

    Parameters
    ----------
    current_val : the value to test
    history     : historical values to compare against (None entries are
                  dropped automatically)
    method      : "ZSCORE" | "IQR" | "BOTH"
    zscore_threshold : |z| above this is flagged (ZSCORE method)
    iqr_multiplier    : Tukey fence multiplier (IQR method; needs >= 4
                        clean history points regardless of min_history)
    min_history : minimum clean historical points required to run at all

    Returns
    -------
    List of detection dicts (0, 1, or 2 entries — one per method that ran):
        {"method": "ZSCORE", "z_score": float, "is_anomaly": bool,
         "severity": str, "historical_mean": float, "historical_std": float|None}
        {"method": "IQR", "iqr_lower": float, "iqr_upper": float,
         "is_anomaly": bool, "severity": str,
         "historical_mean": float, "historical_std": float|None}
    Empty list if there isn't enough clean history to compare against.
    """
    hist_clean = [v for v in history if v is not None]
    if len(hist_clean) < min_history:
        return []

    hist_mean = statistics.mean(hist_clean)
    hist_std  = None
    try:
        hist_std = statistics.stdev(hist_clean)
    except statistics.StatisticsError:
        pass

    method = (method or "BOTH").upper()
    detections = []

    if method in ("ZSCORE", "BOTH") and hist_std is not None and hist_std > 0:
        z = (current_val - hist_mean) / hist_std
        is_anomaly = abs(z) > zscore_threshold
        detections.append({
            "method":          "ZSCORE",
            "z_score":         round(z, 4),
            "is_anomaly":      is_anomaly,
            "severity":        _zscore_severity(abs(z), zscore_threshold),
            "historical_mean": round(hist_mean, 6),
            "historical_std":  round(hist_std, 6),
        })

    if method in ("IQR", "BOTH") and len(hist_clean) >= 4:
        lower, upper = _iqr_bounds(hist_clean, iqr_multiplier)
        is_anomaly = (current_val < lower) or (current_val > upper)
        detections.append({
            "method":          "IQR",
            "iqr_lower":       round(lower, 4),
            "iqr_upper":       round(upper, 4),
            "is_anomaly":      is_anomaly,
            "severity":        _iqr_severity(current_val, lower, upper),
            "historical_mean": round(hist_mean, 6),
            "historical_std":  round(hist_std, 6) if hist_std is not None else None,
        })

    return detections


# ---------------------------------------------------------------------------
# Math helpers
# ---------------------------------------------------------------------------

def _iqr_bounds(data: list, multiplier: float) -> Tuple[float, float]:
    """Return (lower_fence, upper_fence) using Tukey IQR method."""
    sorted_data = sorted(data)
    q1          = _percentile(sorted_data, 25)
    q3          = _percentile(sorted_data, 75)
    iqr         = q3 - q1
    return q1 - multiplier * iqr, q3 + multiplier * iqr


def _percentile(sorted_data: list, pct: int) -> float:
    """Simple linear interpolation percentile (no numpy dependency)."""
    n   = len(sorted_data)
    idx = (pct / 100) * (n - 1)
    lo  = int(idx)
    hi  = min(lo + 1, n - 1)
    frac = idx - lo
    return sorted_data[lo] * (1 - frac) + sorted_data[hi] * frac


def _zscore_severity(abs_z: float, threshold: float) -> str:
    """Map |z-score| to a human-readable severity label."""
    ratio = abs_z / max(threshold, 0.001)
    if ratio >= 3.0:
        return "CRITICAL"
    if ratio >= 2.0:
        return "HIGH"
    if ratio >= 1.5:
        return "MEDIUM"
    if ratio >= 1.0:
        return "LOW"
    return "INFO"


def _iqr_severity(val: float, lower: float, upper: float) -> str:
    """Map IQR fence deviation to severity."""
    if val < lower:
        dev = (lower - val) / max(abs(lower), 1e-9)
    elif val > upper:
        dev = (val - upper) / max(abs(upper), 1e-9)
    else:
        return "INFO"

    if dev >= 1.0:
        return "CRITICAL"
    if dev >= 0.5:
        return "HIGH"
    if dev >= 0.25:
        return "MEDIUM"
    return "LOW"


# ---------------------------------------------------------------------------
# Data access helpers
# ---------------------------------------------------------------------------

def _failed_rule_pct(row: Optional[dict]) -> Optional[float]:
    """Compute failed_rule_pct from a metrics summary row."""
    if row is None:
        return None
    total  = row.get("total_rules") or 0
    failed = row.get("failed_rules") or 0
    return round(failed / total * 100, 4) if total else 0.0


def _load_current_metrics(
    td_conn, scope_id, run_type, batch_id, dataset_id, run_month, meta_db, execute_query
) -> Optional[dict]:
    """
    Load the exact dq_metrics_summary bucket this run's numbers were just
    merged into.

    Filtering by batch_id/dataset_id/run_month (not just scope_id/run_type
    + "most recent created_at") matters under concurrent execution: two
    different batches of the same run_type can both update dq_metrics_summary
    around the same time, and picking "whichever row has the latest
    created_at" can silently grab a DIFFERENT batch's bucket than the one
    this run actually just wrote to.
    """
    try:
        rows = execute_query(
            td_conn,
            f"""
            SELECT *
            FROM {meta_db}.dq_metrics_summary
            WHERE scope_id    = ?
              AND run_type    = ?
              AND batch_id    = ?
              AND dataset_id  = ?
              AND run_month   = CAST(? AS DATE)
            """,
            [scope_id, run_type, batch_id, dataset_id, run_month],
        )
        return rows[0] if rows else None
    except Exception as exc:
        logger.warning("Could not load current metrics for anomaly detection: %s", exc, exc_info=True)
        return None


def _load_history(
    td_conn, scope_id, run_type, batch_id, dataset_id, run_month, meta_db, execute_query
) -> list:
    """
    Load historical dq_metrics_summary rows for the same scope/run_type,
    EXCLUDING the exact (batch_id, dataset_id, run_month) bucket this run's
    numbers were just merged into.

    dq_metrics_summary rows are rolling aggregates across every run that has
    ever matched a given bucket key, not one row per individual run --
    without this exclusion the "history" used to judge whether the current
    run is anomalous would include the current run's own just-written
    contribution, silently biasing the comparison toward "not anomalous".

    Returns up to 100 most recent rows (sufficient for statistical purposes).
    """
    try:
        return execute_query(
            td_conn,
            f"""
            SELECT dq_score, avg_failure_pct, total_rules, failed_rules
            FROM {meta_db}.dq_metrics_summary
            WHERE scope_id    = ?
              AND run_type    = ?
              AND NOT (batch_id = ? AND dataset_id = ? AND run_month = CAST(? AS DATE))
            ORDER BY created_at DESC
            """,
            [scope_id, run_type, batch_id, dataset_id, run_month],
        )[:100]
    except Exception as exc:
        logger.warning("Could not load metrics history for anomaly detection: %s", exc, exc_info=True)
        return []


def _load_config(
    td_conn, project, process, run_type, meta_db, execute_query
) -> dict:
    """
    Load the most-specific matching dq_anomaly_config row.
    Specificity = fewest NULLs in (project_name, process_name, run_type).
    Falls back to built-in defaults when no row matches.
    """
    defaults = {
        "method":            _DEFAULT_METHOD,
        "zscore_threshold":  _DEFAULT_ZSCORE_THRESHOLD,
        "iqr_multiplier":    _DEFAULT_IQR_MULTIPLIER,
        "min_history_runs":  _DEFAULT_MIN_HISTORY,
        "alert_on_anomaly":  _DEFAULT_ALERT,
    }

    try:
        rows = execute_query(
            td_conn,
            f"""
            SELECT *
            FROM {meta_db}.dq_anomaly_config
            WHERE (project_name IS NULL OR project_name = ?)
              AND (process_name IS NULL OR process_name = ?)
              AND (run_type     IS NULL OR run_type     = ?)
            """,
            [project, process, run_type],
        )
    except Exception as exc:
        logger.warning("Could not load dq_anomaly_config — using defaults: %s", exc)
        return defaults

    if not rows:
        return defaults

    # Pick most-specific row (fewest NULLs)
    def specificity(r):
        return sum([
            r.get("project_name") is not None,
            r.get("process_name") is not None,
            r.get("run_type")     is not None,
        ])

    best = max(rows, key=specificity)
    return {
        # dq_anomaly_config's column is named 'process' (not 'method' --
        # that's a Teradata reserved word); the "method" key here is this
        # module's own internal name for the detection algorithm choice.
        "method":           (best.get("process") or _DEFAULT_METHOD).upper(),
        "zscore_threshold": float(best.get("zscore_threshold") or _DEFAULT_ZSCORE_THRESHOLD),
        "iqr_multiplier":   float(best.get("iqr_multiplier")   or _DEFAULT_IQR_MULTIPLIER),
        "min_history_runs": int(best.get("min_history_runs")   or _DEFAULT_MIN_HISTORY),
        "alert_on_anomaly": bool(int(best.get("alert_on_anomaly") or _DEFAULT_ALERT)),
    }


# ---------------------------------------------------------------------------
# Alert helper
# ---------------------------------------------------------------------------

def _send_anomaly_alert(run: dict, anomalies: list):
    """Send a Teams / email alert summarising detected anomalies."""
    run_id   = run.get("run_id", "")
    project  = run.get("project_name", "")
    process  = run.get("process_name", "")
    run_type = run.get("run_type", "")

    lines = [
        "DQ ANOMALY DETECTED",
        "",
        f"Run ID   : {run_id}",
        f"Project  : {project}",
        f"Process  : {process}",
        f"Run Type : {run_type}",
        "",
        f"Anomalies ({len(anomalies)}):",
    ]
    for a in anomalies:
        lines.append(
            f"  [{a['severity']:8s}] {a['metric_name']} = {a['current_value']:.4f}"
            f"  via {a['method']}"
        )

    try:
        send_alert("\n".join(lines), "WARN")
    except Exception as exc:
        logger.error("Failed to send anomaly alert: %s", exc)
