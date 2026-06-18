"""
core/anomaly_detector.py
------------------------
Statistical anomaly detection on DQ metrics history.

Replaces the simple rolling-average baseline comparison with industry-standard
z-score and IQR-based detection.  A z-score of 4.2 is far more actionable than
"score dropped 5%" — it tells you exactly how unusual this run's result is
relative to the full historical distribution.

Metrics analysed (all sourced from dq_metrics_summary)
-------------------------------------------------------
dq_score           — weighted DQ score (higher = better)
avg_failure_pct    — average failure % across rules (lower = better)
failed_rule_pct    — failed_rules / total_rules * 100 (lower = better)

Detection methods
-----------------
ZSCORE  z = (x − μ) / σ  — flag when |z| > zscore_threshold (default 3.0)
IQR     fence = Q1 − k*IQR  /  Q3 + k*IQR  — flag when x outside fence
BOTH    flag when either method fires

Configuration (per project / process / run_type)
-------------------------------------------------
Insert a row into dq_anomaly_config.  NULL in project_name, process_name, or
run_type acts as a wildcard that matches any value.  Most-specific matching
row (fewest NULLs) wins; if none found, built-in defaults are used.

    INSERT INTO {meta_db}.dq_anomaly_config
        (config_id, project_name, process_name, run_type,
         method, zscore_threshold, iqr_multiplier,
         min_history_runs, alert_on_anomaly)
    VALUES (1, 'CLAIMS', 'MEMBER', NULL,
            'BOTH', 3.0, 1.5, 10, 1);

Public API
----------
detect_and_log(td_conn, run, meta_db)  ->  list[dict]
    Returns list of anomaly dicts (one per metric per detection method).
"""

import logging
import statistics
from typing import List, Optional, Tuple

from utils.alert import send_alert

logger = logging.getLogger(__name__)

# Built-in defaults — used when no dq_anomaly_config row matches
_DEFAULT_METHOD           = "ZSCORE"
_DEFAULT_ZSCORE_THRESHOLD = 3.0
_DEFAULT_IQR_MULTIPLIER   = 1.5
_DEFAULT_MIN_HISTORY      = 10
_DEFAULT_ALERT            = True


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
    from core.executor import execute_query, bulk_insert

    project  = run.get("project_name", "")
    process  = run.get("process_name", "")
    run_type = run.get("run_type", "")
    run_id   = run.get("run_id", "")

    # Load detection config (most-specific match wins)
    cfg = _load_config(td_conn, project, process, run_type, meta_db, execute_query)

    method    = cfg["method"].upper()
    zthresh   = cfg["zscore_threshold"]
    iqr_mult  = cfg["iqr_multiplier"]
    min_hist  = cfg["min_history_runs"]
    do_alert  = cfg["alert_on_anomaly"]

    # Load current run's metrics
    current = _load_current_metrics(td_conn, project, process, run_type,
                                     run_id, meta_db, execute_query)
    if current is None:
        logger.debug("No metrics row found for run %s — skipping anomaly detection.", run_id)
        return []

    # Load historical metrics (excluding current run)
    history = _load_history(td_conn, project, process, run_type,
                            run_id, meta_db, execute_query)

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
        hist_clean = [v for v in hist_vals if v is not None]
        if len(hist_clean) < min_hist:
            continue

        hist_mean = statistics.mean(hist_clean)
        hist_std  = None
        try:
            hist_std = statistics.stdev(hist_clean)
        except statistics.StatisticsError:
            pass

        detections = []

        # ── Z-score detection ─────────────────────────────────────────────
        if method in ("ZSCORE", "BOTH") and hist_std is not None and hist_std > 0:
            z = (current_val - hist_mean) / hist_std
            # For "higher is better" metrics (dq_score), a large negative z is the anomaly.
            # For "lower is better" (failure rates), a large positive z is the anomaly.
            is_anomaly = abs(z) > zthresh
            sev        = _zscore_severity(abs(z), zthresh)
            detections.append({
                "method":    "ZSCORE",
                "z_score":   round(z, 4),
                "is_anomaly": is_anomaly,
                "severity":  sev,
            })
            if is_anomaly:
                direction = "below" if z < 0 else "above"
                logger.warning(
                    "ANOMALY [z-score] %s: %s | current=%.4f μ=%.4f σ=%.4f z=%.2f (%s by %.1fσ)",
                    run_id, metric_name, current_val, hist_mean,
                    hist_std, z, direction, abs(z),
                )

        # ── IQR detection ─────────────────────────────────────────────────
        if method in ("IQR", "BOTH") and len(hist_clean) >= 4:
            lower, upper = _iqr_bounds(hist_clean, iqr_mult)
            is_anomaly   = (current_val < lower) or (current_val > upper)
            sev          = _iqr_severity(current_val, lower, upper)
            detections.append({
                "method":      "IQR",
                "iqr_lower":   round(lower, 4),
                "iqr_upper":   round(upper, 4),
                "is_anomaly":  is_anomaly,
                "severity":    sev,
            })
            if is_anomaly:
                logger.warning(
                    "ANOMALY [IQR] %s: %s | current=%.4f bounds=[%.4f, %.4f]",
                    run_id, metric_name, current_val, lower, upper,
                )

        # Persist each detection as a separate row
        for det in detections:
            if det["is_anomaly"]:
                anomalies.append({
                    "metric_name":   metric_name,
                    "current_value": current_val,
                    "method":        det["method"],
                    "severity":      det["severity"],
                })

            insert_rows.append((
                run_id,
                project,
                process,
                run_type,
                metric_name,
                current_val,
                round(hist_mean, 6),
                round(hist_std, 6) if hist_std is not None else None,
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
            sql = f"""
                INSERT INTO {meta_db}.dq_anomaly_log (
                    run_id, project_name, process_name, run_type,
                    metric_name, current_value, historical_mean, historical_std,
                    z_score, iqr_lower_bound, iqr_upper_bound,
                    is_anomaly, detection_method, severity, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """
            bulk_insert(td_conn, sql, insert_rows)
        except Exception as exc:
            logger.error("Failed to write dq_anomaly_log: %s", exc)

    # Send alert if anomalies found and configured
    if anomalies and do_alert:
        _send_anomaly_alert(run, anomalies)

    return anomalies


# ---------------------------------------------------------------------------
# Math helpers
# ---------------------------------------------------------------------------

def _iqr_bounds(data: list, multiplier: float) -> Tuple[float, float]:
    """Return (lower_fence, upper_fence) using Tukey IQR method."""
    sorted_data = sorted(data)
    n           = len(sorted_data)
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
    td_conn, project, process, run_type, run_id, meta_db, execute_query
) -> Optional[dict]:
    """Load the dq_metrics_summary row for the just-completed run."""
    try:
        rows = execute_query(
            td_conn,
            f"""
            SELECT *
            FROM {meta_db}.dq_metrics_summary
            WHERE project_name = ?
              AND process_name = ?
              AND run_type     = ?
            ORDER BY created_at DESC
            """,
            [project, process, run_type],
        )
        return rows[0] if rows else None
    except Exception as exc:
        logger.warning("Could not load current metrics for anomaly detection: %s", exc)
        return None


def _load_history(
    td_conn, project, process, run_type, current_run_id, meta_db, execute_query
) -> list:
    """
    Load historical dq_metrics_summary rows for the same project/process/run_type,
    excluding the current run's rows.

    Returns up to 100 most recent rows (sufficient for statistical purposes).
    """
    try:
        return execute_query(
            td_conn,
            f"""
            SELECT dq_score, avg_failure_pct, total_rules, failed_rules
            FROM {meta_db}.dq_metrics_summary
            WHERE project_name = ?
              AND process_name = ?
              AND run_type     = ?
            ORDER BY created_at DESC
            """,
            [project, process, run_type],
        )[:100]
    except Exception as exc:
        logger.warning("Could not load metrics history for anomaly detection: %s", exc)
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
        "method":           (best.get("method") or _DEFAULT_METHOD).upper(),
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
        f"DQ ANOMALY DETECTED",
        f"",
        f"Run ID   : {run_id}",
        f"Project  : {project}",
        f"Process  : {process}",
        f"Run Type : {run_type}",
        f"",
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
