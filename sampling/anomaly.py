"""
sampling/anomaly.py
---------------------
Candidate-pool volume drift detection for the Sampling Framework.

Reuses core.metrics.evaluate_metric_drift() (z-score/IQR statistics) as a
plain library call -- the same "sampling/ may use core/ as a library,
core/ never imports sampling/" rule documented in DESIGN.md that
sampling/engine.py already follows for core.executor/core.rule_sql.
core/ has no knowledge this module exists.

No new metadata table is needed: candidate-pool volume history is fully
recoverable from dq_sample_selections, which run_stratified_sampling()
already writes one row per candidate per sample_run_id to (see
sampling/engine.py). This module is pure read + pure math on top of that.

Why volume, specifically: a stratified sample's quality depends on the
candidate pool being roughly the size it always is. A silent drop usually
means an upstream feed broke and fewer rows landed than normal; a silent
jump can mean a universe_table/exclusion_sql config change let unintended
rows through. Both are worth a human's attention before N cases get
pulled from a pool that's quietly the wrong size -- catching that here is
cheaper than someone noticing months later that a whole review cycle drew
from a broken pool.

Public API
----------
detect_candidate_pool_drift(td, config: dict, sample_run_id: str,
                             candidate_count: int, meta_db: str) -> dict
"""

import logging

from core.metrics import evaluate_metric_drift
from utils.alert import send_alert
from utils.metadata_writers import log_message

logger = logging.getLogger(__name__)

_DEFAULT_ZSCORE_THRESHOLD = 3.0
_DEFAULT_MIN_HISTORY      = 4     # need >= 4 clean history points for the IQR fence anyway
_HISTORY_LOOKBACK         = 20    # most-recent N prior runs considered "history"


def detect_candidate_pool_drift(
    td,
    config: dict,
    sample_run_id: str,
    candidate_count: int,
    meta_db: str,
    min_history: int = _DEFAULT_MIN_HISTORY,
    zscore_threshold: float = _DEFAULT_ZSCORE_THRESHOLD,
    alert_on_anomaly: bool = True,
) -> dict:
    """
    Compare this run's candidate-pool size against the same sampling
    config's own history (config_id-scoped -- a different sample_name/
    universe_table has its own independent volume baseline).

    Called by run_stratified_sampling() (sampling/engine.py) after the
    candidate pool is pulled, non-fatally -- a drift-detection failure
    must never block a sampling run from completing, the same principle
    core/engine.py applies to its own post-run metrics/anomaly steps.

    Returns {} if there isn't enough history yet or nothing looks
    anomalous; otherwise a dict describing the drift (method, severity,
    z_score/iqr bounds, historical_mean/historical_std, plus
    sample_run_id/sample_name/candidate_count for context) -- the same
    shape core.metrics.evaluate_metric_drift() returns per detection,
    flattened with the run context merged in.
    """
    from core.executor import execute_query

    config_id   = config.get("config_id")
    sample_name = config.get("sample_name", "")

    try:
        history_rows = execute_query(td, f"""
            SELECT sample_run_id, COUNT(*) AS candidate_count
            FROM {meta_db}.dq_sample_selections
            WHERE config_id = ? AND sample_run_id <> ?
            GROUP BY sample_run_id
            ORDER BY MAX(sample_cycle) DESC
        """, [config_id, sample_run_id])
    except Exception as exc:
        logger.warning(
            "Could not load candidate-pool history for config_id=%s (non-fatal): %s",
            config_id, exc,
        )
        return {}

    history = [r["candidate_count"] for r in history_rows[:_HISTORY_LOOKBACK]]

    detections = evaluate_metric_drift(
        candidate_count, history, method="BOTH",
        zscore_threshold=zscore_threshold, min_history=min_history,
    )
    anomaly = next((d for d in detections if d["is_anomaly"]), None)
    if anomaly is None:
        return {}

    direction = "dropped" if candidate_count < anomaly["historical_mean"] else "spiked"
    message = (
        f"[SAMPLING] Candidate pool {direction} for '{sample_name}' "
        f"(config_id={config_id}): {candidate_count} candidates this cycle vs "
        f"historical mean {anomaly['historical_mean']:.1f} over {len(history)} prior run(s) "
        f"({anomaly['method']}, severity={anomaly['severity']}). sample_run_id={sample_run_id}"
    )
    logger.warning(message)

    try:
        log_message(td, sample_run_id, "WARN", message, meta_db=meta_db)
    except Exception as exc:
        logger.debug("Could not write dq_run_logs for sampling drift (non-fatal): %s", exc)

    if alert_on_anomaly:
        try:
            send_alert(message, "WARN")
        except Exception as exc:
            logger.debug("Sampling drift alert dispatch failed (non-fatal): %s", exc)

    return {
        "sample_run_id":   sample_run_id,
        "sample_name":     sample_name,
        "config_id":       config_id,
        "candidate_count": candidate_count,
        "history_points":  len(history),
        **anomaly,
    }
