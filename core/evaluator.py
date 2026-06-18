import logging

logger = logging.getLogger(__name__)


def evaluate_rule(
    total: int,
    failed: int,
    threshold_pct=None,    # float | None
    threshold_count=None,  # int   | None
    severity: str = "WARN",
) -> str:
    """
    Determine the DQ status for a single rule execution.

    Severity semantics:
      - "ERROR"  → breaches return "FAIL"
      - anything else (WARN, WARNING, etc.) → breaches return "WARN"

    Logic:
      1. total == 0          → PASS  (no data to evaluate)
      2. failed == 0         → PASS
      3. threshold_count set and failed > threshold_count → FAIL / WARN
      4. threshold_pct   set and pct    > threshold_pct   → FAIL / WARN
      5. Neither threshold set and failed > 0             → FAIL / WARN
      6. Thresholds set but not breached                  → PASS
    """
    severity = (severity or "WARN").upper()

    if total == 0:
        logger.info("total=0 — PASS (no data).")
        return "PASS"

    if failed == 0:
        logger.info("failed=0 — PASS.")
        return "PASS"

    pct = (failed / total) * 100
    breached = False

    if threshold_count is not None and failed > threshold_count:
        logger.info("failed %d > threshold_count %d → breach.", failed, threshold_count)
        breached = True

    if threshold_pct is not None and pct > threshold_pct:
        logger.info("pct %.2f > threshold_pct %.2f → breach.", pct, threshold_pct)
        breached = True

    if threshold_count is None and threshold_pct is None:
        # No thresholds configured — any failure triggers the rule
        breached = True

    if breached:
        outcome = "FAIL" if severity == "ERROR" else "WARN"
        logger.info("Severity=%s → %s.", severity, outcome)
        return outcome

    logger.info("Thresholds not breached — PASS.")
    return "PASS"
