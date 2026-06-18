"""
core/evaluator.py
-----------------
Determines the DQ status (PASS / FAIL / WARN) for a single rule execution.

Fix #13 (v2): `require_rows` parameter — when True, a table with zero records
returns FAIL/WARN instead of the previous silent PASS.  Controlled by
dq_rules.require_rows (BYTEINT, default 0).

Fix #14 (v2): `threshold_operator` parameter — supports 'OR' (default,
existing behaviour) and 'AND' (breach requires BOTH pct AND count to exceed
their thresholds simultaneously).  Controlled by dq_rules.threshold_operator.
"""

import logging

logger = logging.getLogger(__name__)


def evaluate_rule(
    total: int,
    failed: int,
    threshold_pct=None,         # float | None
    threshold_count=None,       # int   | None
    severity: str = "WARN",
    require_rows: bool = False,  # v2: True → empty table is a breach
    threshold_operator: str = "OR",  # v2: 'OR' | 'AND'
) -> str:
    """
    Determine the DQ status for one rule execution.

    Severity semantics
    ------------------
    "ERROR"  → a breach returns "FAIL"
    anything else (WARN, etc.) → a breach returns "WARN"

    Evaluation logic
    ----------------
    1. total == 0 and require_rows=False  → PASS  (no data; not required)
    2. total == 0 and require_rows=True   → FAIL / WARN  (data was expected)
    3. failed == 0                        → PASS
    4. threshold_operator='OR'  (default) → breach if pct OR count exceeded
    5. threshold_operator='AND'           → breach only if BOTH exceeded
    6. No thresholds configured           → any failure triggers breach
    7. Thresholds configured but not met  → PASS
    """
    severity           = (severity           or "WARN").upper()
    threshold_operator = (threshold_operator or "OR" ).upper()

    # ── Empty-table handling ──────────────────────────────────────────────────
    if total == 0:
        if require_rows:
            outcome = "FAIL" if severity == "ERROR" else "WARN"
            logger.info("total=0 with require_rows=True → %s.", outcome)
            return outcome
        logger.info("total=0 — PASS (no data; require_rows=False).")
        return "PASS"

    if failed == 0:
        logger.info("failed=0 — PASS.")
        return "PASS"

    # ── Threshold breach evaluation ───────────────────────────────────────────
    pct = (failed / total) * 100

    count_breached = (threshold_count is not None) and (failed > threshold_count)
    pct_breached   = (threshold_pct   is not None) and (pct   > threshold_pct)

    if threshold_count is not None or threshold_pct is not None:
        # At least one threshold is configured
        if threshold_operator == "AND":
            # Both must be set and both must be breached
            both_set    = (threshold_count is not None) and (threshold_pct is not None)
            breached    = both_set and count_breached and pct_breached
            if not both_set:
                # Only one threshold set with AND operator — treat as OR
                breached = count_breached or pct_breached
        else:
            # OR (default): either threshold breaching is enough
            breached = count_breached or pct_breached
    else:
        # No thresholds configured — any failure is a breach
        breached = True

    if breached:
        outcome = "FAIL" if severity == "ERROR" else "WARN"
        logger.info(
            "Breach detected | pct=%.2f threshold_pct=%s count=%d threshold_count=%s "
            "operator=%s severity=%s → %s.",
            pct, threshold_pct, failed, threshold_count,
            threshold_operator, severity, outcome,
        )
        return outcome

    logger.info(
        "Thresholds not breached | pct=%.2f threshold_pct=%s count=%d threshold_count=%s "
        "operator=%s → PASS.",
        pct, threshold_pct, failed, threshold_count, threshold_operator,
    )
    return "PASS"
