"""
sampling/engine.py
-------------------
The Sampling Framework's entry point: config-driven stratified/ranked
sample selection.

This is a SEPARATE FRAMEWORK from the DQ Rules Engine in rules_engine/ — not a
submodule of it. It answers a different question than dq_rules does.
Rules ask "is this row valid?" (pass/fail against the whole universe).
Sampling asks "which N cases, out of a clean universe, are the
highest-value ones for a human reviewer to look at this cycle?" That's a
ranking/quota problem, not a pass/fail problem, with its own config table
(dq_sampling_config), its own output table (dq_sample_selections), and its
own algorithm — it doesn't extend or wrap rule evaluation, and nothing in
rules_engine/ imports from this package.

What it reuses from the rules engine, as a plain library dependency (the
direction only ever goes this way — rules_engine/ never imports from sampling/):
  - rules_engine.executor.execute_query / bulk_insert — the same thin DB-driver
    wrappers every adapter already uses; no reason to reimplement them.
  - rules_engine.rule_sql.build_filter — the same BATCH/DATE/FULL run-mode
    scoping machinery every rule uses, so a sampling config's
    scope_column gets identical date-window handling for free.
  - db.connection_factory.ConnectionFactory — the shared connector layer
    (Teradata/Postgres/S3/etc.) — sampling pulls its universe table
    through the exact same adapters rules do.
  - rules_engine.metrics.evaluate_metric_drift, via sampling/anomaly.py — the
    same z-score/IQR statistics rules_engine/ uses for DQ-score anomaly
    detection, reused here to flag candidate-pool volume drift. See
    sampling/anomaly.py's module docstring for why this is a sanctioned,
    narrowly-scoped exception rather than a re-coupling: it's one pure,
    DB-free function, never the rules-engine metrics tables themselves.
That's the full extent of the coupling: three function imports and the
connection factory. A deployment could run the sampling framework against
a metadata store that has never run a single dq_rules row, and vice versa.

Every project-specific detail lives in config (dq_sampling_config), not
code: which connection/table to pull from, which column holds the
stratification dimensions, the target mix percentages for each, an
exclusion WHERE-fragment, and a priority ORDER BY expression. A different
project defines a completely different sampling scheme by inserting a
different dq_sampling_config row — this module never changes, and it has
no knowledge of any specific project's team names, case categories, or
review cadence.

Algorithm
---------
1. Pull all in-scope, non-excluded candidate rows from the source table
   (exclusion_sql is a WHERE-fragment; matching rows are EXCLUDED).
2. Compute each row's priority rank via priority_rank_sql (lower = picked
   first) — an arbitrary SQL ORDER BY expression, chained by the config
   author to encode whatever priority order their process needs.
3. Bucket candidates by determination_column, apply determination_mix_json
   to compute each bucket's target count (target_volume * mix fraction).
4. Within each bucket, sub-bucket by functional_area_column and apply
   functional_area_mix_json the same way (any category not named in the
   config JSON falls into an implicit "remainder" bucket that absorbs the
   leftover share, rather than being silently dropped).
5. Within each stratum, take the top-N candidates by priority rank until
   that stratum's target is met.
6. Persist EVERY candidate considered — selected or not — to
   dq_sample_selections (audit defensibility: "why wasn't case X selected"
   must be answerable later, not just "here are the N selected"). Never
   updated after the run; a re-run uses a new sample_run_id.

Public API
----------
run_stratified_sampling(cf, td, config: dict, run: dict, meta_db: str) -> dict
"""

import json
import logging
import math
import re
from datetime import datetime

logger = logging.getLogger(__name__)


def run_stratified_sampling(cf, td, config: dict, run: dict, meta_db: str) -> dict:
    """
    Execute one stratified sampling pass.

    Parameters
    ----------
    cf      : ConnectionFactory — used to fetch the source connection named
              in config['connection_name']
    td      : metadata connection (writes to dq_sample_selections)
    config  : a row from dq_sampling_config (dict)
    run     : run context dict — must include run_id and, if config declares
              scope_column, start_date/end_date to scope to this cycle's pull
    meta_db : metadata schema name

    Returns
    -------
    dict summary: {sample_run_id, candidates, selected, target_volume, by_stratum}
    """
    from rules_engine.executor import execute_query, bulk_insert
    from rules_engine.rule_sql import build_filter, check_no_dml_ddl
    from sampling.anomaly import detect_candidate_pool_drift

    # sample_name is NOT NULL on dq_sampling_config — no project_name/
    # process_name fallback needed (config no longer carries those columns
    # anyway; it's scope_id-keyed, see ddl_shared.sql v7).
    sample_name = _slug(config["sample_name"])
    sample_run_id = f"{sample_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    conn_name = config["connection_name"]
    db_conn   = cf.get(conn_name)
    if db_conn is None:
        raise RuntimeError(f"Stratified sampling: connection '{conn_name}' unavailable.")

    table       = config["universe_table"]
    key_cols    = [c.strip() for c in config["key_columns"].split(",") if c.strip()]
    determ_col  = config.get("determination_column")
    func_col    = config.get("functional_area_column")
    exclusion   = (config.get("exclusion_sql") or "").strip()
    priority    = (config.get("priority_rank_sql") or "").strip()
    target_vol  = int(config.get("target_volume") or 100)

    # exclusion_sql / priority_rank_sql are operator-authored SQL fragments
    # from dq_sampling_config, embedded directly into the query built below
    # -- the same trust level as dq_rules.rule_syntax, which gets this exact
    # guard (see rules_engine/rule_sql.py::check_no_dml_ddl's module docstring
    # for why a data-modifying CTE still parses as part of a read and would
    # otherwise execute as a side effect the moment the query runs).
    if exclusion:
        check_no_dml_ddl(exclusion, config.get("sample_name"))
    if priority:
        check_no_dml_ddl(priority, config.get("sample_name"))

    try:
        determ_mix = json.loads(config.get("determination_mix_json") or "{}")
        func_mix   = json.loads(config.get("functional_area_mix_json") or "{}")
    except (json.JSONDecodeError, TypeError) as exc:
        logger.error(
            "Malformed mix JSON in dq_sampling_config for '%s' (config_id=%s): %s",
            config.get("sample_name"), config.get("config_id"), exc, exc_info=True,
        )
        raise ValueError(
            f"dq_sampling_config row '{config.get('sample_name')}' "
            f"(config_id={config.get('config_id')}) has malformed "
            f"determination_mix_json/functional_area_mix_json: {exc}"
        ) from exc

    # ── Scope filter (this cycle's pull) — reuses the same generic
    #    filter-building machinery every rule uses, so BATCH/DATE/FULL
    #    run-mode scoping works identically here. ─────────────────────────
    scope_rule = {
        "filter_column": config.get("scope_column"),
        "filter_type":   "DATE" if run.get("start_date") else None,
    }
    scope_where = build_filter(scope_rule, run) if config.get("scope_column") else "1=1"

    where_parts = [scope_where]
    if exclusion:
        where_parts.append(f"NOT ({exclusion})")
    where_clause = " AND ".join(f"({w})" for w in where_parts)

    order_clause = f"ORDER BY {priority}" if priority else ""
    select_cols  = ", ".join(key_cols + [c for c in (determ_col, func_col) if c])

    query = f"SELECT {select_cols} FROM {table} WHERE {where_clause} {order_clause}"
    logger.info("Stratified sampling query (sample_run_id=%s):\n%s", sample_run_id, query)

    candidates = execute_query(db_conn, query)
    logger.info("Stratified sampling: %d candidate rows after exclusions.", len(candidates))

    # Candidate-pool volume drift check (config_id-scoped, against this
    # config's own history in dq_sample_selections) -- non-fatal, mirrors
    # how rules_engine/engine.py treats calculate_metrics()/detect_anomalies() as
    # opt-in observability that must never block the run itself.
    drift = {}
    try:
        drift = detect_candidate_pool_drift(td, config, sample_run_id, len(candidates), meta_db)
    except Exception as exc:
        logger.error("Candidate-pool drift detection failed (non-fatal): %s", exc, exc_info=True)

    if not candidates:
        return {"sample_run_id": sample_run_id, "candidates": 0,
                "selected": 0, "target_volume": target_vol, "by_stratum": {},
                "volume_drift": drift}

    # Priority already applied via ORDER BY — assign an explicit rank so the
    # persisted record is self-describing even without re-running the query.
    for i, row in enumerate(candidates, start=1):
        row["_priority_rank"] = i

    # ── Stratify: primary bucket -> secondary sub-bucket ──────────────────
    primary_buckets: dict = {}
    for row in candidates:
        dval = str(row.get(determ_col)) if determ_col else "ALL"
        primary_buckets.setdefault(dval, []).append(row)

    selected_rows = []
    by_stratum    = {}

    for dval, drows in primary_buckets.items():
        primary_target = _target_for_bucket(dval, determ_mix, target_vol)

        if func_col:
            sub_buckets: dict = {}
            for row in drows:
                fval = str(row.get(func_col)) if row.get(func_col) is not None else "UNSPECIFIED"
                sub_buckets.setdefault(fval, []).append(row)

            for fval, frows in sub_buckets.items():
                sub_target = _target_for_bucket(fval, func_mix, primary_target)
                take = frows[:sub_target]   # already priority-ordered
                selected_rows.extend(take)
                by_stratum[f"{dval}/{fval}"] = {"candidates": len(frows), "selected": len(take)}
        else:
            take = drows[:primary_target]
            selected_rows.extend(take)
            by_stratum[dval] = {"candidates": len(drows), "selected": len(take)}

    # If stratified quotas under-filled relative to target_vol (e.g. a thin
    # cycle), top up from the highest-priority remaining candidates overall
    # so the sample doesn't fall short of the target for lack of volume in
    # one stratum, without violating exclusions.
    if len(selected_rows) < target_vol:
        selected_keys = {_row_key(r, key_cols) for r in selected_rows}
        remaining = [r for r in candidates if _row_key(r, key_cols) not in selected_keys]
        remaining.sort(key=lambda r: r["_priority_rank"])
        shortfall = target_vol - len(selected_rows)
        selected_rows.extend(remaining[:shortfall])

    selected_rows = selected_rows[:target_vol]
    selected_keys = {_row_key(r, key_cols) for r in selected_rows}

    # ── Persist EVERY candidate (audit defensibility) ─────────────────────
    # project_name/process_name are NOT stored — derivable via config_id ->
    # dq_sampling_config.scope_id (see ddl_shared.sql v7).
    insert_sql = f"""
        INSERT INTO {meta_db}.dq_sample_selections (
            sample_run_id, config_id, sample_cycle,
            case_key, determination_type, functional_area, priority_rank,
            excluded_flag, exclusion_reason, selected_flag, strata_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """
    today = datetime.utcnow().date()
    insert_rows = []
    for row in candidates:
        key = _row_key(row, key_cols)
        is_selected = key in selected_keys
        insert_rows.append((
            sample_run_id,
            config.get("config_id"),
            today,
            key,
            str(row.get(determ_col)) if determ_col else None,
            str(row.get(func_col)) if func_col else None,
            row["_priority_rank"],
            0, None,
            1 if is_selected else 0,
            json.dumps({k: str(v) for k, v in row.items() if not k.startswith("_")}),
        ))
    bulk_insert(td, insert_sql, insert_rows)

    logger.info(
        "Stratified sampling complete: sample_run_id=%s candidates=%d selected=%d/%d target.",
        sample_run_id, len(candidates), len(selected_rows), target_vol,
    )

    return {
        "sample_run_id": sample_run_id,
        "candidates":    len(candidates),
        "selected":      len(selected_rows),
        "target_volume": target_vol,
        "by_stratum":    by_stratum,
        "volume_drift":  drift,
    }


def _target_for_bucket(bucket_value: str, mix: dict, total: int) -> int:
    """
    Return the integer target count for a stratum.

    mix values are exact-match keys mapping to a fraction of `total` (e.g.
    {"A": 0.80, "B": 0.10}). Any bucket_value not present in `mix` shares the
    REMAINDER fraction (1 - sum(named fractions)) — this is how an
    open-ended "everything else" category is expressed without hardcoding
    category names anywhere in this module.
    """
    if not mix:
        return total   # no stratification configured — take everything in this bucket

    if bucket_value in mix:
        return max(0, math.floor(total * float(mix[bucket_value])))

    named_fraction = sum(float(v) for v in mix.values())
    remainder_fraction = max(0.0, 1.0 - named_fraction)
    return max(0, math.floor(total * remainder_fraction))


def _row_key(row: dict, key_cols: list) -> str:
    return "|".join(f"{c}={row.get(c)}" for c in key_cols)


def _slug(value: str) -> str:
    """Turn a free-text sample_name/process_name into a filesystem/id-safe token."""
    return re.sub(r"[^A-Za-z0-9]+", "_", value.strip()).strip("_").upper() or "SAMPLE"
