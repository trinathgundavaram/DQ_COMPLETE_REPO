import logging
import time
from datetime import datetime

from rules_engine.db_ops import (
    execute_dml, execute_query, bulk_insert_or_skip, bulk_execute,
    _substitute_params, _run_source_query, _escape_sql_literal,
    build_extra_filters_clause, log_error, EXCEPTION_CHUNK,
)

logger = logging.getLogger(__name__)


_SOFT_SEVERITIES = {"WARN", "WARNING", "INFO", "NOTICE"}


def _log_error(meta_conn, meta_db: str, run_id: str, rule: dict, run_key: str,
                error_type: str, message: str, detail: str = None) -> None:
    log_error(meta_conn, meta_db, run_id, rule.get("rule_id"), rule.get("rule_group"),
              run_key, error_type, message, detail, rule_variant=rule.get("rule_variant"))


def _scan_violations(db_conn, query: str, src_key_cols: list = None) -> tuple:
    lowered_key_cols = {c.strip().lower() for c in (src_key_cols or []) if c.strip()}

    cursor = db_conn.cursor()
    try:
        cursor.execute(query)
        if cursor.description is None:
            return 0, []
        columns = [c[0].lower() for c in cursor.description]


        key_idx = [i for i, c in enumerate(columns) if c in lowered_key_cols]

        failed = 0
        rows = []
        while True:
            batch = cursor.fetchmany(EXCEPTION_CHUNK)
            if not batch:
                break
            for r in batch:
                failed += 1
                if key_idx:
                    rows.append({columns[i]: r[i] for i in key_idx})
                else:
                    rows.append(dict(zip(columns, r)))
        return failed, rows
    finally:
        cursor.close()


def _distinct_failed(rule: dict, rows: list) -> int:
    keys = set()
    for row in rows:
        try:
            keys.add(build_src_key(rule, row))
        except Exception:
            keys.add(tuple(sorted(row.items(), key=lambda kv: kv[0])))
    return len(keys)


def _format_src_key(cols: list, row: dict) -> str:
    def _fmt(c):
        key = c.lower()
        if key not in row:
            raise KeyError(
                f"src_key_cols column '{c}' not found among this rule's result "
                f"columns {sorted(row.keys())} -- gre_rules.src_key_cols must name "
                "columns rule_syntax actually SELECTs (case-insensitive)."
            )
        v = row[key]
        return "NULL" if v is None else v
    return "|".join(f"{c}={_fmt(c)}" for c in cols)


def split_src_key_cols(rule: dict) -> list:
    return [c.strip() for c in (rule.get("src_key_cols") or "").split(",") if c.strip()]


def build_src_key(rule: dict, row: dict) -> str:
    cols = split_src_key_cols(rule)
    if not cols:
        raise ValueError(
            f"rule_id={rule.get('rule_id')} has no src_key_cols -- "
            "every rule must declare one to write idempotent exception rows."
        )
    return _format_src_key(cols, row)


def parse_src_key(src_key_value: str) -> dict:
    parsed = {}
    for segment in (src_key_value or "").split("|"):
        if not segment:
            continue
        col, _, val = segment.partition("=")
        parsed[col] = None if val == "NULL" else val
    return parsed


_TIEBACK_SQL_DIALECTS = {"teradata", "postgres"}


def _tieback_split_expr(dialect: str, src_key_value_expr: str, token_index: int, token_count: int) -> str:
    token = src_key_value_expr if token_count == 1 else None
    if dialect == "teradata":
        if token is None:
            token = f"STRTOK({src_key_value_expr}, '|', {token_index})"
        return f"STRTOK({token}, '=', 2)"


    if token is None:
        token = f"split_part({src_key_value_expr}, '|', {token_index})"
    return f"split_part({token}, '=', 2)"


def _tieback_scope_clause(scope_params: dict, alias: str) -> str:
    if not scope_params:
        return ""
    clauses = [
        f"{alias}.{key} = '{_escape_sql_literal(value)}'"
        for key, value in sorted(scope_params.items())
    ]
    return "AND " + " AND ".join(clauses)


def build_source_tieback_sql(rule: dict, run_key: str, meta_db: str, scope_params: dict = None) -> str:
    dialect = (rule.get("sql_dialect") or "").lower()
    if dialect not in _TIEBACK_SQL_DIALECTS:
        return None

    cols = split_src_key_cols(rule)
    if not cols:
        return None

    rule_id = rule.get("rule_id")
    database_name = rule.get("database_name")
    src_tbl_nm = rule.get("src_tbl_nm")
    src_table = f"{database_name}.{src_tbl_nm}" if database_name else src_tbl_nm

    conditions = []
    for i, col in enumerate(cols, start=1):
        value_expr = _tieback_split_expr(dialect, "e.src_key_value", i, len(cols))


        conditions.append(
            f"(({value_expr} = 'NULL' AND s.{col} IS NULL) "
            f"OR ({value_expr} <> 'NULL' AND s.{col} = {value_expr}))"
        )
    where_join = "\n  AND ".join(conditions)
    scope_clause = _tieback_scope_clause(scope_params, "s")

    return (
        f"SELECT s.*, e.record_id AS _record_id, e.rule_id AS _rule_id, e.rule_nm AS _rule_nm,\n"
        f"       e.process_name AS _process_name, e.project_name AS _project_name,\n"
        f"       e.src_key_value AS _src_key_value,\n"
        f"       e.exception_flag AS _exception_flag\n"
        f"FROM {src_table} s\n"
        f"JOIN {meta_db}.gre_exceptions e\n"
        f"  ON {where_join}\n"
        f"WHERE e.rule_id = {rule_id}\n"
        f"  AND e.run_key = '{_escape_sql_literal(run_key)}'\n"
        f"  AND e.etl_is_curr_ind = 'Y'"
        + (f"\n  {scope_clause}" if scope_clause else "")
    )


def evaluate_threshold(
    total: int,
    failed: int,
    threshold_pct=None,
    threshold_count=None,
    threshold_operator: str = "OR",
    severity: str = "Data Validation Error",
) -> dict:
    severity = (severity or "").upper()
    threshold_operator = (threshold_operator or "OR").upper()

    if total == 0:
        return {
            "status": "PASS",
            "write_result": False,
            "threshold_pct_used": None,
            "threshold_count_used": None,
            "threshold_operator_used": None,
        }

    has_threshold = threshold_pct is not None or threshold_count is not None

    if has_threshold:
        pct = (failed / total) * 100
        pct_breached = threshold_pct is not None and pct > threshold_pct
        count_breached = threshold_count is not None and failed > threshold_count

        if threshold_operator == "AND":
            both_set = threshold_pct is not None and threshold_count is not None
            breached = (pct_breached and count_breached) if both_set else (pct_breached or count_breached)
        else:
            breached = pct_breached or count_breached

        status = ("WARN" if severity in _SOFT_SEVERITIES else "FAIL") if breached else "PASS"

        return {
            "status": status,
            "write_result": True,
            "threshold_pct_used": threshold_pct,
            "threshold_count_used": threshold_count,
            "threshold_operator_used": threshold_operator,
        }


    breached = failed > 0
    if not breached:
        return {
            "status": "PASS",
            "write_result": False,
            "threshold_pct_used": None,
            "threshold_count_used": None,
            "threshold_operator_used": None,
        }

    status = "WARN" if severity in _SOFT_SEVERITIES else "FAIL"
    return {
        "status": status,
        "write_result": True,
        "threshold_pct_used": None,
        "threshold_count_used": 0,
        "threshold_operator_used": threshold_operator,
    }


def _write_exceptions(meta_conn, meta_db: str, rule: dict, run_id: str, run_key: str, rows: list,
                       run_params: dict = None) -> dict:
    run_params = run_params or {}
    rule_id = rule.get("rule_id")


    if rows:
        key_cols = split_src_key_cols(rule)
        missing = [c for c in key_cols if c.strip().lower() not in rows[0]]
        if missing:
            raise KeyError(
                f"rule_id={rule_id}: src_key_cols {missing} not found among "
                f"result columns {sorted(rows[0].keys())} -- gre_rules.src_key_cols "
                "must name columns rule_syntax actually SELECTs (case-insensitive)."
            )


    new_rows_by_key = {}
    key_build_errors = 0
    for row in rows:
        try:
            nk = build_src_key(rule, row)
        except Exception:
            key_build_errors += 1
            continue
        new_rows_by_key.setdefault(nk, row)

    if key_build_errors:
        logger.warning(
            "rule_id=%s run_key=%s: %d/%d violating rows failed key-building "
            "and were skipped this attempt",
            rule_id, run_key, key_build_errors, len(rows),
        )
        _log_error(
            meta_conn, meta_db, run_id, rule, run_key, "KEY_BUILD_FAILURE",
            f"{key_build_errors}/{len(rows)} violating rows failed key-building",
        )

    if len(rows) > len(new_rows_by_key) + key_build_errors:
        collapsed = len(rows) - key_build_errors - len(new_rows_by_key)
        logger.warning(
            "rule_id=%s run_key=%s: %d violating rows collapsed into %d "
            "distinct src_key_value(s) -- src_key_cols may not be a true "
            "natural key for this rule's table",
            rule_id, run_key, len(rows), len(new_rows_by_key),
        )
        _log_error(
            meta_conn, meta_db, run_id, rule, run_key, "KEY_NOT_DISTINCT",
            f"{collapsed} duplicate-key row(s) collapsed into existing "
            f"src_key_value entries ({len(new_rows_by_key)} distinct keys "
            f"from {len(rows)} violating rows)",
        )


    existing = execute_query(
        meta_conn,
        f"""
        SELECT record_id, src_key_value, etl_is_curr_ind
        FROM {meta_db}.gre_exceptions
        WHERE rule_id = ? AND run_key = ?
        """,
        [rule_id, run_key],
        log_params=True,
    )
    existing_by_key = {r["src_key_value"]: r for r in existing}

    to_insert = []
    to_reactivate = []
    for nk, row in new_rows_by_key.items():
        existing_row = existing_by_key.get(nk)
        if existing_row is None:
            to_insert.append([
                run_id,
                rule_id,
                rule.get("database_name"),
                rule.get("src_tbl_nm"),
                rule.get("project_name"),
                rule.get("process_name"),
                rule.get("rule_group"),
                rule.get("rule_variant"),
                rule.get("element_name"),
                rule.get("sql_dialect"),
                run_key,
                nk,
                rule.get("rule_nm"),
                rule.get("dgr_nbr"),
                rule.get("universe_version"),
                run_params.get("run_type"),
                run_params.get("batch_schedule"),
            ])
        elif existing_row["etl_is_curr_ind"] != "Y":
            to_reactivate.append([run_id, run_key, existing_row["record_id"]])


    insert_sql = f"""
        INSERT INTO {meta_db}.gre_exceptions (
            run_id, rule_id, database_name, src_tbl_nm, project_name, process_name,
            rule_group, rule_variant, element_name, source_name, run_key,
            src_key_value, rule_nm, dgr_nbr, universe_version, run_type, batch_schedule,
            etl_load_dt, etl_last_updt_dt
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_DATE, CURRENT_TIMESTAMP)
    """
    inserted = bulk_insert_or_skip(meta_conn, insert_sql, to_insert) if to_insert else 0

    reactivate_sql = f"""
        UPDATE {meta_db}.gre_exceptions
        SET etl_is_curr_ind = 'Y', run_id = ?, last_updated_by = 'SYSTEM',
            last_updated_datetime = CURRENT_TIMESTAMP, etl_last_updt_dt = CURRENT_TIMESTAMP
        WHERE run_key = ? AND record_id = ?
    """
    reactivated = bulk_execute(meta_conn, reactivate_sql, to_reactivate) if to_reactivate else 0


    deactivated = 0
    to_deactivate = [
        [r["record_id"]]
        for nk, r in existing_by_key.items()
        if r["etl_is_curr_ind"] == "Y" and nk not in new_rows_by_key
    ]
    if to_deactivate:
        deactivate_sql = f"""
            UPDATE {meta_db}.gre_exceptions
            SET etl_is_curr_ind = 'N', last_updated_by = 'SYSTEM',
                last_updated_datetime = CURRENT_TIMESTAMP, etl_last_updt_dt = CURRENT_TIMESTAMP
            WHERE record_id = ?
        """
        deactivated = bulk_execute(meta_conn, deactivate_sql, to_deactivate)

    return {"inserted": inserted, "reactivated": reactivated, "deactivated": deactivated}


def _write_result(meta_conn, meta_db: str, run_id: str, rule: dict, run_key: str, start_time: float,
                   status: str, total_records=None, failed_records: int = 0, failure_pct=None,
                   threshold_pct_used=None, threshold_count_used=None, threshold_operator_used=None,
                   source_tieback_sql: str = None, error_message: str = None,
                   executed_sql: str = None, distinct_failed_records: int = None,
                   duplicate_records: int = None) -> None:
    rule_id = rule.get("rule_id")

    sql = f"""
        INSERT INTO {meta_db}.gre_results (
            run_id, rule_id, rule_group, rule_variant, project_name, process_name, run_key, seq_no,
            start_time, end_time, total_records, failed_records, distinct_failed_records, duplicate_records,
            failure_pct, threshold_pct_used, threshold_count_used, threshold_operator_used,
            severity, status, error_message, executed_sql, source_tieback_sql, active_ind
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Y')
    """
    execute_dml(meta_conn, sql, [
        run_id, rule_id, rule.get("rule_group"), rule.get("rule_variant"),
        rule.get("project_name"), rule.get("process_name"),
        run_key, rule.get("seq_no"), datetime.fromtimestamp(start_time), datetime.now(),
        total_records, failed_records, distinct_failed_records, duplicate_records, failure_pct,
        threshold_pct_used, threshold_count_used,
        threshold_operator_used, rule.get("severity"), status, error_message, executed_sql,
        source_tieback_sql,
    ], log_params=True)


def _write_result_safe(meta_conn, meta_db: str, run_id: str, rule: dict, run_key: str, start_time: float,
                        status: str, **kwargs) -> None:
    try:
        _write_result(meta_conn, meta_db, run_id, rule, run_key, start_time, status, **kwargs)
    except Exception as exc:
        logger.error("Failed to write gre_results row for rule_id=%s: %s", rule.get("rule_id"), exc)


def _build_total_query(table_ref: str, run_params: dict) -> str:
    where_clause = " AND ".join(f"{key} = '{{{key}}}'" for key in sorted(run_params)) or "1=1"
    template = f"SELECT COUNT(*) AS total_count FROM {table_ref} WHERE {where_clause}"
    return _substitute_params(template, run_params)


class _CachedTotalFailure:
    __slots__ = ("exc",)

    def __init__(self, exc: Exception):
        self.exc = exc


def _compute_total(db_conn, rule: dict, run_params: dict, total_cache: dict = None) -> int:
    query = _build_total_query(db_conn.qualified_name(rule), run_params)

    cache_key = (rule.get("sql_dialect"), query)
    if total_cache is not None and cache_key in total_cache:
        cached = total_cache[cache_key]
        if isinstance(cached, _CachedTotalFailure):
            raise cached.exc
        return cached

    try:
        rows = _run_source_query(db_conn, query)
    except Exception as exc:
        if total_cache is not None:
            total_cache[cache_key] = _CachedTotalFailure(exc)
        raise

    if not rows:
        total = 0
    else:
        first_row = rows[0]
        first_value = next(iter(first_row.values()))
        total = int(first_value or 0)

    if total_cache is not None:
        total_cache[cache_key] = total
    return total


def execute_rule(rule: dict, db_conn, meta_conn, run_id: str, run_key: str, run_params: dict, meta_db: str,
                  total_cache: dict = None, extra_filters: dict = None, text_params: dict = None,
                  extra_filters_sql: str = None) -> str:
    start = time.time()


    try:
        db_conn.prepare(rule)
    except Exception as exc:
        logger.error("Rule %s: source prepare failed: %s", rule.get("rule_id"), exc, exc_info=True)
        _log_error(meta_conn, meta_db, run_id, rule, run_key, "SOURCE_PREPARE_ERROR", str(exc))
        _write_result_safe(meta_conn, meta_db, run_id, rule, run_key, start, "ERROR", error_message=str(exc),
                            executed_sql=rule.get("rule_syntax"))
        return "ERROR"


    if extra_filters_sql is None:
        try:
            extra_filters_sql = build_extra_filters_clause(extra_filters)
        except ValueError as exc:
            logger.error("Rule %s: extra_filters rejected: %s", rule.get("rule_id"), exc)
            _log_error(meta_conn, meta_db, run_id, rule, run_key, "PARAM_SUBSTITUTION_ERROR", str(exc))
            _write_result_safe(meta_conn, meta_db, run_id, rule, run_key, start, "ERROR", error_message=str(exc),
                                executed_sql=rule.get("rule_syntax"))
            return "ERROR"
    extra_filters_marker_present = "{extra_filters}" in rule["rule_syntax"] or "$extra_filters" in rule["rule_syntax"]
    if extra_filters_marker_present:
        templated = rule["rule_syntax"].replace("{extra_filters}", extra_filters_sql).replace(
            "$extra_filters", extra_filters_sql)
    elif extra_filters:


        outer_where = extra_filters_sql[4:] if extra_filters_sql.startswith("AND ") else extra_filters_sql
        inner_sql = rule["rule_syntax"].strip().rstrip(";")
        templated = (
            f"SELECT * FROM (\n{inner_sql}\n) gre_extra_filters_wrap WHERE {outer_where}"
        )
        logger.debug(
            "Rule %s: extra_filters=%s applied via DERIVED-TABLE WRAP (no {extra_filters}/"
            "$extra_filters marker in rule_syntax) -- rule_syntax's full result set is "
            "filtered from the OUTSIDE, so this is correct regardless of how many top-level "
            "AND/OR branches rule_syntax's own WHERE clause has, or whether it has one at "
            "all. Fails loudly instead if rule_syntax's own SELECT list doesn't project the "
            "filtered column(s) at all -- add the {extra_filters}/$extra_filters marker "
            "directly into gre_rules.rule_syntax if that happens.",
            rule.get("rule_id"), extra_filters,
        )
    else:
        templated = rule["rule_syntax"]


    substitution_params = {**run_params, **(text_params or {})}
    try:
        query = _substitute_params(templated, substitution_params)
    except ValueError as exc:
        logger.error("Rule %s: run_params substitution failed: %s", rule.get("rule_id"), exc)
        _log_error(meta_conn, meta_db, run_id, rule, run_key, "PARAM_SUBSTITUTION_ERROR", str(exc))
        _write_result_safe(meta_conn, meta_db, run_id, rule, run_key, start, "ERROR", error_message=str(exc),
                            executed_sql=templated)
        return "ERROR"


    logger.info("Rule %s (%s): running SQL: %s", rule.get("rule_id"), rule.get("rule_nm"), " ".join(query.split()))
    try:
        failed, violating_rows = _scan_violations(db_conn, query, src_key_cols=split_src_key_cols(rule))
    except Exception as exc:
        logger.error("Rule %s: rule_syntax scan failed: %s", rule.get("rule_id"), exc, exc_info=True)
        _log_error(meta_conn, meta_db, run_id, rule, run_key, "SQL_RUNTIME", str(exc))
        _write_result_safe(meta_conn, meta_db, run_id, rule, run_key, start, "ERROR", error_message=str(exc),
                            executed_sql=query)
        return "ERROR"


    total_params = {**run_params, **(extra_filters or {})}
    extra_filters_only = extra_filters or None


    tieback_scope_params = total_params
    try:
        total = _compute_total(db_conn, rule, total_params, total_cache=total_cache)
    except Exception as exc:


        fallback_params = extra_filters_only or {}
        try:
            total = _compute_total(db_conn, rule, fallback_params, total_cache=total_cache)
            tieback_scope_params = fallback_params
            logger.warning(
                "Rule %s: total-record count failed when scoped by run_params %s (%s) -- "
                "retried WITHOUT run_params (extra_filters only: %s) and got total=%s. "
                "A run_params/--param key here doesn't name a real column on this rule's "
                "table -- pass it via text_params/--text-param instead to avoid this "
                "fallback and its extra query round trip, and to keep the denominator "
                "correctly scoped by whichever run_params keys ARE real columns.",
                rule.get("rule_id"), sorted(run_params), exc, fallback_params, total,
            )
        except Exception as exc2:
            logger.error("Rule %s: scope/count query failed even without run_params: %s",
                         rule.get("rule_id"), exc2, exc_info=True)
            _log_error(meta_conn, meta_db, run_id, rule, run_key, "SCOPE_QUERY_FAILURE", str(exc2))
            _write_result_safe(meta_conn, meta_db, run_id, rule, run_key, start, "ERROR", error_message=str(exc2),
                                executed_sql=query)
            return "ERROR"


    distinct_failed = _distinct_failed(rule, violating_rows)
    duplicate_records = failed - distinct_failed

    reconcile = {"inserted": 0, "reactivated": 0, "deactivated": 0}
    try:
        reconcile = _write_exceptions(meta_conn, meta_db, rule, run_id, run_key, violating_rows,
                                      run_params={**run_params, **(text_params or {})})
    except Exception as exc:
        logger.warning(
            "Rule %s: exception-row reconciliation failed (failed_records is still accurate): %s",
            rule.get("rule_id"), exc, exc_info=True,
        )
        _log_error(meta_conn, meta_db, run_id, rule, run_key, "WRITE_FAILURE", str(exc))

    verdict = evaluate_threshold(
        total, failed,
        threshold_pct=rule.get("threshold_pct"),
        threshold_count=rule.get("threshold_count"),
        threshold_operator=rule.get("threshold_operator", "OR"),
        severity=rule.get("severity"),
    )

    failure_pct = round((failed / total * 100), 6) if total else 0.0


    source_tieback_sql = None
    if failed > 0:
        try:
            source_tieback_sql = build_source_tieback_sql(rule, run_key, meta_db,
                                                            scope_params=tieback_scope_params)
        except Exception as exc:
            logger.warning(
                "Rule %s: source_tieback_sql generation failed (non-fatal): %s",
                rule.get("rule_id"), exc, exc_info=True,
            )
            source_tieback_sql = None

    if verdict["write_result"]:
        try:
            _write_result(
                meta_conn, meta_db, run_id, rule, run_key, start, verdict["status"],
                total_records=total, failed_records=failed, failure_pct=failure_pct,
                distinct_failed_records=distinct_failed, duplicate_records=duplicate_records,
                threshold_pct_used=verdict["threshold_pct_used"],
                threshold_count_used=verdict["threshold_count_used"],
                threshold_operator_used=verdict["threshold_operator_used"],
                source_tieback_sql=source_tieback_sql,
                executed_sql=query,
            )
        except Exception as exc:
            logger.error("Rule %s: gre_results write failed: %s", rule.get("rule_id"), exc, exc_info=True)
            _log_error(meta_conn, meta_db, run_id, rule, run_key, "RESULTS_WRITE_FAILURE", str(exc))
            _write_result_safe(meta_conn, meta_db, run_id, rule, run_key, start, "ERROR",
                                failed_records=failed, error_message=str(exc), executed_sql=query)
            return "ERROR"
    else:


        _write_result_safe(
            meta_conn, meta_db, run_id, rule, run_key, start, verdict["status"],
            total_records=total, failed_records=failed, failure_pct=failure_pct,
            distinct_failed_records=distinct_failed, duplicate_records=duplicate_records,
            source_tieback_sql=source_tieback_sql, executed_sql=query,
        )

    logger.info(
        "rule_id=%s | total=%d failed=%d distinct=%d duplicates=%d inserted=%d reactivated=%d deactivated=%d | "
        "verdict=%s (write_result=%s) | %.3fs",
        rule.get("rule_id"), total, failed, distinct_failed, duplicate_records, reconcile["inserted"], reconcile["reactivated"],
        reconcile["deactivated"], verdict["status"], verdict["write_result"], time.time() - start,
    )
    return "SUCCESS"
