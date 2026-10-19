import logging

from rules_engine.db_ops import execute_query, _run_source_query, _escape_sql_literal, EXCEPTION_CHUNK
from rules_engine.executor import parse_src_key, _format_src_key

logger = logging.getLogger(__name__)


def get_breaches(meta_conn, meta_db: str, run_id: str) -> list:
    return execute_query(
        meta_conn,
        f"""
        SELECT *
        FROM {meta_db}.gre_results
        WHERE run_id = ? AND status IN ('FAIL', 'WARN')
        ORDER BY status, rule_id
        """,
        [run_id],
        log_params=True,
    )


def get_records_for_result(meta_conn, meta_db: str, rule_id, run_key: str) -> list:
    return execute_query(
        meta_conn,
        f"""
        SELECT e.*
        FROM {meta_db}.gre_exceptions e
        WHERE e.rule_id = ? AND e.run_key = ? AND e.etl_is_curr_ind = 'Y'
        ORDER BY e.record_id
        """,
        [rule_id, run_key],
        log_params=True,
    )


def _build_src_key_where(keys: list) -> str:
    if not keys:
        return "1=0"

    cols = list(keys[0].keys())

    if len(cols) == 1:
        col = cols[0]
        values = [k[col] for k in keys]
        parts = []
        non_null = [v for v in values if v is not None]
        if non_null:
            in_list = ", ".join(f"'{_escape_sql_literal(v)}'" for v in non_null)
            parts.append(f"{col} IN ({in_list})")
        if any(v is None for v in values):
            parts.append(f"{col} IS NULL")
        return " OR ".join(parts)

    record_clauses = []
    for key in keys:
        conds = [
            f"{col} IS NULL" if val is None else f"{col} = '{_escape_sql_literal(val)}'"
            for col, val in key.items()
        ]
        record_clauses.append("(" + " AND ".join(conds) + ")")
    return " OR ".join(record_clauses)


def get_source_records_for_rule(cf, meta_conn, meta_db: str, rule_id, run_key: str) -> list:
    exceptions = execute_query(
        meta_conn,
        f"""
        SELECT record_id, src_key_value, database_name, src_tbl_nm, source_name,
               exception_flag, rule_nm, process_name, project_name
        FROM {meta_db}.gre_exceptions
        WHERE rule_id = ? AND run_key = ? AND etl_is_curr_ind = 'Y'
        """,
        [rule_id, run_key],
        log_params=True,
    )
    if not exceptions:
        return []


    groups = {}
    for exc in exceptions:
        key = (exc.get("database_name"), exc.get("src_tbl_nm"), exc.get("source_name"))
        groups.setdefault(key, []).append(exc)

    records = []
    for (database_name, src_tbl_nm, source_name), group in groups.items():
        db_conn = cf.get(source_name)
        if db_conn is None:
            raise RuntimeError(
                f"Source connection '{source_name}' unavailable -- cannot tie "
                f"rule_id={rule_id} run_key={run_key} exceptions back to source records."
            )


        table_ref_rule = {"database_name": database_name, "src_tbl_nm": src_tbl_nm}
        db_conn.prepare(table_ref_rule)
        table_ref = db_conn.qualified_name(table_ref_rule)

        keys = [parse_src_key(exc["src_key_value"]) for exc in group]
        cols = list(keys[0].keys())
        by_src_key = {exc["src_key_value"]: exc for exc in group}
        matched_keys = set()

        for chunk_start in range(0, len(keys), EXCEPTION_CHUNK):
            chunk = keys[chunk_start:chunk_start + EXCEPTION_CHUNK]
            where = _build_src_key_where(chunk)
            query = f"SELECT * FROM {table_ref} WHERE {where}"
            for row in _run_source_query(db_conn, query):
                try:
                    nk = _format_src_key(cols, row)
                except KeyError as err:


                    logger.warning(
                        "get_source_records_for_rule: rule_id=%s run_key=%s -- %s",
                        rule_id, run_key, err,
                    )
                    continue
                exc = by_src_key.get(nk)
                matched_keys.add(nk)
                merged = dict(row)
                merged["_record_id"] = exc.get("record_id") if exc else None
                merged["_rule_id"] = rule_id
                merged["_rule_nm"] = exc.get("rule_nm") if exc else None
                merged["_process_name"] = exc.get("process_name") if exc else None
                merged["_project_name"] = exc.get("project_name") if exc else None
                merged["_src_key_value"] = nk
                merged["_exception_flag"] = exc.get("exception_flag") if exc else None
                records.append(merged)

        missing = set(by_src_key) - matched_keys
        if missing:
            logger.info(
                "get_source_records_for_rule: %d of %d exception(s) for rule_id=%s "
                "run_key=%s no longer match a row in %s.%s (likely corrected/deleted "
                "upstream since the rule ran).",
                len(missing), len(group), rule_id, run_key, database_name, src_tbl_nm,
            )

    return records


def get_source_records_for_process(cf, meta_conn, meta_db: str, process_name: str, run_key: str,
                                   project_name: str = None, rule_nm: str = None) -> list:
    where = ["process_name = ?", "run_key = ?", "etl_is_curr_ind = 'Y'"]
    params = [process_name, run_key]
    if project_name is not None:
        where.append("project_name = ?")
        params.append(project_name)
    if rule_nm is not None:
        where.append("rule_nm = ?")
        params.append(rule_nm)

    rows = execute_query(
        meta_conn,
        f"""
        SELECT DISTINCT rule_id
        FROM {meta_db}.gre_exceptions
        WHERE {' AND '.join(where)}
        ORDER BY rule_id
        """,
        params,
        log_params=True,
    )
    rule_ids = [r["rule_id"] for r in rows]
    if not rule_ids:
        logger.info(
            "get_source_records_for_process: no current exceptions for process_name=%s "
            "run_key=%s%s%s -- nothing to tie back.",
            process_name, run_key,
            f" project_name={project_name}" if project_name else "",
            f" rule_nm={rule_nm}" if rule_nm else "",
        )
        return []

    records = []
    for rule_id in rule_ids:
        records.extend(get_source_records_for_rule(cf, meta_conn, meta_db, rule_id, run_key))
    return records
