"""Declarative registry of the 11 gre_* tables this tool mirrors.
sync_from_teradata.py and create_postgres_tables.py just loop over
TABLE_SPECS -- add a table here (+ its CREATE TABLE in ddl_postgres.sql)
and nothing else needs to change.

    name           gre_* table name, identical in both databases
    primary_key    tuple of column(s); the Postgres ON CONFLICT target
    columns        ordered column list -- SELECT order on Teradata side,
                   INSERT order on Postgres side
    mode           'full_refresh' | 'incremental'
    watermark_col  incremental only: column (or expression) compared
                   against the stored watermark
    reopen_filter  incremental only, optional: extra predicate OR'd into
                   the WHERE clause so rows updated without bumping
                   watermark_col keep getting re-pulled (see gre_audit)
"""

GRE_RULES = {
    "name": "gre_rules",
    "primary_key": ("rule_id",),
    "mode": "full_refresh",
    "columns": [
        "rule_id", "rule_nm", "database_name", "src_tbl_nm", "sql_dialect",
        "rule_syntax", "project_name", "process_name", "rule_group", "rule_variant",
        "seq_no", "sequencing_mode", "on_failure", "threshold_pct", "threshold_count",
        "threshold_operator", "severity", "src_key_cols", "element_name", "act_ind",
        "universe_version", "universe_year", "dgr_nbr", "issue_category_name",
        "business_rule", "rule_description", "created_by", "last_updated_by",
        "load_datetime", "last_updated_datetime",
    ],
}

GRE_LOG = {
    "name": "gre_log",
    "primary_key": ("log_id",),
    "mode": "incremental",
    "watermark_col": "load_datetime",
    "columns": [
        "log_id", "run_id", "rule_id", "rule_group", "project_name", "process_name",
        "run_key", "seq_no", "start_time", "end_time", "status", "rowcount",
        "error_message", "load_datetime",
    ],
}

# exception_flag can be updated later (compliance disposition), so
# watermark on whichever timestamp actually moved.
GRE_EXCEPTIONS = {
    "name": "gre_exceptions",
    "primary_key": ("record_id",),
    "mode": "incremental",
    "watermark_col": "COALESCE(last_updated_datetime, load_datetime)",
    "columns": [
        "record_id", "run_id", "rule_id", "database_name", "src_tbl_nm",
        "project_name", "process_name", "element_name", "source_name", "issue_desc",
        "exception_flag", "exception_approver", "run_key", "etl_is_curr_ind",
        "etl_load_dt", "etl_last_updt_dt", "src_key_value", "rule_nm", "dgr_nbr",
        "universe_version", "run_type", "batch_schedule", "load_datetime",
        "last_updated_by", "last_updated_datetime",
    ],
}

GRE_RESULTS = {
    "name": "gre_results",
    "primary_key": ("result_id",),
    "mode": "incremental",
    "watermark_col": "evaluated_at",
    "columns": [
        "result_id", "rule_id", "run_key", "run_id", "project_name", "process_name",
        "total_records", "failed_records", "failure_pct", "threshold_pct_used",
        "threshold_count_used", "threshold_operator_used", "severity", "status",
        "evaluated_at",
    ],
}

# gre_audit's ended_at/status UPDATE never bumps load_datetime (see
# rules_engine/runner.py::_finish_audit, sampling/sampling.py::_write_audit),
# so a plain watermark sync would miss every run's completion --
# reopen_filter re-pulls any still-RUNNING row every time.
GRE_AUDIT = {
    "name": "gre_audit",
    "primary_key": ("run_id",),
    "mode": "incremental",
    "watermark_col": "load_datetime",
    "reopen_filter": "status = 'RUNNING'",
    "columns": [
        "run_id", "run_type", "rule_group", "project_name", "process_name", "run_key",
        "rule_variant", "started_at", "ended_at", "status", "total_rules",
        "rules_succeeded", "rules_errored", "sample_config_id", "sampling_method",
        "random_seed", "target_volume", "total_candidates", "total_selected",
        "triggered_by", "load_datetime",
    ],
}

GRE_ERRORS = {
    "name": "gre_errors",
    "primary_key": ("error_id",),
    "mode": "incremental",
    "watermark_col": "occurred_at",
    "columns": [
        "error_id", "run_id", "rule_id", "rule_group", "run_key", "error_type",
        "error_message", "error_detail", "occurred_at",
    ],
}

GRE_SAMPLING_CONFIG = {
    "name": "gre_sampling_config",
    "primary_key": ("config_id",),
    "mode": "full_refresh",
    "columns": [
        "config_id", "project_name", "process_name", "sample_name", "source_type",
        "universe_table", "key_columns", "scope_sql", "exclusion_sql", "target_volume",
        "sampling_method", "priority_rank_sql", "rounding_mode", "schedule_cron",
        "act_ind", "created_by", "last_updated_by", "load_datetime",
    ],
}

GRE_SAMPLING_STRATA = {
    "name": "gre_sampling_strata",
    "primary_key": ("strata_id",),
    "mode": "full_refresh",
    "columns": ["strata_id", "config_id", "level_order", "level_name", "stratify_expr"],
}

GRE_SAMPLING_MIX = {
    "name": "gre_sampling_mix",
    "primary_key": ("mix_id",),
    "mode": "full_refresh",
    "columns": ["mix_id", "strata_id", "bucket_value", "target_fraction"],
}

# Natural key is (sample_run_id, case_key) -- sample_run_id alone is just
# Teradata's PRIMARY INDEX for distribution, not a uniqueness constraint.
GRE_SAMPLE_SELECTIONS = {
    "name": "gre_sample_selections",
    "primary_key": ("sample_run_id", "case_key"),
    "mode": "incremental",
    "watermark_col": "load_datetime",
    "columns": [
        "sample_run_id", "config_id", "project_name", "process_name", "sample_cycle",
        "case_key", "priority_rank", "excluded_flag", "exclusion_reason",
        "selected_flag", "load_datetime",
    ],
}

GRE_SAMPLE_SELECTION_ATTRS = {
    "name": "gre_sample_selection_attrs",
    "primary_key": ("sample_run_id", "case_key", "strata_id"),
    "mode": "incremental",
    "watermark_col": "load_datetime",
    "columns": [
        "sample_run_id", "case_key", "strata_id", "level_order", "bucket_value",
        "load_datetime",
    ],
}

TABLE_SPECS = [
    GRE_RULES, GRE_LOG, GRE_EXCEPTIONS, GRE_RESULTS, GRE_AUDIT, GRE_ERRORS,
    GRE_SAMPLING_CONFIG, GRE_SAMPLING_STRATA, GRE_SAMPLING_MIX,
    GRE_SAMPLE_SELECTIONS, GRE_SAMPLE_SELECTION_ATTRS,
]

TABLE_SPECS_BY_NAME = {spec["name"]: spec for spec in TABLE_SPECS}
