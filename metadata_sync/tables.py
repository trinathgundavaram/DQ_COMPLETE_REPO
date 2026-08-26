"""Declarative registry of the 12 gre_* tables this tool mirrors.
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
                   watermark_col keep getting re-pulled (see gre_rule_audit/
                   gre_sampling_audit)

Note: gre_audit (the old combined run-tracking table) and gre_errors (the
old combined error table) were both fully split, and there is no longer a
compatibility VIEW standing in for either old name on either database --
rules_engine/ and sampling/ are now fully independent packages that share
no tables at all (see README.md's "Package separation"). Anything still
querying gre_audit or gre_errors directly needs to move to the
package-specific tables below.

Note: gre_log (the old per-attempt execution log) was folded into
gre_results, which now carries one row per rule per execution attempt
(not just per rule_id+run_key) -- see rules_engine/schema.sql's "gre_results"
section and rules_engine/executor.py::_write_result()'s docstring for the
full rationale. Anything still querying gre_log directly needs to move to
gre_results, filtering active_ind/status as needed.
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

# Consolidated with the old gre_log (see this file's module docstring) --
# one row per rule PER EXECUTION ATTEMPT now, not per rule_id+run_key,
# with active_ind marking the current attempt for a given (rule_id,
# run_key) exactly the way gre_log used to. watermark on whichever
# timestamp actually moved, same COALESCE pattern gre_exceptions uses,
# since a rerun deactivating an OLDER row (last_updated_datetime) is just
# as real a change as a brand new row's load_datetime.
GRE_RESULTS = {
    "name": "gre_results",
    "primary_key": ("result_id",),
    "mode": "incremental",
    "watermark_col": "COALESCE(last_updated_datetime, load_datetime)",
    "columns": [
        "result_id", "run_id", "rule_id", "rule_group", "project_name", "process_name",
        "run_key", "seq_no", "start_time", "end_time", "total_records", "failed_records",
        "failure_pct", "threshold_pct_used", "threshold_count_used", "threshold_operator_used",
        "severity", "status", "error_message", "executed_sql", "source_tieback_sql", "active_ind",
        "load_datetime", "last_updated_datetime",
    ],
}

# gre_audit split into gre_rule_audit / gre_sampling_audit (2026-08) --
# rule-engine users no longer drag along six always-NULL sampling columns
# and vice versa. Both replace the single GRE_AUDIT entry that used to
# sync the old combined table. There is no compatibility view standing in
# for the old gre_audit name on either database -- rules_engine/ and
# sampling/ share no tables at all (see README.md's "Package separation").
# A consumer that needs both run-tracking tables together should query
# gre_rule_audit and gre_sampling_audit separately, or UNION them itself.
#
# Both tables' ended_at/status UPDATE never bumps load_datetime (see
# rules_engine/runner.py::_finish_audit, sampling/sampling.py::_write_audit),
# so a plain watermark sync would miss every run's completion --
# reopen_filter re-pulls any still-RUNNING row every time.
GRE_RULE_AUDIT = {
    "name": "gre_rule_audit",
    "primary_key": ("run_id",),
    "mode": "incremental",
    "watermark_col": "load_datetime",
    "reopen_filter": "status = 'RUNNING'",
    "columns": [
        "run_id", "rule_group", "project_name", "process_name", "run_key",
        "rule_variant", "run_params", "extra_filters",
        "started_at", "ended_at", "status", "total_rules",
        "rules_succeeded", "rules_errored", "triggered_by", "load_datetime",
    ],
}

GRE_SAMPLING_AUDIT = {
    "name": "gre_sampling_audit",
    "primary_key": ("run_id",),
    "mode": "incremental",
    "watermark_col": "load_datetime",
    "reopen_filter": "status = 'RUNNING'",
    "columns": [
        "run_id", "run_key", "sample_config_id", "sampling_method",
        "random_seed", "target_volume", "total_candidates", "total_selected",
        "started_at", "ended_at", "status", "triggered_by", "load_datetime",
    ],
}

# gre_errors split into gre_rule_errors / gre_sampling_errors (2026-08),
# same split as gre_audit above and for the same reason: the two packages
# share no tables. gre_rule_errors keeps rule_id/rule_group (always
# populated for rules_engine); gre_sampling_errors drops rule_id entirely
# and carries process_name instead (sampling has no rule concept).
GRE_RULE_ERRORS = {
    "name": "gre_rule_errors",
    "primary_key": ("error_id",),
    "mode": "incremental",
    # Same reasoning as gre_log above: active_ind flips without bumping
    # occurred_at, so watermark on whichever timestamp actually moved.
    "watermark_col": "COALESCE(last_updated_datetime, occurred_at)",
    "columns": [
        "error_id", "run_id", "rule_id", "rule_group", "run_key", "error_type",
        "error_message", "error_detail", "active_ind", "occurred_at", "last_updated_datetime",
    ],
}

GRE_SAMPLING_ERRORS = {
    "name": "gre_sampling_errors",
    "primary_key": ("error_id",),
    "mode": "incremental",
    "watermark_col": "COALESCE(last_updated_datetime, occurred_at)",
    "columns": [
        "error_id", "run_id", "process_name", "run_key", "error_type",
        "error_message", "error_detail", "active_ind", "occurred_at", "last_updated_datetime",
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
    # etl_is_curr_ind (sampling's own active_ind, see
    # sampling/sampling.py::_deactivate_prior_sampling_runs()) flips
    # without bumping load_datetime -- watermark on whichever timestamp
    # actually moved, same COALESCE pattern gre_exceptions/gre_log use.
    # NOTE: etl_is_curr_ind/last_updated_datetime were already live columns
    # on this table (added by migrate_gre_sampling_reconciliation.sql) but were missing
    # from this sync spec entirely -- the Postgres mirror was silently
    # never receiving the "is this the current sample run" flag or its
    # flip timestamp. Fixed here alongside the gre_log/gre_errors/
    # gre_results active_ind additions, same underlying bug class.
    "watermark_col": "COALESCE(last_updated_datetime, load_datetime)",
    "columns": [
        "sample_run_id", "config_id", "project_name", "process_name", "sample_cycle",
        "case_key", "priority_rank", "excluded_flag", "exclusion_reason",
        "selected_flag", "etl_is_curr_ind", "load_datetime", "last_updated_datetime",
    ],
}

GRE_SAMPLE_SELECTION_ATTRS = {
    "name": "gre_sample_selection_attrs",
    "primary_key": ("sample_run_id", "case_key", "strata_id"),
    "mode": "incremental",
    "watermark_col": "COALESCE(last_updated_datetime, load_datetime)",
    "columns": [
        "sample_run_id", "case_key", "strata_id", "level_order", "bucket_value",
        "etl_is_curr_ind", "load_datetime", "last_updated_datetime",
    ],
}

TABLE_SPECS = [
    GRE_RULES, GRE_EXCEPTIONS, GRE_RESULTS, GRE_RULE_AUDIT, GRE_RULE_ERRORS,
    GRE_SAMPLING_CONFIG, GRE_SAMPLING_STRATA, GRE_SAMPLING_MIX,
    GRE_SAMPLE_SELECTIONS, GRE_SAMPLE_SELECTION_ATTRS, GRE_SAMPLING_AUDIT,
    GRE_SAMPLING_ERRORS,
]

TABLE_SPECS_BY_NAME = {spec["name"]: spec for spec in TABLE_SPECS}
