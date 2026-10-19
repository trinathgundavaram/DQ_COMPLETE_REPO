CREATE MULTISET TABLE {{META_DB}}.gre_rules (
    rule_id              INTEGER NOT NULL,
    rule_nm              VARCHAR(500) NOT NULL,
    act_ind              BYTEINT DEFAULT 1,

    rule_group           VARCHAR(100) NOT NULL,
    rule_variant         VARCHAR(100),

    project_name         VARCHAR(100) NOT NULL,

    process_name         VARCHAR(100) NOT NULL,
    seq_no               INTEGER DEFAULT 100,
    sequencing_mode      VARCHAR(20) DEFAULT 'independent',
    on_failure           VARCHAR(20) DEFAULT 'skip_and_continue',

    database_name        VARCHAR(200) NOT NULL,

    src_tbl_nm           VARCHAR(200) NOT NULL,

    sql_dialect          VARCHAR(20)  NOT NULL,

    rule_syntax          CLOB NOT NULL,

    src_key_cols         VARCHAR(500) NOT NULL,
    element_name         VARCHAR(200),

    threshold_pct        FLOAT,
    threshold_count      INTEGER,

    threshold_operator   CHAR(3) DEFAULT 'OR',
    severity             VARCHAR(50) DEFAULT 'Data Validation Error',

    universe_version     VARCHAR(50),

    universe_year        INTEGER,
    dgr_nbr              VARCHAR(50),

    issue_category_name  VARCHAR(200),
    business_rule        VARCHAR(2000),

    rule_description     VARCHAR(2000),

    created_by           VARCHAR(100),
    last_updated_by      VARCHAR(100),
    load_datetime            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_datetime    TIMESTAMP
)
PRIMARY INDEX (rule_id);

CREATE INDEX gre_rules_group_variant_ix (rule_group, act_ind, rule_variant)
ON {{META_DB}}.gre_rules;

CREATE INDEX gre_rules_project_process_ix (project_name, process_name, act_ind)
ON {{META_DB}}.gre_rules;

CREATE MULTISET TABLE {{META_DB}}.gre_exceptions (
    record_id            BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id                VARCHAR(200),
    run_key               VARCHAR(100) NOT NULL,
    rule_id               INTEGER NOT NULL,

    rule_nm               VARCHAR(500),
    rule_group            VARCHAR(100),
    rule_variant          VARCHAR(100),

    database_name         VARCHAR(200),
    src_tbl_nm            VARCHAR(200),
    project_name          VARCHAR(200),
    process_name          VARCHAR(200),
    element_name          VARCHAR(200),

    source_name           VARCHAR(100),
    src_key_value         VARCHAR(1000) NOT NULL,

    dgr_nbr               VARCHAR(50),
    universe_version      VARCHAR(50),
    run_type              VARCHAR(50),
    batch_schedule        VARCHAR(100),

    exception_flag        VARCHAR(20) DEFAULT 'OPEN',
    exception_approver     VARCHAR(100),

    etl_is_curr_ind       CHAR(1) DEFAULT 'Y',
    etl_load_dt           DATE,
    etl_last_updt_dt      TIMESTAMP,

    load_datetime             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_by       VARCHAR(100),
    last_updated_datetime     TIMESTAMP
)
PRIMARY INDEX (record_id);

CREATE UNIQUE INDEX gre_exceptions_uix (rule_id, run_key, src_key_value)
ON {{META_DB}}.gre_exceptions;

CREATE INDEX gre_exceptions_rule_run_key_ix (rule_id, run_key)
ON {{META_DB}}.gre_exceptions;

CREATE MULTISET TABLE {{META_DB}}.gre_results (
    result_id                 BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id                     VARCHAR(200) NOT NULL,
    rule_id                    INTEGER NOT NULL,
    rule_group                 VARCHAR(100),
    rule_variant                VARCHAR(100),

    project_name                VARCHAR(100),
    process_name                VARCHAR(100),
    run_key                    VARCHAR(100) NOT NULL,
    seq_no                      INTEGER,
    start_time                  TIMESTAMP,
    end_time                    TIMESTAMP,
    total_records               BIGINT,
    failed_records              BIGINT,

    failure_pct                 FLOAT,
    threshold_pct_used          FLOAT,
    threshold_count_used        INTEGER,
    threshold_operator_used     CHAR(3),
    severity                    VARCHAR(50),
    status                      VARCHAR(10),

    error_message                VARCHAR(2000),
    executed_sql                 CLOB,

    source_tieback_sql          CLOB,

    active_ind                  CHAR(1) DEFAULT 'Y',

    load_datetime                TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_datetime        TIMESTAMP
)
PRIMARY INDEX (run_id, rule_id);

CREATE INDEX gre_results_rule_run_key_active_ix (rule_id, run_key, active_ind)
ON {{META_DB}}.gre_results;

CREATE INDEX gre_results_group_run_key_ix (rule_group, run_key, status)
ON {{META_DB}}.gre_results;

CREATE MULTISET TABLE {{META_DB}}.gre_rule_audit (
    run_id                 VARCHAR(200) NOT NULL,
    rule_group              VARCHAR(100),
    rule_variant               VARCHAR(100),
    project_name              VARCHAR(100),
    process_name              VARCHAR(100),
    run_key                   VARCHAR(100),
    run_params                 CLOB,

    extra_filters               CLOB,

    started_at                TIMESTAMP,
    ended_at                   TIMESTAMP,
    status                      VARCHAR(20),
    total_rules                  INTEGER,
    rules_succeeded                INTEGER,
    rules_errored                    INTEGER,
    triggered_by                      VARCHAR(100),
    load_datetime                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (run_id);

CREATE INDEX gre_rule_audit_group_run_key_ix (rule_group, run_key)
ON {{META_DB}}.gre_rule_audit;

CREATE INDEX gre_rule_audit_status_ix (status)
ON {{META_DB}}.gre_rule_audit;

CREATE MULTISET TABLE {{META_DB}}.gre_rule_errors (
    error_id         BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id           VARCHAR(200),
    rule_id          INTEGER NOT NULL,
    rule_group       VARCHAR(100),
    rule_variant     VARCHAR(100),

    run_key          VARCHAR(100),
    error_type       VARCHAR(50),
    error_message    VARCHAR(2000),
    error_detail     CLOB,
    active_ind           CHAR(1) DEFAULT 'Y',

    occurred_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated_datetime TIMESTAMP
)
PRIMARY INDEX (run_id, rule_id);

CREATE INDEX gre_rule_errors_rule_run_key_active_ix (rule_id, run_key, active_ind)
ON {{META_DB}}.gre_rule_errors;
