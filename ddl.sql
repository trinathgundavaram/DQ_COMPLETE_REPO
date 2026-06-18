-- ============================================================
-- Data Quality Framework DDL
-- Schema : CMSUNIV_FILELAND_DEV_T  (DEV)
-- DB     : Teradata  (metadata store)
-- ============================================================
-- NOTE: The metadata store is always Teradata.
-- Source systems (PostgreSQL, Databricks, SQL Server, file)
-- are configured via environment variables — see
-- db/connection_factory.py and db/adapters/ for details.
-- The dq_connections table below is a catalogue / reference;
-- runtime credentials are read from env vars, NOT from this table.
-- ============================================================

CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_rules (
    rule_id              INTEGER NOT NULL,
    rule_code            VARCHAR(200) NOT NULL,
    project_name         VARCHAR(100) NOT NULL,
    process_name         VARCHAR(100),
    src_tbl_nm           VARCHAR(200) NOT NULL,
    src_db_name          VARCHAR(200),
    src_schema           VARCHAR(100),
    rule_name            VARCHAR(500),
    rule_description     VARCHAR(1000),
    rule_syntax          CLOB,
    join_sql             CLOB,
    source_system        VARCHAR(50),
    filter_column        VARCHAR(100),
    filter_type          VARCHAR(20),
    primary_key_columns  VARCHAR(500),
    severity             VARCHAR(20),
    threshold_pct        FLOAT,
    threshold_count      INTEGER,
    rule_group           VARCHAR(100),
    table_group          VARCHAR(100),
    active_flag          BYTEINT,
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (rule_id);


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_run_control (
    run_id        VARCHAR(200) NOT NULL,
    run_seq_id    BIGINT GENERATED ALWAYS AS IDENTITY,
    project_name  VARCHAR(100),
    process_name  VARCHAR(100),
    run_type      VARCHAR(50),
    run_mode      VARCHAR(20),
    batch_id      VARCHAR(100),
    dataset_id    VARCHAR(200),
    start_date    DATE,
    end_date      DATE,
    triggered_by  VARCHAR(100),
    start_time    TIMESTAMP,
    end_time      TIMESTAMP,
    status        VARCHAR(20),
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (run_id);


-- Connection catalogue (reference only — credentials stored in env vars)
CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_connections (
    connection_id    INTEGER NOT NULL,
    connection_name  VARCHAR(100) NOT NULL,    -- matches DQ_CONNECTION_NAMES entry
    source_type      VARCHAR(50) NOT NULL,     -- teradata | postgresql | aurora |
                                               -- databricks | sqlserver | file
    host             VARCHAR(500),
    port             INTEGER,
    database_name    VARCHAR(200),
    description      VARCHAR(500),
    active_flag      BYTEINT DEFAULT 1,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (connection_name);


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_execution (
    run_id          VARCHAR(200),
    rule_id         INTEGER,
    rule_code       VARCHAR(200),
    project_name    VARCHAR(100),
    process_name    VARCHAR(100),
    table_name      VARCHAR(200),
    run_type        VARCHAR(50),
    run_mode        VARCHAR(20),
    batch_id        VARCHAR(100),
    dataset_id      VARCHAR(200),
    start_date      DATE,
    end_date        DATE,
    total_records   BIGINT,
    failed_records  BIGINT,
    passed_records  BIGINT,
    failure_pct     FLOAT,
    pass_pct        FLOAT,
    severity        VARCHAR(20),
    status          VARCHAR(20),
    execution_time  FLOAT,
    run_timestamp   TIMESTAMP,
    run_date        DATE,
    run_month       DATE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (run_id);


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_exceptions (
    exception_id     BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id           VARCHAR(200),
    rule_id          INTEGER,
    rule_code        VARCHAR(200),
    project_name     VARCHAR(100),
    process_name     VARCHAR(100),
    table_name       VARCHAR(200),
    run_type         VARCHAR(50),
    run_mode         VARCHAR(20),
    batch_id         VARCHAR(100),
    dataset_id       VARCHAR(200),
    key_json         CLOB,
    primary_key_str  VARCHAR(500),
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (exception_id);


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_metrics_summary (
    project_name     VARCHAR(100),
    process_name     VARCHAR(100),
    run_type         VARCHAR(50),
    batch_id         VARCHAR(100),
    dataset_id       VARCHAR(200),
    run_month        DATE,
    total_runs       INTEGER,
    total_rules      INTEGER,
    failed_rules     INTEGER,
    passed_rules     INTEGER,
    total_records    BIGINT,
    failed_records   BIGINT,
    avg_failure_pct  FLOAT,
    dq_score         FLOAT,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (project_name, process_name, run_type, run_month);


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_run_logs (
    log_id        BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id        VARCHAR(200),
    rule_id       INTEGER,
    rule_code     VARCHAR(200),
    log_level     VARCHAR(20),
    message       CLOB,
    error_code    VARCHAR(50),
    error_detail  CLOB,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (run_id);


CREATE MULTISET TABLE CMSUNIV_FILELAND_DEV_T.dq_rule_issues (
    issue_id      BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id        VARCHAR(200),
    rule_id       INTEGER,
    rule_code     VARCHAR(200),
    project_name  VARCHAR(100),
    process_name  VARCHAR(100),
    table_name    VARCHAR(200),
    issue_type    VARCHAR(50),
    issue_message CLOB,
    error_detail  CLOB,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
PRIMARY INDEX (run_id);
