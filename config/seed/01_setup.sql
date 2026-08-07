-- ============================================================
-- HealthSpring UM — connections, stratified sample config,
-- notification routing, and auto-determined thresholds.
--
-- Load this file BEFORE 02_rules.sql. Credentials are NEVER stored here —
-- see the DQ_<NAME>_* env vars documented next to each connection row.
-- ============================================================

-- ── Scope (project/process dimension) ───────────────────────────────────
-- dq_rules and dq_sampling_config below reference these by scope_id
-- instead of each repeating its own project_name/process_name pair — see
-- ddl.sql v7. HealthSpring UM uses two distinct scopes: the rule-engine
-- process (UNIVERSE_VALIDATION) and the sampling process
-- (COMO_WEEKLY_SAMPLE) are tracked separately since they run on different
-- cadences and are two different "processes" within the same project.
INSERT INTO CMSUNIV_FILELAND_T.dq_scope (project_name, process_name)
VALUES ('HEALTHSPRING_UM', 'UNIVERSE_VALIDATION');

INSERT INTO CMSUNIV_FILELAND_T.dq_scope (project_name, process_name)
VALUES ('HEALTHSPRING_UM', 'COMO_WEEKLY_SAMPLE');


-- ── Connections ──────────────────────────────────────────────────────────
-- connection_name must match DQ_CONNECTION_NAMES and dq_rules.source_system.

-- Primary source: the ODAG1-format UM universe extract, pulled from MHK.
-- Env: DQ_TERADATA_TYPE=teradata, DQ_TERADATA_HOST/USER/PASSWORD/LOGMECH
INSERT INTO CMSUNIV_FILELAND_T.dq_connections
    (connection_id, connection_name, source_type, host, port, database_name, description, active_flag)
VALUES
    (1, 'teradata', 'teradata', NULL, NULL, 'CMSUNIV_FILELAND_T',
     'HealthSpring UM ODAG1 universe extract (weekly pull from MHK)', 1);

-- Reference data: SHRPA clinical-review flags + provider contract status.
-- Landed in RDS Postgres by an upstream ETL.
-- Env: DQ_UM_REFDATA_TYPE=postgresql, DQ_UM_REFDATA_HOST/DATABASE/USER/PASSWORD
INSERT INTO CMSUNIV_FILELAND_T.dq_connections
    (connection_id, connection_name, source_type, host, port, database_name, description, active_flag)
VALUES
    (2, 'um_refdata', 'postgresql', NULL, 5432, 'um_reference',
     'SHRPA + provider-contract reference tables (RDS Postgres)', 1);

-- 10-year immutable archive of weekly universe pulls (Section 3.7) —
-- Parquet partitioned by pull_date, read directly via DuckDB/httpfs.
-- Env: DQ_UM_ARCHIVE_TYPE=s3, DQ_UM_ARCHIVE_REGION, (+ IAM role or keys)
INSERT INTO CMSUNIV_FILELAND_T.dq_connections
    (connection_id, connection_name, source_type, host, port, database_name, description, active_flag)
VALUES
    (3, 'um_archive', 's3', NULL, NULL, 's3://healthspring-dq-archive/um_universe/',
     '10-year immutable archive of weekly universe pulls (Parquet, partitioned by pull_date)', 1);


-- ── COMO weekly stratified sample (Section 3.4) ─────────────────────────
INSERT INTO CMSUNIV_FILELAND_T.dq_sampling_config (
    config_id, scope_id, sample_name, connection_name,
    universe_table, key_columns, scope_column, target_volume,
    determination_column, determination_mix_json,
    functional_area_column, functional_area_mix_json,
    exclusion_sql, priority_rank_sql, schedule_cron, active_flag
) VALUES (
    1,
    (SELECT scope_id FROM CMSUNIV_FILELAND_T.dq_scope
     WHERE project_name = 'HEALTHSPRING_UM' AND process_name = 'COMO_WEEKLY_SAMPLE'),
    'COMO_WEEKLY_SAMPLE', 'teradata',
    'um_universe', 'enrollee_id, auth_or_claim_number', 'pull_date', 150,
    'request_disposition',
    '{"Denied": 0.80, "Withdrawn": 0.10, "Dismissed": 0.02, "Approved": 0.08}',
    'functional_area',
    '{"Part B": 0.13, "Behavioral Health": 0.08}',
    -- Exclusion list: auto-approvals, SHRPA-no-PA, diabetic supplies.
    'auto_approved = ''Y'' OR shrpa_no_pa_flag = ''Y'' OR issue_type = ''DIABETIC_SUPPLIES''',
    -- Priority ranking: revision=1 first, then inpatient/outpatient balance,
    -- expedited/standard balance, code variety, clinical-review-required.
    'CASE WHEN revision = 1 THEN 0 ELSE 1 END, ' ||
    'CASE WHEN place_of_service = ''Inpatient'' THEN 0 ELSE 1 END, ' ||
    'CASE WHEN service_type IN (''EXPEDITED'',''EXPEDITED_PART_B'') THEN 0 ELSE 1 END, ' ||
    'procedure_code_family, ' ||
    'CASE WHEN clinical_review_required_flag = ''Y'' THEN 0 ELSE 1 END',
    '0 8 * * FRI',
    1
);
-- TODO once confirmed with COMO: the out-of-network subset of denials
-- ("only a subset of denials being out-of-network") needs a hard cap
-- beyond the mix config above — add a `secondary_cap_json` column or a
-- second-pass filter in sampling/engine.py once the real target
-- percentage is known. Not guessed at here.


-- ── Notification routing (Section 3.5) ──────────────────────────────────
-- DATA VIOLATION -> ROAR gets everything.
INSERT INTO CMSUNIV_FILELAND_T.dq_notification_routes
    (route_id, project_name, process_name, finding_class, audience, channel_type, destination, business_correctable_only, active_flag)
VALUES
    (1, 'HEALTHSPRING_UM', NULL, 'DATA_VIOLATION', 'ROAR', 'TEAMS',
     'https://outlook.office.com/webhook/ROAR_CHANNEL_WEBHOOK', 0, 1);

-- DATA VIOLATION -> BUSINESS gets only business_correctable=1 findings.
INSERT INTO CMSUNIV_FILELAND_T.dq_notification_routes
    (route_id, project_name, process_name, finding_class, audience, channel_type, destination, business_correctable_only, active_flag)
VALUES
    (2, 'HEALTHSPRING_UM', NULL, 'DATA_VIOLATION', 'BUSINESS', 'EMAIL',
     'um-intake-correction-team@healthspring.example.com', 1, 1);

-- ENGINE FAILURE -> ENGINEERING only. NEVER routed to ROAR/BUSINESS —
-- this is enforced by core/reporting.py routing on finding_class, not by
-- anything in this table; these rows just say where each class goes.
INSERT INTO CMSUNIV_FILELAND_T.dq_notification_routes
    (route_id, project_name, process_name, finding_class, audience, channel_type, destination, business_correctable_only, active_flag)
VALUES
    (3, 'HEALTHSPRING_UM', NULL, 'ENGINE_FAILURE', 'ENGINEERING', 'TEAMS',
     'https://outlook.office.com/webhook/DQ_ENGINEERING_CHANNEL_WEBHOOK', 0, 1);

-- Clinical Operations QA — oversight visibility across both data findings
-- and engine health (Section 3.1), without being the audience that acts
-- on either.
INSERT INTO CMSUNIV_FILELAND_T.dq_notification_routes
    (route_id, project_name, process_name, finding_class, audience, channel_type, destination, business_correctable_only, active_flag)
VALUES
    (4, 'HEALTHSPRING_UM', NULL, 'DATA_VIOLATION', 'QA', 'EMAIL',
     'clinical-ops-qa@healthspring.example.com', 0, 1);


-- ── Auto-determined thresholds (Section 3.4) ────────────────────────────
-- z-score/IQR against each rule's OWN run history — reuses the engine's
-- existing anomaly detector (core/metrics.py), not a new mechanism.
INSERT INTO CMSUNIV_FILELAND_T.dq_anomaly_config
    (config_id, project_name, process_name, run_type, method,
     zscore_threshold, iqr_multiplier, min_history_runs, alert_on_anomaly)
VALUES
    (1, 'HEALTHSPRING_UM', 'UNIVERSE_VALIDATION', NULL, 'BOTH', 3.0, 1.5, 8, 1);

-- Fixed thresholds live directly on individual dq_rules rows (see
-- 02_rules.sql — e.g. UM-VOL-001's check_params encodes the known
-- 25k-30k weekly volume band). The ~10-20/day and ~10/month flagged-error
-- volume expectations are intentionally NOT hardcoded anywhere — they're
-- judged via dq_anomaly_config above, per Section 3.4's guidance that a
-- number like that "was never reliable to begin with."
