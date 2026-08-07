-- ============================================================
-- dq_rules — HealthSpring UM Clinical Audit rule set
-- 38 rules covering every category in Section 3.2.
-- Hand-maintained: this is the source of truth, edit directly.
-- ============================================================

-- UM-REQ-001: Beneficiary first name must not be blank
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1001,
    'UM-REQ-001',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Beneficiary first name must not be blank',
    'Beneficiary first name is required on every universe row.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE beneficiary_first_name IS NULL OR TRIM(CAST(beneficiary_first_name AS VARCHAR(4000))) = ''''',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'NOT_EMPTY',
    'beneficiary_first_name',
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    20,
    'UM_AUDIT',
    1
);

-- UM-REQ-002: Beneficiary last name must not be blank
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1002,
    'UM-REQ-002',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Beneficiary last name must not be blank',
    'Beneficiary last name is required on every universe row.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE beneficiary_last_name IS NULL OR TRIM(CAST(beneficiary_last_name AS VARCHAR(4000))) = ''''',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'NOT_EMPTY',
    'beneficiary_last_name',
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    20,
    'UM_AUDIT',
    1
);

-- UM-REQ-003: Enrollee ID must not be blank
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1003,
    'UM-REQ-003',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Enrollee ID must not be blank',
    'Enrollee ID is required on every universe row.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE enrollee_id IS NULL OR TRIM(CAST(enrollee_id AS VARCHAR(4000))) = ''''',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'NOT_EMPTY',
    'enrollee_id',
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    20,
    'UM_AUDIT',
    1
);

-- UM-REQ-004: Contract ID must not be blank
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1004,
    'UM-REQ-004',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Contract ID must not be blank',
    'Contract ID is required on every universe row.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE contract_id IS NULL OR TRIM(CAST(contract_id AS VARCHAR(4000))) = ''''',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'NOT_EMPTY',
    'contract_id',
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    20,
    'UM_AUDIT',
    1
);

-- UM-REQ-005: Plan Benefit Package (PBP) must not be blank
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1005,
    'UM-REQ-005',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Plan Benefit Package (PBP) must not be blank',
    'Plan Benefit Package (PBP) is required on every universe row.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE pbp IS NULL OR TRIM(CAST(pbp AS VARCHAR(4000))) = ''''',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'NOT_EMPTY',
    'pbp',
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    20,
    'UM_AUDIT',
    1
);

-- UM-REQ-006: Auth/claim number must not be blank
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1006,
    'UM-REQ-006',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Auth/claim number must not be blank',
    'Auth/claim number is required on every universe row.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE auth_or_claim_number IS NULL OR TRIM(CAST(auth_or_claim_number AS VARCHAR(4000))) = ''''',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'NOT_EMPTY',
    'auth_or_claim_number',
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    20,
    'UM_AUDIT',
    1
);

-- UM-REQ-007: Request received date/time must not be blank
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1007,
    'UM-REQ-007',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Request received date/time must not be blank',
    'Request received date/time is required on every universe row.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE request_received_ts IS NULL OR TRIM(CAST(request_received_ts AS VARCHAR(4000))) = ''''',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'NOT_EMPTY',
    'request_received_ts',
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    20,
    'UM_AUDIT',
    1
);

-- UM-REQ-008: Decision date/time must not be blank
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1008,
    'UM-REQ-008',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Decision date/time must not be blank',
    'Decision date/time is required on every universe row.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE decision_ts IS NULL OR TRIM(CAST(decision_ts AS VARCHAR(4000))) = ''''',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'NOT_EMPTY',
    'decision_ts',
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    20,
    'UM_AUDIT',
    1
);

-- UM-REQ-009: Service type must not be blank
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1009,
    'UM-REQ-009',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Service type must not be blank',
    'Service type is required on every universe row.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE service_type IS NULL OR TRIM(CAST(service_type AS VARCHAR(4000))) = ''''',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'NOT_EMPTY',
    'service_type',
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    20,
    'UM_AUDIT',
    1
);

-- UM-REQ-010: Member ID must not be blank
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1010,
    'UM-REQ-010',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Member ID must not be blank',
    'Member ID is required on every universe row.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE member_id IS NULL OR TRIM(CAST(member_id AS VARCHAR(4000))) = ''''',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'NOT_EMPTY',
    'member_id',
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    20,
    'UM_AUDIT',
    1
);

-- UM-REQ-011: Provider ID must not be blank
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1011,
    'UM-REQ-011',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Provider ID must not be blank',
    'Provider ID is required on every universe row.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE provider_id IS NULL OR TRIM(CAST(provider_id AS VARCHAR(4000))) = ''''',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'NOT_EMPTY',
    'provider_id',
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    20,
    'UM_AUDIT',
    1
);

-- UM-FMT-001: Auth/claim number must not contain letters other than 'H'
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1012,
    'UM-FMT-001',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Auth/claim number must not contain letters other than ''H''',
    'Source-system-specific: auth/claim numbers are numeric except for a leading/embedded ''H'' (contract-number carryover); any other letter indicates a data-entry or extract error.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE auth_or_claim_number IS NOT NULL
  AND REGEXP_SIMILAR(auth_or_claim_number, ''[A-GI-Za-gi-z]'', ''c'') = 1',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'REGEX_MATCH',
    'auth_or_claim_number',
    '{"pattern": "^[0-9H]+$"}',
    'teradata',
    1,
    'pull_date',
    'DATE',
    30,
    'UM_AUDIT',
    1
);

-- UM-FMT-002: Contract ID must match Medicare contract-number format
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1013,
    'UM-FMT-002',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Contract ID must match Medicare contract-number format',
    'Contract ID must be an ''H'' followed by 4 digits (e.g. H1234).',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE contract_id IS NOT NULL
  AND REGEXP_SIMILAR(contract_id, ''^H[0-9]{4}$'', ''c'') = 0',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'REGEX_MATCH',
    NULL,
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    30,
    'UM_AUDIT',
    1
);

-- UM-FMT-003: Enrollee ID must be numeric
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1014,
    'UM-FMT-003',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Enrollee ID must be numeric',
    'Enrollee ID is expected to be a purely numeric identifier per the MHK ODAG1 extract spec.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE enrollee_id IS NOT NULL
  AND REGEXP_SIMILAR(enrollee_id, ''^[0-9]+$'', ''c'') = 0',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'REGEX_MATCH',
    NULL,
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    30,
    'UM_AUDIT',
    1
);

-- UM-VAL-001: Request disposition must be an allowed value
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1015,
    'UM-VAL-001',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Request disposition must be an allowed value',
    'Request disposition must be Approved / Denied / Withdrawn / Dismissed.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE request_disposition NOT IN (''Approved'', ''Denied'', ''Withdrawn'', ''Dismissed'')
   OR request_disposition IS NULL',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'IN_LIST',
    'request_disposition',
    '{"values": ["Approved", "Denied", "Withdrawn", "Dismissed"]}',
    'teradata',
    1,
    'pull_date',
    'DATE',
    30,
    'UM_AUDIT',
    1
);

-- UM-VAL-002: Service type must be an allowed value
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1016,
    'UM-VAL-002',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Service type must be an allowed value',
    'Service type drives which SLA clock applies and must be one of the five recognised categories.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE service_type NOT IN (''STANDARD_PRESERVICE'', ''PART_B_DRUG'', ''EXPEDITED'',
                            ''EXPEDITED_PART_B'', ''DSNP_AIP'')
   OR service_type IS NULL',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'IN_LIST',
    NULL,
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    30,
    'UM_AUDIT',
    1
);

-- UM-VAL-003: Network status must be an allowed value
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1017,
    'UM-VAL-003',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Network status must be an allowed value',
    'Network status must be In-Network or Out-of-Network (used by COMO''s denial/out-of-network stratification).',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE network_status NOT IN (''In-Network'', ''Out-of-Network'')
   OR network_status IS NULL',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'IN_LIST',
    NULL,
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    30,
    'UM_AUDIT',
    1
);

-- UM-COND-001: Denied requests must have a denial reason
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1018,
    'UM-COND-001',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Denied requests must have a denial reason',
    'If disposition = Denied, denial_reason must not be blank.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE request_disposition = ''Denied''
  AND (denial_reason IS NULL OR TRIM(denial_reason) = '''')',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'CONDITIONAL',
    NULL,
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    40,
    'UM_AUDIT',
    1
);

-- UM-COND-002: Approved requests must have a real notification date
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1019,
    'UM-COND-002',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Approved requests must have a real notification date',
    'If disposition = Approved, written or oral notification date must not be the ''NA'' sentinel or blank.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE request_disposition = ''Approved''
  AND (
        (written_notification_date IS NULL OR UPPER(written_notification_date) = ''NA'')
    AND (oral_notification_date    IS NULL OR UPPER(oral_notification_date)    = ''NA'')
  )',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'CONDITIONAL',
    NULL,
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    40,
    'UM_AUDIT',
    1
);

-- UM-COND-003: Withdrawn requests must have a withdrawal reason
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1020,
    'UM-COND-003',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Withdrawn requests must have a withdrawal reason',
    'If disposition = Withdrawn, withdrawal_reason must not be blank.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE request_disposition = ''Withdrawn''
  AND (withdrawal_reason IS NULL OR TRIM(withdrawal_reason) = '''')',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'CONDITIONAL',
    NULL,
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    40,
    'UM_AUDIT',
    1
);

-- UM-COND-004: Denied requests must carry a written notification date
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1021,
    'UM-COND-004',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Denied requests must carry a written notification date',
    'If disposition = Denied, written_notification_date must not be blank or the ''NA'' sentinel (CMS requires written notice of any denial).',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE request_disposition = ''Denied''
  AND (written_notification_date IS NULL OR UPPER(written_notification_date) = ''NA'')',
    'enrollee_id, auth_or_claim_number',
    'Compliance Flag',
    'CONDITIONAL',
    NULL,
    NULL,
    'teradata',
    0,
    'pull_date',
    'DATE',
    40,
    'UM_AUDIT',
    1
);

-- UM-XFIELD-001: Decision must not precede the request
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1022,
    'UM-XFIELD-001',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Decision must not precede the request',
    'Decision date/time must not be earlier than request-received date/time.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE decision_ts IS NOT NULL AND request_received_ts IS NOT NULL
  AND decision_ts < request_received_ts',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'CROSS_COLUMN',
    NULL,
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    50,
    'UM_AUDIT',
    1
);

-- UM-XFIELD-002: Effectuation must not precede the decision
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1023,
    'UM-XFIELD-002',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Effectuation must not precede the decision',
    'Effectuation date must not be earlier than decision date.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE effectuation_date IS NOT NULL AND decision_ts IS NOT NULL
  AND effectuation_date < CAST(decision_ts AS DATE)',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'CROSS_COLUMN',
    NULL,
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    50,
    'UM_AUDIT',
    1
);

-- UM-SLA-STD-001: Standard pre-service — 7 calendar days (no extension)
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1024,
    'UM-SLA-STD-001',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Standard pre-service — 7 calendar days (no extension)',
    'Standard pre-service determinations without an extension must be decided within 7 calendar days of the request.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE service_type = ''STANDARD_PRESERVICE''
  AND (extension_flag IS NULL OR extension_flag = ''N'')
  AND decision_ts IS NOT NULL AND request_received_ts IS NOT NULL
  AND decision_ts > request_received_ts + INTERVAL ''7'' DAY',
    'enrollee_id, auth_or_claim_number',
    'Timeliness',
    'CROSS_COLUMN',
    NULL,
    NULL,
    'teradata',
    0,
    'pull_date',
    'DATE',
    60,
    'UM_AUDIT',
    1
);

-- UM-SLA-STD-002: Standard pre-service with extension — 28 days (eff. 1/1/2026)
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1025,
    'UM-SLA-STD-002',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Standard pre-service with extension — 28 days (eff. 1/1/2026)',
    'Standard pre-service determinations WITH an extension must be decided within 28 calendar days, effective 1/1/2026 (was 14/28 previously — filter_sql on this rule should be reviewed if backfilling pre-2026 runs).',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE service_type = ''STANDARD_PRESERVICE''
  AND extension_flag = ''Y''
  AND decision_ts IS NOT NULL AND request_received_ts IS NOT NULL
  AND decision_ts > request_received_ts + INTERVAL ''28'' DAY',
    'enrollee_id, auth_or_claim_number',
    'Timeliness',
    'CROSS_COLUMN',
    NULL,
    NULL,
    'teradata',
    0,
    'pull_date',
    'DATE',
    60,
    'UM_AUDIT',
    1
);

-- UM-SLA-PARTB-001: Part B drugs — 72 hours
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1026,
    'UM-SLA-PARTB-001',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Part B drugs — 72 hours',
    'Part B drug determinations must be decided within 72 hours of the request.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE service_type = ''PART_B_DRUG''
  AND decision_ts IS NOT NULL AND request_received_ts IS NOT NULL
  AND decision_ts > request_received_ts + INTERVAL ''72'' HOUR',
    'enrollee_id, auth_or_claim_number',
    'Timeliness',
    'CROSS_COLUMN',
    NULL,
    NULL,
    'teradata',
    0,
    'pull_date',
    'DATE',
    60,
    'UM_AUDIT',
    1
);

-- UM-SLA-EXP-001: Expedited — 72 hours (no extension)
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1027,
    'UM-SLA-EXP-001',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Expedited — 72 hours (no extension)',
    'Expedited determinations without an extension must be decided within 72 hours of the request.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE service_type = ''EXPEDITED''
  AND (extension_flag IS NULL OR extension_flag = ''N'')
  AND decision_ts IS NOT NULL AND request_received_ts IS NOT NULL
  AND decision_ts > request_received_ts + INTERVAL ''72'' HOUR',
    'enrollee_id, auth_or_claim_number',
    'Timeliness',
    'CROSS_COLUMN',
    NULL,
    NULL,
    'teradata',
    0,
    'pull_date',
    'DATE',
    60,
    'UM_AUDIT',
    1
);

-- UM-SLA-EXP-002: Expedited with extension — 17 days
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1028,
    'UM-SLA-EXP-002',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Expedited with extension — 17 days',
    'Expedited determinations WITH an extension must be decided within 17 calendar days.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE service_type = ''EXPEDITED''
  AND extension_flag = ''Y''
  AND decision_ts IS NOT NULL AND request_received_ts IS NOT NULL
  AND decision_ts > request_received_ts + INTERVAL ''17'' DAY',
    'enrollee_id, auth_or_claim_number',
    'Timeliness',
    'CROSS_COLUMN',
    NULL,
    NULL,
    'teradata',
    0,
    'pull_date',
    'DATE',
    60,
    'UM_AUDIT',
    1
);

-- UM-SLA-EXPPARTB-001: Expedited Part B — 24 hours
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1029,
    'UM-SLA-EXPPARTB-001',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Expedited Part B — 24 hours',
    'Expedited Part B drug determinations must be decided within 24 hours.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE service_type = ''EXPEDITED_PART_B''
  AND decision_ts IS NOT NULL AND request_received_ts IS NOT NULL
  AND decision_ts > request_received_ts + INTERVAL ''24'' HOUR',
    'enrollee_id, auth_or_claim_number',
    'Timeliness',
    'CROSS_COLUMN',
    NULL,
    NULL,
    'teradata',
    0,
    'pull_date',
    'DATE',
    60,
    'UM_AUDIT',
    1
);

-- UM-SLA-DSNP-001: DSNP-AIP — 3-day written notice rule
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1030,
    'UM-SLA-DSNP-001',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'DSNP-AIP — 3-day written notice rule',
    'DSNP-AIP determinations must have written notification issued within 3 calendar days of the decision.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE service_type = ''DSNP_AIP''
  AND decision_ts IS NOT NULL
  AND (
        written_notification_date IS NULL
     OR UPPER(written_notification_date) = ''NA''
     OR CAST(written_notification_date AS DATE) > CAST(decision_ts AS DATE) + INTERVAL ''3'' DAY
  )',
    'enrollee_id, auth_or_claim_number',
    'Compliance Flag',
    'CONDITIONAL',
    NULL,
    NULL,
    'teradata',
    0,
    'pull_date',
    'DATE',
    60,
    'UM_AUDIT',
    1
);

-- UM-BIZHRS-001: Decision time must fall within business hours
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1031,
    'UM-BIZHRS-001',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Decision time must fall within business hours',
    'Decision time must be between 06:00 and 22:00.',
    'SELECT enrollee_id, auth_or_claim_number
FROM um_universe
WHERE decision_ts IS NOT NULL
  AND (CAST(decision_ts AS TIME) < TIME ''06:00:00''
       OR CAST(decision_ts AS TIME) > TIME ''22:00:00'')',
    'enrollee_id, auth_or_claim_number',
    'Data Validation Error',
    'CROSS_COLUMN',
    NULL,
    NULL,
    'teradata',
    1,
    'pull_date',
    'DATE',
    70,
    'UM_AUDIT',
    1
);

-- UM-DUP-001: Reopened case within 65 days of a prior denial
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1032,
    'UM-DUP-001',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Reopened case within 65 days of a prior denial',
    'Same member + same issue: a denial was issued, then a new request was opened within 65 days — flagged for compliance review (statistically invalid to treat as an independent case).',
    'SELECT u.enrollee_id, u.auth_or_claim_number
FROM um_universe u
JOIN um_universe prior
  ON  prior.member_id = u.member_id
  AND prior.issue_type = u.issue_type
  AND prior.request_disposition = ''Denied''
  AND prior.auth_or_claim_number <> u.auth_or_claim_number
  AND prior.decision_ts < u.request_received_ts
  AND u.request_received_ts <= prior.decision_ts + INTERVAL ''65'' DAY',
    'enrollee_id, auth_or_claim_number',
    'Compliance Flag',
    'CROSS_COLUMN',
    NULL,
    NULL,
    'teradata',
    0,
    'pull_date',
    'DATE',
    80,
    'UM_AUDIT',
    1
);

-- UM-XTAB-SHRPA-001: Approved determinations requiring SHRPA clinical review
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1033,
    'UM-XTAB-SHRPA-001',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Approved determinations requiring SHRPA clinical review',
    'Determination = Approved requires checking the SHRPA reference table for a ''requires clinical review'' flag; if set, a completed clinical review must be on file.',
    'SELECT u.enrollee_id, u.auth_or_claim_number
FROM um_universe u
JOIN shrpa_reference s ON s.authorization_number = u.auth_or_claim_number
WHERE u.request_disposition = ''Approved''
  AND s.requires_clinical_review = ''Y''
  AND (s.clinical_review_completed_flag IS NULL OR s.clinical_review_completed_flag <> ''Y'')',
    'enrollee_id, auth_or_claim_number',
    'Compliance Flag',
    'REFERENTIAL_INTEGRITY',
    NULL,
    NULL,
    'teradata',
    0,
    'pull_date',
    'DATE',
    90,
    'UM_AUDIT',
    1
);

-- UM-XTAB-PROV-001: Provider must have an active contract at time of decision
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1034,
    'UM-XTAB-PROV-001',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Provider must have an active contract at time of decision',
    'Provider contract status requires checking the separate contract/provider reference table — provider must be under an ACTIVE contract as of the decision date.',
    'SELECT u.enrollee_id, u.auth_or_claim_number
FROM um_universe u
JOIN provider_contract pc ON pc.provider_id = u.provider_id
WHERE u.provider_id IS NOT NULL
  AND (pc.contract_status <> ''Active''
       OR pc.status_effective_date > CAST(u.decision_ts AS DATE))',
    'enrollee_id, auth_or_claim_number',
    'Compliance Flag',
    'REFERENTIAL_INTEGRITY',
    NULL,
    NULL,
    'teradata',
    0,
    'pull_date',
    'DATE',
    90,
    'UM_AUDIT',
    1
);

-- UM-FRESH-001: Universe pull must occur every Friday (even on holidays)
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1035,
    'UM-FRESH-001',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Universe pull must occur every Friday (even on holidays)',
    'The weekly ODAG1 universe pull must have landed within the expected window — MAX(pull_date) must be within 4 days of the run date.',
    NULL,
    'enrollee_id',
    'Data Validation Error',
    'FRESHNESS',
    'pull_date',
    '{"max_age_hours": 96}',
    NULL,
    1,
    NULL,
    NULL,
    10,
    'UM_AUDIT',
    1
);

-- UM-VOL-001: Weekly universe volume must be in the expected range
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1036,
    'UM-VOL-001',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe',
    NULL,
    'teradata',
    'Weekly universe volume must be in the expected range',
    'Expected weekly volume is ~25,000-30,000 cases; a count far outside this range indicates a truncated or duplicated pull.',
    NULL,
    'enrollee_id',
    'Data Validation Error',
    'ROW_COUNT_RANGE',
    NULL,
    '{"min_rows": 20000, "max_rows": 35000}',
    NULL,
    1,
    'pull_date',
    'DATE',
    10,
    'UM_AUDIT',
    1
);

-- UM-REFDATA-PG-001: SHRPA reference rows must have a non-blank auth number
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1037,
    'UM-REFDATA-PG-001',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'shrpa_reference',
    NULL,
    'um_refdata',
    'SHRPA reference rows must have a non-blank auth number',
    'Standalone data-quality check directly against the RDS Postgres reference store (independent of the join rules above) — every SHRPA row must key back to a real authorization number.',
    'SELECT authorization_number
FROM shrpa_reference
WHERE authorization_number IS NULL OR TRIM(authorization_number) = ''''',
    'authorization_number',
    'Data Validation Error',
    'NOT_EMPTY',
    NULL,
    NULL,
    'postgres',
    1,
    NULL,
    NULL,
    95,
    'UM_AUDIT',
    1
);

-- UM-ARCHIVE-S3-001: Archived weekly snapshot must be present in S3
INSERT INTO CMSUNIV_FILELAND_T.dq_rules (rule_id, rule_code, project_name, process_name, src_tbl_nm, src_db_name, source_system, rule_name, rule_description, rule_syntax, primary_key_columns, severity, check_type, check_column, check_params, sql_dialect, business_correctable, filter_column, filter_type, priority, rule_group, active_flag) VALUES (
    1038,
    'UM-ARCHIVE-S3-001',
    'HEALTHSPRING_UM',
    'UNIVERSE_VALIDATION',
    'um_universe_archive',
    's3://healthspring-dq-archive/um_universe/*.parquet',
    'um_archive',
    'Archived weekly snapshot must be present in S3',
    'The 10-year immutable archive (Section 3.7) must contain a Parquet snapshot for the current pull — queried directly via DuckDB/httpfs against the landed S3 objects (Section 7), no separate copy step.',
    'SELECT enrollee_id
FROM um_universe_archive
WHERE pull_date < CURRENT_DATE - 10',
    'enrollee_id',
    'Data Validation Error',
    'FRESHNESS',
    NULL,
    NULL,
    'ansi',
    1,
    NULL,
    NULL,
    95,
    'UM_AUDIT',
    1
);
