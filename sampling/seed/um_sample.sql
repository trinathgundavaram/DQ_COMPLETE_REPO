-- ============================================================
-- The original COMO weekly UM sample (the dq_* engine's
-- dq_sampling_config.config_id=1), re-expressed as gre_sampling_config /
-- gre_sampling_strata / gre_sampling_mix rows.
--
-- This is the DONE CRITERIA regression fixture: tests/test_sampling.py's
-- test_um_regression_matches_frozen_dq_stratified_sampling_output runs
-- this exact config (via the equivalent in-memory rows, same values)
-- and asserts matching per-bucket selected counts against a frozen
-- expected-output snapshot -- proving the redesign doesn't regress the
-- one real case that already existed, not just "should work" on the
-- recursive design in the abstract. (That snapshot was originally
-- produced by running this same config side-by-side against the dq_*
-- engine's core/stratified_sampling.py, on the add-generic-rules-engine
-- branch, before this repo was reorganized into rules_engine/ + sampling/
-- and the old dq_* engine was dropped from this branch -- see that
-- branch's history for the live side-by-side comparison.)
--
-- Level 1 (request_disposition) and Level 2 (functional_area) reproduce
-- the original's two hardcoded columns exactly; a third level would be one
-- more gre_sampling_strata row plus its gre_sampling_mix rows, zero code
-- changes.
-- ============================================================

INSERT INTO CMSUNIV_FILELAND_DEV_T.gre_sampling_config (
    config_id, project_name, process_name, sample_name, source_type,
    universe_table, key_columns, scope_sql, exclusion_sql, target_volume,
    sampling_method, priority_rank_sql, rounding_mode, schedule_cron, act_ind
) VALUES (
    1, 'HEALTHSPRING_UM', 'COMO_WEEKLY_SAMPLE', 'COMO_WEEKLY_SAMPLE', 'teradata',
    'um_universe', 'enrollee_id, auth_or_claim_number',
    -- scope_sql: a WHERE-fragment (unlike gre_rules.scope_sql, which is a
    -- COUNT query) -- {batch_id} is this cycle's pull_date, substituted
    -- the same way gre_rules.rule_syntax substitutes it.
    'pull_date = ''{batch_id}''',
    -- Exclusion list: auto-approvals, SHRPA-no-PA, diabetic supplies --
    -- identical to dq_sampling_config.config_id=1.
    'auto_approved = ''Y'' OR shrpa_no_pa_flag = ''Y'' OR issue_type = ''DIABETIC_SUPPLIES''',
    150,
    'RANKED',
    -- Priority ranking: revision=1 first, then inpatient/outpatient balance,
    -- expedited/standard balance, code variety, clinical-review-required --
    -- identical to dq_sampling_config.config_id=1.
    'CASE WHEN revision = 1 THEN 0 ELSE 1 END, ' ||
    'CASE WHEN place_of_service = ''Inpatient'' THEN 0 ELSE 1 END, ' ||
    'CASE WHEN service_type IN (''EXPEDITED'',''EXPEDITED_PART_B'') THEN 0 ELSE 1 END, ' ||
    'procedure_code_family, ' ||
    'CASE WHEN clinical_review_required_flag = ''Y'' THEN 0 ELSE 1 END',
    'FLOOR',   -- matches the proven pattern's math.floor()
    '0 8 * * FRI',
    1
);

-- ── Level 0: request_disposition ───────────────────────────────────────
INSERT INTO CMSUNIV_FILELAND_DEV_T.gre_sampling_strata (
    strata_id, config_id, level_order, level_name, stratify_expr
) VALUES (
    1, 1, 0, 'request_disposition', 'request_disposition'
);

INSERT INTO CMSUNIV_FILELAND_DEV_T.gre_sampling_mix (mix_id, strata_id, bucket_value, target_fraction)
VALUES (1, 1, 'Denied', 0.80);
INSERT INTO CMSUNIV_FILELAND_DEV_T.gre_sampling_mix (mix_id, strata_id, bucket_value, target_fraction)
VALUES (2, 1, 'Withdrawn', 0.10);
INSERT INTO CMSUNIV_FILELAND_DEV_T.gre_sampling_mix (mix_id, strata_id, bucket_value, target_fraction)
VALUES (3, 1, 'Dismissed', 0.02);
INSERT INTO CMSUNIV_FILELAND_DEV_T.gre_sampling_mix (mix_id, strata_id, bucket_value, target_fraction)
VALUES (4, 1, 'Approved', 0.08);

-- ── Level 1: functional_area ────────────────────────────────────────────
INSERT INTO CMSUNIV_FILELAND_DEV_T.gre_sampling_strata (
    strata_id, config_id, level_order, level_name, stratify_expr
) VALUES (
    2, 1, 1, 'functional_area', 'functional_area'
);

INSERT INTO CMSUNIV_FILELAND_DEV_T.gre_sampling_mix (mix_id, strata_id, bucket_value, target_fraction)
VALUES (5, 2, 'Part B', 0.13);
INSERT INTO CMSUNIV_FILELAND_DEV_T.gre_sampling_mix (mix_id, strata_id, bucket_value, target_fraction)
VALUES (6, 2, 'Behavioral Health', 0.08);

-- Adding a third level later (e.g. by provider network) is:
--   INSERT INTO gre_sampling_strata (strata_id, config_id, level_order, level_name, stratify_expr)
--     VALUES (3, 1, 2, 'provider_network', 'provider_network');
--   INSERT INTO gre_sampling_mix (mix_id, strata_id, bucket_value, target_fraction) VALUES (...);
-- -- zero changes to sampling/sampling.py.
