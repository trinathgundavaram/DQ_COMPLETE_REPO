-- One-time: clone ODAG1 rules into rule group ODAG1_FILE so they run against a file on the NAS.
-- NAS folder is set below. SRC_TBL_NM 'odag1_file.csv' is only the fixed name the rule SQL reads
-- FROM (odag1_file). The real file name changes every run: pass "file_name" in the trigger config.
-- Only the 3 rules below are rewritten and active. Any other ODAG1 rule is cloned INACTIVE (ACT_IND=0)
-- with its Teradata SQL untouched -- rewrite it before turning it on.
-- Run the group with:  rule_group = ODAG1_FILE, text_params = {"RUNTYPE": "MNT"}, file_name = <the file>, Variable nas_connection_id set.

INSERT INTO HSABC_DEV.GRE_RULES
(RULE_ID, RULE_NM, ACT_IND, RULE_GROUP, RULE_VARIANT, PROJECT_NAME, PROCESS_NAME, SEQ_NO, SEQUENCING_MODE, ON_FAILURE,
 DATABASE_NAME, SRC_TBL_NM, SQL_DIALECT, RULE_SYNTAX, SRC_KEY_COLS, ELEMENT_NAME, THRESHOLD_PCT, THRESHOLD_COUNT,
 THRESHOLD_OPERATOR, SEVERITY, UNIVERSE_VERSION, UNIVERSE_YEAR, DGR_NBR, ISSUE_CATEGORY_NAME, BUSINESS_RULE,
 RULE_DESCRIPTION, CREATED_BY, LAST_UPDATED_BY, LOAD_DATETIME, LAST_UPDATED_DATETIME)
SELECT
    (SELECT COALESCE(MAX(RULE_ID),0) FROM HSABC_DEV.GRE_RULES) + ROW_NUMBER() OVER (ORDER BY RULE_ID),
    RULE_NM || '_FILE',
    CASE WHEN RULE_ID IN (2902, 2937, 2896) THEN ACT_IND ELSE 0 END,
    'ODAG1_FILE', RULE_VARIANT, PROJECT_NAME, PROCESS_NAME, SEQ_NO, SEQUENCING_MODE, ON_FAILURE,
    '\\iadnasd001_cifs.healthspring.inside\chs_dms\Dev\OSS_Inbound\External\CMSUniverse\RE',
    'odag1_file.csv',
    'file',
    CASE RULE_ID
        WHEN 2902 THEN 'SELECT * FROM odag1_file WHERE
 CASE WHEN length(aor_notc_rcpt_dt_txt)<>10 AND aor_notc_rcpt_dt_txt=''None'' THEN ''1900-01-01''
      WHEN length(aor_notc_rcpt_dt_txt)<>10 OR aor_notc_rcpt_dt_txt IS NULL OR aor_notc_rcpt_dt_txt='''' THEN ''9999-01-01''
      WHEN NOT (date(replace(aor_notc_rcpt_dt_txt,''/'',''-'')) IS replace(aor_notc_rcpt_dt_txt,''/'',''-'')) THEN ''9999-01-01''
      ELSE replace(aor_notc_rcpt_dt_txt,''/'',''-'') END > date(stg_cret_dtm,''+3 day'')'
        WHEN 2937 THEN 'SELECT * FROM odag1_file WHERE sdo_cmsuniv_odag1_2022_key IN (
 SELECT sdo_cmsuniv_odag1_2022_key FROM (
  SELECT sdo_cmsuniv_odag1_2022_key,
         ROW_NUMBER() OVER (PARTITION BY mbr_frst_nm,mbr_last_nm,mbr_id,cntrct_num,pbp_num,fdr_nm,authzn_clm_num,req_recv_dt_txt,req_recv_tm_txt,part_b_drug_req_ind,aor_notc_rcpt_dt_txt,aor_notc_rcpt_tm_txt,req_detrmtn_ty,req_procs_ty_cd,timeframe_ext_ind,detrmtn_dt_txt,detrmtn_tm_txt,oral_notftn_mbr_dt_txt,oral_notftn_mbr_tm_txt,wrtn_notftn_mbr_dt_txt,wrtn_notftn_mbr_tm_txt,req_by_ty_cd,iss_desc,expdt_req_procs_std_ind,req_lack_med_necty_deny_ind ORDER BY src_data_key) AS rn
  FROM odag1_file WHERE run_ty=''$RUNTYPE'') WHERE rn>1)'
        WHEN 2896 THEN 'SELECT * FROM odag1_file WHERE wrtn_notftn_mbr_dt_txt IN ('''',''NA'',''N/A'') OR wrtn_notftn_mbr_dt_txt IS NULL
 OR (wrtn_notftn_mbr_dt_txt NOT IN (''None'') AND (length(wrtn_notftn_mbr_dt_txt)<>10 OR NOT (date(replace(wrtn_notftn_mbr_dt_txt,''/'',''-'')) IS replace(wrtn_notftn_mbr_dt_txt,''/'',''-''))))'
        ELSE RULE_SYNTAX END,
    SRC_KEY_COLS, ELEMENT_NAME, THRESHOLD_PCT, THRESHOLD_COUNT, THRESHOLD_OPERATOR, SEVERITY, UNIVERSE_VERSION,
    UNIVERSE_YEAR, DGR_NBR, ISSUE_CATEGORY_NAME, BUSINESS_RULE, RULE_DESCRIPTION,
    CREATED_BY, LAST_UPDATED_BY, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
FROM HSABC_DEV.GRE_RULES
WHERE RULE_GROUP = 'ODAG1';
