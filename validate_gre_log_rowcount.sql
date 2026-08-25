-- Validation query for the gre_log.rowcount inconsistency (see
-- rules_engine/executor.py::execute_rule()'s final _log_attempt() call and
-- its comment). Run this against CMSUNIV_FILELAND_DEV_T to find every
-- (rule_id, run_key) where the CURRENT attempt's gre_log.rowcount
-- disagrees with gre_results.failed_records for the same run -- these are
-- rows written under the OLD logic (rowcount = inserted + reactivated,
-- not the true failed count), before the fix in
-- rules_engine/executor.py landed. Every row this returns will self-
-- correct the next time that rule_id/run_key reruns under the fixed code;
-- this is read-only, for auditing/spot-checking, not a repair script.

SELECT
    l.rule_id,
    l.run_key,
    l.run_id,
    l.rowcount            AS gre_log_rowcount,
    r.failed_records       AS gre_results_failed_records,
    l.rowcount - r.failed_records AS diff,
    l.status,
    l.load_datetime
FROM CMSUNIV_FILELAND_DEV_T.gre_log l
JOIN CMSUNIV_FILELAND_DEV_T.gre_results r
    ON r.rule_id = l.rule_id AND r.run_key = l.run_key AND r.run_id = l.run_id
WHERE l.active_ind = 'Y'                 -- only the current attempt per (rule_id, run_key)
  AND l.status = 'SUCCESS'
  AND l.rowcount <> r.failed_records
ORDER BY ABS(l.rowcount - r.failed_records) DESC;

-- A second, independent check: gre_log.rowcount for the CURRENT attempt
-- should also equal a straight COUNT(*) of that rule_id/run_key's
-- currently-active gre_exceptions rows -- gre_exceptions detail capture
-- is uncapped, so unlike the note this file used to carry, there is no
-- "capped attempt" case where these are expected to disagree any more.
SELECT
    l.rule_id,
    l.run_key,
    l.run_id,
    l.rowcount AS gre_log_rowcount,
    COUNT(e.record_id) AS active_gre_exceptions_count
FROM CMSUNIV_FILELAND_DEV_T.gre_log l
LEFT JOIN CMSUNIV_FILELAND_DEV_T.gre_exceptions e
    ON e.rule_id = l.rule_id AND e.run_key = l.run_key AND e.etl_is_curr_ind = 'Y'
WHERE l.active_ind = 'Y'
  AND l.status = 'SUCCESS'
GROUP BY l.rule_id, l.run_key, l.run_id, l.rowcount
HAVING l.rowcount <> COUNT(e.record_id)
ORDER BY l.rule_id, l.run_key;
