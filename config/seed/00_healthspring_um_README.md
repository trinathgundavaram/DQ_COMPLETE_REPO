# HealthSpring UM — seed configuration

These files onboard the **Clinical Audit Sample Optimization** project onto
the generic engine purely via configuration — no engine code changes. They
are the concrete proof of the genericness claim in Section 1/9.

Load order:
1. `01_setup.sql`   — dq_connections catalogue rows (credentials live in env
   vars per the connection name, never in this table), dq_sampling_config
   (COMO weekly sample), dq_notification_routes (ROAR/BUSINESS/ENGINEERING
   routing), and dq_anomaly_config (auto-determined thresholds)
2. `02_rules.sql`   — ~40 hand-maintained SQL rules covering every category
   in Section 3.2 of the requirements

## Column-name assumption

The actual MHK ODAG1 extract layout was not available to this build — the
rules below assume a landed table `um_universe` with columns named per the
audit rule descriptions (e.g. `enrollee_id`, `request_disposition`,
`decision_ts`). **Before go-live, replace the column names in
`02_rules.sql` with the real ODAG1 field names** (a one-time mapping
exercise) — the rule LOGIC (SLA day counts, allowed values, conditional
checks) does not change, only the column identifiers do. This is a
configuration edit, not an engine change, which is exactly the point of the
raw-SQL authoring model.

## Assumed `um_universe` columns

| Column | Meaning |
|---|---|
| `enrollee_id`, `contract_id`, `pbp` | Medicare plan identifiers |
| `beneficiary_first_name`, `beneficiary_last_name` | Member name |
| `auth_or_claim_number` | Auth/claim number (Section 3.2 format rule) |
| `request_disposition` | Approved / Denied / Withdrawn / Dismissed |
| `denial_reason`, `withdrawal_reason` | Free text |
| `written_notification_date`, `oral_notification_date` | Notification dates ('NA' sentinel used in source) |
| `request_received_ts`, `decision_ts`, `effectuation_date` | Timestamps for SLA / cross-field checks |
| `service_type` | STANDARD_PRESERVICE / PART_B_DRUG / EXPEDITED / EXPEDITED_PART_B / DSNP_AIP |
| `extension_flag` | Y/N — extension invoked |
| `member_id`, `issue_type` | For duplicate/reopened-case detection |
| `provider_id` | Joins to `provider_contract` reference table |
| `network_status` | In-Network / Out-of-Network |
| `pull_date` | The weekly ODAG1 extract pull date (scoping column) |
| `functional_area` | Part B / Behavioral Health / Pre-Cert/OP (for COMO mix) |
| `revision`, `auto_approved`, `shrpa_no_pa`, `diabetic_supplies` | COMO exclusion/priority inputs |

## Reference tables

- `shrpa_reference` (Postgres RDS) — `authorization_number`, `requires_clinical_review`, `clinical_review_completed_flag`
- `provider_contract` (Postgres RDS) — `provider_id`, `contract_status`, `status_effective_date`
