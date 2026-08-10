# DQ Framework — sample entries for every metadata table

All 17 tables live in one Teradata schema (`CMSUNIV_FILELAND_DEV_T` in dev),
split across three DDL files: `ddl_shared.sql` (`dq_scope`), `rules_engine/ddl.sql`
(`dq_rules` through `dq_notification_routes`), and `sampling/ddl.sql`
(`dq_sampling_config`, `dq_sample_selections`).

They fall into three groups, and that distinction matters more than the SQL
syntax itself:

- **A. You author these by hand** — one-time or occasional config, usually
  seeded via a `config/seed/*.sql` file. This is where onboarding a new
  project actually happens.
- **B. The engine writes these for you** — every run, `rules_engine/main.py`
  / `sampling/engine.py` populate these automatically. You never hand-insert
  these in production; the samples below exist so you know what shape to
  *expect*, and so you can hand-craft a row for local testing without running
  a full pipeline.
- **C. Ops writes these occasionally, outside a run** — a suppression when a
  rule is known-broken, or a disposition when someone reviews a finding.
  Human- or script-triggered, but not part of onboarding.

One running example is used throughout — a fictional `ACME_CLAIMS` /
`MONTHLY_AUDIT` project — so the rows below are internally consistent and
you can trace one rule from definition through to a finding. Swap in your
own project/process/rule names; the shapes don't change.

`<your_meta_db>` = your Teradata metadata schema (`CMSUNIV_FILELAND_DEV_T` in
the DDL files above).

---

## A. Tables you author by hand

### 1. `dq_scope` — project/process dimension

Every other table hangs off `scope_id`. Create this first for any new
project. `scope_id` is `GENERATED ALWAYS AS IDENTITY` — never insert it
yourself.

```sql
INSERT INTO <your_meta_db>.dq_scope (project_name, process_name)
VALUES ('ACME_CLAIMS', 'MONTHLY_AUDIT');
```

`process_name` can be `NULL` if a project has no sub-process concept.
`(project_name, process_name)` is unique (`dq_scope_lookup_uix`) — inserting
the same pair twice is an error, not a silent duplicate. In practice you
rarely INSERT this directly: `utils/db_helpers.get_scope_id()` does a
get-or-create for you from `main.py --project/--process` flags.

### 2. `dq_rules` — the rule definitions

Every rule is a complete, self-contained **negative-SQL SELECT**: it returns
the rows that VIOLATE the rule; zero rows = PASS. `sql_dialect` is
`NOT NULL` — every rule must declare it.

```sql
INSERT INTO <your_meta_db>.dq_rules (
    rule_id, rule_code, scope_id, src_tbl_nm, src_db_name, source_system,
    rule_name, rule_description, rule_syntax, primary_key_columns,
    severity, check_type, sql_dialect, business_correctable,
    filter_column, filter_type, priority, rule_group, active_flag
) VALUES (
    1, 'ACME-001',
    (SELECT scope_id FROM <your_meta_db>.dq_scope
     WHERE project_name = 'ACME_CLAIMS' AND process_name = 'MONTHLY_AUDIT'),
    'claims', NULL, 'teradata',
    'Claim amount must be non-negative',
    'A negative claim_amount indicates an upstream data error, not a legitimate adjustment.',
    'SELECT claim_id, claim_amount
FROM claims
WHERE claim_amount < 0',
    'claim_id',
    'HIGH', 'RANGE_CHECK', 'teradata', 0,
    'pull_date', 'DATE',
    20, 'FINANCIAL', 1
);
```

Notes:
- `check_type` (`'RANGE_CHECK'` above) is **purely a free-text
  classification tag** shown as a findings column on the dashboard — it
  never generates or affects the rule's SQL. Any label you like.
- `filter_column`/`filter_type` are optional run-window scoping (`BATCH` or
  `DATE`) — the engine wraps your `rule_syntax` as an outer
  `SELECT * FROM (<rule_syntax>) x WHERE (<filter>)`, so `pull_date` (or
  whatever `filter_column` names) must be selectable in that outer wrap.
  Leave both `NULL` for an unscoped, always-full-table rule.
- `primary_key_columns` is required — it's how a violating row becomes a
  storable, comparable exception key. Must be columns your `rule_syntax`
  actually returns.
- `sql_dialect` must be one the target connection's `source_type` accepts
  (`teradata`→teradata/ansi, `postgres`/`aurora`/`s3`/`file`→postgres/ansi).
  A mismatch is caught before the rule ever runs, not as a mid-run SQL error.

### 3. `dq_profile_config` — opt-in column profiling

Wildcard config: `NULL` in `project_name`/`process_name` matches anything.

```sql
INSERT INTO <your_meta_db>.dq_profile_config (
    config_id, project_name, process_name, table_name,
    active, columns_include, columns_exclude, top_n_values,
    run_frequency
) VALUES (
    1, 'ACME_CLAIMS', 'MONTHLY_AUDIT', 'claims',
    1, NULL, 'internal_notes', 10,
    'ALWAYS'
);
```

`columns_include = NULL` means "profile every column"; `columns_exclude` is
a CSV of columns to skip regardless. `run_frequency` is
`ALWAYS | DAILY | WEEKLY | MANUAL`.

### 4. `dq_anomaly_config` — anomaly-detection sensitivity

Also wildcard-matched; most-specific row wins at read time.

```sql
INSERT INTO <your_meta_db>.dq_anomaly_config (
    config_id, project_name, process_name, run_type, process,
    zscore_threshold, iqr_multiplier, min_history_runs, alert_on_anomaly
) VALUES (
    1, 'ACME_CLAIMS', 'MONTHLY_AUDIT', NULL, 'BOTH',
    3.0, 1.5, 8, 1
);
```

`process` is the detection algorithm (`ZSCORE | IQR | BOTH`) — named
`process`, not `method`, because `METHOD` is a Teradata reserved word.
Needs `min_history_runs` prior runs before it starts alerting, so a brand
new project won't see anomaly alerts for a while — that's by design, not a
bug.

### 5. `dq_notification_routes` — who gets told what

```sql
INSERT INTO <your_meta_db>.dq_notification_routes (
    route_id, project_name, process_name, finding_class, audience,
    channel_type, destination, business_correctable_only, active_flag
) VALUES (
    1, 'ACME_CLAIMS', NULL, 'DATA_VIOLATION', 'BUSINESS',
    'EMAIL', 'acme-finance-dq@example.com', 0, 1
);
```

`finding_class` is `DATA_VIOLATION` (a rule failed) or `ENGINE_FAILURE` (a
rule errored/couldn't run) — never both on one route. Add a second row for
the engineering audience's `ENGINE_FAILURE` alerts, a third for a
`business_correctable_only = 1` route that only pages business users for
findings marked correctable on `dq_rules`, etc. — one row per audience/channel.

### 6. `dq_sampling_config` — stratified sampling setup

Only needed if this project also does "pick N highest-priority cases for
human review" on top of pass/fail findings — a separate, optional framework.

```sql
INSERT INTO <your_meta_db>.dq_sampling_config (
    config_id, scope_id, sample_name, connection_name, universe_table,
    key_columns, scope_column, target_volume,
    determination_column, determination_mix_json,
    functional_area_column, functional_area_mix_json,
    exclusion_sql, priority_rank_sql, schedule_cron, active_flag
) VALUES (
    1,
    (SELECT scope_id FROM <your_meta_db>.dq_scope
     WHERE project_name = 'ACME_CLAIMS' AND process_name = 'MONTHLY_AUDIT'),
    'MONTHLY_CLAIMS_REVIEW_SAMPLE', 'acme_claims_db', 'claims',
    'claim_id', 'pull_date', 150,
    'claim_status', '{"Denied": 0.60, "Pended": 0.40}',
    'business_line', '{"Commercial": 0.70, "Medicare": 0.30}',
    'claim_status = ''Withdrawn''',
    'claim_amount DESC',
    '0 8 1 * *', 1
);
```

`connection_name` must match an entry in `config/connections.yaml`.
`exclusion_sql`/`priority_rank_sql` are raw SQL fragments (a WHERE
condition and an ORDER BY expression) evaluated against `universe_table`.
`*_mix_json` percentages describe the target stratification, not a hard cap.

---

## B. Tables the engine writes for you (reference only)

You don't normally hand-insert these — `rules_engine/main.py` and
`sampling/engine.py` do it automatically every run. Shown here so you know
the exact shape, e.g. for a manual backfill or a local test fixture.

### 7. `dq_run_control` — one row per run, written at run start

```sql
INSERT INTO <your_meta_db>.dq_run_control (
    run_id, scope_id, run_type, run_mode, batch_id, dataset_id,
    start_date, end_date, triggered_by, start_time, end_time, status
) VALUES (
    'ACME_CLAIMS_MONTHLY_AUDIT_20260201_083000',
    (SELECT scope_id FROM <your_meta_db>.dq_scope
     WHERE project_name = 'ACME_CLAIMS' AND process_name = 'MONTHLY_AUDIT'),
    'MONTHLY', 'DATE', NULL, NULL,
    DATE '2026-01-01', DATE '2026-01-31',
    'scheduler', TIMESTAMP '2026-02-01 08:30:00', TIMESTAMP '2026-02-01 08:34:12',
    'COMPLETED'
);
```

`run_id` is application-generated (not an identity column) — the engine
builds it from project/process/timestamp. `status` moves
`RUNNING → COMPLETED | FAILED` as the run progresses.

### 8. `dq_rule_execution` — one row per rule per run

```sql
INSERT INTO <your_meta_db>.dq_rule_execution (
    run_id, rule_id, rule_code, table_name,
    total_records, failed_records, passed_records,
    failure_pct, pass_pct, severity, status,
    execution_time, run_timestamp, run_date, run_month
) VALUES (
    'ACME_CLAIMS_MONTHLY_AUDIT_20260201_083000', 1, 'ACME-001', 'claims',
    48213, 7, 48206,
    0.014518, 99.985482, 'HIGH', 'FAIL',
    2.341, TIMESTAMP '2026-02-01 08:31:05', DATE '2026-02-01', DATE '2026-02-01'
);
```

`rule_code`/`severity`/`table_name` are frozen snapshots of what `dq_rules`
said *at execution time* — deliberately not a live join, since `dq_rules`
can be edited later and an audit record has to reflect what was judged, not
what the rule says today. `status` is `PASS | FAIL | WARN | ERROR | SKIP`.

### 9. `dq_exceptions` — one row per violating record (capped)

```sql
INSERT INTO <your_meta_db>.dq_exceptions (
    run_id, rule_id, rule_code, table_name, key_json, primary_key_str
) VALUES (
    'ACME_CLAIMS_MONTHLY_AUDIT_20260201_083000', 1, 'ACME-001', 'claims',
    '{"claim_id": "CLM-88231"}', 'claim_id=CLM-88231'
);
```

`exception_id` is `GENERATED ALWAYS AS IDENTITY`. `key_json`/
`primary_key_str` are both built from `dq_rules.primary_key_columns` against
the failing row — same key, two formats (structured vs. human-readable).
Capped at `DQ_MAX_EXCEPTIONS` (default 10,000) rows per rule per run; the
`failed_records` count on `dq_rule_execution` above is always accurate even
when this cap truncates which *rows* get captured.

### 10. `dq_metrics_summary` — monthly rollup

```sql
INSERT INTO <your_meta_db>.dq_metrics_summary (
    scope_id, run_type, batch_id, dataset_id, run_month,
    total_runs, total_rules, failed_rules, passed_rules,
    total_records, failed_records, avg_failure_pct, dq_score
) VALUES (
    (SELECT scope_id FROM <your_meta_db>.dq_scope
     WHERE project_name = 'ACME_CLAIMS' AND process_name = 'MONTHLY_AUDIT'),
    'MONTHLY', NULL, NULL, DATE '2026-02-01',
    1, 12, 1, 11,
    48213, 7, 0.014518, 91.6667
);
```

Upserted (not appended) — `dq_metrics_summary_uix` on
`(scope_id, run_type, batch_id, dataset_id, run_month)` prevents a
double-write if two runs merge concurrently into the same month's row.

### 11. `dq_run_logs` — run log lines, plus engine-side failures

A plain run-level log line leaves `table_name`/`issue_type` `NULL`:

```sql
INSERT INTO <your_meta_db>.dq_run_logs (
    run_id, rule_id, rule_code, log_level, message, error_code, error_detail
) VALUES (
    'ACME_CLAIMS_MONTHLY_AUDIT_20260201_083000', 1, 'ACME-001', 'INFO',
    'Rule ACME-001 completed: 7 failed / 48213 total (0.0145%)', NULL, NULL
);
```

An engine-side failure (never a data finding) sets `issue_type` and
`table_name`, marking the row as triageable:

```sql
INSERT INTO <your_meta_db>.dq_run_logs (
    run_id, rule_id, rule_code, table_name, log_level, message,
    error_code, error_detail, issue_type
) VALUES (
    'ACME_CLAIMS_MONTHLY_AUDIT_20260201_083000', 4, 'ACME-004', 'refunds', 'ERROR',
    'SQL build failed for rule ACME-004', 'CONFIG_ERROR',
    'Rule ACME-004 has no sql_dialect set.', 'CONFIG_ERROR'
);
```

`log_id` is identity. `rule_id`/`rule_code` are `NULL` for run-level
(non-rule-specific) log lines. `log_level` is typically `INFO | WARNING | ERROR`.
`issue_type` values include `CONFIG_ERROR`, `DIALECT_MISMATCH`,
`UNSAFE_RULE_SQL`, `SQL_SYNTAX`, `DATA_RUNTIME`, `UNRESOLVED_DEPENDENCY`,
`QUERY_RISK` — a row with `issue_type` set is always paired with
`status='ERROR'` on that rule's `dq_rule_execution` row (except the
advisory `QUERY_RISK` warning, which never blocks a run), and is never
written to `dq_exceptions`. `rules_engine/engine.py::_count_issues()`
counts exactly the rows where `issue_type IS NOT NULL` for a run's
issue_count.

### 12. `dq_rule_versions` — automatic snapshot on every tracked-field change

```sql
INSERT INTO <your_meta_db>.dq_rule_versions (
    rule_id, rule_code, version_num, change_type,
    rule_syntax, check_type, filter_sql, threshold_pct, threshold_count,
    threshold_operator, severity, active_flag, change_reason
) VALUES (
    1, 'ACME-001', 1, 'CREATED',
    'SELECT claim_id, claim_amount FROM claims WHERE claim_amount < 0',
    'RANGE_CHECK', NULL, NULL, NULL,
    'OR', 'HIGH', 1, NULL
);
```

`version_id` is identity, `version_num` auto-increments per `rule_id`
(handled by `rules_engine/rule_lifecycle.py`, not something you compute by
hand). `change_type` is `CREATED` (first version) or `MODIFIED`.

### 13. `dq_column_profile` — per-column statistics (opt-in via `dq_profile_config`)

```sql
INSERT INTO <your_meta_db>.dq_column_profile (
    run_id, table_name, column_name, total_rows, null_count, null_pct,
    distinct_count, distinct_pct, min_value, max_value, mean_value,
    stddev_value, top_values, profile_date, source_type
) VALUES (
    'ACME_CLAIMS_MONTHLY_AUDIT_20260201_083000', 'claims', 'claim_amount',
    48213, 0, 0.0,
    41209, 85.46, '0.00', '184320.55', 612.44,
    2140.9,
    '[{"value":"0.00","count":812},{"value":"150.00","count":390}]',
    DATE '2026-02-01', 'teradata'
);
```

`profile_id` is identity. `min_value`/`max_value` are stored as text so the
same column works for numeric, date, and string columns alike.

### 14. `dq_anomaly_log` — one row per metric per detection method per run

```sql
INSERT INTO <your_meta_db>.dq_anomaly_log (
    run_id, metric_name, current_value, historical_mean, historical_std,
    z_score, iqr_lower_bound, iqr_upper_bound, is_anomaly,
    detection_method, severity
) VALUES (
    'ACME_CLAIMS_MONTHLY_AUDIT_20260201_083000', 'dq_score', 91.67, 97.2, 1.4,
    -3.95, NULL, NULL, 1,
    'ZSCORE', 'HIGH'
);
```

`dq_anomaly_config.process = 'BOTH'` (see table 4 above) means one run can
write both a `ZSCORE` row and an `IQR` row for the *same* `metric_name` —
that's why the primary index is `(run_id, metric_name, detection_method)`,
not just `(run_id, metric_name)`.

### 15. `dq_sample_selections` — every candidate case a sampling run considered

```sql
INSERT INTO <your_meta_db>.dq_sample_selections (
    sample_run_id, config_id, sample_cycle, case_key, determination_type,
    functional_area, priority_rank, excluded_flag, exclusion_reason,
    selected_flag, strata_json
) VALUES (
    'SAMPLE_ACME_CLAIMS_20260201_090000', 1, DATE '2026-01-31', 'CLM-88231',
    'Denied', 'Commercial', 1, 0, NULL,
    1, '{"claim_amount": 18420.00, "claim_status": "Denied"}'
);
```

This is the largest table by row count in the whole schema by design — it
logs **every** case evaluated for the sample, not just the ones selected
(`selected_flag = 1`). Expect one row per candidate per cycle, not one row
per final sample.

---

## C. Ops writes these occasionally, outside a run

### 16. `dq_rule_suppressions` — temporarily mute a known-broken rule

```sql
INSERT INTO <your_meta_db>.dq_rule_suppressions (
    suppression_id, rule_id, rule_code, reason, suppressed_by,
    expires_at
) VALUES (
    1, 4, 'ACME-004', 'Upstream refunds feed truncated — TICKET-4821',
    'jsmith',
    TIMESTAMP '2026-02-15 00:00:00'
);
```

`suppression_id` is application-assigned, not identity — pick your own
sequence. A suppression is active while `lifted_at IS NULL AND
(expires_at IS NULL OR expires_at > NOW)`. To lift it early:

```sql
UPDATE <your_meta_db>.dq_rule_suppressions
SET lifted_at = CURRENT_TIMESTAMP, lifted_by = 'jsmith'
WHERE suppression_id = 1;
```

### 17. `dq_exception_dispositions` — case review outcome on a finding

```sql
INSERT INTO <your_meta_db>.dq_exception_dispositions (
    exception_id, disposition_type, disposition_reason, disposed_by
) VALUES (
    5001, 'FALSE_POSITIVE', 'Negative amount is a legitimate contra-claim adjustment, confirmed with finance.', 'jsmith'
);
```

`exception_id` must reference a real `dq_exceptions.exception_id`.
`disposition_type` is one of `WAIVED | RESOLVED | FALSE_POSITIVE |
CORRECTED | UNDER_REVIEW | REOPENED`. The underlying `dq_exceptions` row is
**never** updated or deleted — a new disposition on the same case is a new
row with `effective_flag = 1`; the prior row's `effective_flag` flips to 0
in the same transaction. The current state is always "most recent
`effective_flag = 1` row per `exception_id`."

---

## Quick reference

| # | Table | Who writes it | Grows with |
|---|-------|---------------|------------|
| 1 | `dq_scope` | you (once per project) | # of projects |
| 2 | `dq_rules` | you | # of rules you write |
| 3 | `dq_profile_config` | you (optional) | # of profiled tables |
| 4 | `dq_anomaly_config` | you (optional) | # of sensitivity overrides |
| 5 | `dq_notification_routes` | you | # of audiences × channels |
| 6 | `dq_sampling_config` | you (optional) | # of sampling schemes |
| 7 | `dq_run_control` | engine | # of runs |
| 8 | `dq_rule_execution` | engine | # of rules × # of runs |
| 9 | `dq_exceptions` | engine | # of violating rows found (capped) |
| 10 | `dq_metrics_summary` | engine | # of scope × run_type × month combos |
| 11 | `dq_run_logs` | engine | free-text log volume + # of engine-side failures (`issue_type IS NOT NULL`) |
| 12 | `dq_rule_versions` | engine (on rule edit) | # of rule edits |
| 13 | `dq_column_profile` | engine (if profiling on) | # of profiled columns × runs |
| 14 | `dq_anomaly_log` | engine (if anomaly detection on) | # of metrics × runs |
| 15 | `dq_sample_selections` | engine (sampling) | # of candidates evaluated × cycles — largest table in the schema |
| 16 | `dq_rule_suppressions` | ops, ad hoc | # of suppression events |
| 17 | `dq_exception_dispositions` | ops, ad hoc | # of case reviews |

For the full column-by-column DDL and index rationale, see
`ddl_shared.sql`, `rules_engine/ddl.sql`, and `sampling/ddl.sql`. For a
guided first walkthrough (scope → rule → dry-run → run), see `ONBOARDING.md`.
