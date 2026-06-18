# DQ Framework — Improvement Scope Review v2

Full review of all 32 files. Issues grouped by impact tier.

---

## TIER 1 — Correctness (wrong results in production today)

### 1. Failed-record count is wrong when exceptions are capped
**File:** `core/executor.py` — `execute_rule`

When `MAX_EXCEPTIONS` is hit, `_fetch_failed_rows` stops fetching but `failed = len(failed_rows)` is used as the recorded failure count. The real number of failing rows is higher.

```python
# Current (WRONG when cap is hit)
failed_rows = _fetch_failed_rows(db_conn, query)  # stops at MAX_EXCEPTIONS
failed = len(failed_rows)                          # e.g. 10,000 — not the truth

# Fix: run a COUNT of the rule query separately
failed_count_sql = f"SELECT COUNT(*) AS cnt FROM ({query}) dq_sub"
failed = execute_query(db_conn, failed_count_sql)[0]["cnt"]  # true count
failed_rows = _fetch_failed_rows(db_conn, query)             # for exceptions only
```

This also means `passed_records = max(total - failed, 0)` and `failure_pct` are wrong whenever the cap fires.

---

### 2. SKIP status is never written to `dq_rule_execution`
**File:** `core/executor.py` — `execute_rule`

When `validate_table_exists` returns False, the function returns `"SKIP"` immediately without inserting a row into `dq_rule_execution`. As a result:
- `total_rules` in metrics is undercounted
- `dq_score` is computed over only the rules that actually ran — silent inflation
- There is no audit trail that the rule was attempted

Fix: insert a SKIP row with `status='SKIP'`, `total_records=0`, `failed_records=0` before returning.

---

### 3. Race condition in `_upsert_metrics` MERGE
**File:** `core/metrics.py` — `_upsert_metrics`

If two runs for the same `(project, process, run_type, batch_id, dataset_id, run_month)` finish within milliseconds of each other (parallel CI runs, re-run after crash), both MERGEs can evaluate the `ON` clause as "no match" simultaneously and both INSERT — violating uniqueness and doubling the totals.

Fix: add a `UNIQUE INDEX` on `dq_metrics_summary(project_name, process_name, run_type, run_month)` in DDL, and catch/retry the MERGE on duplicate-key errors.

---

### 4. Stale `RUNNING` runs after a crash
**File:** `core/engine.py`

If the process is killed (OOM, SIGKILL, host reboot) after `_insert_run_control` but before `_update_run_control`, `dq_run_control.status` stays `'RUNNING'` forever. Subsequent runs see no error.

Fix: add a startup check in `run_engine` that marks any run older than `DQ_STALE_RUN_HOURS` (default 4) as `'ABORTED'`:
```python
execute_dml(td, f"""
    UPDATE {meta_db}.dq_run_control
    SET status = 'ABORTED', end_time = CURRENT_TIMESTAMP
    WHERE status = 'RUNNING'
      AND start_time < CURRENT_TIMESTAMP - INTERVAL '4' HOUR
""")
```

---

### 5. Metadata connection name is hardcoded to `"teradata"`
**Files:** `core/engine.py` lines 47, 100

```python
td = cf.get("teradata")           # main-thread metadata conn
td_local = cf.new_connection("teradata")   # per-thread metadata conn
```

If any source connection is named `"teradata"` or the user renames the metadata connection, this silently uses the wrong connection. Fix: add env var `DQ_META_CONNECTION=teradata` (default `"teradata"`) and read it instead of the string literal.

---

## TIER 2 — Security

### 6. f-string SQL with manual escaping instead of parameterized queries
**Files:** `utils/logger.py`, `utils/issue_logger.py`, `core/engine.py`, `core/metrics.py`

Only single quotes are escaped via `.replace("'", "''")`. This misses other injection vectors and is fragile. The executor already uses `?` placeholders in `bulk_insert/executemany`. The same should apply to all metadata writes.

```python
# Current (fragile)
sql = f"INSERT INTO ... VALUES ('{run_id}', '{safe_msg}', ...)"
cursor.execute(sql)

# Fix
sql = "INSERT INTO ... VALUES (?, ?, ?)"
cursor.execute(sql, [run_id, message, error_detail])
```

Affects: `log_message`, `log_issue`, `_insert_run_control`, `_update_run_control`, `_upsert_metrics`.

---

### 7. Sensitive credentials captured at module import
**File:** `utils/alert.py` lines 11-17

`EMAIL_PASSWORD`, `TEAMS_WEBHOOK_URL`, and other secrets are read via `os.getenv()` at import time and stored as module-level strings. They persist in memory for the process lifetime. If env vars are rotated mid-process or set after the first import, the stale value is used silently.

Fix: read inside `send_alert()` (lazy) or use a `@dataclass`/config object that is instantiated once when first called.

---

### 8. Teams webhook uses deprecated `MessageCard` format
**File:** `utils/alert.py` — `_send_teams_alert`

`@type: "MessageCard"` was deprecated by Microsoft in 2022 in favour of Adaptive Cards. Teams will eventually stop rendering them.

Fix: migrate to Adaptive Card payload format, or use the `requests` library with `pymsteams` to handle format versioning.

---

## TIER 3 — Architecture / Design Gaps

### 9. No retry on transient source errors
**File:** `core/executor.py`

A network hiccup during `execute_query(db_conn, count_query)` returns `"ERROR"` immediately with no retry. For production pipelines hitting cloud databases (Aurora, Databricks), transient errors are common.

Fix: wrap the main execution steps in a `tenacity.retry` decorator with exponential backoff:
```python
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _run_with_retry(db_conn, rule, run, ...):
    ...
```

---

### 10. `MAX_WORKERS` is hardcoded
**File:** `core/engine.py` line 18

```python
MAX_WORKERS = 5
```

Should be `int(os.getenv("DQ_MAX_WORKERS", "5"))` so it can be tuned per environment without code changes. For Databricks or Aurora sources that support high concurrency, 5 may be too low. For Teradata, it may be too high.

---

### 11. No rule priority / execution ordering
**File:** `ddl.sql` — `dq_rules` table

Rules run in whatever order `SELECT * FROM dq_rules` returns (undefined). There is no `priority` column. A common pattern is: first validate that the source table has rows, then validate column-level rules. Without ordering, a column null-check can fail before the "table has rows" check reveals the root cause.

Fix: add `priority INTEGER DEFAULT 100` to `dq_rules` and `ORDER BY priority ASC` in `_load_rules`.

---

### 12. No rule dependency support
**File:** DDL + engine

Can't express "run rule B only if rule A passed." This leads to cascading noise: if a source table is empty, every downstream rule fails for the wrong reason.

Fix: add `depends_on_rule_id INTEGER` to `dq_rules`. In the engine, after loading rules, build a dependency graph (topological sort) and skip dependent rules if their parent failed/errored.

---

### 13. `total == 0` always returns PASS — empty table blindspot
**File:** `core/evaluator.py` line 31

An empty source table returns `PASS` unconditionally. For rules designed to monitor tables that must always have rows, this silently passes a broken feed.

Fix: add a `require_rows BYTEINT DEFAULT 0` column to `dq_rules`. When `require_rows = 1` and `total == 0`, return `FAIL` instead of `PASS`.

---

### 14. Threshold logic is OR, not configurable AND
**File:** `core/evaluator.py` lines 41-46

When both `threshold_pct` and `threshold_count` are set, a breach in EITHER triggers failure. There's no way to require BOTH to be exceeded simultaneously (e.g., "fail only if >5% AND >1000 rows").

Fix: add `threshold_operator VARCHAR(3) DEFAULT 'OR'` to `dq_rules` and pass it through to `evaluate_rule`.

---

### 15. `filter_column` supports only a single column
**File:** `core/query_builder.py` — `build_filter`

Many real tables need compound filters: `process_date >= X AND batch_type = 'DAILY'`. The current design only supports a single `filter_column`.

Fix: add a `filter_sql CLOB` column to `dq_rules`. If present, use it verbatim instead of building the filter from `filter_column`. `filter_column` stays for simple cases.

---

### 16. No dry-run mode
**File:** `core/engine.py`

There is no way to validate all rules (SQL syntax, table existence) without actually writing execution results to the metadata tables. This makes CI/CD rule validation impossible without a separate environment.

Fix: add `run_engine(..., dry_run=False)`. In dry-run mode: load rules, validate SQL, validate table existence, then log results to stdout only — no DB writes.

---

### 17. No pre-validation pass
**File:** `core/engine.py`

Currently, a rule with invalid SQL is only discovered when its worker thread starts. If the 50th rule has a syntax error, 49 rules have already written results before the error surfaces.

Fix: before the `ThreadPoolExecutor`, loop over all rules and call `validate_sql(db_conn, build_query(rule, run))`. Collect all errors, log them, and optionally abort the run if any rule fails validation.

---

### 18. No single-rule test harness
There is no way for a developer to test one rule in isolation without running the full engine. Debugging rule SQL requires either running the full framework or manually executing the query.

Fix: add a `core/rule_tester.py` module:
```python
def test_rule(rule_code: str, project: str, process: str, run_type: str, run_mode: str):
    """Run a single rule and print results — no metadata writes."""
```

---

## TIER 4 — Observability

### 19. No structured JSON logging
**File:** `config/env_config.py` + `core/engine.py`

All logs use Python standard `logging` with string formatting. In production (CloudWatch, Splunk, Datadog), structured JSON logs are far more queryable.

Fix: add a JSON formatter:
```python
import json_logging
json_logging.init_non_web(enable_json=True)
```
Or use `python-json-logger` with fields: `run_id`, `rule_code`, `project`, `process`, `status`.

---

### 20. No per-step timing breakdown
**File:** `core/executor.py`

`execution_time` records total rule wall time but gives no visibility into which step is slow: table validation, count query, row fetch, or exception insert. For a 60-second rule, you can't tell if it's slow on the DB query or on the Teradata INSERT.

Fix: add step-level timing and log it to `dq_run_logs`:
```
Step: count_query — 0.8s
Step: rule_query  — 54.2s
Step: exception_insert — 2.1s
```

---

### 21. No end-of-run summary report
After a run completes, there is no human-readable artifact (HTML, CSV, PDF) summarising which rules passed/failed, failure percentages, and exceptions. Stakeholders must query `dq_rule_execution` directly.

Fix: add a `core/reporter.py` that generates an HTML summary after `calculate_metrics()` and optionally emails it as an attachment.

---

### 22. Alert severity routing missing
**File:** `utils/alert.py`

Every alert level (INFO, WARN, ERROR) goes to both Teams AND email. For production, INFO-level completion alerts to email creates noise. ERROR alerts to Teams-only means they might be missed.

Fix: add severity routing config:
```
DQ_ALERT_TEAMS_LEVELS=INFO,WARN,ERROR
DQ_ALERT_EMAIL_LEVELS=ERROR
```

---

## TIER 5 — Code Quality

### 23. `main.py` has no CLI
**File:** `main.py`

To run a different project/process/run_type, you must edit the source file. In production, jobs are triggered with parameters.

Fix:
```python
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--project",  required=True)
parser.add_argument("--process",  required=True)
parser.add_argument("--run-type", required=True)
parser.add_argument("--run-mode", default="FULL")
parser.add_argument("--batch-id")
parser.add_argument("--start-date")
parser.add_argument("--end-date")
args = parser.parse_args()
run_engine(project=args.project, ...)
```

---

### 24. No unit or integration tests
Zero test coverage. At minimum these functions must have unit tests:

| Function | Why it matters |
|---|---|
| `evaluate_rule` | Core business logic — thresholds, severity |
| `build_filter` | filter_type override logic |
| `build_query` | SQL construction correctness |
| `resolve_table` | File vs DB routing |
| `build_json_pk` | Exception key serialization |
| `_build_dataset_id` | The None_None bug was missed without tests |
| `generate_run_id` | Uniqueness guarantee |

Fix: add `tests/` directory with `pytest` + mock connections. Use `duckdb` as a test DB to avoid needing live Teradata.

---

### 25. Config and alert values captured at module import
**Files:** `config/env_config.py`, `utils/alert.py`

`ENV = os.getenv("DQ_ENV", "DEV").upper()` and `EMAIL_PASSWORD = os.getenv(...)` are evaluated the moment the module is first imported. If `DQ_ENV` is set after the import (e.g., in a test setup), it has no effect.

Fix: wrap in functions or `@functools.lru_cache`-decorated loaders that read env at call time.

---

### 26. `utils/pk_builder.py` is a dead file
**File:** `utils/pk_builder.py`

Empty file, marked deprecated. Still imported in `db/adapters/__init__.py`? It isn't, but it exists in the repo and confuses readers.

Fix: delete the file and add `# pk_builder.py removed in v1.1` to the changelog.

---

### 27. No `pyproject.toml` / `setup.py`
The repo has no package metadata. It cannot be installed via `pip install .`, which means:
- It can't be version-pinned in other projects
- The import paths only work if you `cd` into the repo root
- No entry-points for CLI (`dq-run --project CLAIMS ...`)

Fix: add `pyproject.toml` with `[project]` metadata and `[project.scripts]` entry point.

---

### 28. `db_resolver.py` is too strict for file paths
**File:** `utils/db_resolver.py`

```python
if "{ENV}" not in db_pattern:
    raise ValueError(...)
```

The table resolver now bypasses `resolve_db_name` for file sources, but if `src_db_name` is ever passed to `resolve_db_name` with a plain path (e.g., `/data/inputs/`), the error message "must contain {ENV} token" is deeply confusing.

Fix: change the guard to a warning + pass-through instead of an exception, or accept an `allow_no_token=False` param.

---

### 29. `REVIEW.md` committed to repo
The first code review document is committed to the main branch. This is internal tooling noise.

Fix: move to `docs/REVIEW_V1.md` and `docs/REVIEW_V2.md`, or add to `.gitignore` if these are meant to be ephemeral.

---

## Summary Table

| # | Area | Severity | File(s) |
|---|---|---|---|
| 1 | Failed count wrong when capped | 🔴 Critical | executor.py |
| 2 | SKIP not recorded in execution | 🔴 Critical | executor.py |
| 3 | MERGE race condition | 🔴 Critical | metrics.py, ddl.sql |
| 4 | Stale RUNNING runs on crash | 🔴 Critical | engine.py |
| 5 | Metadata conn name hardcoded | 🔴 Critical | engine.py |
| 6 | f-string SQL injection risk | 🟠 High | logger.py, issue_logger.py, metrics.py |
| 7 | Credentials at module import | 🟠 High | alert.py |
| 8 | Deprecated Teams card format | 🟠 High | alert.py |
| 9 | No retry on transient errors | 🟠 High | executor.py |
| 10 | MAX_WORKERS hardcoded | 🟡 Medium | engine.py |
| 11 | No rule priority ordering | 🟡 Medium | ddl.sql, engine.py |
| 12 | No rule dependencies | 🟡 Medium | ddl.sql, engine.py |
| 13 | Empty table → silent PASS | 🟡 Medium | evaluator.py, ddl.sql |
| 14 | Threshold OR only, no AND | 🟡 Medium | evaluator.py, ddl.sql |
| 15 | Single-column filter only | 🟡 Medium | query_builder.py, ddl.sql |
| 16 | No dry-run mode | 🟡 Medium | engine.py |
| 17 | No pre-validation pass | 🟡 Medium | engine.py |
| 18 | No single-rule test harness | 🟡 Medium | (new file) |
| 19 | No structured JSON logging | 🟡 Medium | engine.py, executor.py |
| 20 | No per-step timing | 🟡 Medium | executor.py |
| 21 | No end-of-run report | 🟡 Medium | (new file) |
| 22 | Alert severity routing | 🟡 Medium | alert.py |
| 23 | No CLI in main.py | 🟢 Low | main.py |
| 24 | No unit/integration tests | 🟢 Low | (new tests/) |
| 25 | Config at module import | 🟢 Low | env_config.py, alert.py |
| 26 | Dead pk_builder.py | 🟢 Low | utils/pk_builder.py |
| 27 | No pyproject.toml | 🟢 Low | (new file) |
| 28 | db_resolver too strict | 🟢 Low | db_resolver.py |
| 29 | REVIEW.md in main branch | 🟢 Low | REVIEW.md |
