# DQ Framework — Code Review

Reviewed: all 16 Python files + ddl.sql (1,420 lines)
Severity: 🔴 Critical · 🟠 Bug · 🟡 Design · 🟢 Minor

---

## 🔴 CRITICAL

### 1. Shared Teradata connection across parallel threads
**File:** `core/engine.py` — `ThreadPoolExecutor`, `execute_rule(rule, db_conn, td, ...)`

`td` (the metadata write connection) is passed into every worker thread and used simultaneously
for INSERT into `dq_rule_execution`, `dq_exceptions`, `dq_run_logs`, and `dq_rule_issues`.
Teradata connections are **not thread-safe**. Under concurrent load this produces
`OperationalError: connection in use` or silent partial commits.

**Fix:** Give each thread its own metadata connection, or use a thread-local connection pool,
or serialise all metadata writes through a queue consumed by a single writer thread.

```python
# Option A — per-thread connection (simplest)
def run_single(rule):
    td_local = cf.get_fresh("teradata")   # new connection per thread
    execute_rule(rule, db_conn, td_local, run, meta_db)
    td_local.close()
```

---

### 2. Wrong total count — failure_pct incorrect in DATE / BATCH modes
**File:** `core/executor.py` line 109

```python
# CURRENT (wrong) — counts ALL rows regardless of filter
total = execute_query(db_conn, f"SELECT COUNT(*) AS cnt FROM {table}")[0]["cnt"]

# failed_rows was fetched with: WHERE (rule_syntax) AND (filter_cond)
# total was fetched without the filter
# → failure_pct = filtered_failed / unfiltered_total  ← wrong ratio
```

For a DATE-range run with 1M total rows but only 50k in-scope, and 500 failures:
- Wrong:  `500 / 1,000,000 = 0.05%`
- Correct: `500 / 50,000   = 1.0%`

**Fix:** Apply the same filter to the count query.

```python
filter_cnd = build_filter(rule, run)
count_query = f"SELECT COUNT(*) AS cnt FROM {table} t WHERE ({filter_cnd})"
total = execute_query(db_conn, count_query)[0]["cnt"]
```

---

## 🟠 BUGS

### 3. `validation.py` logs issues to the SOURCE connection, not metadata
**File:** `utils/validation.py` lines 23, 34

```python
log_issue_fn(db_conn, run, rule, "TABLE_NOT_FOUND", ...)  # ← db_conn is source
log_message_fn(db_conn, ...)                               # ← should be td
```

When the source table doesn't exist, we try to INSERT the error into the
source DB — which also doesn't exist. The error silently swallows and
nothing gets logged.

**Fix:** Pass `td_conn` through to `validate_table_exists`, or resolve the
table and log to `td` inside `executor.py` before calling validation.

---

### 4. Metrics rolling average is wrong
**File:** `core/metrics.py` lines 233–234

```sql
-- CURRENT (wrong — "last 2 runs average")
avg_failure_pct = (tgt.avg_failure_pct + {avg_fail_pct}) / 2.0,
dq_score        = (tgt.dq_score        + {dq_score})     / 2.0
```

After 10 runs the stored value is still just `(run9 + run10) / 2`.
Runs 1–8 are completely lost from the average.

**Fix:** Use a proper cumulative mean:

```sql
avg_failure_pct = (tgt.avg_failure_pct * tgt.total_runs + {avg_fail_pct}) / (tgt.total_runs + 1),
dq_score        = (tgt.dq_score        * tgt.total_runs + {dq_score})     / (tgt.total_runs + 1)
```

---

### 5. Baseline query uses `SAMPLE` instead of `TOP` / `QUALIFY`
**File:** `core/metrics.py` lines 265–271

```sql
-- CURRENT (wrong) — SAMPLE picks random rows, ORDER BY is not honoured
ORDER BY run_month DESC
SAMPLE 10
```

`SAMPLE` in Teradata is random sampling. It does not respect the `ORDER BY`.
You will get 10 random months, not the 10 most-recent.

**Fix:**

```sql
QUALIFY ROW_NUMBER() OVER (ORDER BY run_month DESC) <= 10
```

---

### 6. `dataset_id` becomes the string `"None_None"` in FULL run mode
**File:** `core/engine.py` line 55

```python
dataset_id = batch_id if batch_id else f"{start_date}_{end_date}"
# run_mode=FULL, no dates → f"{None}_{None}" → "None_None"
```

This gets inserted verbatim into `dq_run_control.dataset_id` and
`dq_rule_execution.dataset_id`.

**Fix:**

```python
if run_mode == "BATCH" and batch_id:
    dataset_id = batch_id
elif run_mode == "DATE" and start_date and end_date:
    dataset_id = f"{start_date}_{end_date}"
else:
    dataset_id = "FULL"
```

---

### 7. `LOGIC_WARN` fires on every rule that passes
**File:** `core/executor.py` lines 120–123

```python
if total > 0 and failed == 0:
    log_issue(td_conn, run, rule, "LOGIC_WARN", ...)
```

In a healthy run where 95% of rules pass, this inserts a `LOGIC_WARN`
row into `dq_rule_issues` for every passing rule. This floods the table
with false-positive issues and makes `_count_issues` return a non-zero
count, causing every clean run to be marked `COMPLETED_WITH_ISSUES`.

**Fix:** Remove this block entirely, or move it behind an opt-in flag
(`rule.get("warn_on_zero_failures")`).

---

## 🟡 DESIGN ISSUES

### 8. `meta_db` hardcoded DEV default in logger / issue_logger
**Files:** `utils/logger.py` line 15, `utils/issue_logger.py` line 13

```python
meta_db: str = "CMSUNIV_FILELAND_DEV_T"   # ← hardcoded DEV
```

A caller on PROD that forgets to pass `meta_db` silently writes
all logs and issues to the DEV table. No error, no warning.

**Fix:** Remove the default and require the caller to always supply it,
OR pull it from `get_meta_db()` at import time:

```python
from config.env_config import get_meta_db
_DEFAULT_META_DB = get_meta_db()

def log_message(..., meta_db: str = None):
    meta_db = meta_db or _DEFAULT_META_DB
```

---

### 9. All failed rows loaded into memory
**File:** `core/executor.py` line 95

```python
failed_rows = execute_query(db_conn, query)  # fetchall()
```

For a rule that flags 2M rows, this loads 2M dicts into RAM before
writing exceptions. On a 5-worker pool this could exhaust memory.

**Fix:** Use `fetchmany()` chunks for the exception insert, or cap
exceptions at a configurable limit (e.g., first 10,000):

```python
MAX_EXCEPTIONS = int(os.getenv("DQ_MAX_EXCEPTIONS", "10000"))
# fetch in chunks and stop at limit
```

---

### 10. No connection health-check or reconnect
**File:** `db/connection_factory.py`

Connections are opened once at startup. Long-running batch jobs (hours)
will encounter `OperationalError: connection reset` mid-run with no
recovery. Teradata sessions have a default idle timeout.

**Fix:** Add a ping/reconnect wrapper:

```python
def get(self, name: str):
    conn = self._conns.get(name)
    if conn is None or not self._is_alive(conn):
        self._conns[name] = self._build(name)
    return self._conns[name]

def _is_alive(self, conn) -> bool:
    try:
        conn.cursor().execute("SELECT 1")
        return True
    except Exception:
        return False
```

---

### 11. `execute_rule` always returns `None` — engine loses pass/fail signal
**File:** `core/executor.py` — all return paths return `None`

The engine cannot distinguish "rule ran and passed", "rule ran and failed",
or "rule errored and was skipped". It only infers health via `_count_issues`
which is polluted by `LOGIC_WARN` entries (see issue #7).

**Fix:** Return a status string from `execute_rule` and track it in the engine
to produce a proper `failed_rules` count without needing a secondary query.

---

### 12. `dq_metrics_summary` PRIMARY INDEX is a single low-cardinality column
**File:** `ddl.sql`

```sql
PRIMARY INDEX (project_name)
```

All rows for the same project land on the same AMP. For a framework
running dozens of projects, most data ends up skewed to a handful of AMPs,
causing hot-spots and slow MERGE performance.

**Fix:**

```sql
PRIMARY INDEX (project_name, process_name, run_type, run_month)
```

---

### 13. `filter_type` column in `dq_rules` is defined but never read
**File:** `ddl.sql` + `core/query_builder.py`

`filter_type VARCHAR(20)` exists in the schema but `build_filter` uses
`run.get("run_mode")` from the run context, not the rule's `filter_type`.
Either wire it up (e.g. allow per-rule filter override) or drop the column.

---

## 🟢 MINOR / HOUSEKEEPING

### 14. `utils/pk_builder.py` is an orphaned file
Replaced by `utils/json_builder.py` but never deleted. Will cause
confusion and may be accidentally imported.

**Fix:** Delete `utils/pk_builder.py`.

---

### 15. No `requirements.txt`
No pinned dependencies. Different team members may install different
`teradatasql` versions causing driver-level behaviour differences.

**Fix:** Add `requirements.txt`:
```
teradatasql>=17.20
```

---

### 16. `run_id` has no uniqueness guarantee at sub-minute resolution
**File:** `utils/id_builder.py`

Timestamp precision is `HH:MM`. Two runs of the same project/process
started in the same minute produce the same `run_id`. `dq_run_control`
has no UNIQUE constraint on `run_id`.

**Fix:** Add seconds or a short UUID suffix:
```python
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
```

---

## Summary Table

| # | Severity | File | Issue |
|---|----------|------|-------|
| 1 | 🔴 Critical | engine.py | Shared `td` across threads — not thread-safe |
| 2 | 🔴 Critical | executor.py | Total count unfiltered → wrong failure_pct |
| 3 | 🟠 Bug | validation.py | Logs errors to source conn instead of metadata conn |
| 4 | 🟠 Bug | metrics.py | Rolling average uses (old+new)/2, loses history |
| 5 | 🟠 Bug | metrics.py | `SAMPLE` instead of `QUALIFY TOP N` — random rows |
| 6 | 🟠 Bug | engine.py | `dataset_id = "None_None"` in FULL mode |
| 7 | 🟠 Bug | executor.py | `LOGIC_WARN` on every passing rule → floods issues table |
| 8 | 🟡 Design | logger.py / issue_logger.py | `meta_db` defaults to DEV hardcode |
| 9 | 🟡 Design | executor.py | All failed rows loaded into memory — OOM risk |
| 10 | 🟡 Design | connection_factory.py | No reconnect logic for long-running jobs |
| 11 | 🟡 Design | executor.py | `execute_rule` always returns `None` |
| 12 | 🟡 Design | ddl.sql | `dq_metrics_summary` PI on single low-cardinality column |
| 13 | 🟡 Design | ddl.sql / query_builder.py | `filter_type` column unused |
| 14 | 🟢 Minor | utils/ | `pk_builder.py` orphan file |
| 15 | 🟢 Minor | root | Missing `requirements.txt` |
| 16 | 🟢 Minor | id_builder.py | `run_id` not unique within same minute |

---

## Recommended Fix Priority

**Sprint 1 (before any production use):**
Issues 1, 2, 3, 7 — these will cause silent data corruption or incorrect metrics.

**Sprint 2 (before scale-out):**
Issues 4, 5, 6, 8, 9 — incorrect historical metrics, memory risk, env cross-contamination.

**Sprint 3 (hardening):**
Issues 10, 11, 12, 13, 14, 15, 16 — reliability, performance, housekeeping.
