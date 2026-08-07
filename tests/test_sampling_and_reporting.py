"""
Integration tests for core/stratified_sampling.py and core/reporting.py
(the static audit report generator), both against in-memory DuckDB.

Uses a generic "case review" scenario (not tied to any one project's
vocabulary) to prove the stratification/exclusion/priority-ranking logic
works purely from config — these modules have zero knowledge of what a
"disposition" or "functional area" means for any particular project.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import os as _os
import tempfile
import random
import duckdb

from core.stratified_sampling import run_stratified_sampling
from core.reporting import generate_report


# ── Stratified sampling ──────────────────────────────────────────────────

class _FakeCF:
    def __init__(self, conn):
        self._conn = conn
    def get(self, name):
        return self._conn


def _build_universe(conn, n=1000):
    conn.execute("""
        CREATE TABLE case_universe (
            case_id VARCHAR, category VARCHAR, area VARCHAR,
            revision INTEGER, pull_date DATE, auto_closed INTEGER
        )
    """)
    categories = (["Denied"] * 80 + ["Withdrawn"] * 10 + ["Dismissed"] * 2 + ["Approved"] * 8)
    areas = (["Area A"] * 13 + ["Area B"] * 8 + ["Area C"] * 79)
    random.seed(42)
    rows = []
    for i in range(n):
        rows.append((
            f"C{i}",
            random.choice(categories),
            random.choice(areas),
            random.choice([0, 1]),
            "2026-08-01",
            1 if i % 50 == 0 else 0,   # 2% auto-closed -> excluded
        ))
    conn.executemany("INSERT INTO case_universe VALUES (?, ?, ?, ?, ?, ?)", rows)


def _build_meta(conn):
    conn.execute("""
        CREATE TABLE dq_sample_selections (
            sample_run_id VARCHAR, config_id INTEGER, project_name VARCHAR,
            process_name VARCHAR, sample_cycle DATE, case_key VARCHAR,
            determination_type VARCHAR, functional_area VARCHAR, priority_rank INTEGER,
            excluded_flag INTEGER, exclusion_reason VARCHAR, selected_flag INTEGER,
            strata_json VARCHAR, created_at TIMESTAMP
        )
    """)


def test_weekly_sample_hits_target_and_respects_exclusions():
    conn = duckdb.connect(":memory:")
    _build_universe(conn, n=1000)
    _build_meta(conn)

    cf = _FakeCF(conn)
    config = {
        "config_id": 1,
        "project_name": "ANY_PROJECT",
        "process_name": "WEEKLY_REVIEW_SAMPLE",
        "sample_name": "weekly review sample",
        "connection_name": "primary",
        "universe_table": "case_universe",
        "key_columns": "case_id",
        "scope_column": "pull_date",
        "target_volume": 150,
        "determination_column": "category",
        "determination_mix_json": json.dumps({
            "Denied": 0.80, "Withdrawn": 0.10, "Dismissed": 0.02, "Approved": 0.08,
        }),
        "functional_area_column": "area",
        "functional_area_mix_json": json.dumps({
            "Area A": 0.13, "Area B": 0.08,
        }),
        "exclusion_sql": "auto_closed = 1",
        "priority_rank_sql": "revision DESC",
    }
    run = {"run_id": "TEST_RUN", "start_date": "2026-08-01", "end_date": "2026-08-01"}

    result = run_stratified_sampling(cf, conn, config, run, "main")

    assert result["candidates"] > 0
    assert result["selected"] <= 150
    assert result["selected"] >= 140
    assert result["sample_run_id"].startswith("WEEKLY_REVIEW_SAMPLE")

    rows = conn.execute(
        "SELECT case_key, selected_flag FROM dq_sample_selections WHERE sample_run_id = ?",
        [result["sample_run_id"]],
    ).fetchall()
    assert len(rows) == result["candidates"]

    excluded_ids = {r[0] for r in conn.execute(
        "SELECT case_id FROM case_universe WHERE auto_closed = 1"
    ).fetchall()}
    selected_ids = {r[0].split("=")[1] for r in rows if r[1] == 1}
    assert not (excluded_ids & selected_ids)

    denied_ids = {r[0] for r in conn.execute(
        "SELECT case_id FROM case_universe WHERE category = 'Denied'"
    ).fetchall()}
    denied_selected = len(selected_ids & denied_ids)
    assert denied_selected / max(len(selected_ids), 1) > 0.5


def test_generic_module_has_no_project_specific_vocabulary():
    import core.stratified_sampling as mod
    src = open(mod.__file__).read().lower()
    for banned in ("healthspring", "roar", "como", "shrpa", "medicare"):
        assert banned not in src, f"core/stratified_sampling.py should not reference '{banned}'"


# ── Static audit report ──────────────────────────────────────────────────

def test_generate_report_end_to_end():
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE dq_run_control (run_id VARCHAR, project_name VARCHAR,
            process_name VARCHAR, run_type VARCHAR, run_mode VARCHAR)
    """)
    conn.execute("INSERT INTO dq_run_control VALUES ('RUN1','HEALTHSPRING_UM','UNIVERSE_VALIDATION','WEEKLY','DATE')")

    conn.execute("""
        CREATE TABLE dq_rule_execution (run_id VARCHAR, rule_code VARCHAR, status VARCHAR,
            total_records INTEGER, failed_records INTEGER, failure_pct FLOAT,
            severity VARCHAR, execution_time FLOAT, run_timestamp TIMESTAMP)
    """)
    conn.execute("""
        INSERT INTO dq_rule_execution VALUES
        ('RUN1','UM-REQ-001','FAIL',1000,5,0.5,'Data Validation Error',1.2, CURRENT_TIMESTAMP),
        ('RUN1','UM-SLA-STD-001','PASS',1000,0,0.0,'Timeliness',0.9, CURRENT_TIMESTAMP)
    """)

    conn.execute("""
        CREATE TABLE dq_exceptions (exception_id INTEGER, run_id VARCHAR, rule_code VARCHAR,
            table_name VARCHAR, primary_key_str VARCHAR, created_at TIMESTAMP)
    """)
    conn.execute("""
        INSERT INTO dq_exceptions VALUES
        (1,'RUN1','UM-REQ-001','um_universe','enrollee_id=E1', CURRENT_TIMESTAMP)
    """)

    with tempfile.TemporaryDirectory() as tmp:
        path = generate_report(conn, "main", "RUN1", tmp)
        assert _os.path.exists(path)
        content = open(path).read()
        assert "UM-REQ-001" in content
        assert "RUN1" in content
        assert "immutable" in content.lower()

        # Re-generating with identical data should not create a second file / should be idempotent.
        path2 = generate_report(conn, "main", "RUN1", tmp)
        assert path == path2
