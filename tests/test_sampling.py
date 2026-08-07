"""
Integration tests for the Sampling Framework (sampling/engine.py) against
in-memory DuckDB. This is a separate framework from the DQ rules engine in
core/ — these tests exercise it standalone, with no dq_rules/dq_run_control
involved at all.

Uses a generic "case review" scenario (not tied to any one project's
vocabulary) to prove the stratification/exclusion/priority-ranking logic
works purely from config — the module has zero knowledge of what a
"disposition" or "functional area" means for any particular project.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import random
import duckdb

from sampling.engine import run_stratified_sampling


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
    # project_name/process_name are NOT columns here — dq_sample_selections
    # is scoped via config_id -> dq_sampling_config.scope_id (see ddl.sql v7).
    conn.execute("""
        CREATE TABLE dq_sample_selections (
            sample_run_id VARCHAR, config_id INTEGER,
            sample_cycle DATE, case_key VARCHAR,
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


def _seed_prior_pool_sizes(conn, config_id: int, counts: list):
    """Write `len(counts)` prior sample_run_id groups to dq_sample_selections,
    each with the given candidate-row count, so drift-detection tests have a
    history to compare against."""
    for i, count in enumerate(counts):
        run_id = f"PRIOR_{config_id}_{i}"
        rows = [(run_id, config_id, "2026-01-01", f"K{i}-{j}", j) for j in range(count)]
        conn.executemany(
            "INSERT INTO dq_sample_selections "
            "(sample_run_id, config_id, sample_cycle, case_key, priority_rank) "
            "VALUES (?, ?, ?, ?, ?)",
            rows,
        )


def test_candidate_pool_drift_detected_on_sharp_drop():
    from sampling.anomaly import detect_candidate_pool_drift

    conn = duckdb.connect(":memory:")
    _build_meta(conn)
    config = {"config_id": 7, "sample_name": "weekly review sample"}
    _seed_prior_pool_sizes(conn, 7, [505, 498, 510, 495, 502, 500])

    result = detect_candidate_pool_drift(conn, config, "CURRENT_RUN", 40, "main")

    assert result != {}
    assert result["is_anomaly"] is True
    assert result["candidate_count"] == 40
    assert result["sample_name"] == "weekly review sample"
    assert result["config_id"] == 7
    assert result["method"] in ("ZSCORE", "IQR")


def test_candidate_pool_drift_not_flagged_when_stable():
    from sampling.anomaly import detect_candidate_pool_drift

    conn = duckdb.connect(":memory:")
    _build_meta(conn)
    config = {"config_id": 8, "sample_name": "stable sample"}
    _seed_prior_pool_sizes(conn, 8, [505, 498, 510, 495, 502, 500])

    result = detect_candidate_pool_drift(conn, config, "CURRENT_RUN", 503, "main")

    assert result == {}


def test_candidate_pool_drift_insufficient_history_returns_empty():
    from sampling.anomaly import detect_candidate_pool_drift

    conn = duckdb.connect(":memory:")
    _build_meta(conn)
    config = {"config_id": 9, "sample_name": "brand new sample"}
    # No prior runs at all for this config_id.

    result = detect_candidate_pool_drift(conn, config, "CURRENT_RUN", 100, "main")

    assert result == {}


def test_run_stratified_sampling_includes_volume_drift_key():
    """End-to-end: run_stratified_sampling() wires the drift check in and
    always returns a "volume_drift" key (empty dict on a first-ever run,
    since there's no history yet -- never a KeyError for callers)."""
    conn = duckdb.connect(":memory:")
    _build_universe(conn, n=200)
    _build_meta(conn)
    cf = _FakeCF(conn)
    config = {
        "config_id": 42,
        "sample_name": "first ever run",
        "connection_name": "primary",
        "universe_table": "case_universe",
        "key_columns": "case_id",
        "target_volume": 50,
    }
    run = {"run_id": "TEST_RUN"}

    result = run_stratified_sampling(cf, conn, config, run, "main")

    assert "volume_drift" in result
    assert result["volume_drift"] == {}   # no history yet on the very first run


def test_generic_module_has_no_project_specific_vocabulary():
    import sampling.engine as mod
    src = open(mod.__file__).read().lower()
    for banned in ("healthspring", "roar", "como", "shrpa", "medicare"):
        assert banned not in src, f"sampling/engine.py should not reference '{banned}'"


def _code_below_docstring(module) -> str:
    """Strip a module's leading docstring so prose mentions of a rules-engine
    name (e.g. contrasting what the two frameworks each answer) don't trip
    the import-boundary checks below — only executable code matters."""
    src = open(module.__file__).read()
    marker = '"""\n\nimport'
    assert marker in src, f"expected module docstring followed by a blank line then imports in {module.__file__}"
    return src.split(marker, 1)[1]


def test_sampling_framework_does_not_import_core_engine_or_rules_tables():
    """
    Guards the framework boundary across every module in sampling/: it may
    use core.executor and core.rule_sql as a plain library (see
    sampling/engine.py's module docstring), and — deliberately, as of the
    candidate-pool drift check — core.metrics.evaluate_metric_drift (see
    sampling/anomaly.py's module docstring for the rationale: it's a pure,
    DB-free statistics function, not a re-coupling to the rules engine's
    metrics tables). What it must never do is import core.engine or
    core.reporting, or query dq_rule_execution/dq_exceptions/dq_rules —
    those are rules-engine-specific I/O and result tables; pulling any of
    them in here would silently re-couple the two frameworks.
    """
    import sampling.engine as engine_mod
    import sampling.anomaly as anomaly_mod

    for mod in (engine_mod, anomaly_mod):
        code_only = _code_below_docstring(mod)
        assert "core.engine" not in code_only, mod.__file__
        assert "core.reporting" not in code_only, mod.__file__
        for banned_table in ("dq_rule_execution", "dq_exceptions", "dq_rules"):
            assert banned_table not in code_only, \
                f"{mod.__file__} should not reference '{banned_table}'"

    # core.metrics reuse is intentionally scoped to exactly one pure
    # function — evaluate_metric_drift — never the DB-querying parts
    # (calculate_metrics, detect_and_log, _load_current_metrics, etc.)
    # that actually touch dq_metrics_summary/dq_anomaly_config.
    engine_code = _code_below_docstring(engine_mod)
    assert "core.metrics" not in engine_code, \
        "sampling/engine.py should reach core.metrics only indirectly, via sampling/anomaly.py"

    anomaly_code = _code_below_docstring(anomaly_mod)
    assert "from core.metrics import evaluate_metric_drift" in anomaly_code
    for banned_symbol in ("calculate_metrics", "detect_and_log", "_load_current_metrics",
                          "_load_history", "_load_config", "dq_metrics_summary", "dq_anomaly_config"):
        assert banned_symbol not in anomaly_code, \
            f"sampling/anomaly.py should not reach into core.metrics's DB-querying internals ({banned_symbol})"
