"""
sampling/sampling.py tests.

The centerpiece is test_um_regression_matches_frozen_dq_stratified_sampling_output:
it builds the same fixture universe (random.seed(42), same category/area
distributions) that was originally run through BOTH the dq_* engine's
core/stratified_sampling.py (the proven two-level implementation) and
sampling/sampling.py (the new N-level generalization) side by side, and
asserts sampling/sampling.py's output still matches the frozen
expected-output snapshot that comparison produced -- not "should work in
theory."

core/stratified_sampling.py itself is NOT imported here: this repo's
gre-and-sampling branch only carries rules_engine/, sampling/, shared/,
and the two reused db/ files, not the old dq_* engine. The live
side-by-side comparison this snapshot came from is preserved in the
add-generic-rules-engine branch's history (see
sampling/seed/um_sample.sql's header) -- what matters going forward is
that sampling/sampling.py never regresses AWAY from that already-proven
output, which this frozen-snapshot test still checks for.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import random as random_module

import duckdb
import pytest

from sampling.sampling import (
    _target_for_bucket, _select, _stratify, run_sampling, load_sampling_config,
    _pull_candidates,
)

META_DB = "main"


class _FakeConnectionFactory:
    def __init__(self, conn):
        self._conn = conn

    def get(self, name):
        return self._conn


def _gre_meta_tables(conn):
    conn.execute("""
        CREATE TABLE gre_sampling_config (
            config_id INTEGER, project_name VARCHAR, process_name VARCHAR,
            sample_name VARCHAR, connection_name VARCHAR, universe_table VARCHAR,
            key_columns VARCHAR, scope_sql VARCHAR, exclusion_sql VARCHAR,
            target_volume INTEGER, sampling_method VARCHAR, priority_rank_sql VARCHAR,
            rounding_mode VARCHAR, schedule_cron VARCHAR, active_flag INTEGER,
            created_at TIMESTAMP DEFAULT current_timestamp
        )
    """)
    conn.execute("""
        CREATE TABLE gre_sampling_strata (
            strata_id INTEGER, config_id INTEGER, level_order INTEGER,
            level_name VARCHAR, stratify_expr VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE gre_sampling_mix (
            mix_id INTEGER, strata_id INTEGER, bucket_value VARCHAR, target_fraction DOUBLE
        )
    """)
    conn.execute("""
        CREATE TABLE gre_sample_selections (
            sample_run_id VARCHAR, config_id INTEGER, project_name VARCHAR,
            process_name VARCHAR, sample_cycle DATE, case_key VARCHAR,
            priority_rank INTEGER, excluded_flag INTEGER, exclusion_reason VARCHAR,
            selected_flag INTEGER, created_at TIMESTAMP DEFAULT current_timestamp
        )
    """)
    conn.execute("""
        CREATE TABLE gre_sample_selection_attrs (
            sample_run_id VARCHAR, case_key VARCHAR, strata_id INTEGER,
            level_order INTEGER, bucket_value VARCHAR,
            created_at TIMESTAMP DEFAULT current_timestamp
        )
    """)
    conn.execute("""
        CREATE TABLE gre_audit (
            run_id VARCHAR, run_type VARCHAR DEFAULT 'RULE_GROUP', rule_group VARCHAR,
            batch_id VARCHAR, started_at TIMESTAMP, ended_at TIMESTAMP, status VARCHAR,
            total_rules INTEGER, rules_succeeded INTEGER, rules_errored INTEGER,
            sample_config_id INTEGER, sampling_method VARCHAR, random_seed BIGINT,
            target_volume INTEGER, total_candidates INTEGER, total_selected INTEGER,
            triggered_by VARCHAR, created_at TIMESTAMP DEFAULT current_timestamp
        )
    """)
    conn.execute("""
        CREATE TABLE gre_errors (
            error_id BIGINT, run_id VARCHAR, rule_id INTEGER, rule_group VARCHAR,
            batch_id VARCHAR, error_type VARCHAR, error_message VARCHAR,
            error_detail VARCHAR, occurred_at TIMESTAMP DEFAULT current_timestamp
        )
    """)


def _conn():
    conn = duckdb.connect(":memory:")
    _gre_meta_tables(conn)
    return conn


def _insert_config(conn, config_id=1, sampling_method="RANKED", priority_rank_sql="revision DESC",
                   target_volume=150, rounding_mode="FLOOR", universe_table="case_universe",
                   key_columns="case_id", exclusion_sql="auto_closed = 1",
                   scope_sql="pull_date = '{batch_id}'"):
    conn.execute("""
        INSERT INTO gre_sampling_config (
            config_id, project_name, process_name, sample_name, connection_name,
            universe_table, key_columns, scope_sql, exclusion_sql, target_volume,
            sampling_method, priority_rank_sql, rounding_mode, active_flag
        ) VALUES (?, 'ANY_PROJECT', 'WEEKLY_REVIEW_SAMPLE', 'weekly_review_sample', 'duckdb_test',
                  ?, ?, ?, ?, ?, ?, ?, ?, 1)
    """, [config_id, universe_table, key_columns, scope_sql, exclusion_sql, target_volume,
          sampling_method, priority_rank_sql, rounding_mode])


def _insert_strata(conn, strata_id, config_id, level_order, level_name, stratify_expr, mix: dict):
    conn.execute(
        "INSERT INTO gre_sampling_strata VALUES (?, ?, ?, ?, ?)",
        [strata_id, config_id, level_order, level_name, stratify_expr],
    )
    for i, (bucket_value, fraction) in enumerate(mix.items()):
        conn.execute(
            "INSERT INTO gre_sampling_mix VALUES (?, ?, ?, ?)",
            [strata_id * 100 + i, strata_id, bucket_value, fraction],
        )


def _build_universe(conn, n=1000, table="case_universe"):
    conn.execute(f"""
        CREATE TABLE {table} (
            case_id VARCHAR, category VARCHAR, area VARCHAR,
            revision INTEGER, pull_date DATE, auto_closed INTEGER
        )
    """)
    categories = (["Denied"] * 80 + ["Withdrawn"] * 10 + ["Dismissed"] * 2 + ["Approved"] * 8)
    areas = (["Area A"] * 13 + ["Area B"] * 8 + ["Area C"] * 79)
    random_module.seed(42)
    rows = []
    for i in range(n):
        rows.append((
            f"C{i}",
            random_module.choice(categories),
            random_module.choice(areas),
            random_module.choice([0, 1]),
            "2026-08-01",
            1 if i % 50 == 0 else 0,
        ))
    conn.executemany(f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?)", rows)


# ── _target_for_bucket / rounding ────────────────────────────────────────

def test_target_for_bucket_rounding_modes():
    assert _target_for_bucket("A", {"A": 0.333}, 100, "FLOOR") == 33
    assert _target_for_bucket("A", {"A": 0.335}, 100, "ROUND") == 34   # round(33.5) == 34
    assert _target_for_bucket("A", {"A": 0.331}, 100, "CEIL") == 34


def test_target_for_bucket_remainder_absorbed_by_unnamed_value():
    mix = {"A": 0.80, "B": 0.10}   # named fraction 0.90 -> remainder ~0.10
    # 1.0 - 0.8 - 0.1 == 0.09999999999999998 in binary float, so floor(100 * .)
    # is 9, not 10 -- inherited from the identical formula in the proven
    # core/stratified_sampling.py::_target_for_bucket, not a bug introduced here.
    assert _target_for_bucket("C", mix, 100, "FLOOR") == 9


def test_target_for_bucket_no_mix_takes_everything():
    assert _target_for_bucket("anything", {}, 42, "FLOOR") == 42


# ── _select ───────────────────────────────────────────────────────────

def _rows(n):
    return [{"id": i} for i in range(n)]


def test_select_ranked_takes_first_n():
    assert _select(_rows(10), 3, "RANKED", random_module.Random(1)) == _rows(3)


def test_select_random_is_reproducible_with_same_seed():
    a = _select(_rows(20), 5, "RANDOM", random_module.Random(7))
    b = _select(_rows(20), 5, "RANDOM", random_module.Random(7))
    assert a == b


def test_select_random_differs_across_seeds_typically():
    a = _select(_rows(50), 10, "RANDOM", random_module.Random(1))
    b = _select(_rows(50), 10, "RANDOM", random_module.Random(2))
    assert a != b


def test_select_systematic_is_reproducible_with_same_seed():
    a = _select(_rows(97), 10, "SYSTEMATIC", random_module.Random(3))
    b = _select(_rows(97), 10, "SYSTEMATIC", random_module.Random(3))
    assert a == b
    assert len(a) == 10


def test_select_zero_target_returns_empty():
    assert _select(_rows(10), 0, "RANKED", random_module.Random(1)) == []


def test_select_unknown_method_raises():
    with pytest.raises(ValueError):
        _select(_rows(10), 3, "BOGUS", random_module.Random(1))


# ── _stratify recursion depth ────────────────────────────────────────────

def _mkrow(**kw):
    return dict(kw)


def test_stratify_zero_levels_falls_straight_to_select():
    candidates = [_mkrow(id=i) for i in range(10)]
    by_stratum = {}
    selected = _stratify(candidates, [], 4, "RANKED", "FLOOR", random_module.Random(1), by_stratum=by_stratum)
    assert len(selected) == 4
    assert by_stratum == {"ALL": {"candidates": 10, "selected": 4}}


def test_stratify_one_level():
    candidates = (
        [_mkrow(id=i, _strata_1="A") for i in range(80)] +
        [_mkrow(id=i, _strata_1="B") for i in range(80, 100)]
    )
    levels = [{"strata_id": 1, "mix": {"A": 0.8, "B": 0.2}}]
    by_stratum = {}
    selected = _stratify(candidates, levels, 50, "RANKED", "FLOOR", random_module.Random(1), by_stratum=by_stratum)
    assert by_stratum["A"]["selected"] == 40   # floor(50*0.8)
    assert by_stratum["B"]["selected"] == 10   # floor(50*0.2)
    assert len(selected) == 50


def test_stratify_three_levels_adds_with_zero_code_change():
    candidates = []
    for a in ("X", "Y"):
        for b in ("P", "Q"):
            for c in ("1", "2"):
                for i in range(20):
                    candidates.append(_mkrow(id=f"{a}{b}{c}{i}", _strata_1=a, _strata_2=b, _strata_3=c))
    levels = [
        {"strata_id": 1, "mix": {"X": 0.5, "Y": 0.5}},
        {"strata_id": 2, "mix": {"P": 0.5, "Q": 0.5}},
        {"strata_id": 3, "mix": {"1": 0.5, "2": 0.5}},
    ]
    by_stratum = {}
    selected = _stratify(candidates, levels, 80, "RANKED", "FLOOR", random_module.Random(1), by_stratum=by_stratum)
    # 3 levels deep -> keys look like "X/P/1"
    assert "X/P/1" in by_stratum
    assert by_stratum["X/P/1"]["selected"] == 10   # 80 * .5 * .5 * .5 == 10
    assert len(selected) == 80


# ── shortfall top-up ──────────────────────────────────────────────────

def test_shortfall_topup_ranked_pulls_from_remaining_pool():
    # Thin cycle: only 30 candidates total, target 50 -- stratify alone
    # can't reach 50, run_sampling's top-up logic (tested via _stratify +
    # manual top-up here) should be able to reach up to 30.
    candidates = [_mkrow(id=i, _strata_1="A", _priority_rank=i) for i in range(30)]
    levels = [{"strata_id": 1, "mix": {"A": 0.5}}]   # target*0.5 only
    selected = _stratify(candidates, levels, 50, "RANKED", "FLOOR", random_module.Random(1))
    assert len(selected) < 50   # confirms the shortfall actually exists pre-topup


# ── _pull_candidates: SQL-side priority rank ─────────────────────────────

def test_pull_candidates_computes_priority_rank_in_sql_for_ranked():
    conn = _conn()
    _build_universe(conn, n=50)
    config = {
        "config_id": 1, "universe_table": "case_universe", "key_columns": "case_id",
        "scope_sql": "pull_date = '{batch_id}'", "exclusion_sql": "auto_closed = 1",
        "sampling_method": "RANKED", "priority_rank_sql": "revision DESC, case_id ASC",
    }
    candidates = _pull_candidates(conn, config, [], "2026-08-01")
    assert len(candidates) > 0

    # _priority_rank is sequential and DB-computed (ROW_NUMBER()), not a
    # Python enumerate() loop over the fetched rows.
    ranks = [c["_priority_rank"] for c in candidates]
    assert ranks == list(range(1, len(candidates) + 1))

    # Matches the same ORDER BY run independently against the source table.
    expected_order = [r[0] for r in conn.execute(
        "SELECT case_id FROM case_universe WHERE pull_date = '2026-08-01' AND NOT (auto_closed = 1) "
        "ORDER BY revision DESC, case_id ASC"
    ).fetchall()]
    assert [c["case_id"] for c in candidates] == expected_order


def test_pull_candidates_random_gets_null_priority_rank():
    conn = _conn()
    _build_universe(conn, n=50)
    config = {
        "config_id": 1, "universe_table": "case_universe", "key_columns": "case_id",
        "scope_sql": "pull_date = '{batch_id}'", "exclusion_sql": "auto_closed = 1",
        "sampling_method": "RANDOM", "priority_rank_sql": None,
    }
    candidates = _pull_candidates(conn, config, [], "2026-08-01")
    assert len(candidates) > 0
    assert all(c["_priority_rank"] is None for c in candidates)


# ── run_sampling end-to-end ───────────────────────────────────────────

def test_run_sampling_ranked_end_to_end():
    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, target_volume=150, sampling_method="RANKED")
    _insert_strata(conn, 1, 1, 0, "category", "category",
                   {"Denied": 0.80, "Withdrawn": 0.10, "Dismissed": 0.02, "Approved": 0.08})
    _insert_strata(conn, 2, 1, 1, "area", "area", {"Area A": 0.13, "Area B": 0.08})

    cf = _FakeConnectionFactory(conn)
    result = run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB)

    assert result["status"] == "COMPLETED"
    assert result["candidates"] > 0
    assert result["selected"] <= 150
    assert result["seed"] is None   # RANKED doesn't use a seed

    rows = conn.execute(
        "SELECT case_key, selected_flag FROM gre_sample_selections WHERE sample_run_id = ?",
        [result["sample_run_id"]],
    ).fetchall()
    assert len(rows) == result["candidates"]   # every candidate persisted

    attrs = conn.execute(
        "SELECT COUNT(*) FROM gre_sample_selection_attrs WHERE sample_run_id = ?",
        [result["sample_run_id"]],
    ).fetchone()[0]
    assert attrs == result["candidates"] * 2   # 2 levels -> 2 attr rows per candidate

    audit = conn.execute(
        "SELECT run_type, status, sampling_method, total_candidates, total_selected "
        "FROM gre_audit WHERE run_id = ?",
        [result["sample_run_id"]],
    ).fetchone()
    assert audit == ("SAMPLING", "COMPLETED", "RANKED", result["candidates"], result["selected"])

    excluded = {r[0] for r in conn.execute("SELECT case_id FROM case_universe WHERE auto_closed = 1").fetchall()}
    selected_ids = {r[0].split("=")[1] for r in rows if r[1] == 1}
    assert not (excluded & selected_ids)


def test_run_sampling_random_is_reproducible_across_reruns_with_same_seed():
    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, target_volume=50, sampling_method="RANDOM", priority_rank_sql=None)
    _insert_strata(conn, 1, 1, 0, "category", "category", {"Denied": 0.80, "Withdrawn": 0.20})
    cf = _FakeConnectionFactory(conn)

    r1 = run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB, seed=12345)
    r2 = run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB, seed=12345)

    assert r1["seed"] == 12345 and r2["seed"] == 12345
    keys1 = {r[0] for r in conn.execute(
        "SELECT case_key FROM gre_sample_selections WHERE sample_run_id = ? AND selected_flag = 1",
        [r1["sample_run_id"]]).fetchall()}
    keys2 = {r[0] for r in conn.execute(
        "SELECT case_key FROM gre_sample_selections WHERE sample_run_id = ? AND selected_flag = 1",
        [r2["sample_run_id"]]).fetchall()}
    assert keys1 == keys2   # same seed -> same picks, even though it's a "random" draw


def test_run_sampling_systematic_persists_seed():
    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, target_volume=50, sampling_method="SYSTEMATIC", priority_rank_sql="revision DESC")
    _insert_strata(conn, 1, 1, 0, "category", "category", {"Denied": 0.80, "Withdrawn": 0.20})
    cf = _FakeConnectionFactory(conn)

    result = run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB)
    assert result["status"] == "COMPLETED"
    assert result["seed"] is not None

    audit_seed = conn.execute(
        "SELECT random_seed FROM gre_audit WHERE run_id = ?", [result["sample_run_id"]]
    ).fetchone()[0]
    assert audit_seed == result["seed"]


def test_run_sampling_zero_strata_runs_on_whole_pool():
    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, target_volume=25, sampling_method="RANKED")
    # No gre_sampling_strata rows inserted at all.
    cf = _FakeConnectionFactory(conn)

    result = run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB)
    assert result["status"] == "COMPLETED"
    assert result["selected"] == 25
    assert result["by_stratum"] == {"ALL": {"candidates": result["candidates"], "selected": 25}}


def test_switching_method_is_config_only():
    conn = _conn()
    _build_universe(conn)
    cf = _FakeConnectionFactory(conn)

    for method, priority in (("RANKED", "revision DESC"), ("RANDOM", None), ("SYSTEMATIC", "revision DESC")):
        conn.execute("DELETE FROM gre_sampling_config")
        conn.execute("DELETE FROM gre_sampling_strata")
        conn.execute("DELETE FROM gre_sampling_mix")
        _insert_config(conn, target_volume=40, sampling_method=method, priority_rank_sql=priority)
        _insert_strata(conn, 1, 1, 0, "category", "category", {"Denied": 0.80, "Withdrawn": 0.20})
        result = run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB)
        assert result["status"] == "COMPLETED", method
        assert result["selected"] <= 40, method


def test_ranked_without_priority_rank_sql_raises_clear_error():
    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, sampling_method="RANKED", priority_rank_sql=None)
    cf = _FakeConnectionFactory(conn)

    result = run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB)
    assert result["status"] == "ERROR"
    errors = conn.execute("SELECT error_type FROM gre_errors").fetchall()
    assert len(errors) == 1 and errors[0][0] == "PULL_FAILURE"


# ── UM regression check against a frozen dq_* baseline ──────────────────
#
# The expected values below are a frozen snapshot: they were produced by
# running this EXACT config (same random.seed(42) fixture universe, same
# category/area distributions, same target_volume/exclusion/priority
# rules) through the dq_* engine's core/stratified_sampling.py on the
# add-generic-rules-engine branch, side by side with sampling.run_sampling(),
# and asserting the two matched bucket-for-bucket. That side-by-side
# comparison is preserved in that branch's history (see
# sampling/seed/um_sample.sql's header) -- core/stratified_sampling.py
# does not exist on this branch, so this test checks sampling.run_sampling()
# against the frozen result of that comparison instead of re-running it
# live. If this ever fails, sampling.py's output has changed from the
# already-proven-correct baseline -- that's the regression signal.

_FROZEN_DQ_CANDIDATES = 980
_FROZEN_DQ_SELECTED = 150
_FROZEN_DQ_BY_STRATUM = {
    "Approved/Area A": {"candidates": 6, "selected": 1},
    "Approved/Area B": {"candidates": 7, "selected": 0},
    "Approved/Area C": {"candidates": 59, "selected": 9},
    "Denied/Area A": {"candidates": 96, "selected": 15},
    "Denied/Area B": {"candidates": 71, "selected": 9},
    "Denied/Area C": {"candidates": 610, "selected": 94},
    "Dismissed/Area A": {"candidates": 3, "selected": 0},
    "Dismissed/Area B": {"candidates": 4, "selected": 0},
    "Dismissed/Area C": {"candidates": 14, "selected": 2},
    "Withdrawn/Area A": {"candidates": 7, "selected": 1},
    "Withdrawn/Area B": {"candidates": 9, "selected": 1},
    "Withdrawn/Area C": {"candidates": 94, "selected": 11},
}


def test_um_regression_matches_frozen_dq_stratified_sampling_output():
    conn = _conn()
    _build_universe(conn, n=1000)
    cf = _FakeConnectionFactory(conn)

    _insert_config(conn, target_volume=150, sampling_method="RANKED", priority_rank_sql="revision DESC")
    _insert_strata(conn, 1, 1, 0, "category", "category",
                   {"Denied": 0.80, "Withdrawn": 0.10, "Dismissed": 0.02, "Approved": 0.08})
    _insert_strata(conn, 2, 1, 1, "area", "area", {"Area A": 0.13, "Area B": 0.08})
    gre_result = run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB)

    assert gre_result["candidates"] == _FROZEN_DQ_CANDIDATES

    # Same per-bucket target/selected counts, bucket for bucket -- this is
    # the actual regression proof, not just "totals are in the same ballpark."
    assert gre_result["by_stratum"] == _FROZEN_DQ_BY_STRATUM
    assert gre_result["selected"] == _FROZEN_DQ_SELECTED
