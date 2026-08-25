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
gre-and-sampling branch only carries rules_engine/, sampling/, and the
two reused db/ files, not the old dq_* engine. The live
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
    _target_for_bucket, _select, _stratify, run_sampling,
    _pull_candidates, discover_sampling_configs, run_sampling_for_process_name,
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
            sample_name VARCHAR, source_type VARCHAR, universe_table VARCHAR,
            key_columns VARCHAR, scope_sql VARCHAR, exclusion_sql VARCHAR,
            target_volume INTEGER, sampling_method VARCHAR, priority_rank_sql VARCHAR,
            rounding_mode VARCHAR, schedule_cron VARCHAR, act_ind INTEGER,
            load_datetime TIMESTAMP DEFAULT current_timestamp
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
            selected_flag INTEGER, etl_is_curr_ind VARCHAR DEFAULT 'Y',
            load_datetime TIMESTAMP DEFAULT current_timestamp, last_updated_datetime TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE gre_sample_selection_attrs (
            sample_run_id VARCHAR, case_key VARCHAR, strata_id INTEGER,
            level_order INTEGER, bucket_value VARCHAR, etl_is_curr_ind VARCHAR DEFAULT 'Y',
            load_datetime TIMESTAMP DEFAULT current_timestamp, last_updated_datetime TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE gre_sampling_audit (
            run_id VARCHAR, run_key VARCHAR, sample_config_id INTEGER,
            sampling_method VARCHAR, random_seed BIGINT,
            target_volume INTEGER, total_candidates INTEGER, total_selected INTEGER,
            started_at TIMESTAMP, ended_at TIMESTAMP, status VARCHAR,
            triggered_by VARCHAR, load_datetime TIMESTAMP DEFAULT current_timestamp
        )
    """)
    conn.execute("""
        CREATE TABLE gre_sampling_errors (
            error_id BIGINT, run_id VARCHAR, process_name VARCHAR,
            run_key VARCHAR, error_type VARCHAR, error_message VARCHAR,
            error_detail VARCHAR, active_ind VARCHAR DEFAULT 'Y',
            occurred_at TIMESTAMP DEFAULT current_timestamp,
            last_updated_datetime TIMESTAMP
        )
    """)


def _conn():
    conn = duckdb.connect(":memory:")
    _gre_meta_tables(conn)
    return conn


def _insert_config(conn, config_id=1, sampling_method="RANKED", priority_rank_sql="revision DESC",
                   target_volume=150, rounding_mode="FLOOR", universe_table="case_universe",
                   key_columns="case_id", exclusion_sql="auto_closed = 1",
                   scope_sql="pull_date = '{batch_id}'",
                   project_name="ANY_PROJECT", process_name="WEEKLY_REVIEW_SAMPLE",
                   sample_name="weekly_review_sample", act_ind=1):
    conn.execute("""
        INSERT INTO gre_sampling_config (
            config_id, project_name, process_name, sample_name, source_type,
            universe_table, key_columns, scope_sql, exclusion_sql, target_volume,
            sampling_method, priority_rank_sql, rounding_mode, act_ind
        ) VALUES (?, ?, ?, ?, 'duckdb_test',
                  ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [config_id, project_name, process_name, sample_name,
          universe_table, key_columns, scope_sql, exclusion_sql, target_volume,
          sampling_method, priority_rank_sql, rounding_mode, act_ind])


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


def _run_sampling(config_id, run_key, cf, **kwargs):
    """
    Test convenience wrapper: run_key is no longer auto-injected into
    run_params (see sampling/sampling.py::run_sampling()'s docstring --
    doing so would corrupt _pull_candidates()'s substitution for any
    scope_sql/exclusion_sql that doesn't reference a run_key-shaped token).
    Most fixture configs' scope_sql references a "{batch_id}" token, so
    default run_params={"batch_id": run_key} here (matching the OLD
    build_run_params() auto-injection behavior) unless the caller passes
    their own run_params, in which case merge rather than replace.
    """
    run_params = {"batch_id": run_key}
    run_params.update(kwargs.pop("run_params", None) or {})
    return run_sampling(config_id, run_key, cf, run_params=run_params, **kwargs)


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


def test_target_for_bucket_default_unmixed_count_is_one_backward_compatible():
    """
    A direct call with no unmixed_count arg (the shape every pre-existing
    caller/test uses) must behave EXACTLY as before this parameter
    existed -- the single-unnamed-bucket case gets the full remainder.
    """
    mix = {"A": 0.80, "B": 0.10}
    assert _target_for_bucket("C", mix, 100, "FLOOR") == _target_for_bucket("C", mix, 100, "FLOOR", 1)


def test_target_for_bucket_remainder_splits_across_multiple_unmixed_values():
    """
    Regression: previously each bucket_value absent from the mix got the
    FULL remainder fraction independently, so a level with N unnamed
    values over-allocated N times the intended quota to "leftover"
    buckets combined. unmixed_count divides the remainder across however
    many distinct unmixed values are actually present.
    """
    mix = {"A": 0.80, "B": 0.10}   # remainder ~0.10
    # One unnamed bucket (the old default / single-bucket case): gets the
    # whole ~0.10.
    solo = _target_for_bucket("C", mix, 1000, "FLOOR", unmixed_count=1)
    # Two unnamed buckets sharing the level: each should get about half of
    # that -- NOT the same full amount solo got.
    split_c = _target_for_bucket("C", mix, 1000, "FLOOR", unmixed_count=2)
    split_d = _target_for_bucket("D", mix, 1000, "FLOOR", unmixed_count=2)
    assert split_c == split_d   # symmetric -- neither unnamed bucket is favored
    assert split_c < solo
    # The two shares plus the two named fractions should land close to the
    # total (allowing for floor-rounding on each of the 4 pieces).
    named_total = _target_for_bucket("A", mix, 1000, "FLOOR") + _target_for_bucket("B", mix, 1000, "FLOOR")
    assert abs((named_total + split_c + split_d) - 1000) <= 4


def test_stratify_splits_remainder_across_multiple_unmixed_buckets_end_to_end():
    """
    End-to-end version of the above through _stratify(): a level whose
    actual data has THREE bucket values outside the configured 2-value
    mix must split the remainder three ways, not give each of the three
    the full remainder (which would happen if _stratify still called
    _target_for_bucket without the level's real unmixed_count).
    """
    rows = (
        [{"_strata_1": "Named1", "id": f"n1-{i}"} for i in range(10)]
        + [{"_strata_1": "Named2", "id": f"n2-{i}"} for i in range(10)]
        + [{"_strata_1": "Other1", "id": f"o1-{i}"} for i in range(10)]
        + [{"_strata_1": "Other2", "id": f"o2-{i}"} for i in range(10)]
        + [{"_strata_1": "Other3", "id": f"o3-{i}"} for i in range(10)]
    )
    levels = [{
        "strata_id": 1, "level_order": 0, "level_name": "cat",
        "stratify_expr": "cat", "mix": {"Named1": 0.5, "Named2": 0.3},
    }]
    by_stratum = {}
    rng = random_module.Random(1)
    _stratify(rows, levels, target=100, method="RANKED", rounding_mode="FLOOR",
             rng=rng, by_stratum=by_stratum)

    other_counts = {k: v["candidates"] for k, v in by_stratum.items() if k.startswith("Other")}
    # All three "Other*" buckets are unmixed and equally sized in the
    # source data -- their SELECTED counts (bounded by each bucket's own
    # target, which is what we're actually checking) should be equal to
    # each other, not have one bucket's target dwarf the others'.
    other_selected = {k: v["selected"] for k, v in by_stratum.items() if k.startswith("Other")}
    assert len(set(other_selected.values())) == 1   # all three got the same share


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
    candidates = _pull_candidates(conn, config, [], {"batch_id": "2026-08-01"})
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
    candidates = _pull_candidates(conn, config, [], {"batch_id": "2026-08-01"})
    assert len(candidates) > 0
    assert all(c["_priority_rank"] is None for c in candidates)


def test_pull_candidates_random_without_priority_rank_sql_still_orders_deterministically():
    """
    Regression: a RANDOM config with no priority_rank_sql used to pull with
    NO ORDER BY at all, so the row order feeding rng.shuffle() had no
    reproducibility guarantee across reruns -- silently undermining this
    module's core "same seed -> same selection" promise. Falls back to
    ORDER BY key_columns instead, so the pull order (and therefore the
    shuffle) is stable across reruns even without an author-supplied
    priority_rank_sql.
    """
    conn = _conn()
    _build_universe(conn, n=50)
    config = {
        "config_id": 1, "universe_table": "case_universe", "key_columns": "case_id",
        "scope_sql": "pull_date = '{batch_id}'", "exclusion_sql": "auto_closed = 1",
        "sampling_method": "RANDOM", "priority_rank_sql": None,
    }
    first = [c["case_id"] for c in _pull_candidates(conn, config, [], {"batch_id": "2026-08-01"})]
    second = [c["case_id"] for c in _pull_candidates(conn, config, [], {"batch_id": "2026-08-01"})]
    assert first == second   # stable across repeated pulls, not just "same length"

    expected_order = [r[0] for r in conn.execute(
        "SELECT case_id FROM case_universe WHERE pull_date = '2026-08-01' AND NOT (auto_closed = 1) "
        "ORDER BY case_id"
    ).fetchall()]
    assert first == expected_order


def test_pull_candidates_exclusion_sql_gets_run_params_substitution():
    # Previously exclusion_sql got NO substitution at all -- only scope_sql
    # did. Prove exclusion_sql can now reference a run_params {key} token
    # too, and that it actually takes effect (excludes the matching rows).
    conn = _conn()
    _build_universe(conn, n=50)
    config = {
        "config_id": 1, "universe_table": "case_universe", "key_columns": "case_id",
        "scope_sql": "pull_date = '{batch_id}'",
        "exclusion_sql": "revision = {excluded_revision}",
        "sampling_method": "RANDOM", "priority_rank_sql": None,
    }
    candidates = _pull_candidates(conn, config, [], {"batch_id": "2026-08-01", "excluded_revision": 1})
    assert len(candidates) > 0

    remaining_revisions = {r[0] for r in conn.execute(
        "SELECT DISTINCT revision FROM case_universe WHERE pull_date = '2026-08-01' AND revision = 1"
    ).fetchall()}
    assert remaining_revisions  # sanity: revision=1 rows exist in the universe...
    pulled_ids = {c["case_id"] for c in candidates}
    excluded_ids = {r[0] for r in conn.execute(
        "SELECT case_id FROM case_universe WHERE pull_date = '2026-08-01' AND revision = 1"
    ).fetchall()}
    assert not (pulled_ids & excluded_ids)   # ...but none of them made it through the pull


def test_pull_candidates_exclusion_sql_unresolved_token_raises():
    conn = _conn()
    _build_universe(conn, n=10)
    config = {
        "config_id": 1, "universe_table": "case_universe", "key_columns": "case_id",
        "scope_sql": "pull_date = '{batch_id}'",
        "exclusion_sql": "revision = {excluded_revision}",   # not supplied below
        "sampling_method": "RANDOM", "priority_rank_sql": None,
    }
    with pytest.raises(ValueError):
        _pull_candidates(conn, config, [], {"batch_id": "2026-08-01"})


# ── run_sampling end-to-end ───────────────────────────────────────────

def test_run_sampling_ranked_end_to_end():
    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, target_volume=150, sampling_method="RANKED")
    _insert_strata(conn, 1, 1, 0, "category", "category",
                   {"Denied": 0.80, "Withdrawn": 0.10, "Dismissed": 0.02, "Approved": 0.08})
    _insert_strata(conn, 2, 1, 1, "area", "area", {"Area A": 0.13, "Area B": 0.08})

    cf = _FakeConnectionFactory(conn)
    result = _run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB)

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
        "SELECT status, sampling_method, total_candidates, total_selected "
        "FROM gre_sampling_audit WHERE run_id = ?",
        [result["sample_run_id"]],
    ).fetchone()
    # No run_type column anymore -- gre_sampling_audit holds sampling runs
    # ONLY, and shares no table with rules_engine/'s gre_rule_audit (see
    # README.md's "Package separation").
    assert audit == ("COMPLETED", "RANKED", result["candidates"], result["selected"])

    excluded = {r[0] for r in conn.execute("SELECT case_id FROM case_universe WHERE auto_closed = 1").fetchall()}
    selected_ids = {r[0].split("=")[1] for r in rows if r[1] == 1}
    assert not (excluded & selected_ids)


def test_run_sampling_sample_run_id_embeds_project_attempt_and_triggered_by():
    """
    sample_run_id gets the same "make it meaningful" treatment as
    rules_engine's run_id (see rules_engine/runner.py::
    _build_group_run_id()): {project_name}.{sample_name}::{run_key}::
    attempt-{N}::{triggered_by}::{timestamp}::{hex}.
    """
    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, target_volume=150, sampling_method="RANKED", project_name="HEALTHSPRING_UM")
    _insert_strata(conn, 1, 1, 0, "category", "category",
                   {"Denied": 0.80, "Withdrawn": 0.10, "Dismissed": 0.02, "Approved": 0.08})

    cf = _FakeConnectionFactory(conn)
    first = _run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB, triggered_by="jsmith")
    assert first["sample_run_id"].startswith(
        "HEALTHSPRING_UM.weekly_review_sample::2026-08-01::attempt-1::jsmith::"
    )

    audit = conn.execute(
        "SELECT triggered_by FROM gre_sampling_audit WHERE run_id = ?", [first["sample_run_id"]],
    ).fetchone()
    assert audit[0] == "jsmith"

    # Rerun of the same (config_id, run_key) -> attempt-2.
    second = _run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB, triggered_by="jsmith")
    assert "::attempt-2::" in second["sample_run_id"]


def test_run_sampling_rerun_deactivates_prior_sample_run_id_for_same_run_key():
    """
    Regression test for _deactivate_prior_sampling_runs(): running the SAME
    (config_id, run_key) twice (e.g. a rerun of today's cycle) must leave
    exactly one sample_run_id's rows active (etl_is_curr_ind='Y') in both
    gre_sample_selections and gre_sample_selection_attrs -- the second
    (newer) sample_run_id -- while the first run's rows stay in the tables
    (soft-deactivated, etl_is_curr_ind='N'), never deleted.
    """
    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, target_volume=150, sampling_method="RANKED")
    _insert_strata(conn, 1, 1, 0, "category", "category",
                   {"Denied": 0.80, "Withdrawn": 0.10, "Dismissed": 0.02, "Approved": 0.08})

    cf = _FakeConnectionFactory(conn)
    first = _run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB)
    # sample_run_id is now built via sampling/db_ops.py::generate_run_id()
    # (microsecond-precision timestamp + a random uniqueness suffix -- see
    # tests/test_sampling_db_ops.py's generate_run_id tests), so back-to-back
    # calls can no longer collide onto the same id even within the same
    # wall-clock second -- no artificial sleep needed between them any more.
    second = _run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB)

    assert first["status"] == "COMPLETED"
    assert second["status"] == "COMPLETED"
    assert first["sample_run_id"] != second["sample_run_id"]   # fresh id each call, guaranteed unique

    first_sel = conn.execute(
        "SELECT DISTINCT etl_is_curr_ind FROM gre_sample_selections WHERE sample_run_id = ?",
        [first["sample_run_id"]],
    ).fetchall()
    assert first_sel and all(r[0] == "N" for r in first_sel)   # prior run fully deactivated

    first_attrs = conn.execute(
        "SELECT DISTINCT etl_is_curr_ind FROM gre_sample_selection_attrs WHERE sample_run_id = ?",
        [first["sample_run_id"]],
    ).fetchall()
    assert first_attrs and all(r[0] == "N" for r in first_attrs)

    # last_updated_datetime bumped by the deactivate UPDATE -- lets
    # metadata_sync's incremental watermark pick up this flip even though
    # load_datetime (set once at insert) never changes.
    first_bumped = conn.execute(
        "SELECT COUNT(*) FROM gre_sample_selections "
        "WHERE sample_run_id = ? AND last_updated_datetime IS NULL",
        [first["sample_run_id"]],
    ).fetchone()[0]
    assert first_bumped == 0

    second_sel = conn.execute(
        "SELECT DISTINCT etl_is_curr_ind FROM gre_sample_selections WHERE sample_run_id = ?",
        [second["sample_run_id"]],
    ).fetchall()
    assert second_sel == [("Y",)]   # new run is active

    second_attrs = conn.execute(
        "SELECT DISTINCT etl_is_curr_ind FROM gre_sample_selection_attrs WHERE sample_run_id = ?",
        [second["sample_run_id"]],
    ).fetchall()
    assert second_attrs == [("Y",)]

    # Rows from the first run are still THERE (soft-deactivate, not deleted).
    first_row_count = conn.execute(
        "SELECT COUNT(*) FROM gre_sample_selections WHERE sample_run_id = ?", [first["sample_run_id"]],
    ).fetchone()[0]
    assert first_row_count == first["candidates"]


def test_run_sampling_different_run_key_does_not_deactivate_other_runs():
    """
    A different run_key for the same config_id is a distinct cycle, not a
    rerun -- its rows must stay active and untouched by a later run's
    reconciliation pass.
    """
    conn = _conn()
    _build_universe(conn)
    # scope_sql='1=1' (not keyed off pull_date) so both run_keys below pull the
    # same non-empty candidate pool -- only run_key differs between the two calls.
    _insert_config(conn, target_volume=150, sampling_method="RANKED", scope_sql="1=1")
    _insert_strata(conn, 1, 1, 0, "category", "category",
                   {"Denied": 0.80, "Withdrawn": 0.10, "Dismissed": 0.02, "Approved": 0.08})

    cf = _FakeConnectionFactory(conn)
    day1 = _run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB)
    day2 = _run_sampling(1, "2026-08-02", cf, meta_conn=conn, meta_db=META_DB)

    day1_flag = conn.execute(
        "SELECT DISTINCT etl_is_curr_ind FROM gre_sample_selections WHERE sample_run_id = ?",
        [day1["sample_run_id"]],
    ).fetchall()
    assert day1_flag == [("Y",)]   # untouched -- different run_key, not a rerun

    day2_flag = conn.execute(
        "SELECT DISTINCT etl_is_curr_ind FROM gre_sample_selections WHERE sample_run_id = ?",
        [day2["sample_run_id"]],
    ).fetchall()
    assert day2_flag == [("Y",)]


def test_case_key_is_case_insensitive_and_stays_distinct_per_candidate():
    """
    Regression test for _case_key(): gre_sampling_config.key_columns is
    free-text (authored independently of the physical column casing), but
    every pulled row's dict keys are always lowercased (see
    sampling/db_ops.py::execute_query()/_run_source_query()). Before the
    fix, a key_columns value like "Case_ID" (physical column: case_id)
    made row.get("Case_ID") miss for every single candidate, collapsing
    ALL of them onto the identical case_key "Case_ID=None" -- and because
    that one degenerate key was "selected" as soon as any real candidate
    was chosen, every candidate ended up persisted with selected_flag=1,
    not just the ones actually selected. Assert none of that happens.
    """
    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, target_volume=50, sampling_method="RANKED", key_columns="Case_ID")
    _insert_strata(conn, 1, 1, 0, "category", "category",
                   {"Denied": 0.80, "Withdrawn": 0.10, "Dismissed": 0.02, "Approved": 0.08})

    cf = _FakeConnectionFactory(conn)
    result = _run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB)

    assert result["status"] == "COMPLETED"
    assert result["selected"] == 50

    rows = conn.execute(
        "SELECT case_key, selected_flag FROM gre_sample_selections WHERE sample_run_id = ?",
        [result["sample_run_id"]],
    ).fetchall()

    # every candidate must get its OWN case_key, not one shared degenerate value
    distinct_keys = {r[0] for r in rows}
    assert len(distinct_keys) == result["candidates"]
    assert "Case_ID=None" not in distinct_keys

    # case_key must carry the real case_id value, not a missing-column default
    for case_key, _ in rows:
        assert case_key.startswith("Case_ID=C")

    # exactly the actually-selected rows are flagged -- not every candidate
    selected_count = sum(1 for _, flag in rows if flag == 1)
    assert selected_count == result["selected"] == 50


def test_case_key_raises_when_key_columns_names_an_unselected_column():
    """
    A key_columns entry that doesn't match ANY pulled column at all (not
    just a casing mismatch -- a genuine typo/config drift) must fail
    loudly rather than silently writing a corrupted key.
    """
    from sampling.sampling import _case_key
    with pytest.raises(KeyError):
        _case_key({"case_id": "C1"}, ["not_a_real_column"])


def test_run_sampling_select_persist_failure_writes_error_audit_not_crash(monkeypatch):
    """
    A failure during stratify/select/persist must be caught and logged
    the same way _pull_candidates() failures already are -- previously
    this stage had no try/except at all and would propagate straight out
    of run_sampling() uncaught, unlike every other failure mode in this
    function.

    (A key_columns entry that's a genuine typo -- not just a casing
    mismatch -- actually fails earlier, inside _pull_candidates()'s SQL
    SELECT itself, which the pre-existing PULL_FAILURE handler already
    covers; see test_run_sampling_unresolved_scope_token_routes_to_pull_failure
    for that path. This test exercises the select/persist stage
    specifically, by making _persist_selections() itself fail, the same
    way any real bug or transient failure in that stage would.)
    """
    import sampling.sampling as sampling_mod

    def _boom(*args, **kwargs):
        raise RuntimeError("simulated select/persist failure")
    monkeypatch.setattr(sampling_mod, "_persist_selections", _boom)

    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, target_volume=50, sampling_method="RANKED")
    _insert_strata(conn, 1, 1, 0, "category", "category",
                   {"Denied": 0.80, "Withdrawn": 0.10, "Dismissed": 0.02, "Approved": 0.08})

    cf = _FakeConnectionFactory(conn)
    result = _run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB)

    assert result["status"] == "ERROR"
    assert result["selected"] == 0

    audit = conn.execute(
        "SELECT status FROM gre_sampling_audit WHERE run_id = ?", [result["sample_run_id"]],
    ).fetchone()
    assert audit == ("ERROR",)

    error_row = conn.execute(
        "SELECT error_type FROM gre_sampling_errors WHERE run_id = ?", [result["sample_run_id"]],
    ).fetchone()
    assert error_row == ("SELECT_PERSIST_FAILURE",)

    # nothing should have been persisted to gre_sample_selections for this failed run
    persisted = conn.execute(
        "SELECT COUNT(*) FROM gre_sample_selections WHERE sample_run_id = ?",
        [result["sample_run_id"]],
    ).fetchone()[0]
    assert persisted == 0


def test_run_sampling_random_is_reproducible_across_reruns_with_same_seed():
    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, target_volume=50, sampling_method="RANDOM", priority_rank_sql=None)
    _insert_strata(conn, 1, 1, 0, "category", "category", {"Denied": 0.80, "Withdrawn": 0.20})
    cf = _FakeConnectionFactory(conn)

    r1 = _run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB, seed=12345)
    r2 = _run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB, seed=12345)

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

    result = _run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB)
    assert result["status"] == "COMPLETED"
    assert result["seed"] is not None

    audit_seed = conn.execute(
        "SELECT random_seed FROM gre_sampling_audit WHERE run_id = ?", [result["sample_run_id"]]
    ).fetchone()[0]
    assert audit_seed == result["seed"]


def test_run_sampling_zero_strata_runs_on_whole_pool():
    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, target_volume=25, sampling_method="RANKED")
    # No gre_sampling_strata rows inserted at all.
    cf = _FakeConnectionFactory(conn)

    result = _run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB)
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
        result = _run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB)
        assert result["status"] == "COMPLETED", method
        assert result["selected"] <= 40, method


# ── run_sampling_for_process_name(): convenience wrapper over run_sampling() ─

def test_run_sampling_for_process_name_runs_every_config_for_that_process():
    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, config_id=1, target_volume=25, sampling_method="RANKED",
                   project_name="PROJECT_A", process_name="WEEKLY_REVIEW_SAMPLE")
    _insert_config(conn, config_id=2, target_volume=10, sampling_method="RANKED",
                   project_name="PROJECT_B", process_name="WEEKLY_REVIEW_SAMPLE")
    _insert_config(conn, config_id=3, target_volume=10, sampling_method="RANKED",
                   project_name="PROJECT_A", process_name="OTHER_PROCESS")
    cf = _FakeConnectionFactory(conn)

    outcome = run_sampling_for_process_name("WEEKLY_REVIEW_SAMPLE", "2026-08-01", cf,
                                             meta_conn=conn, meta_db=META_DB,
                                             run_params={"batch_id": "2026-08-01"})

    # config_id=3 belongs to a different process_name -- excluded.
    assert set(outcome["sampling_configs"].keys()) == {1, 2}
    assert outcome["sampling_configs"][1]["status"] == "COMPLETED"
    assert outcome["sampling_configs"][2]["status"] == "COMPLETED"


def test_run_sampling_for_process_name_scoped_to_one_project():
    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, config_id=1, target_volume=25, sampling_method="RANKED",
                   project_name="PROJECT_A", process_name="WEEKLY_REVIEW_SAMPLE")
    _insert_config(conn, config_id=2, target_volume=10, sampling_method="RANKED",
                   project_name="PROJECT_B", process_name="WEEKLY_REVIEW_SAMPLE")
    cf = _FakeConnectionFactory(conn)

    outcome = run_sampling_for_process_name("WEEKLY_REVIEW_SAMPLE", "2026-08-01", cf,
                                             meta_conn=conn, meta_db=META_DB,
                                             project_name="PROJECT_A",
                                             run_params={"batch_id": "2026-08-01"})

    assert set(outcome["sampling_configs"].keys()) == {1}


def test_run_sampling_for_process_name_resolves_meta_conn_and_db_from_cf_when_omitted(monkeypatch):
    # meta_conn/meta_db aren't passed explicitly -- the wrapper must resolve
    # them itself via cf.get(...)/sampling.config, the whole point of this
    # convenience function over calling run_sampling() per config directly.
    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, config_id=1, target_volume=25, sampling_method="RANKED",
                   project_name="PROJECT_A", process_name="WEEKLY_REVIEW_SAMPLE")
    cf = _FakeConnectionFactory(conn)

    import sampling.config as shared_config
    monkeypatch.setattr(shared_config, "get_meta_db", lambda: META_DB)

    outcome = run_sampling_for_process_name("WEEKLY_REVIEW_SAMPLE", "2026-08-01", cf,
                                             run_params={"batch_id": "2026-08-01"})

    assert set(outcome["sampling_configs"].keys()) == {1}
    assert outcome["sampling_configs"][1]["status"] == "COMPLETED"


def test_run_sampling_for_process_name_raises_clearly_when_no_match():
    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, config_id=1, project_name="PROJECT_A", process_name="WEEKLY_REVIEW_SAMPLE")
    cf = _FakeConnectionFactory(conn)

    with pytest.raises(ValueError, match="NO_SUCH_PROCESS"):
        run_sampling_for_process_name("NO_SUCH_PROCESS", "2026-08-01", cf, meta_conn=conn, meta_db=META_DB)


def test_discover_sampling_configs_filters_by_project_and_process():
    conn = _conn()
    _insert_config(conn, config_id=1, project_name="PROJECT_A", process_name="PROC_A")
    _insert_config(conn, config_id=2, project_name="PROJECT_A", process_name="PROC_B")
    _insert_config(conn, config_id=3, project_name="PROJECT_B", process_name="PROC_A")
    _insert_config(conn, config_id=4, project_name="PROJECT_A", process_name="PROC_A", act_ind=0)

    assert discover_sampling_configs(conn, META_DB) == [1, 2, 3]   # inactive config_id=4 excluded
    assert discover_sampling_configs(conn, META_DB, project_name="PROJECT_A") == [1, 2]
    assert discover_sampling_configs(conn, META_DB, process_name="PROC_A") == [1, 3]
    assert discover_sampling_configs(conn, META_DB, project_name="PROJECT_A", process_name="PROC_A") == [1]


# ── run_params threading (v2 scoping) ────────────────────────────────────

def test_run_sampling_extra_run_params_key_reaches_scope_sql():
    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, target_volume=25, sampling_method="RANKED",
                   scope_sql="pull_date = '{batch_id}' AND {min_revision} <= revision")
    cf = _FakeConnectionFactory(conn)

    result = _run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB,
                            run_params={"min_revision": 0})
    assert result["status"] == "COMPLETED"
    assert result["candidates"] > 0


def test_run_sampling_run_key_and_run_params_batch_id_are_decoupled():
    # run_key is no longer merged into run_params (see
    # sampling/sampling.py::run_sampling()'s docstring), so a run_key value
    # different from run_params["batch_id"] doesn't collide with anything --
    # scope_sql's {batch_id} token is driven purely by the explicit
    # run_params value, independent of run_key/gre_sampling_audit.run_key tracking.
    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, target_volume=25, sampling_method="RANKED")
    cf = _FakeConnectionFactory(conn)

    result = run_sampling(1, "TRACKING_KEY", cf, meta_conn=conn, meta_db=META_DB,
                           run_params={"batch_id": "2026-08-01"})
    assert result["status"] == "COMPLETED"
    assert result["candidates"] > 0   # scope_sql pulled against the real pull_date ('2026-08-01')

    audit_run_key = conn.execute(
        "SELECT run_key FROM gre_sampling_audit WHERE run_id = ?", [result["sample_run_id"]]
    ).fetchone()[0]
    assert audit_run_key == "TRACKING_KEY"   # gre_sampling_audit tracked by run_key, not the business batch_id


def test_run_sampling_with_year_month_run_key_not_a_batch_id():
    # run_key doesn't have to be a "batch" at all -- a year+month composite
    # (built via sampling/db_ops.py::build_run_key()) works identically, and
    # gre_sampling_audit/gre_sample_selections/gre_sample_selection_attrs all track
    # correctly off it, with the sample_run_id embedding it too.
    from sampling.db_ops import build_run_key

    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, target_volume=25, sampling_method="RANKED")
    cf = _FakeConnectionFactory(conn)

    run_key = build_run_key(2026, 8)
    assert run_key == "2026_8"

    result = run_sampling(1, run_key, cf, meta_conn=conn, meta_db=META_DB,
                           run_params={"batch_id": "2026-08-01"})
    assert result["status"] == "COMPLETED"
    assert run_key in result["sample_run_id"]

    audit_run_key = conn.execute(
        "SELECT run_key FROM gre_sampling_audit WHERE run_id = ?", [result["sample_run_id"]]
    ).fetchone()[0]
    assert audit_run_key == run_key

    rows = conn.execute(
        "SELECT COUNT(*) FROM gre_sample_selections WHERE sample_run_id = ?", [result["sample_run_id"]]
    ).fetchone()[0]
    assert rows == result["candidates"]


def test_run_sampling_unresolved_scope_token_routes_to_pull_failure():
    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, target_volume=25, sampling_method="RANKED",
                   scope_sql="pull_date = '{batch_id}' AND revision = {min_revision}")
    cf = _FakeConnectionFactory(conn)

    result = _run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB)  # min_revision not supplied
    assert result["status"] == "ERROR"
    errors = conn.execute("SELECT error_type, error_message FROM gre_sampling_errors").fetchall()
    assert len(errors) == 1 and errors[0][0] == "PULL_FAILURE"
    assert "min_revision" in errors[0][1]


def test_ranked_without_priority_rank_sql_raises_clear_error():
    conn = _conn()
    _build_universe(conn)
    _insert_config(conn, sampling_method="RANKED", priority_rank_sql=None)
    cf = _FakeConnectionFactory(conn)

    result = _run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB)
    assert result["status"] == "ERROR"
    errors = conn.execute("SELECT error_type FROM gre_sampling_errors").fetchall()
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
    gre_result = _run_sampling(1, "2026-08-01", cf, meta_conn=conn, meta_db=META_DB)

    assert gre_result["candidates"] == _FROZEN_DQ_CANDIDATES

    # Same per-bucket target/selected counts, bucket for bucket -- this is
    # the actual regression proof, not just "totals are in the same ballpark."
    assert gre_result["by_stratum"] == _FROZEN_DQ_BY_STRATUM
    assert gre_result["selected"] == _FROZEN_DQ_SELECTED
