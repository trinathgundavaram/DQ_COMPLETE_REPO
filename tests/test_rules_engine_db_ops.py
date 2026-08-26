"""
rules_engine/db_ops.py tests: the low-level DB helpers this package
owns -- bulk writes with duplicate-key tolerance and {key} run_params
token substitution. No live DB connection required -- DuckDB stands in
for the metadata store.

rules_engine/db_ops.py is a full duplicate of sampling/db_ops.py for the
generic helpers below (execute_query/execute_dml/bulk_insert/
_substitute_params/build_run_key/generate_run_id) -- see that package's
own test_sampling_db_ops.py for the identical coverage on its copy.
Packages share no code (see README.md's "Package separation"). Only
count_prior_attempts()'s rule_group-keyed signature and the
gre_rule_audit fixture below are specific to this package.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime

import duckdb
import pytest

from rules_engine.db_ops import (
    execute_query, execute_dml, bulk_insert, bulk_insert_or_skip,
    _substitute_params, build_run_key, generate_run_id, count_prior_attempts,
)


def _conn():
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE gre_exceptions (
            record_id BIGINT, run_id VARCHAR, rule_id INTEGER, src_tbl_nm VARCHAR,
            element_name VARCHAR, source_name VARCHAR, issue_desc VARCHAR,
            exception_flag VARCHAR DEFAULT 'OPEN', exception_approver VARCHAR,
            run_key VARCHAR, etl_is_curr_ind VARCHAR DEFAULT 'Y',
            etl_load_dt DATE, etl_last_updt_dt TIMESTAMP,
            src_key_value VARCHAR, load_datetime TIMESTAMP DEFAULT current_timestamp
        )
    """)
    conn.execute("CREATE UNIQUE INDEX gre_exceptions_uix ON gre_exceptions(rule_id, run_key, src_key_value)")
    return conn


# ── run_params substitution ─────────────────────────────────────────────

def test_substitute_params_escapes_quotes():
    sql = "WHERE batch_id = '{batch_id}'"
    assert _substitute_params(sql, {"batch_id": "B1"}) == "WHERE batch_id = 'B1'"
    assert _substitute_params(sql, {"batch_id": "O'Brien"}) == "WHERE batch_id = 'O''Brien'"


def test_substitute_params_multi_key():
    sql = "WHERE batch_id = '{batch_id}' AND yr = {year} AND run_type = '{run_type}'"
    resolved = _substitute_params(sql, {"batch_id": "B1", "year": 2026, "run_type": "MONTHLY"})
    assert resolved == "WHERE batch_id = 'B1' AND yr = 2026 AND run_type = 'MONTHLY'"


def test_substitute_params_ignores_extra_keys_not_present_in_sql():
    sql = "WHERE batch_id = '{batch_id}'"
    assert _substitute_params(sql, {"batch_id": "B1", "unused": "x"}) == "WHERE batch_id = 'B1'"


def test_substitute_params_none_or_empty_sql_passthrough():
    assert _substitute_params(None, {"batch_id": "B1"}) is None
    assert _substitute_params("", {"batch_id": "B1"}) == ""


def test_substitute_params_none_value_becomes_empty_string():
    assert _substitute_params("v = '{x}'", {"x": None}) == "v = ''"


def test_substitute_params_unresolved_token_raises():
    sql = "WHERE batch_id = '{batch_id}' AND yr = {year}"
    with pytest.raises(ValueError) as exc_info:
        _substitute_params(sql, {"batch_id": "B1"})
    msg = str(exc_info.value)
    assert "year" in msg
    assert "batch_id" in msg  # lists what WAS supplied, for a fast diagnosis


def test_substitute_params_empty_params_dict_with_no_tokens_ok():
    assert _substitute_params("SELECT 1", {}) == "SELECT 1"
    assert _substitute_params("SELECT 1", None) == "SELECT 1"


# ── $key (bare, no braces) -- interchangeable with {key}, freely mixed ────

def test_substitute_params_dollar_token():
    sql = "WHERE batch_id = '$batch_id'"
    assert _substitute_params(sql, {"batch_id": "B1"}) == "WHERE batch_id = 'B1'"


def test_substitute_params_dollar_and_brace_tokens_mixed_in_one_sql():
    sql = "WHERE batch_id = '{batch_id}' AND yr = $year AND run_type = '$run_type'"
    resolved = _substitute_params(sql, {"batch_id": "B1", "year": 2026, "run_type": "MONTHLY"})
    assert resolved == "WHERE batch_id = 'B1' AND yr = 2026 AND run_type = 'MONTHLY'"


def test_substitute_params_dollar_token_respects_word_boundary():
    # "$year" must not also consume "_end" -- "$year_end" is a DIFFERENT
    # token (year_end), not "$year" followed by literal "_end".
    sql = "WHERE d = $year_end"
    with pytest.raises(ValueError) as exc_info:
        _substitute_params(sql, {"year": 2026})
    assert "year_end" in str(exc_info.value)

    resolved = _substitute_params(sql, {"year": 2026, "year_end": 999})
    assert resolved == "WHERE d = 999"


def test_substitute_params_dollar_token_unresolved_raises():
    sql = "WHERE batch_id = '$batch_id' AND yr = $year"
    with pytest.raises(ValueError) as exc_info:
        _substitute_params(sql, {"batch_id": "B1"})
    msg = str(exc_info.value)
    assert "year" in msg
    assert "batch_id" in msg


def test_substitute_params_dollar_token_escapes_quotes():
    sql = "WHERE batch_id = '$batch_id'"
    assert _substitute_params(sql, {"batch_id": "O'Brien"}) == "WHERE batch_id = 'O''Brien'"


# ── build_run_key ─────────────────────────────────────────────────────────

def test_build_run_key_single_part():
    assert build_run_key("BATCH_2026_08_19") == "BATCH_2026_08_19"


def test_build_run_key_multiple_parts_default_delimiter():
    assert build_run_key(2026, 8) == "2026_8"
    assert build_run_key("NORTHEAST", 2026, 8) == "NORTHEAST_2026_8"


def test_build_run_key_custom_delimiter():
    assert build_run_key(2026, 8, 19, delimiter="-") == "2026-8-19"


def test_build_run_key_zero_parts_raises():
    with pytest.raises(ValueError):
        build_run_key()


# ── generate_run_id ─────────────────────────────────────────────────────────

def test_generate_run_id_embeds_label_parts_and_timestamp():
    ts = datetime(2026, 8, 19, 14, 30, 22, 183045)
    run_id = generate_run_id("claims_dq", "BATCH_2026_08_19", timestamp=ts)
    assert run_id.startswith("claims_dq::BATCH_2026_08_19::20260819T143022.183045::")
    # trailing segment is a 6-hex-char uniqueness suffix
    suffix = run_id.rsplit("::", 1)[-1]
    assert len(suffix) == 6
    int(suffix, 16)   # raises ValueError if not valid hex


def test_generate_run_id_skips_none_and_empty_label_parts():
    ts = datetime(2026, 8, 19, 14, 30, 22, 183045)
    run_id = generate_run_id("claims_dq", None, "", timestamp=ts)
    assert run_id.startswith("claims_dq::20260819T143022.183045::")


def test_generate_run_id_is_lexicographically_sortable_by_time():
    earlier = generate_run_id("g", timestamp=datetime(2026, 8, 19, 10, 0, 0))
    later = generate_run_id("g", timestamp=datetime(2026, 8, 19, 10, 0, 1))
    assert sorted([later, earlier]) == [earlier, later]


def test_generate_run_id_never_collides_within_the_same_microsecond():
    """
    Regression for the old second-precision-only run_id format: two calls
    with identical label parts AND an identical (frozen) timestamp must
    still produce different run_ids, via the random suffix.
    """
    ts = datetime(2026, 8, 19, 14, 30, 22, 183045)
    ids = {generate_run_id("g", "k", timestamp=ts) for _ in range(200)}
    assert len(ids) == 200   # no collisions across 200 calls at the identical timestamp


def test_generate_run_id_defaults_timestamp_to_now():
    before = datetime.now()
    run_id = generate_run_id("g", "k")
    after = datetime.now()
    ts_segment = run_id.split("::")[2]
    parsed = datetime.strptime(ts_segment, "%Y%m%dT%H%M%S.%f")
    assert before <= parsed <= after


def test_generate_run_id_warns_past_200_chars(caplog):
    with caplog.at_level("WARNING"):
        run_id = generate_run_id("x" * 100, "y" * 100)
    assert len(run_id) > 200
    assert any("200" in r.message for r in caplog.records)


# ── count_prior_attempts (rule_group-keyed) ──────────────────────────────

def _audit_conn():
    """gre_rule_audit -- this package's own run-tracking table."""
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE gre_rule_audit (
            run_id VARCHAR, rule_group VARCHAR, run_key VARCHAR, started_at TIMESTAMP
        )
    """)
    return conn


def test_count_prior_attempts_zero_before_any_run():
    conn = _audit_conn()
    assert count_prior_attempts(conn, "main", "BATCH_2026_08_19", rule_group="claims_dq") == 0


def test_count_prior_attempts_increments_per_rule_group_run_key_pair():
    conn = _audit_conn()
    execute_dml(conn, "INSERT INTO gre_rule_audit (run_id, rule_group, run_key) VALUES (?, ?, ?)",
               ["r1", "claims_dq", "BATCH_2026_08_19"])
    assert count_prior_attempts(conn, "main", "BATCH_2026_08_19", rule_group="claims_dq") == 1
    execute_dml(conn, "INSERT INTO gre_rule_audit (run_id, rule_group, run_key) VALUES (?, ?, ?)",
               ["r2", "claims_dq", "BATCH_2026_08_19"])
    assert count_prior_attempts(conn, "main", "BATCH_2026_08_19", rule_group="claims_dq") == 2
    # A different rule_group or run_key doesn't share the count.
    assert count_prior_attempts(conn, "main", "BATCH_2026_08_19", rule_group="other_group") == 0
    assert count_prior_attempts(conn, "main", "OTHER_BATCH", rule_group="claims_dq") == 0


# ── bulk writes ───────────────────────────────────────────────────────────

def test_bulk_insert_batches_across_multiple_chunks():
    conn = _conn()
    rows = [["RUN", i, "t", "e", "s", f"issue {i}", "B1", f"claim_id=C{i}"] for i in range(7)]
    sql = """
        INSERT INTO gre_exceptions (
            run_id, rule_id, src_tbl_nm, element_name, source_name,
            issue_desc, run_key, src_key_value
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    bulk_insert(conn, sql, rows, chunk_size=2)   # 7 rows, chunk_size=2 -> 4 chunks
    written = execute_query(conn, "SELECT COUNT(*) AS cnt FROM gre_exceptions")[0]["cnt"]
    assert written == 7


def test_bulk_insert_or_skip_chunk_falls_back_on_duplicate():
    conn = _conn()
    sql = """
        INSERT INTO gre_exceptions (
            run_id, rule_id, src_tbl_nm, element_name, source_name,
            issue_desc, run_key, src_key_value
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    first = [["RUN1", 1, "t", "e", "s", "d", "B1", "claim_id=C1"],
             ["RUN1", 1, "t", "e", "s", "d", "B1", "claim_id=C2"]]
    inserted1 = bulk_insert_or_skip(conn, sql, first, chunk_size=10)
    assert inserted1 == 2

    # One brand-new key plus one duplicate of an already-committed key, in
    # the SAME chunk -- the whole-chunk executemany collides, exercising
    # the row-by-row fallback path rather than the happy-path executemany.
    # (DuckDB applies C3 before hitting the C1 collision, so the fallback's
    # own count of "newly inserted" can undercount here -- see
    # bulk_insert_or_skip()'s docstring; what must hold is the DATA: no
    # duplicate row, no dropped row, and no exception escaping.)
    second = [["RUN2", 1, "t", "e", "s", "d", "B1", "claim_id=C3"],
              ["RUN2", 1, "t", "e", "s", "d", "B1", "claim_id=C1"]]
    bulk_insert_or_skip(conn, sql, second, chunk_size=10)

    total = execute_query(conn, "SELECT COUNT(*) AS cnt FROM gre_exceptions")[0]["cnt"]
    assert total == 3   # C1, C2, C3 -- no duplicate row, no lost row
    keys = {r["src_key_value"] for r in execute_query(conn, "SELECT src_key_value FROM gre_exceptions")}
    assert keys == {"claim_id=C1", "claim_id=C2", "claim_id=C3"}
