"""
shared/db_ops.py tests: the low-level DB helpers used by BOTH
rules_engine/ and sampling/ -- bulk writes with duplicate-key tolerance
and {key} run_params token substitution. No live DB connection required --
DuckDB stands in for the metadata store.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime

import duckdb
import pytest

from shared.db_ops import (
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
    still produce different run_ids, via the random suffix -- this is
    exactly the scenario that used to require tests/test_sampling.py to
    sleep(1.1) between two run_sampling() calls just to dodge a collision.
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


# ── count_prior_attempts ─────────────────────────────────────────────────

def _audit_conn():
    """
    gre_rule_audit / gre_sampling_audit -- the split of the old combined
    gre_audit table (see shared/schema.sql's module header): rule-engine
    runs and sampling runs each get their own table now, so
    count_prior_attempts() has one to query per branch (rule_group vs
    sample_config_id) instead of one shared table with a run_type filter.
    """
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE gre_rule_audit (
            run_id VARCHAR, rule_group VARCHAR, run_key VARCHAR, started_at TIMESTAMP
        )
    """)
    conn.execute("""
        CREATE TABLE gre_sampling_audit (
            run_id VARCHAR, sample_config_id INTEGER, run_key VARCHAR, started_at TIMESTAMP
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


def test_count_prior_attempts_keys_sampling_runs_by_config_id():
    conn = _audit_conn()
    execute_dml(conn, "INSERT INTO gre_sampling_audit (run_id, sample_config_id, run_key) VALUES (?, ?, ?)",
               ["r1", 7, "2026-08-14"])
    assert count_prior_attempts(conn, "main", "2026-08-14", sample_config_id=7) == 1
    assert count_prior_attempts(conn, "main", "2026-08-14", sample_config_id=8) == 0


def test_count_prior_attempts_requires_exactly_one_of_rule_group_or_config_id():
    conn = _audit_conn()
    with pytest.raises(ValueError):
        count_prior_attempts(conn, "main", "BATCH_2026_08_19")
    with pytest.raises(ValueError):
        count_prior_attempts(conn, "main", "BATCH_2026_08_19", rule_group="claims_dq", sample_config_id=7)


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


# ── gre_audit compatibility VIEW (shared/schema.sql) ─────────────────────
#
# Not exercised by any application code (rules_engine/ and sampling/ both
# read/write gre_rule_audit/gre_sampling_audit directly now -- see
# shared/schema.sql's module header) -- this is purely a regression check
# on the migration itself: anything still pointed at gre_audit must keep
# seeing the exact same combined shape it saw before the split.

def _split_audit_conn_with_view():
    """
    Same two tables shared/schema.sql defines, plus the gre_audit VIEW
    that UNIONs them -- built directly in DuckDB (which supports CREATE
    VIEW/UNION ALL/CAST the same way) so this is a genuine executable
    check of the view definition, not just a read of the .sql file.
    """
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE gre_rule_audit (
            run_id VARCHAR, rule_group VARCHAR, project_name VARCHAR, process_name VARCHAR,
            run_key VARCHAR, rule_variant VARCHAR, started_at TIMESTAMP, ended_at TIMESTAMP,
            status VARCHAR, total_rules INTEGER, rules_succeeded INTEGER, rules_errored INTEGER,
            triggered_by VARCHAR, load_datetime TIMESTAMP DEFAULT current_timestamp
        )
    """)
    conn.execute("""
        CREATE TABLE gre_sampling_audit (
            run_id VARCHAR, run_key VARCHAR, sample_config_id INTEGER, sampling_method VARCHAR,
            random_seed BIGINT, target_volume INTEGER, total_candidates INTEGER,
            total_selected INTEGER, started_at TIMESTAMP, ended_at TIMESTAMP, status VARCHAR,
            triggered_by VARCHAR, load_datetime TIMESTAMP DEFAULT current_timestamp
        )
    """)
    conn.execute("""
        CREATE VIEW gre_audit AS
        SELECT
            run_id, 'RULE_GROUP' AS run_type,
            rule_group, project_name, process_name, run_key, rule_variant,
            started_at, ended_at, status,
            total_rules, rules_succeeded, rules_errored,
            CAST(NULL AS INTEGER)      AS sample_config_id,
            CAST(NULL AS VARCHAR)      AS sampling_method,
            CAST(NULL AS BIGINT)       AS random_seed,
            CAST(NULL AS INTEGER)      AS target_volume,
            CAST(NULL AS INTEGER)      AS total_candidates,
            CAST(NULL AS INTEGER)      AS total_selected,
            triggered_by, load_datetime
        FROM gre_rule_audit
        UNION ALL
        SELECT
            run_id, 'SAMPLING' AS run_type,
            CAST(NULL AS VARCHAR) AS rule_group,
            CAST(NULL AS VARCHAR) AS project_name,
            CAST(NULL AS VARCHAR) AS process_name,
            run_key,
            CAST(NULL AS VARCHAR) AS rule_variant,
            started_at, ended_at, status,
            CAST(NULL AS INTEGER) AS total_rules,
            CAST(NULL AS INTEGER) AS rules_succeeded,
            CAST(NULL AS INTEGER) AS rules_errored,
            sample_config_id, sampling_method, random_seed,
            target_volume, total_candidates, total_selected,
            triggered_by, load_datetime
        FROM gre_sampling_audit
    """)
    return conn


def test_gre_audit_view_reproduces_old_combined_shape():
    conn = _split_audit_conn_with_view()
    execute_dml(
        conn,
        """
        INSERT INTO gre_rule_audit (
            run_id, rule_group, project_name, run_key, status,
            total_rules, rules_succeeded, rules_errored, triggered_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ["RID1", "claims_dq", "HEALTHSPRING_UM", "BATCH_2026_08_19", "COMPLETED", 3, 3, 0, "jsmith"],
    )
    execute_dml(
        conn,
        """
        INSERT INTO gre_sampling_audit (
            run_id, run_key, sample_config_id, sampling_method,
            target_volume, total_candidates, total_selected, status, triggered_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ["SID1", "2026-08-01", 1, "RANKED", 150, 900, 150, "COMPLETED", "SYSTEM"],
    )

    rows = {r["run_id"]: r for r in execute_query(conn, "SELECT * FROM gre_audit ORDER BY run_id")}
    assert set(rows.keys()) == {"RID1", "SID1"}

    rule_row = rows["RID1"]
    assert rule_row["run_type"] == "RULE_GROUP"
    assert rule_row["rule_group"] == "claims_dq"
    assert rule_row["total_rules"] == 3
    # Sampling-only columns are NULL on a rule-group row -- the exact old
    # combined-table behavior this view is preserving.
    assert rule_row["sample_config_id"] is None
    assert rule_row["sampling_method"] is None

    sample_row = rows["SID1"]
    assert sample_row["run_type"] == "SAMPLING"
    assert sample_row["sample_config_id"] == 1
    assert sample_row["sampling_method"] == "RANKED"
    # Rule-only columns are NULL on a sampling row.
    assert sample_row["rule_group"] is None
    assert sample_row["total_rules"] is None

    # Same 20-column shape on both sides of the UNION -- a caller doing
    # "SELECT * FROM gre_audit" never sees a shape difference by run_type.
    assert set(rule_row.keys()) == set(sample_row.keys())
