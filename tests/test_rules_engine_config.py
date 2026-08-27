"""
tests/test_rules_engine_config.py
--------------------------
Covers rules_engine/config.py: the local .env loader (_load_env_file) and the
small env-driven helpers around it. Runs entirely with monkeypatched env
vars / paths -- never touches a real Teradata connection or the repo's
own dev.env (which shouldn't exist in this environment anyway; the
whole point of _load_env_file() is that its absence is a silent no-op).

rules_engine/config.py is a full duplicate of sampling/config.py -- see that
package's own test_sampling_config.py for the identical coverage on
its copy. Packages share no code (see README.md's "Package separation").
"""

import importlib
import logging

import pytest

import rules_engine.config as shared_config


@pytest.fixture
def reload_config(monkeypatch):
    """
    Reload rules_engine.config after env/monkeypatch changes so module-level state
    (META_CONNECTION, META_DB, and the _load_env_file() call at import
    time) is recomputed under the patched environment. Restores the real
    module afterward so later tests in the same run see normal state.
    """
    def _reload():
        return importlib.reload(shared_config)

    yield _reload
    # Restore to a clean, unpatched reload so later tests aren't affected.
    importlib.reload(shared_config)


def test_load_env_file_noop_when_file_absent(tmp_path, monkeypatch, reload_config):
    """No dev.env present (the normal/test-environment case): silent no-op,
    module import doesn't raise, and unrelated env vars are untouched."""
    monkeypatch.delenv("SOME_TEST_ONLY_VAR", raising=False)
    monkeypatch.setenv("GRE_ENV_FILE", "definitely_does_not_exist.env")

    module = reload_config()

    assert module.get_meta_connection_name() == "teradata"
    assert "SOME_TEST_ONLY_VAR" not in __import__("os").environ


def test_load_env_file_loads_present_file(tmp_path, monkeypatch, reload_config):
    """When GRE_ENV_FILE points at a real file (resolved relative to the
    repo root, i.e. rules_engine/config.py's parent's parent), its KEY=VALUE lines
    land in os.environ."""
    import os

    repo_root = __import__("pathlib").Path(shared_config.__file__).resolve().parent.parent
    env_file = repo_root / "test_only_dev.env"
    env_file.write_text("GRE_TEST_LOADED_VAR=hello_from_dev_env\n")
    monkeypatch.setenv("GRE_ENV_FILE", "test_only_dev.env")

    try:
        reload_config()
        assert os.environ.get("GRE_TEST_LOADED_VAR") == "hello_from_dev_env"
    finally:
        env_file.unlink(missing_ok=True)
        os.environ.pop("GRE_TEST_LOADED_VAR", None)


def test_load_env_file_override_wins(tmp_path, monkeypatch, reload_config):
    """override=True means a value already set in the process env is
    replaced by the .env file's value -- matches the sibling project's
    load_dotenv(..., override=True) pattern this was modeled on."""
    import os

    repo_root = __import__("pathlib").Path(shared_config.__file__).resolve().parent.parent
    env_file = repo_root / "test_only_dev.env"
    env_file.write_text("GRE_TEST_OVERRIDE_VAR=from_file\n")
    monkeypatch.setenv("GRE_TEST_OVERRIDE_VAR", "from_process_env")
    monkeypatch.setenv("GRE_ENV_FILE", "test_only_dev.env")

    try:
        reload_config()
        assert os.environ.get("GRE_TEST_OVERRIDE_VAR") == "from_file"
    finally:
        env_file.unlink(missing_ok=True)
        os.environ.pop("GRE_TEST_OVERRIDE_VAR", None)


def test_load_env_file_missing_dotenv_package_is_noop(monkeypatch, reload_config, caplog):
    """If python-dotenv isn't installed, _load_env_file() must not raise --
    it's a debug-logged no-op, same as a missing file."""
    monkeypatch.setattr(shared_config, "_DOTENV_AVAILABLE", False)
    with caplog.at_level(logging.DEBUG, logger=shared_config.__name__):
        shared_config._load_env_file()  # should not raise
    assert "not installed" in caplog.text


def test_get_meta_connection_and_db_defaults(reload_config):
    module = reload_config()
    assert module.get_meta_connection_name() == "teradata"
    assert module.get_meta_db() == "CMSUNIV_FILELAND_DEV_T"


def test_get_meta_connection_and_db_env_overrides(monkeypatch, reload_config):
    monkeypatch.setenv("GRE_META_CONNECTION", "other_conn")
    monkeypatch.setenv("GRE_META_DB", "OTHER_DB")
    module = reload_config()
    assert module.get_meta_connection_name() == "other_conn"
    assert module.get_meta_db() == "OTHER_DB"


def test_get_max_parallel_rules_default_is_one():
    assert shared_config.get_max_parallel_rules() == 1


def test_get_max_parallel_rules_env_override(monkeypatch):
    monkeypatch.setenv("GRE_MAX_PARALLEL_RULES", "8")
    assert shared_config.get_max_parallel_rules() == 8


def test_get_max_parallel_rules_never_below_one(monkeypatch):
    # A misconfigured 0 or negative value must not disable the engine entirely.
    monkeypatch.setenv("GRE_MAX_PARALLEL_RULES", "0")
    assert shared_config.get_max_parallel_rules() == 1
    monkeypatch.setenv("GRE_MAX_PARALLEL_RULES", "-3")
    assert shared_config.get_max_parallel_rules() == 1


def test_get_max_parallel_for_connection_default_is_one():
    assert shared_config.get_max_parallel_for_connection("postgres") == 1


def test_get_max_parallel_for_connection_env_override(monkeypatch):
    monkeypatch.setenv("GRE_POSTGRES_MAX_PARALLEL", "3")
    assert shared_config.get_max_parallel_for_connection("postgres") == 3
    # Case-insensitive source_type, same as elsewhere.
    assert shared_config.get_max_parallel_for_connection("POSTGRES") == 3


def test_get_max_parallel_for_connection_is_independent_per_type(monkeypatch):
    monkeypatch.setenv("GRE_TERADATA_MAX_PARALLEL", "10")
    assert shared_config.get_max_parallel_for_connection("teradata") == 10
    assert shared_config.get_max_parallel_for_connection("s3") == 1


def test_check_run_ready_defaults_true_when_unregistered():
    assert shared_config.check_run_ready("some_unregistered_group", "run_1") is True


def test_check_run_ready_uses_registered_check():
    calls = []

    def fake_check(run_key, meta_conn):
        calls.append((run_key, meta_conn))
        return run_key == "ready_run"

    shared_config.register_readiness_check("my_group", fake_check)
    try:
        assert shared_config.check_run_ready("my_group", "ready_run") is True
        assert shared_config.check_run_ready("my_group", "other_run") is False
        assert calls == [("ready_run", None), ("other_run", None)]
    finally:
        shared_config._READINESS_CHECKS.pop("my_group", None)


# ── resolve_database_name(): $env/$ENV token + legacy GRE_DB_MAP_ override ──

def test_resolve_database_name_no_token_no_mapping_returns_unchanged(monkeypatch, reload_config):
    reload_config()
    assert shared_config.resolve_database_name("STATIC_LOOKUP_T") == "STATIC_LOOKUP_T"


def test_resolve_database_name_empty_input_returns_unchanged(reload_config):
    reload_config()
    assert shared_config.resolve_database_name("") is None or shared_config.resolve_database_name("") == ""
    assert shared_config.resolve_database_name(None) is None


@pytest.mark.parametrize("environment,expected", [
    ("DEV", "QNXT_core_dev_T"),
    ("QA", "QNXT_core_qa_T"),
    ("INT", "QNXT_core_int_T"),
    ("UAT", "QNXT_core_T"),     # no environment segment -- collapses cleanly
    ("PROD", "QNXT_core_T"),    # shares UAT's physical database
])
def test_resolve_database_name_lowercase_env_token(monkeypatch, reload_config, environment, expected):
    monkeypatch.setenv("GRE_ENVIRONMENT", environment)
    reload_config()
    assert shared_config.resolve_database_name("QNXT_core_$env_T") == expected


@pytest.mark.parametrize("environment,expected", [
    ("DEV", "CMSUNIV_FILELAND_DEV_T"),
    ("QA", "CMSUNIV_FILELAND_QA_T"),
    ("INT", "CMSUNIV_FILELAND_INT_T"),
    ("UAT", "CMSUNIV_FILELAND_T"),
    ("PROD", "CMSUNIV_FILELAND_T"),
])
def test_resolve_database_name_uppercase_env_token(monkeypatch, reload_config, environment, expected):
    monkeypatch.setenv("GRE_ENVIRONMENT", environment)
    reload_config()
    assert shared_config.resolve_database_name("CMSUNIV_FILELAND_$ENV_T") == expected


def test_resolve_database_name_env_value_override_wins_for_one_environment(monkeypatch, reload_config):
    # A source system whose UAT copy DOES carry a suffix, unlike every
    # other database that collapses UAT by default -- overridden without
    # touching any other environment's default.
    monkeypatch.setenv("GRE_ENVIRONMENT", "UAT")
    monkeypatch.setenv("GRE_ENV_VALUE_UAT", "uat")
    reload_config()
    assert shared_config.resolve_database_name("QNXT_core_$env_T") == "QNXT_core_uat_T"


def test_resolve_database_name_legacy_db_map_takes_priority_over_token(monkeypatch, reload_config):
    # A database that happens to contain "$env" in its authored name AND
    # has an explicit GRE_DB_MAP_ entry -- the legacy override wins.
    monkeypatch.setenv("GRE_ENVIRONMENT", "QA")
    monkeypatch.setenv("GRE_DB_MAP_LEGACYDB", "DEV=LEGACYDB_DEV,QA=LEGACYDB_QA")
    reload_config()
    assert shared_config.resolve_database_name("legacydb") == "LEGACYDB_QA"


def test_resolve_database_name_legacy_db_map_falls_through_to_token_when_env_missing(monkeypatch, reload_config, caplog):
    # GRE_DB_MAP_ is set but has no entry for the current environment --
    # falls through to $env/$ENV token substitution instead of returning
    # the authored name with an unresolved token still in it.
    monkeypatch.setenv("GRE_ENVIRONMENT", "PROD")
    monkeypatch.setenv("GRE_DB_MAP_QNXT_CORE_$ENV_T", "DEV=SOMETHING_ELSE")
    reload_config()
    with caplog.at_level(logging.WARNING):
        assert shared_config.resolve_database_name("QNXT_core_$env_T") == "QNXT_core_T"


def test_resolve_database_name_token_is_case_insensitive_and_case_preserving(monkeypatch, reload_config):
    # "$env" is matched regardless of how it's cased, and the substituted
    # value follows the SAME casing the author used for the token itself.
    monkeypatch.setenv("GRE_ENVIRONMENT", "DEV")
    reload_config()
    assert shared_config.resolve_database_name("QNXT_core_$env_T") == "QNXT_core_dev_T"    # all-lowercase token
    assert shared_config.resolve_database_name("QNXT_core_$ENV_T") == "QNXT_core_DEV_T"    # all-uppercase token
    assert shared_config.resolve_database_name("QNXT_core_$Env_T") == "QNXT_core_Dev_T"    # mixed -> Title case
    assert shared_config.resolve_database_name("QNXT_core_$enV_T") == "QNXT_core_Dev_T"    # mixed -> Title case


def test_resolve_database_name_mixed_case_token_still_collapses_for_uat_prod(monkeypatch, reload_config):
    monkeypatch.setenv("GRE_ENVIRONMENT", "UAT")
    reload_config()
    assert shared_config.resolve_database_name("QNXT_core_$Env_T") == "QNXT_core_T"


# ── resolve_env_tokens(): $env/$ENV substitution against arbitrary SQL text ──

def test_resolve_env_tokens_no_token_returns_unchanged(reload_config):
    reload_config()
    sql = "SELECT * FROM STATIC_LOOKUP_T.claims WHERE claim_id IS NOT NULL"
    assert shared_config.resolve_env_tokens(sql) == sql


def test_resolve_env_tokens_empty_input_returns_unchanged(reload_config):
    reload_config()
    assert shared_config.resolve_env_tokens("") == ""
    assert shared_config.resolve_env_tokens(None) is None


def test_resolve_env_tokens_multiple_database_names_in_one_query(monkeypatch, reload_config):
    # The exact case this exists for: one rule_syntax joining several
    # source databases, each carrying its own $env token, resolved in a
    # single pass -- not just the one database_name column.
    monkeypatch.setenv("GRE_ENVIRONMENT", "QA")
    reload_config()
    sql = (
        "SELECT c.claim_id FROM QNXT_core_$env_T.claims c "
        "JOIN QNXT_ref_$env_T.codes r ON c.code_id = r.code_id "
        "WHERE c.denial_reason IS NULL"
    )
    expected = (
        "SELECT c.claim_id FROM QNXT_core_qa_T.claims c "
        "JOIN QNXT_ref_qa_T.codes r ON c.code_id = r.code_id "
        "WHERE c.denial_reason IS NULL"
    )
    assert shared_config.resolve_env_tokens(sql) == expected


def test_resolve_env_tokens_case_insensitive_and_case_preserving(monkeypatch, reload_config):
    monkeypatch.setenv("GRE_ENVIRONMENT", "DEV")
    reload_config()
    assert shared_config.resolve_env_tokens("FROM QNXT_core_$env_T") == "FROM QNXT_core_dev_T"
    assert shared_config.resolve_env_tokens("FROM QNXT_core_$ENV_T") == "FROM QNXT_core_DEV_T"
    assert shared_config.resolve_env_tokens("FROM QNXT_core_$Env_T") == "FROM QNXT_core_Dev_T"


def test_resolve_env_tokens_collapses_double_underscore_for_uat_prod(monkeypatch, reload_config):
    monkeypatch.setenv("GRE_ENVIRONMENT", "UAT")
    reload_config()
    sql = "SELECT * FROM QNXT_core_$env_T.claims"
    assert shared_config.resolve_env_tokens(sql) == "SELECT * FROM QNXT_core_T.claims"


def test_resolve_env_tokens_ignores_legacy_db_map_override(monkeypatch, reload_config):
    # GRE_DB_MAP_ is keyed to one exact, whole authored database_name
    # value -- it has no meaning against free-form SQL text, so
    # resolve_env_tokens() only ever applies the $env/$ENV token
    # mechanism, never the legacy per-database override (unlike
    # resolve_database_name(), which checks GRE_DB_MAP_ first).
    monkeypatch.setenv("GRE_ENVIRONMENT", "QA")
    monkeypatch.setenv("GRE_DB_MAP_QNXT_CORE_$ENV_T", "QA=SHOULD_NOT_BE_USED")
    reload_config()
    sql = "SELECT * FROM QNXT_core_$env_T.claims"
    assert shared_config.resolve_env_tokens(sql) == "SELECT * FROM QNXT_core_qa_T.claims"


def test_resolve_env_tokens_does_not_disturb_run_params_style_tokens(reload_config):
    # $key/{key} run_params tokens (a totally separate substitution
    # mechanism -- see rules_engine/db_ops.py::_substitute_params()) are
    # left completely alone; only the literal $env/$ENV token is touched.
    reload_config()
    sql = "SELECT * FROM claims WHERE claim_year = {year} AND claim_month = $month"
    assert shared_config.resolve_env_tokens(sql) == sql


# ── configure_logging(): daily (not size-based) rotation, append-not-overwrite ──
#
# configure_logging() only installs its own handler when the root logger
# has none yet ("won't clobber a caller's existing setup"); pytest's own
# log-capturing plugin keeps at least one handler on the root logger for
# the whole test body regardless of what a fixture does around it, so
# these tests clear logging.getLogger().handlers INLINE, immediately
# before calling configure_logging(), rather than via a fixture -- that's
# the one place in the test body guaranteed to run with nothing else
# touching root.handlers in between.

@pytest.fixture
def isolated_logging():
    """Save/restore root logger handlers+level around a test that clears
    them itself (see note above) -- restores whatever was there before
    (pytest's own handler(s) included) once the test finishes, and closes
    whatever configure_logging() installed so no file handle leaks."""
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    yield
    for h in list(root.handlers):
        if h not in saved_handlers:
            h.close()
        root.removeHandler(h)
    for h in saved_handlers:
        root.addHandler(h)
    root.setLevel(saved_level)


def test_configure_logging_uses_timed_daily_rotation_not_size(tmp_path, monkeypatch, reload_config, isolated_logging):
    from logging.handlers import TimedRotatingFileHandler

    monkeypatch.setenv("GRE_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("GRE_LOG_FILE", "rules_engine.log")
    reload_config()

    logging.getLogger().handlers = []
    shared_config.configure_logging()

    root = logging.getLogger()
    assert len(root.handlers) == 1
    handler = root.handlers[0]
    assert isinstance(handler, TimedRotatingFileHandler)
    assert handler.when.upper() == "MIDNIGHT"
    assert handler.suffix == "%Y-%m-%d"
    assert (tmp_path / "rules_engine.log").exists()


def test_configure_logging_default_retention_is_30_days(tmp_path, monkeypatch, reload_config, isolated_logging):
    monkeypatch.setenv("GRE_LOG_DIR", str(tmp_path))
    monkeypatch.delenv("GRE_LOG_RETENTION_DAYS", raising=False)
    reload_config()

    logging.getLogger().handlers = []
    shared_config.configure_logging()

    assert logging.getLogger().handlers[0].backupCount == 30


def test_configure_logging_retention_env_override(tmp_path, monkeypatch, reload_config, isolated_logging):
    monkeypatch.setenv("GRE_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("GRE_LOG_RETENTION_DAYS", "90")
    reload_config()

    logging.getLogger().handlers = []
    shared_config.configure_logging()

    assert logging.getLogger().handlers[0].backupCount == 90


def test_configure_logging_retention_zero_means_keep_forever(tmp_path, monkeypatch, reload_config, isolated_logging):
    monkeypatch.setenv("GRE_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("GRE_LOG_RETENTION_DAYS", "0")
    reload_config()

    logging.getLogger().handlers = []
    shared_config.configure_logging()

    # backupCount=0 is TimedRotatingFileHandler's own "never delete old
    # rotated files" setting -- confirms "0" is passed through, not
    # silently coerced to some other default.
    assert logging.getLogger().handlers[0].backupCount == 0


def test_configure_logging_malformed_retention_falls_back_to_default(tmp_path, monkeypatch, reload_config, isolated_logging):
    monkeypatch.setenv("GRE_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("GRE_LOG_RETENTION_DAYS", "not-a-number")
    reload_config()

    logging.getLogger().handlers = []
    shared_config.configure_logging()

    assert logging.getLogger().handlers[0].backupCount == 30


def test_configure_logging_appends_across_calls_never_truncates(tmp_path, monkeypatch, reload_config, isolated_logging):
    # Restarting/reconfiguring should never wipe what's already in the log
    # file -- everything opens in append mode.
    monkeypatch.setenv("GRE_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("GRE_LOG_FILE", "rules_engine.log")
    reload_config()

    log_path = tmp_path / "rules_engine.log"
    log_path.write_text("PRE-EXISTING LINE FROM AN EARLIER RUN\n")

    logging.getLogger().handlers = []
    shared_config.configure_logging()
    logging.getLogger("rules_engine").info("fresh line from this run")
    for h in logging.getLogger().handlers:
        h.flush()

    content = log_path.read_text()
    assert "PRE-EXISTING LINE FROM AN EARLIER RUN" in content
    assert "fresh line from this run" in content
