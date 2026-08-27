"""
tests/test_sampling_config.py
--------------------------
Covers sampling/config.py: the local .env loader (_load_env_file) and the
small env-driven helpers around it. Runs entirely with monkeypatched env
vars / paths -- never touches a real Teradata connection or the repo's
own dev.env (which shouldn't exist in this environment anyway; the
whole point of _load_env_file() is that its absence is a silent no-op).

sampling/config.py is a full duplicate of rules_engine/config.py -- see that
package's own test_rules_engine_config.py for the identical coverage on
its copy. Packages share no code (see README.md's "Package separation").
"""

import importlib
import logging

import pytest

import sampling.config as shared_config


@pytest.fixture
def reload_config(monkeypatch):
    """
    Reload sampling.config after env/monkeypatch changes so module-level state
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
    repo root, i.e. sampling/config.py's parent's parent), its KEY=VALUE lines
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


# ── configure_logging(): daily (not size-based) rotation, append-not-overwrite ──
#
# Same rationale as rules_engine/config.py's identical test block: clear
# logging.getLogger().handlers INLINE, immediately before calling
# configure_logging(), rather than via fixture setup -- pytest's own
# log-capturing plugin keeps a handler on the root logger through the
# whole test body regardless of what a fixture does around it.

@pytest.fixture
def isolated_logging():
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
    monkeypatch.setenv("GRE_LOG_FILE", "sampling.log")
    reload_config()

    logging.getLogger().handlers = []
    shared_config.configure_logging()

    root = logging.getLogger()
    assert len(root.handlers) == 1
    handler = root.handlers[0]
    assert isinstance(handler, TimedRotatingFileHandler)
    assert handler.when.upper() == "MIDNIGHT"
    assert handler.suffix == "%Y-%m-%d"
    assert (tmp_path / "sampling.log").exists()


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

    assert logging.getLogger().handlers[0].backupCount == 0


def test_configure_logging_malformed_retention_falls_back_to_default(tmp_path, monkeypatch, reload_config, isolated_logging):
    monkeypatch.setenv("GRE_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("GRE_LOG_RETENTION_DAYS", "not-a-number")
    reload_config()

    logging.getLogger().handlers = []
    shared_config.configure_logging()

    assert logging.getLogger().handlers[0].backupCount == 30


def test_rotate_stale_log_at_startup_renames_yesterdays_file(tmp_path):
    """
    Direct unit test of the manual catch-up TimedRotatingFileHandler's own
    timer can never perform for a short-lived process (see the function's
    docstring): a log file last written YESTERDAY gets renamed with
    yesterday's date suffix, so a fresh process starting today doesn't
    just keep appending into the same undated file forever.
    """
    import os
    import time
    from datetime import datetime, timedelta

    log_path = tmp_path / "rules_engine.log"
    log_path.write_text("yesterday's activity\n")
    yesterday = datetime.now().date() - timedelta(days=1)
    stale_time = time.mktime(yesterday.timetuple())
    os.utime(log_path, (stale_time, stale_time))

    shared_config._rotate_stale_log_at_startup(log_path, retention_days=30)

    dated_path = tmp_path / f"rules_engine.log.{yesterday.isoformat()}"
    assert not log_path.exists()
    assert dated_path.exists()
    assert dated_path.read_text() == "yesterday's activity\n"


def test_rotate_stale_log_at_startup_leaves_todays_file_alone(tmp_path):
    log_path = tmp_path / "rules_engine.log"
    log_path.write_text("today's activity so far\n")   # mtime is "now" by default

    shared_config._rotate_stale_log_at_startup(log_path, retention_days=30)

    assert log_path.exists()
    assert log_path.read_text() == "today's activity so far\n"
    assert list(tmp_path.glob("rules_engine.log.*")) == []


def test_rotate_stale_log_at_startup_no_file_is_a_no_op(tmp_path):
    # First-ever run on a fresh machine/log dir -- nothing to rotate.
    shared_config._rotate_stale_log_at_startup(tmp_path / "rules_engine.log", retention_days=30)
    assert list(tmp_path.iterdir()) == []


def test_rotate_stale_log_at_startup_avoids_overwriting_existing_dated_file(tmp_path):
    import os
    import time
    from datetime import datetime, timedelta

    log_path = tmp_path / "rules_engine.log"
    log_path.write_text("second stale file from the same day\n")
    yesterday = datetime.now().date() - timedelta(days=1)
    stale_time = time.mktime(yesterday.timetuple())
    os.utime(log_path, (stale_time, stale_time))

    already_there = tmp_path / f"rules_engine.log.{yesterday.isoformat()}"
    already_there.write_text("first dated file from that same day\n")

    shared_config._rotate_stale_log_at_startup(log_path, retention_days=30)

    # Original dated file untouched, new content landed under a ".2" suffix
    # instead of clobbering it.
    assert already_there.read_text() == "first dated file from that same day\n"
    collision_path = tmp_path / f"rules_engine.log.{yesterday.isoformat()}.2"
    assert collision_path.exists()
    assert collision_path.read_text() == "second stale file from the same day\n"


def test_prune_old_dated_logs_deletes_beyond_retention_keeps_within(tmp_path):
    from datetime import datetime, timedelta

    log_path = tmp_path / "rules_engine.log"
    old_date = (datetime.now().date() - timedelta(days=45)).isoformat()
    recent_date = (datetime.now().date() - timedelta(days=5)).isoformat()
    old_file = tmp_path / f"rules_engine.log.{old_date}"
    recent_file = tmp_path / f"rules_engine.log.{recent_date}"
    old_file.write_text("old")
    recent_file.write_text("recent")

    shared_config._prune_old_dated_logs(log_path, retention_days=30)

    assert not old_file.exists()
    assert recent_file.exists()


def test_configure_logging_rotates_a_stale_log_before_attaching_handler(tmp_path, monkeypatch, reload_config, isolated_logging):
    # End-to-end: configure_logging() itself (not just the helper directly)
    # performs the startup catch-up rotation before opening today's file.
    import os
    import time
    from datetime import datetime, timedelta

    monkeypatch.setenv("GRE_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("GRE_LOG_FILE", "rules_engine.log")
    reload_config()

    log_path = tmp_path / "rules_engine.log"
    log_path.write_text("stale content from a prior day's process\n")
    yesterday = datetime.now().date() - timedelta(days=1)
    stale_time = time.mktime(yesterday.timetuple())
    os.utime(log_path, (stale_time, stale_time))

    logging.getLogger().handlers = []
    shared_config.configure_logging()

    dated_path = tmp_path / f"rules_engine.log.{yesterday.isoformat()}"
    assert dated_path.exists()
    assert dated_path.read_text() == "stale content from a prior day's process\n"
    # Today's log_path is fresh (handler re-created it in append mode after
    # the rename moved the stale content out of the way).
    assert log_path.exists()
    assert "stale content" not in log_path.read_text()


def test_configure_logging_appends_across_calls_never_truncates(tmp_path, monkeypatch, reload_config, isolated_logging):
    monkeypatch.setenv("GRE_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("GRE_LOG_FILE", "sampling.log")
    reload_config()

    log_path = tmp_path / "sampling.log"
    log_path.write_text("PRE-EXISTING LINE FROM AN EARLIER RUN\n")

    logging.getLogger().handlers = []
    shared_config.configure_logging()
    logging.getLogger("sampling").info("fresh line from this run")
    for h in logging.getLogger().handlers:
        h.flush()

    content = log_path.read_text()
    assert "PRE-EXISTING LINE FROM AN EARLIER RUN" in content
    assert "fresh line from this run" in content
