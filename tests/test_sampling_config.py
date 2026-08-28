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


# ── configure_logging(): one uniquely-named log file per RUN, not a shared
# daily-rotated file ──
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


def test_configure_logging_creates_a_plain_file_handler_not_rotating(tmp_path, monkeypatch, reload_config, isolated_logging):
    monkeypatch.setenv("GRE_LOG_DIR", str(tmp_path))
    monkeypatch.delenv("GRE_LOG_FILE", raising=False)
    reload_config()

    logging.getLogger().handlers = []
    shared_config.configure_logging()

    root = logging.getLogger()
    assert len(root.handlers) == 1
    handler = root.handlers[0]
    assert isinstance(handler, logging.FileHandler)
    assert not isinstance(handler, __import__("logging.handlers", fromlist=["TimedRotatingFileHandler"]).TimedRotatingFileHandler)
    created = list(tmp_path.glob("sampling_*.log"))
    assert len(created) == 1


def test_configure_logging_two_calls_in_the_same_process_get_two_distinct_files(tmp_path, monkeypatch, reload_config, isolated_logging):
    # The core of this change: every configure_logging() call gets its OWN
    # log file -- never shared with, or appended to by, a different
    # invocation. Simulated here as two separate "runs" against the same
    # log dir (root.handlers cleared between them, same as two separate
    # process invocations would each see an empty root logger).
    monkeypatch.setenv("GRE_LOG_DIR", str(tmp_path))
    reload_config()

    logging.getLogger().handlers = []
    shared_config.configure_logging()
    first_files = set(tmp_path.glob("sampling_*.log"))
    for h in logging.getLogger().handlers:
        h.close()

    logging.getLogger().handlers = []
    shared_config.configure_logging()
    second_files = set(tmp_path.glob("sampling_*.log"))

    assert len(first_files) == 1
    assert len(second_files) == 2   # first file still present, plus the new one
    assert first_files < second_files   # first file untouched, not reused/renamed


def test_configure_logging_base_name_from_env_strips_dot_log_suffix(tmp_path, monkeypatch, reload_config, isolated_logging):
    # An old-style GRE_LOG_FILE=sampling.log value (the exact literal
    # filename under the previous shared-file design) must still produce
    # a clean "sampling_<timestamp>_<pid>.log" name, not
    # "sampling.log_<timestamp>_<pid>.log".
    monkeypatch.setenv("GRE_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("GRE_LOG_FILE", "sampling.log")
    reload_config()

    logging.getLogger().handlers = []
    shared_config.configure_logging()

    created = list(tmp_path.glob("sampling_*.log"))
    assert len(created) == 1
    assert ".log_" not in created[0].name


def test_configure_logging_custom_base_name_used_as_prefix(tmp_path, monkeypatch, reload_config, isolated_logging):
    monkeypatch.setenv("GRE_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("GRE_LOG_FILE", "my_custom_run")
    reload_config()

    logging.getLogger().handlers = []
    shared_config.configure_logging()

    assert list(tmp_path.glob("my_custom_run_*.log"))
    assert not list(tmp_path.glob("sampling_*.log"))


def test_build_run_log_path_embeds_timestamp_and_pid(tmp_path):
    path = shared_config._build_run_log_path(tmp_path, "sampling")
    assert path.parent == tmp_path
    assert path.name.startswith("sampling_")
    assert path.name.endswith(".log")
    # "<base>_<8-digit-date>_<6-digit-time>_<6-digit-micros>_<pid>.log"
    stem = path.stem[len("sampling_"):]
    parts = stem.split("_")
    assert len(parts) == 4
    date_part, time_part, micros_part, pid_part = parts
    assert len(date_part) == 8 and date_part.isdigit()
    assert len(time_part) == 6 and time_part.isdigit()
    assert len(micros_part) == 6 and micros_part.isdigit()
    assert pid_part.isdigit()


def test_base_log_name_default_when_env_unset(monkeypatch):
    monkeypatch.delenv("GRE_LOG_FILE", raising=False)
    assert shared_config._base_log_name("sampling") == "sampling"


def test_base_log_name_strips_dot_log_suffix(monkeypatch):
    monkeypatch.setenv("GRE_LOG_FILE", "sampling.log")
    assert shared_config._base_log_name("sampling") == "sampling"


def test_base_log_name_strips_dot_log_case_insensitively(monkeypatch):
    monkeypatch.setenv("GRE_LOG_FILE", "MyLog.LOG")
    assert shared_config._base_log_name("sampling") == "MyLog"


def test_base_log_name_no_suffix_passed_through_unchanged(monkeypatch):
    monkeypatch.setenv("GRE_LOG_FILE", "custom_prefix")
    assert shared_config._base_log_name("sampling") == "custom_prefix"


def test_prune_old_run_logs_deletes_beyond_retention_keeps_within(tmp_path):
    from datetime import datetime, timedelta

    old_stamp = (datetime.now() - timedelta(days=45)).strftime("%Y%m%d_%H%M%S_%f")
    recent_stamp = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d_%H%M%S_%f")
    old_file = tmp_path / f"sampling_{old_stamp}_1234.log"
    recent_file = tmp_path / f"sampling_{recent_stamp}_5678.log"
    old_file.write_text("old run")
    recent_file.write_text("recent run")

    shared_config._prune_old_run_logs(tmp_path, "sampling", retention_days=30)

    assert not old_file.exists()
    assert recent_file.exists()


def test_prune_old_run_logs_zero_retention_keeps_everything(tmp_path):
    from datetime import datetime, timedelta

    old_stamp = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d_%H%M%S_%f")
    old_file = tmp_path / f"sampling_{old_stamp}_1234.log"
    old_file.write_text("ancient run")

    shared_config._prune_old_run_logs(tmp_path, "sampling", retention_days=0)

    assert old_file.exists()


def test_prune_old_run_logs_ignores_files_not_matching_our_pattern(tmp_path):
    unrelated = tmp_path / "sampling_notes.txt"
    unrelated.write_text("not a log file at all")
    also_unrelated = tmp_path / "sampling_.log"   # empty stem, not one of ours
    also_unrelated.write_text("")

    shared_config._prune_old_run_logs(tmp_path, "sampling", retention_days=1)

    assert unrelated.exists()
    assert also_unrelated.exists()


def test_configure_logging_prunes_old_run_logs_before_creating_new_one(tmp_path, monkeypatch, reload_config, isolated_logging):
    from datetime import datetime, timedelta

    monkeypatch.setenv("GRE_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("GRE_LOG_RETENTION_DAYS", "30")
    reload_config()

    old_stamp = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d_%H%M%S_%f")
    old_file = tmp_path / f"sampling_{old_stamp}_9999.log"
    old_file.write_text("a run from 90 days ago")

    logging.getLogger().handlers = []
    shared_config.configure_logging()

    assert not old_file.exists()
    assert list(tmp_path.glob("sampling_*.log"))   # this run's own fresh file


def test_configure_logging_retention_zero_means_keep_forever_end_to_end(tmp_path, monkeypatch, reload_config, isolated_logging):
    from datetime import datetime, timedelta

    monkeypatch.setenv("GRE_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("GRE_LOG_RETENTION_DAYS", "0")
    reload_config()

    old_stamp = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d_%H%M%S_%f")
    old_file = tmp_path / f"sampling_{old_stamp}_9999.log"
    old_file.write_text("a very old run")

    logging.getLogger().handlers = []
    shared_config.configure_logging()

    assert old_file.exists()


def test_configure_logging_malformed_retention_falls_back_to_default(tmp_path, monkeypatch, reload_config, isolated_logging):
    from datetime import datetime, timedelta

    monkeypatch.setenv("GRE_LOG_DIR", str(tmp_path))
    monkeypatch.setenv("GRE_LOG_RETENTION_DAYS", "not-a-number")
    reload_config()

    old_stamp = (datetime.now() - timedelta(days=45)).strftime("%Y%m%d_%H%M%S_%f")
    old_file = tmp_path / f"sampling_{old_stamp}_9999.log"
    old_file.write_text("older than the 30-day default fallback")

    logging.getLogger().handlers = []
    shared_config.configure_logging()

    # A malformed GRE_LOG_RETENTION_DAYS falls back to the 30-day default,
    # not "unlimited" -- a 45-day-old file still gets pruned.
    assert not old_file.exists()


def test_configure_logging_writes_a_log_line_naming_this_runs_file(tmp_path, monkeypatch, reload_config, isolated_logging):
    monkeypatch.setenv("GRE_LOG_DIR", str(tmp_path))
    reload_config()

    logging.getLogger().handlers = []
    shared_config.configure_logging()
    for h in logging.getLogger().handlers:
        h.flush()

    created = list(tmp_path.glob("sampling_*.log"))
    assert len(created) == 1
    content = created[0].read_text()
    assert "one file per run" in content
    assert str(created[0]) in content or created[0].name in content
