"""
rules_engine/config.py
------------------------
Small, env-driven config surface for rules_engine/ -- credential loading
and metadata-store resolution. This used to live in shared/config.py,
shared verbatim with sampling/ (which has its own identical copy of this
file, sampling/config.py) -- the two packages are now fully independent,
so each keeps its own copy rather than importing a common shared/ module.
See README.md's "Package separation" for why.

Connector
----------
Connections are opened via db.connection_factory.ConnectionFactory,
imported directly. Callers (rules_engine/runner.py, tests) own a
ConnectionFactory instance and pass adapters into rules_engine/executor.py.

Metadata connection
--------------------
GRE_META_CONNECTION picks which source_type ('teradata', 'postgres', 's3',
'file' -- see db/connection_factory.py) holds the gre_ tables. Defaults to
"teradata". No separate connection setup is required out of the box.

Run-readiness precondition (deferred for v1)
-------------------------------------------------
The original prompt calls for a "don't evaluate rules until the source
data for this run is complete" precondition, expressed as config rather
than hardcoded per rule. For v1 this ships as a no-op extension point:
check_run_ready() always returns True unless a check has been registered
for that rule_group via register_readiness_check(). Wiring up a real
check (e.g. a status-table SELECT keyed off the run's run_key) is then a
one-line addition here, not an engine code change.

Local dev credentials (.env)
-------------------------------
For local development, credentials are loaded from a plain .env file via
python-dotenv -- the same mechanics used for local Teradata access
elsewhere (load_dotenv(dotenv_path=..., override=True), file resolved
relative to the repo root, silently skipped if absent).

Critically, this does NOT invent a new credential path: it loads the SAME
env var names db/connection_factory.py's adapters already read at connect
time (TERADATA_HOST/USER/PASSWORD/LOGMECH, POSTGRES_HOST/PORT/DATABASE/
USER/PASSWORD, S3_*, FILE_BASE_PATH). Once the .env file is loaded into
the process environment here, connection_factory.py sees already-populated
env vars and behaves exactly as it would in a real deployment. See
dev.env.example at the repo root for the exact variable names to fill in.

The file is entirely optional: absent (the normal case for a real
deployment, or for tests, which never open a real Teradata connection)
it's a silent no-op -- required-credential validation already happens
downstream, in each adapter's own _require() checks, so nothing here
duplicates that. GRE_ENV_FILE overrides the filename (default
"dev.env"). Pivoting to Secrets Manager later replaces ONLY
_load_env_file() below -- nothing else in this package needs to change,
since every other file only ever sees already-populated env vars either
way. sampling/config.py's identical _load_env_file() would need the same
edit made twice -- accepted cost of full separation over a shared module.
"""

import logging
import os
from pathlib import Path
from typing import Callable, Dict

logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
    _DOTENV_AVAILABLE = True
except ImportError:
    _DOTENV_AVAILABLE = False


def configure_logging(level=None) -> None:
    """
    Opt-in DEBUG/INFO logging setup for rules_engine/ -- NEVER called
    automatically at import time (a library configuring global logging
    state would clobber a host application's own setup). Call this
    explicitly from a standalone entrypoint (a CLI script, a debug REPL,
    an ad-hoc troubleshooting session) when you want to see this
    package's log output.

    What gets logged at DEBUG (see rules_engine/db_ops.py's module
    docstring for the full contract): SQL statement TEXT (one-lined,
    truncated) and row COUNTS only -- never fetched row data or bind
    parameter values. At INFO: which Teradata/Postgres host/db a
    connection actually points at (db/connection_factory.py), and a
    "run_rule_group starting" line per attempt (rules_engine/runner.py) --
    both aimed squarely at catching a run pointed at the wrong environment,
    which otherwise surfaces as a confusing "column not present" error
    against a schema that looks identical by name.

    Parameters
    ----------
    level : explicit level (e.g. logging.DEBUG, or the string "DEBUG") --
            defaults to the GRE_LOG_LEVEL env var if set, else DEBUG --
            detailed logging is the default so nothing extra has to be passed
            to see it; set GRE_LOG_LEVEL=INFO (or pass level="INFO") to quiet
            it down to just the connection/run-starting lines.

    This sets the level on rules_engine's own logger namespace plus
    db.connection_factory's (the one intentionally-shared dependency --
    see README.md's "Package separation"), and calls logging.basicConfig()
    ONLY if the root logger has no handlers yet, so it won't clobber a
    caller's existing logging setup if one is already in place.
    """
    resolved = level or os.getenv("GRE_LOG_LEVEL", "DEBUG")
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=resolved,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
    logging.getLogger("rules_engine").setLevel(resolved)
    logging.getLogger("db.connection_factory").setLevel(resolved)


def _load_env_file() -> None:
    """
    Load local dev credentials from a .env file into the process
    environment, if one is present. Never raises: a missing file (normal
    for a real deployment) or a missing python-dotenv install is a silent
    no-op, not an error -- see this module's docstring for why validation
    is deliberately left to db/connection_factory.py's own _require() calls instead
    of being duplicated here.
    """
    if not _DOTENV_AVAILABLE:
        logger.debug("python-dotenv not installed -- skipping local .env load.")
        return

    env_path = Path(__file__).resolve().parent.parent / os.getenv("GRE_ENV_FILE", "dev.env")
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
        logger.info("Loaded local credentials from %s.", env_path)
    else:
        logger.debug("No local env file at %s -- skipping (expected in a real deployment).", env_path)


_load_env_file()

# ── Metadata store (same schema/connection dq_* uses, by default) ─────────
META_CONNECTION = os.getenv("GRE_META_CONNECTION", "teradata")
META_DB = os.getenv("GRE_META_DB", "CMSUNIV_FILELAND_DEV_T")


def get_meta_connection_name() -> str:
    """Named connection (see db/connection_factory.py) holding the gre_ tables."""
    return META_CONNECTION


def get_meta_db() -> str:
    """Schema/database name the gre_ tables live in."""
    return META_DB


# ── Parallel rule execution (opt-in; see rules_engine/parallel.py) ────────
# runner.py's default is, and has always been, a single-threaded loop over
# rules_engine/executor.py::execute_rule() -- see run_rule_group()'s own
# docstring. These two settings are the ONLY way that changes, and only
# for sequencing_mode='independent' groups (a 'sequential' group's ordering
# and on_failure=halt_group semantics are incompatible with running rules
# out of order, so it never parallelizes regardless of these values).
#
# GRE_MAX_PARALLEL_RULES caps how many rules in one group may execute
# concurrently at all. GRE_<TYPE>_MAX_PARALLEL further caps how many of
# those concurrent rules may simultaneously hold an open session against
# one specific source_type (e.g. Postgres can't tolerate as much
# concurrent load as the Teradata warehouse) -- the two caps compose: a
# rule_group with GRE_MAX_PARALLEL_RULES=8 where 6 of its rules target
# Postgres, capped at GRE_POSTGRES_MAX_PARALLEL=2, still only ever has 2
# of those 6 running against Postgres at once, even though up to 8 rules
# total may be in flight across every source.
#
# Both default to 1 -- i.e. off, matching today's behavior exactly with no
# config changes required. A source_type needs an explicit opt-in
# (GRE_<TYPE>_MAX_PARALLEL > 1) before the engine will ever open more than
# one concurrent session against it, specifically so raising the group-wide
# cap alone can never silently increase load on a source that wasn't sized
# for it.
def get_max_parallel_rules() -> int:
    """
    GRE_MAX_PARALLEL_RULES -- max rules that may execute concurrently
    within one run_rule_group() call. Default 1 (no parallelism).
    """
    return max(1, int(os.getenv("GRE_MAX_PARALLEL_RULES", "1")))


def get_max_parallel_for_connection(source_type: str) -> int:
    """
    GRE_<TYPE>_MAX_PARALLEL -- how many concurrent sessions `source_type`
    ('teradata', 'postgres', 's3', 'file') may serve during a parallel run.
    Default 1 (never more than one concurrent session, even if
    GRE_MAX_PARALLEL_RULES is raised).
    """
    return max(1, int(os.getenv(f"GRE_{source_type.upper()}_MAX_PARALLEL", "1")))


# ── Run-readiness precondition (deferred; see module docstring) ───────────
_READINESS_CHECKS: Dict[str, Callable[[str, object], bool]] = {}


def register_readiness_check(rule_group: str, fn: Callable[[str, object], bool]) -> None:
    """
    Register a readiness check for a rule_group.

    fn receives (run_key, meta_conn) and returns True when that run is
    ready to be evaluated. Not called anywhere in v1 unless registered --
    see check_run_ready() below.
    """
    _READINESS_CHECKS[rule_group] = fn


def check_run_ready(rule_group: str, run_key: str, meta_conn=None) -> bool:
    """
    Return True when `rule_group` is clear to run against `run_key`.

    v1 default: always True (no-op) unless a check was registered for this
    rule_group via register_readiness_check(). This keeps the precondition
    a config decision, not an engine code change, once a real check is
    needed.
    """
    fn = _READINESS_CHECKS.get(rule_group)
    if fn is None:
        return True
    return bool(fn(run_key, meta_conn))
