"""
sampling/config.py
--------------------
Small, env-driven config surface for sampling/ -- credential loading and
metadata-store resolution. This used to live in shared/config.py, shared
verbatim with rules_engine/ (which has its own identical copy of this
file, rules_engine/config.py) -- the two packages are now fully
independent, so each keeps its own copy rather than importing a common
shared/ module. See README.md's "Package separation" for why.

Connector
----------
Connections are opened via db.connection_factory.ConnectionFactory,
imported directly. Callers (sampling/sampling.py, tests) own a
ConnectionFactory instance and pass adapters into sampling/sampling.py.

Metadata connection
--------------------
GRE_META_CONNECTION picks which source_type ('teradata', 'postgres', 's3',
'file' -- see db/connection_factory.py) holds the gre_ tables. Defaults to
"teradata". No separate connection setup is required out of the box.

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
way. rules_engine/config.py's identical _load_env_file() would need the
same edit made twice -- accepted cost of full separation over a shared
module.

get_max_parallel_rules()/get_max_parallel_for_connection() below are kept
for parity with rules_engine/config.py (both packages' config.py started
as the same shared/config.py file) but are currently UNUSED here --
sampling/sampling.py runs one sampling config at a time, single-threaded;
there is no sampling/parallel.py. Harmless to leave in place should
concurrent sampling runs ever be added.
"""

import logging
import os
from logging.handlers import TimedRotatingFileHandler
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
    Opt-in DEBUG/INFO logging setup for sampling/ -- NEVER called
    automatically at import time (a library configuring global logging
    state would clobber a host application's own setup). Call this
    explicitly from a standalone entrypoint (a CLI script, a debug REPL,
    an ad-hoc troubleshooting session) when you want to see this
    package's log output.

    What gets logged at DEBUG (see sampling/db_ops.py's module docstring
    for the full contract): SQL statement TEXT (one-lined, truncated) and
    row COUNTS only -- never fetched row data or bind parameter values.
    At INFO: which Teradata/Postgres host/db a connection actually points
    at (db/connection_factory.py), and a "run_sampling starting" line per
    attempt (sampling/sampling.py) -- both aimed squarely at catching a
    run pointed at the wrong environment, which otherwise surfaces as a
    confusing "column not present" error against a schema that looks
    identical by name.

    Parameters
    ----------
    level : explicit level (e.g. logging.DEBUG, or the string "DEBUG") --
            defaults to the GRE_LOG_LEVEL env var if set, else DEBUG --
            detailed logging is the default so nothing extra has to be passed
            to see it; set GRE_LOG_LEVEL=INFO (or pass level="INFO") to quiet
            it down to just the connection/run-starting lines.

    Writes to a FILE, not the console -- GRE_LOG_DIR (default "logs" at
    the repo root, created if missing) / GRE_LOG_FILE (default
    "sampling.log"). Never overwrites on a fresh run -- every handler
    below opens in APPEND mode, so restarting the CLI/process only adds
    new lines to whatever's already there; nothing is ever truncated or
    replaced just because a new run started.

    Rotates at MIDNIGHT (local time), not by file size -- one file per
    CALENDAR DAY. Today's activity always lives at the same unchanging
    path (GRE_LOG_FILE itself, e.g. "sampling.log"), so a caller tailing
    the log doesn't need to know today's date; at midnight the
    just-finished day's file is renamed with a date suffix (e.g.
    "sampling.log.2026-08-27") and a fresh "sampling.log" starts for the
    new day, so a busy day's history is never partially overwritten or
    cut off mid-day the way a fixed SIZE-based rotation could (the old
    10MB/5-backup scheme could drop same-day history the moment a heavy
    run pushed past the 5th backup). GRE_LOG_RETENTION_DAYS (default 30)
    caps how many PAST days are kept before the oldest dated file is
    deleted -- set it to "0" to keep every day's log forever (no
    automatic deletion at all) -- so a long-running or
    frequently-scheduled process still can't silently fill the disk
    without a deliberate opt-in to unlimited retention.

    This sets the level on sampling's own logger namespace plus
    db.connection_factory's (the one intentionally-shared dependency --
    see README.md's "Package separation"), and calls
    logging.basicConfig() ONLY if the root logger has no handlers yet, so
    it won't clobber a caller's existing logging setup (file, console, or
    otherwise) if one is already in place -- including a caller that has
    already called rules_engine.config.configure_logging() first in the
    same process, so the two packages' calls compose rather than one
    silently overriding the other's handler.
    """
    resolved = level or os.getenv("GRE_LOG_LEVEL", "DEBUG")
    if not logging.getLogger().handlers:
        log_dir = Path(os.getenv("GRE_LOG_DIR") or (Path(__file__).resolve().parent.parent / "logs"))
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / os.getenv("GRE_LOG_FILE", "sampling.log")
        try:
            retention_days = int(os.getenv("GRE_LOG_RETENTION_DAYS", "30"))
        except ValueError:
            retention_days = 30
        handler = TimedRotatingFileHandler(
            log_path, when="midnight", backupCount=retention_days, encoding="utf-8",
        )
        handler.suffix = "%Y-%m-%d"
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        logging.basicConfig(level=resolved, handlers=[handler])
        logging.getLogger(__name__).info(
            "sampling logging to %s (level=%s, rotating daily at midnight, retention=%s).",
            log_path, resolved, f"{retention_days} day(s)" if retention_days else "unlimited",
        )
    logging.getLogger("sampling").setLevel(resolved)
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


# ── Parallel execution settings -- see module docstring: currently unused
# here, kept only for parity with rules_engine/config.py. ─────────────────
def get_max_parallel_rules() -> int:
    return max(1, int(os.getenv("GRE_MAX_PARALLEL_RULES", "1")))


def get_max_parallel_for_connection(source_type: str) -> int:
    return max(1, int(os.getenv(f"GRE_{source_type.upper()}_MAX_PARALLEL", "1")))


# ── Run-readiness precondition (parity with rules_engine/config.py; not
# currently called anywhere in sampling/ -- sampling runs on demand, with
# no equivalent readiness gate). ───────────────────────────────────────────
_READINESS_CHECKS: Dict[str, Callable[[str, object], bool]] = {}


def register_readiness_check(rule_group: str, fn: Callable[[str, object], bool]) -> None:
    _READINESS_CHECKS[rule_group] = fn


def check_run_ready(rule_group: str, run_key: str, meta_conn=None) -> bool:
    fn = _READINESS_CHECKS.get(rule_group)
    if fn is None:
        return True
    return bool(fn(run_key, meta_conn))
