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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict

logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
    _DOTENV_AVAILABLE = True
except ImportError:
    _DOTENV_AVAILABLE = False


def _base_log_name(default: str) -> str:
    """
    Resolve GRE_LOG_FILE into a base FILENAME PREFIX, not a literal
    filename. Historically GRE_LOG_FILE named the exact shared file
    (e.g. "rules_engine.log") that every run appended into; now every
    run gets its OWN uniquely-timestamped file (see configure_logging()'s
    docstring for why), so a literal ".log" suffix in an old-style
    GRE_LOG_FILE value would otherwise end up embedded mid-filename
    (e.g. "rules_engine.log_20260828_...log") -- strip it if present so
    an existing GRE_LOG_FILE=rules_engine.log setting still produces a
    clean "rules_engine_20260828_....log" name instead of a confusing
    double extension.
    """
    name = os.getenv("GRE_LOG_FILE", default)
    if name.lower().endswith(".log"):
        name = name[: -len(".log")]
    return name or default


def _build_run_log_path(log_dir: Path, base_name: str) -> Path:
    """
    A fresh, unique log file PATH for this process invocation --
    "<base_name>_<YYYYMMDD_HHMMSS_ffffff>_<pid>.log". Every call to
    configure_logging() (i.e. every run of this normally short-lived,
    invoked-fresh-per-run CLI package) gets its OWN file, never shared
    with or appended to by any other invocation -- see
    configure_logging()'s docstring for why this replaced the old
    shared-file-with-daily-rotation design.

    Microsecond precision alone makes a same-timestamp collision between
    two independently-started processes astronomically unlikely; the pid
    suffix is added anyway as a cheap, unconditional guarantee -- two
    OS processes can never share both a pid and a microsecond-precision
    start time.
    """
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    return log_dir / f"{base_name}_{stamp}_{os.getpid()}.log"


def _prune_old_run_logs(log_dir: Path, base_name: str, retention_days: int) -> None:
    """
    Delete this package's own past per-run log files
    ("<base_name>_<timestamp>_<pid>.log") older than retention_days,
    based on each file's embedded timestamp (not filesystem mtime, which
    an antivirus scan, backup tool, or file copy can silently bump) --
    run once at the start of every configure_logging() call, since there
    is no longer a single handler whose own rollover could ever perform
    this cleanup (see configure_logging()'s docstring). retention_days=0
    means "keep forever" -- no automatic deletion at all, same meaning
    as under the old rotation scheme. Never raises: a file that can't be
    parsed (not one of ours) is left alone; one that can't be deleted
    (permissions, in use) is left in place rather than failing logging
    setup over disk cleanup.
    """
    if not retention_days:
        return
    cutoff = datetime.now() - timedelta(days=retention_days)
    prefix = f"{base_name}_"
    for candidate in log_dir.glob(f"{prefix}*.log"):
        stem = candidate.name[len(prefix): -len(".log")]
        # stem is "<YYYYMMDD_HHMMSS_ffffff>_<pid>" -- take just the
        # timestamp portion (first three underscore-separated parts).
        parts = stem.split("_")
        if len(parts) < 3:
            continue   # not one of our own timestamped run logs -- leave it alone
        try:
            file_time = datetime.strptime("_".join(parts[:3]), "%Y%m%d_%H%M%S_%f")
        except ValueError:
            continue
        if file_time < cutoff:
            try:
                candidate.unlink()
            except OSError:
                pass


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
    attempt -- both aimed squarely at catching a run pointed at the wrong
    environment, which otherwise surfaces as a confusing "column not
    present" error against a schema that looks identical by name.

    Parameters
    ----------
    level : explicit level (e.g. logging.DEBUG, or the string "DEBUG") --
            defaults to the GRE_LOG_LEVEL env var if set, else DEBUG --
            detailed logging is the default so nothing extra has to be passed
            to see it; set GRE_LOG_LEVEL=INFO (or pass level="INFO") to quiet
            it down to just the connection/run-starting lines.

    Writes to a FILE, not the console -- GRE_LOG_DIR (default "logs" at
    the repo root, created if missing). **Every run (every call to
    configure_logging() -- this package is normally invoked fresh per
    run, a short-lived CLI process, not a long-running daemon) gets its
    OWN, uniquely-named log file** -- "<base>_<YYYYMMDD_HHMMSS_ffffff>_<pid>.log",
    where <base> is GRE_LOG_FILE (default "sampling", a ".log" suffix in an
    old-style GRE_LOG_FILE value is stripped automatically -- see
    _base_log_name()). This replaced an earlier shared-file-with-
    daily-rotation design: multiple runs on the same day used to
    interleave into one "sampling.log" file, which meant grepping
    one run's activity out of a busy day meant filtering by timestamp or
    run_id after the fact. Now every invocation's log is already its own
    file from the start -- nothing to filter, nothing shared, nothing
    ever appended to by a different process. GRE_LOG_RETENTION_DAYS
    (default 30) still caps how many PAST DAYS of these per-run files are
    kept -- _prune_old_run_logs() deletes any whose own embedded
    timestamp is older than the cutoff, every time configure_logging()
    runs -- set it to "0" to keep every run's log forever (no automatic
    deletion at all), so a long-running or frequently-scheduled process
    still can't silently fill the disk without a deliberate opt-in to
    unlimited retention.

    This sets the level on sampling's own logger namespace plus
    db.connection_factory's (the one intentionally-shared dependency --
    see README.md's "Package separation"), and calls
    logging.basicConfig() ONLY if the root logger has no handlers yet, so
    it won't clobber a caller's existing logging setup (file, console, or
    otherwise) if one is already in place -- including a caller that has
    already called rules_engine.config.configure_logging() first in the same
    process, so the two packages' calls compose rather than one silently
    overriding the other's handler.
    """
    resolved = level or os.getenv("GRE_LOG_LEVEL", "DEBUG")
    if not logging.getLogger().handlers:
        log_dir = Path(os.getenv("GRE_LOG_DIR") or (Path(__file__).resolve().parent.parent / "logs"))
        log_dir.mkdir(parents=True, exist_ok=True)
        base_name = _base_log_name("sampling")
        try:
            retention_days = int(os.getenv("GRE_LOG_RETENTION_DAYS", "30"))
        except ValueError:
            retention_days = 30
        # Clean up past runs' log files BEFORE creating this run's own --
        # no handler-driven rollover exists anymore to do this from
        # inside doRollover() (there's no shared file being rolled over
        # at all now), so it's done explicitly, once, at the top of every
        # configure_logging() call. See _prune_old_run_logs()'s docstring.
        _prune_old_run_logs(log_dir, base_name, retention_days)
        log_path = _build_run_log_path(log_dir, base_name)
        handler = logging.FileHandler(log_path, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
        logging.basicConfig(level=resolved, handlers=[handler])
        logging.getLogger(__name__).info(
            "sampling logging to %s (level=%s, one file per run, retention=%s).",
            log_path, resolved, f"{retention_days} day(s)" if retention_days else "unlimited",
        )


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
