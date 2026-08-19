"""
shared/config.py
------------------
Small, env-driven config surface used by BOTH rules_engine/ and sampling/
-- credential loading and metadata-store resolution neither package
duplicates on its own.

Connector reuse
----------------
Connections are opened via db.connection_factory.ConnectionFactory /
db.adapters.py, imported directly -- these are the two files this project
is explicitly allowed to depend on from the existing dq_* engine. Nothing
here reimplements a connection factory: callers (rules_engine/runner.py,
sampling/sampling.py, tests) own a ConnectionFactory instance and pass
adapters into rules_engine/executor.py / sampling/sampling.py.

Why this file does NOT import config/env_config.py
----------------------------------------------------
config/env_config.py belongs to the dq_* engine and is not in the reuse
exemption (only db/adapters.py and db/connection_factory.py are; the rest
of the old dq_* engine isn't even part of this branch). So the metadata
schema name is resolved independently here via GRE_META_DB, defaulting to
the same schema dq_* uses in DEV.

Metadata connection
--------------------
GRE_META_CONNECTION picks which already-configured connection (from
DQ_CONNECTION_NAMES) holds the gre_ tables. Defaults to "teradata" -- the
same connection name dq_* uses for its metadata store. No new connection
setup is required out of the box.

Batch-readiness precondition (deferred for v1)
-------------------------------------------------
The original prompt calls for a "don't evaluate rules until the source
batch/load is complete" precondition, expressed as config rather than
hardcoded per rule. For v1 this ships as a no-op extension point:
check_batch_ready() always returns True unless a check has been
registered for that rule_group via register_readiness_check(). Wiring up
a real check (e.g. a status-table SELECT) for a given rule_group is then
a one-line addition here, not an engine code change. (Sampling has no
equivalent readiness gate -- it runs on demand against a pull_date.)

Local dev credentials (.env)
-------------------------------
For local development, credentials are loaded from a plain .env file via
python-dotenv -- the same mechanics used for local Teradata access
elsewhere (load_dotenv(dotenv_path=..., override=True), file resolved
relative to the repo root, silently skipped if absent).

Critically, this does NOT invent a new credential path or touch
db/adapters.py / db/connection_factory.py: it loads the SAME env var
names TeradataAdapter.build() already reads at connect time
(DQ_CONNECTION_NAMES, DQ_<NAME>_TYPE, DQ_<NAME>_HOST, DQ_<NAME>_USER,
DQ_<NAME>_PASSWORD, DQ_<NAME>_LOGMECH). Once the .env file is loaded into
the process environment here, db/adapters.py sees already-populated env
vars and behaves exactly as it would in a real deployment -- zero code
changes to either reused file. See dev.env.example at the repo root for
the exact variable names to fill in.

The file is entirely optional: absent (the normal case for a real
deployment, or for tests, which never open a real Teradata connection)
it's a silent no-op -- required-credential validation already happens
downstream, in db/adapters.py's own _require() checks, so nothing here
duplicates that. GRE_ENV_FILE overrides the filename (default
"dev.env"). Pivoting to Secrets Manager later replaces ONLY
_load_env_file() below -- nothing else in either package needs to
change, since every other file only ever sees already-populated env
vars either way.
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


def _load_env_file() -> None:
    """
    Load local dev credentials from a .env file into the process
    environment, if one is present. Never raises: a missing file (normal
    for a real deployment) or a missing python-dotenv install is a silent
    no-op, not an error -- see this module's docstring for why validation
    is deliberately left to db/adapters.py's own _require() calls instead
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
# concurrently at all. DQ_<NAME>_MAX_PARALLEL further caps how many of
# those concurrent rules may simultaneously hold an open session against
# one specific named connection (e.g. a Postgres OLTP source that can't
# tolerate as much concurrent load as the Teradata warehouse) -- the two
# caps compose: a rule_group with GRE_MAX_PARALLEL_RULES=8 where 6 of its
# rules target a Postgres connection capped at DQ_CLAIMS_PG_MAX_PARALLEL=2
# still only ever has 2 of those 6 running against Postgres at once, even
# though up to 8 rules total may be in flight across every source.
#
# Both default to 1 -- i.e. off, matching today's behavior exactly with no
# config changes required. A connection needs an explicit opt-in
# (DQ_<NAME>_MAX_PARALLEL > 1) before the engine will ever open more than
# one concurrent session against it, specifically so raising the group-wide
# cap alone can never silently increase load on a source that wasn't sized
# for it.
#
# These live here (not in db/connection_factory.py, alongside the other
# per-connection DQ_<NAME>_* settings it already reads) because that file
# and db/adapters.py are the two files this project is explicitly allowed
# to REUSE from the dq_* engine but never MODIFY -- see the repo root
# README's reuse policy, and rules_engine/parallel.py's module docstring
# for how a connection's cap is honored without connection_factory.py
# needing to know parallelism is a concept at all.
def get_max_parallel_rules() -> int:
    """
    GRE_MAX_PARALLEL_RULES -- max rules that may execute concurrently
    within one run_rule_group() call. Default 1 (no parallelism).
    """
    return max(1, int(os.getenv("GRE_MAX_PARALLEL_RULES", "1")))


def get_max_parallel_for_connection(name: str) -> int:
    """
    DQ_<NAME>_MAX_PARALLEL -- how many concurrent sessions the named
    connection `name` may serve during a parallel run. Default 1 (that
    connection is never given more than one concurrent session, even if
    GRE_MAX_PARALLEL_RULES is raised) -- same DQ_<NAME>_* naming
    convention db/connection_factory.py already uses for
    DQ_<NAME>_TYPE/DQ_<NAME>_HOST/etc.
    """
    return max(1, int(os.getenv(f"DQ_{name.upper()}_MAX_PARALLEL", "1")))


# ── Batch-readiness precondition (deferred; see module docstring) ─────────
_READINESS_CHECKS: Dict[str, Callable[[str, object], bool]] = {}


def register_readiness_check(rule_group: str, fn: Callable[[str, object], bool]) -> None:
    """
    Register a readiness check for a rule_group.

    fn receives (batch_id, meta_conn) and returns True when the batch is
    ready to be evaluated. Not called anywhere in v1 unless registered --
    see check_batch_ready() below.
    """
    _READINESS_CHECKS[rule_group] = fn


def check_batch_ready(rule_group: str, batch_id: str, meta_conn=None) -> bool:
    """
    Return True when `rule_group` is clear to run against `batch_id`.

    v1 default: always True (no-op) unless a check was registered for this
    rule_group via register_readiness_check(). This keeps the precondition
    a config decision, not an engine code change, once a real check is
    needed.
    """
    fn = _READINESS_CHECKS.get(rule_group)
    if fn is None:
        return True
    return bool(fn(batch_id, meta_conn))
