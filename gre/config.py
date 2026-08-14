"""
gre/config.py
--------------
Small, env-driven config surface for the Generic Rules Engine.

Connector reuse
----------------
Connections are opened via db.connection_factory.ConnectionFactory /
db.adapters.py, imported directly -- these are the two files this project
is explicitly allowed to depend on from the existing dq_* engine. Nothing
here reimplements a connection factory: callers (gre/runner.py, tests) own
a ConnectionFactory instance and pass adapters into gre/executor.py.

Why this file does NOT import config/env_config.py
----------------------------------------------------
config/env_config.py belongs to the dq_* engine and is not in the reuse
exemption (only db/adapters.py and db/connection_factory.py are). So the
metadata schema name is resolved independently here via GRE_META_DB,
defaulting to the same schema dq_* already uses in DEV -- see the "Same
schema, same connection" decision recorded in gre/schema.sql's header.

Metadata connection
--------------------
GRE_META_CONNECTION picks which already-configured connection (from
DQ_CONNECTION_NAMES) holds the gre_ tables. Defaults to "teradata" -- the
same connection name dq_* already uses for its metadata store, per the
"same schema, same connection" decision. No new connection setup is
required out of the box.

Batch-readiness precondition (deferred for v1)
-------------------------------------------------
The prompt calls for a "don't evaluate rules until the source batch/load
is complete" precondition, expressed as config rather than hardcoded per
rule. For v1 this ships as a no-op extension point: check_batch_ready()
always returns True unless a check has been registered for that
rule_group via register_readiness_check(). Wiring up a real check (e.g. a
status-table SELECT) for a given rule_group is then a one-line addition
here, not an engine code change.
"""

import os
from typing import Callable, Dict, Optional

# ── Metadata store (same schema/connection dq_* uses, by default) ─────────
META_CONNECTION = os.getenv("GRE_META_CONNECTION", "teradata")
META_DB = os.getenv("GRE_META_DB", "CMSUNIV_FILELAND_DEV_T")


def get_meta_connection_name() -> str:
    """Named connection (see db/connection_factory.py) holding the gre_ tables."""
    return META_CONNECTION


def get_meta_db() -> str:
    """Schema/database name the gre_ tables live in."""
    return META_DB


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
