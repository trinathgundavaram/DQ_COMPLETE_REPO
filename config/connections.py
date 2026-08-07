"""
config/connections.py
-----------------------
Loads and validates config/connections.yaml -- the connection catalogue.

Replaces the old dq_connections DB table (removed -- see ddl_shared.sql's
history). Connection metadata (name, source_type, host, port, database,
...) is non-secret, deployment-time configuration: it belongs in a
version-controlled file that's reviewed like any other code change, not a
database row a DBA edits out-of-band and nobody else sees. Credentials
(user, password, token, access keys) are NEVER stored here -- they stay
in DQ_<NAME>_* environment variables exactly as before, read lazily at
connect time by each adapter's build() classmethod (see db/adapters.py).
This file only ever answers "what connections exist and where do they
point", never "what's the password".

Fail-fast by design: load_connections() raises ConnectionConfigError with
a specific, actionable message on any schema problem (missing file,
invalid YAML, missing required fields, unknown source_type, duplicate
names) instead of letting a malformed config surface later as a
confusing KeyError/AttributeError deep inside connection_factory.py or,
worse, silently produce zero usable connections. ConnectionFactory.load()
(db/connection_factory.py) does not catch this exception -- a bad
connections file stops the process at startup, the same way a missing
required env var already does via db/adapters.py::_require().

Public API
----------
load_connections(path=None) -> {name: entry_dict}   (active connections only)
get_connections_file() -> str                          (resolves DQ_CONNECTIONS_FILE)
ConnectionConfigError                                    (raised on any schema problem)
"""

import logging
import os
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

# source_type values db/connection_factory.py's _TYPE_MAP recognises.
# Kept as a plain set here (rather than importing _TYPE_MAP) so this module
# has no dependency on db/ -- config/ stays loadable standalone, the same
# layering principle config/env_config.py already follows.
_KNOWN_SOURCE_TYPES = {
    "teradata",
    "postgresql", "postgres", "aurora",
    "s3",
    "sqlserver", "mssql",
    "file", "csv", "excel",
}

_REQUIRED_FIELDS = ("name", "source_type")


class ConnectionConfigError(Exception):
    """Raised on a malformed config/connections.yaml -- meant to fail the
    process at startup, not be caught and worked around."""


def get_connections_file() -> str:
    """Path to the connections config file. DQ_CONNECTIONS_FILE overrides
    the default so a deployment can point at a different file per
    environment without a code change (same pattern DQ_META_DB uses for
    config/env_config.py)."""
    return os.getenv("DQ_CONNECTIONS_FILE", "config/connections.yaml")


def load_connections(path: str = None) -> dict:
    """
    Parse and validate the connections config file.

    Returns {name: entry_dict} for every connection with active: true (or
    no 'active' key at all -- defaults to active). Inactive entries are
    dropped here so callers never have to remember to check the flag
    themselves.

    Raises ConnectionConfigError on:
      - missing file
      - invalid YAML syntax
      - missing top-level 'connections' key, or it isn't a list
      - a connection entry that isn't a mapping
      - a connection entry missing 'name' or 'source_type'
      - an unrecognised source_type
      - a duplicate connection name
    """
    path = path or get_connections_file()
    p = Path(path)
    if not p.exists():
        raise ConnectionConfigError(
            f"Connections config file not found: '{path}'. Set DQ_CONNECTIONS_FILE "
            "to point at your file, or create config/connections.yaml -- see "
            "the example connections already in that file for the expected shape."
        )

    try:
        with p.open() as f:
            raw = yaml.safe_load(f) or {}
    except yaml.YAMLError as exc:
        raise ConnectionConfigError(f"Could not parse '{path}' as YAML: {exc}") from exc

    if not isinstance(raw, dict):
        raise ConnectionConfigError(
            f"'{path}' must be a YAML mapping with a top-level 'connections' key, "
            f"got {type(raw).__name__}."
        )

    entries = raw.get("connections")
    if entries is None:
        raise ConnectionConfigError(f"'{path}' has no top-level 'connections' key.")
    if not isinstance(entries, list):
        raise ConnectionConfigError(f"'{path}': 'connections' must be a list.")

    by_name: dict = {}
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ConnectionConfigError(
                f"'{path}': connections[{i}] must be a mapping, got {type(entry).__name__}."
            )

        missing = [f for f in _REQUIRED_FIELDS if not entry.get(f)]
        if missing:
            raise ConnectionConfigError(
                f"'{path}': connections[{i}] is missing required field(s): {missing}. "
                f"Entry: {entry!r}"
            )

        name = str(entry["name"]).strip()
        source_type = str(entry["source_type"]).strip().lower()

        if source_type not in _KNOWN_SOURCE_TYPES:
            raise ConnectionConfigError(
                f"'{path}': connection '{name}' has unknown source_type "
                f"'{source_type}'. Must be one of: {', '.join(sorted(_KNOWN_SOURCE_TYPES))}."
            )

        if name in by_name:
            raise ConnectionConfigError(
                f"'{path}': duplicate connection name '{name}' -- connection names "
                "must be unique (they're also the DQ_<NAME>_* env var prefix and "
                "dq_rules.source_system value)."
            )

        normalised = dict(entry)
        normalised["name"] = name
        normalised["source_type"] = source_type
        normalised.setdefault("active", True)
        by_name[name] = normalised

    active = {n: e for n, e in by_name.items() if e.get("active", True)}
    inactive_count = len(by_name) - len(active)

    if not active:
        logger.warning(
            "No active connections in '%s' (%d total, %d inactive).",
            path, len(by_name), inactive_count,
        )
    else:
        logger.info(
            "Loaded %d active connection(s) from '%s'%s: %s",
            len(active), path,
            f" ({inactive_count} inactive)" if inactive_count else "",
            ", ".join(sorted(active)),
        )

    return active
