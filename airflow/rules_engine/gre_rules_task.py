"""
gre_rules_task.py
------------------
The ONLY Airflow-aware module in this package. Everything else in this
rules_engine/ folder (its own original files, db/, run_rules.py) is
either an untouched copy of the rules-engine framework or a thin
rules-only CLI wrapper that has never heard of Airflow; only
gre_rules_dag.py, one level up in airflow/, is Airflow-aware alongside
this file. This module is the bridge between the two:

    1. Read ONE source connection -- Teradata OR Postgres, never both --
       from a single Airflow Connection. Which one, and which Connection
       id, is itself driven by the Airflow Variable (see
       _load_runtime_variable()'s docstring) -- nothing is hardcoded in
       the DAG.
    2. Read every other runtime input (environment, metadata db, log
       level, and the run's scope -- project/process/rule_group/
       rule_variant/run_key/run_params/extra_filters/text_params) from
       that SAME Airflow Variable.
    3. Populate the environment variables rules_engine/ and
       db/connection_factory.py already expect (TERADATA_HOST/USER/
       PASSWORD/LOGMECH, POSTGRES_HOST/PORT/DATABASE/USER/PASSWORD/
       SSLMODE, GRE_ENVIRONMENT, GRE_META_DB, ...).
    4. Call run_rules.run_rules() -- the existing rules engine, unchanged.
    5. Raise on failure / return the outcome dict on success, so a
       PythonOperator (or a TaskFlow @task) surfaces success/failure to
       Airflow the normal way.

One connection, not two
-------------------------
This package supports Teradata + Postgres source types (see
db/connection_factory.py), but any given run uses exactly ONE of them --
whichever `connection_type` the Airflow Variable (or an explicit
run_gre_rules() argument) names. The other type's env vars are simply
never set, so db/connection_factory.py's ConnectionFactory.load() skips
building it (logged, not fatal -- same as today when a driver/credential
is missing). This also drives GRE_META_CONNECTION: unless the Variable
sets "meta_connection" explicitly, it defaults to the same
`connection_type` -- there is only one connection loaded, so the
metadata store has to be the one that's actually there.

Everything is Variable-driven -- nothing hardcoded in the DAG
-----------------------------------------------------------------
Every input this bridge needs -- which connection to use, which
Connection id, environment, metadata db, log level, and the run's scope
at whatever level you want (project / process / rule_group /
rule_variant, per rules_engine.runner.run_by_scope()'s own scoping
rules) -- lives in ONE Airflow Variable (JSON), not in the DAG file. A
DAG built on run_gre_rules() needs to hardcode nothing but which
Variable to read (`variable_key`), and even that can be overridden at
trigger time via DAG params (see gre_rules_dag.py, one level up in airflow/). To run at a
different level or against a different scope, edit the Variable (or
pass a one-off `overrides` dict / DAG run config) -- never the DAG code.

No .env file is used here. .env (rules_engine/config.py's dev.env
loading) is for local development only -- in Airflow, an Airflow
Connection supplies credentials and an Airflow Variable supplies runtime
configuration; both are turned into the SAME env vars a local dev.env
would have populated, so run_rules.py / rules_engine/ never know the
difference.

Order of execution matters: every env var below is set BEFORE run_rules
is imported/called (imports are deliberately deferred to inside
run_gre_rules(), not module level) -- rules_engine/config.py and
db/connection_factory.py read several of these at IMPORT time (e.g.
rules_engine/config.py's dev.env auto-load), not just at call time, so
importing them before the env is populated would silently pick up
defaults instead.

No business logic lives here, and nothing under rules_engine/ or db/ is
modified by this module -- it only ever reads Airflow config and writes
process env vars.
"""
import logging
import os

logger = logging.getLogger(__name__)

# This file lives INSIDE rules_engine/ (alongside run_rules.py and db/,
# its siblings here), so this directory is what needs to be on sys.path
# for `from run_rules import run_rules` below to resolve -- regardless
# of the Airflow worker's CWD (Airflow DAG files commonly run with an
# unpredictable one, so this is not optional). run_rules.py does its own,
# further sys.path bootstrap for the "rules_engine.X"/"db.X" imports IT
# needs once it's loaded -- see its module docstring.
_PACKAGE_ROOT = os.path.dirname(os.path.abspath(__file__))

# Airflow Variable key holding the JSON blob of runtime config (which
# connection to use, environment/meta db/log level, and the run's scope).
# Override by passing variable_key= explicitly to run_gre_rules() -- or,
# per DAG run, via the DAG's `variable_key` param (see
# gre_rules_dag.py, one level up) -- if a DAG needs a different Variable per
# environment/team/run.
DEFAULT_VARIABLE_KEY = "gre_rules_config"

# Default Airflow Connection id per connection_type, used only when the
# Variable (or an explicit run_gre_rules(connection_id=...) argument)
# doesn't name one itself.
_DEFAULT_CONN_ID_BY_TYPE = {
    "teradata": "gre_teradata",
    "postgres": "gre_postgres",
}

_SUPPORTED_CONNECTION_TYPES = tuple(_DEFAULT_CONN_ID_BY_TYPE)


def _ensure_package_on_path() -> None:
    import sys
    if _PACKAGE_ROOT not in sys.path:
        sys.path.insert(0, _PACKAGE_ROOT)


def _set_env(key: str, value) -> None:
    """Set an env var only when value is non-empty -- never overwrite an
    already-populated env var (e.g. one an operator/pod template set) with
    an empty string, which would break db/connection_factory.py's
    _require() checks in a much more confusing way than "not set at all"."""
    if value is None or value == "":
        return
    os.environ[str(key)] = str(value)


def _load_teradata_connection(conn_id: str) -> None:
    """
    Populate TERADATA_HOST / TERADATA_USER / TERADATA_PASSWORD /
    TERADATA_LOGMECH from an Airflow Connection -- these are exactly the
    env vars db/connection_factory.py's TeradataAdapter.build() already
    reads, so nothing downstream needs to know these came from an Airflow
    Connection rather than a .env file.

    Uses the Airflow Connection's host/login/password fields directly.
    logmech (default "LDAP") is read from the Connection's `extra` JSON
    (key "logmech") if present, so a per-connection override doesn't
    require touching the DAG or this module -- falls back to the
    TERADATA_LOGMECH env var if already set (e.g. via Airflow Variables/
    pod env), then to "LDAP".
    """
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(conn_id)

    _set_env("TERADATA_HOST", conn.host)
    _set_env("TERADATA_USER", conn.login)
    _set_env("TERADATA_PASSWORD", conn.password)

    logmech = None
    extra = conn.extra_dejson or {}
    if extra.get("logmech"):
        logmech = extra["logmech"]
    _set_env("TERADATA_LOGMECH", logmech or os.environ.get("TERADATA_LOGMECH") or "LDAP")

    logger.info(
        "Loaded Teradata connection '%s': host=%s user=%s logmech=%s (password not logged)",
        conn_id, conn.host, conn.login, os.environ.get("TERADATA_LOGMECH"),
    )


def _load_postgres_connection(conn_id: str) -> None:
    """
    Populate POSTGRES_HOST / POSTGRES_PORT / POSTGRES_DATABASE /
    POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_SSLMODE from an Airflow
    Connection -- exactly the env vars db/connection_factory.py's
    PostgresAdapter.build() already reads.

    Uses the Airflow Connection's host/schema/login/password/port fields
    directly. sslmode (default "prefer") is read from the Connection's
    `extra` JSON (key "sslmode") if present.
    """
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(conn_id)

    _set_env("POSTGRES_HOST", conn.host)
    _set_env("POSTGRES_PORT", conn.port)
    _set_env("POSTGRES_DATABASE", conn.schema)
    _set_env("POSTGRES_USER", conn.login)
    _set_env("POSTGRES_PASSWORD", conn.password)

    extra = conn.extra_dejson or {}
    sslmode = extra.get("sslmode")
    _set_env("POSTGRES_SSLMODE", sslmode or os.environ.get("POSTGRES_SSLMODE") or "prefer")

    logger.info(
        "Loaded Postgres connection '%s': host=%s port=%s database=%s user=%s "
        "(password not logged)",
        conn_id, conn.host, conn.port, conn.schema, conn.login,
    )


_CONNECTION_LOADERS = {
    "teradata": _load_teradata_connection,
    "postgres": _load_postgres_connection,
}


def _load_source_connection(connection_type: str, connection_id: str) -> None:
    """
    Load exactly ONE source connection's credentials -- dispatches to
    _load_teradata_connection() or _load_postgres_connection() by
    `connection_type`. The other type's env vars are simply never set.
    Raises ValueError up front (before touching Airflow at all) if
    `connection_type` isn't one of the two this package supports.
    """
    loader = _CONNECTION_LOADERS.get(connection_type)
    if loader is None:
        raise ValueError(
            f"connection_type must be one of {_SUPPORTED_CONNECTION_TYPES!r}, "
            f"got {connection_type!r}."
        )
    loader(connection_id)


def _load_runtime_variable(variable_key: str) -> dict:
    """
    Read the Airflow Variable (JSON) holding EVERY non-secret runtime
    input this bridge needs -- which connection to use, environment/meta
    db/log level, and the run's scope. Shape (all keys optional unless
    noted):

        {
          "connection_type": "teradata",    -> REQUIRED (unless passed
                                                explicitly to
                                                run_gre_rules()) -- exactly
                                                one of "teradata" or
                                                "postgres". Only that
                                                type's Airflow Connection
                                                is read; the other
                                                connection's env vars are
                                                never set.
          "connection_id": "gre_teradata_prod", -> optional -- Airflow
                                                Connection id to load
                                                credentials from. Defaults
                                                to "gre_teradata" or
                                                "gre_postgres" (matching
                                                connection_type) if omitted.

          "environment": "PROD",            -> GRE_ENVIRONMENT
          "meta_db": "GRE_META_PROD",       -> GRE_META_DB
          "meta_connection": "teradata",    -> GRE_META_CONNECTION (optional --
                                                defaults to connection_type
                                                if omitted, since only one
                                                connection is ever loaded)
          "log_level": "INFO",              -> GRE_LOG_LEVEL / run_rules() log_level
                                                (omit this key to get this package's
                                                 default of "ERROR" -- errors only)
          "log_dir": "/opt/airflow/logs/gre",-> GRE_LOG_DIR
          "max_parallel_rules": "4",        -> GRE_MAX_PARALLEL_RULES

          "project_name": "HEALTHSPRING_UM",   -> run_rules() scope args --
          "process_name": "UNIVERSE_VALIDATION",  set however many/few of
          "rule_group": null,                     these to run at whatever
          "rule_variant": null,                   level you need (project,
          "run_key": null,                        process, rule_group, or
          "run_params": {"year": "2026", "month": "9"},  rule_group+rule_variant
          "extra_filters": {},                    -- see
          "text_params": {}                       rules_engine/runner.py::
                                                    run_by_scope()'s docstring
                                                    for the full scoping rules)
        }

    "connection_type"/"connection_id" are consumed directly by
    run_gre_rules() (not passed through to run_rules()). Any key under
    "environment"/"meta_db"/"meta_connection"/"log_level"/"log_dir"/
    "max_parallel_rules" is applied as its corresponding GRE_* env var;
    every other key (project_name, process_name, rule_group,
    rule_variant, run_params, extra_filters, text_params, run_key) is
    passed straight through to run_rules() as a matching keyword
    argument. Unknown keys are ignored.

    This is deliberately the ONLY place a DAG needs to point at -- change
    scope, environment, or which connection to use by editing this
    Variable (or a one-off `overrides` dict / DAG run config), never the
    DAG file itself.
    """
    from airflow.models import Variable

    config = Variable.get(variable_key, deserialize_json=True, default_var={})
    if not isinstance(config, dict):
        raise ValueError(
            f"Airflow Variable '{variable_key}' must deserialize to a JSON object, "
            f"got {type(config).__name__}."
        )
    return config


_ENV_KEYS = {
    "environment": "GRE_ENVIRONMENT",
    "meta_db": "GRE_META_DB",
    "meta_connection": "GRE_META_CONNECTION",
    "log_level": "GRE_LOG_LEVEL",
    "log_dir": "GRE_LOG_DIR",
    "max_parallel_rules": "GRE_MAX_PARALLEL_RULES",
}

# Config keys consumed directly by run_gre_rules() -- never forwarded to
# run_rules() and never treated as a GRE_* env var.
_CONNECTION_KEYS = ("connection_type", "connection_id")

_RUN_RULES_KEYS = (
    "project_name", "process_name", "rule_group", "rule_variant",
    "run_key", "run_params", "extra_filters", "text_params",
)


def run_gre_rules(
    variable_key: str = DEFAULT_VARIABLE_KEY,
    overrides: dict = None,
    connection_type: str = None,
    connection_id: str = None,
    **run_rules_kwargs,
):
    """
    The function an Airflow task calls. Wire it up either as a
    PythonOperator's python_callable, or a TaskFlow @task -- see
    gre_rules_dag.py (one level up, in airflow/) for both. In the common case a
    DAG passes nothing but `variable_key` (or not even that, if the
    default Variable name is fine) -- every other input, including which
    single connection to use, comes from the Airflow Variable itself (see
    _load_runtime_variable()'s docstring for its full shape).

    Parameters
    ----------
    variable_key : Airflow Variable name to load runtime config from
        (default "gre_rules_config").
    overrides : optional dict merged over the Airflow Variable's config
        (e.g. from `{{ dag_run.conf }}` on a manually-triggered run) --
        lets one run override just a couple of keys (including
        connection_type/connection_id, or the run's scope) without
        editing the Variable.
    connection_type : "teradata" or "postgres" -- which single source
        connection to load. Passed here directly, this WINS over the
        Variable's/`overrides`' "connection_type" -- normally left None
        so the Variable decides. Exactly one connection is ever loaded;
        this package does not support loading both at once.
    connection_id : Airflow Connection id for that connection_type.
        Passed here directly, this wins over the Variable's/`overrides`'
        "connection_id" -- normally left None so the Variable decides (or
        so the per-type default -- "gre_teradata"/"gre_postgres" -- is
        used).
    **run_rules_kwargs : any run_rules.run_rules() keyword (project_name,
        process_name, rule_group, rule_variant, run_key, run_params,
        extra_filters, text_params, log_level) passed directly here wins
        over both the Variable and `overrides` -- highest precedence, for
        a caller that wants to hardcode scope in code rather than
        configuration (most DAGs should prefer the Variable/`overrides`
        instead, per this module's "Everything is Variable-driven" note
        above).

    Returns
    -------
    The outcome dict from run_rules.run_rules() (run_by_scope()'s own
    {"rule_groups": {...}} shape) on success.

    Raises
    ------
    ValueError if connection_type isn't resolved to exactly one of
    "teradata"/"postgres" (from an explicit argument, `overrides`, or the
    Variable -- in that precedence order).
    RuntimeError if any rule_group in scope did not COMPLETE, or if
    run_rules() short-circuited on its own ValueError (bad/empty scope)
    -- either way, Airflow marks the task failed. Whatever
    BaseHook.get_connection()/Variable.get() themselves raise (missing
    Connection/Variable) propagates unchanged.
    """
    # 1) Runtime configuration from the Airflow Variable, then `overrides`
    #    on top of it -- read BEFORE loading any connection, since which
    #    connection to load is itself part of this configuration.
    config = _load_runtime_variable(variable_key)
    if overrides:
        config = {**config, **overrides}

    # 2) Resolve which ONE connection to load: explicit function argument
    #    wins, then `overrides`/the Variable's "connection_type"/
    #    "connection_id". Never both types -- exactly one Connection is
    #    read, and the other source type's env vars are simply never set.
    resolved_connection_type = connection_type or config.get("connection_type")
    if not resolved_connection_type:
        raise ValueError(
            "connection_type is required -- set it in the Airflow Variable "
            f"('{variable_key}'), pass it via `overrides`, or pass "
            "connection_type= directly to run_gre_rules(). Must be one of "
            f"{_SUPPORTED_CONNECTION_TYPES!r}."
        )
    resolved_connection_id = (
        connection_id
        or config.get("connection_id")
        or _DEFAULT_CONN_ID_BY_TYPE.get(resolved_connection_type)
    )
    _load_source_connection(resolved_connection_type, resolved_connection_id)

    # 3) Populate the GRE_* env vars the rules engine/config reads.
    for config_key, env_key in _ENV_KEYS.items():
        if config_key in config:
            _set_env(env_key, config[config_key])
    # GRE_META_CONNECTION defaults to whichever single connection_type we
    # just loaded, unless the Variable/overrides named a different one
    # explicitly via "meta_connection" above -- there's only ever one
    # connection available in this package, so the metadata store has to
    # be it.
    if "GRE_META_CONNECTION" not in os.environ:
        _set_env("GRE_META_CONNECTION", resolved_connection_type)

    # 4) Only NOW import run_rules -- after every env var above is set,
    #    since rules_engine/config.py and db/connection_factory.py read
    #    some of these at import time, not just at call time.
    _ensure_package_on_path()
    from run_rules import run_rules as _run_rules

    call_kwargs = {k: config.get(k) for k in _RUN_RULES_KEYS if k in config}
    call_kwargs.update({k: v for k, v in run_rules_kwargs.items() if v is not None})
    if "log_level" not in call_kwargs and "log_level" in config:
        call_kwargs["log_level"] = config["log_level"]
    # Leaving "log_level" out of both `config` and run_rules_kwargs falls
    # through to run_rules()'s own default of "ERROR" (errors only) --
    # this package's default, quieter than rules_engine/config.py's own
    # DEBUG default. Set "log_level" in the Airflow Variable (or pass
    # log_level=... directly to run_gre_rules()) to opt back into more
    # detail, e.g. "INFO" or "DEBUG".

    outcome, exit_code = _run_rules(**call_kwargs)

    # 5) Success/failure back to Airflow.
    if exit_code != 0:
        raise RuntimeError(
            f"GRE rules run failed (exit_code={exit_code}). "
            f"outcome={outcome!r}"
        )
    return outcome
