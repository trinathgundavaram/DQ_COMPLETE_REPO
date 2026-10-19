import logging
import os

logger = logging.getLogger(__name__)


_PACKAGE_ROOT = os.path.dirname(os.path.abspath(__file__))


DEFAULT_VARIABLE_KEY = "gre_rules_config"


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
    if value is None or value == "":
        return
    os.environ[str(key)] = str(value)


def _load_teradata_connection(conn_id: str) -> None:
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


def _load_nas_connection(conn_id: str) -> None:
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(conn_id)
    _set_env("NAS_HOST", conn.host)
    _set_env("NAS_USER", conn.login)
    _set_env("NAS_PASSWORD", conn.password)
    _set_env("NAS_PORT", conn.port)
    logger.info("Loaded NAS connection '%s': host=%s user=%s (password not logged)",
                conn_id, conn.host, conn.login)


_CONNECTION_LOADERS = {
    "teradata": _load_teradata_connection,
    "postgres": _load_postgres_connection,
}


def _load_source_connection(connection_type: str, connection_id: str) -> None:
    loader = _CONNECTION_LOADERS.get(connection_type)
    if loader is None:
        raise ValueError(
            f"connection_type must be one of {_SUPPORTED_CONNECTION_TYPES!r}, "
            f"got {connection_type!r}."
        )
    loader(connection_id)


def _load_runtime_variable(variable_key: str) -> dict:
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
    "file_name": "GRE_FILE_NAME",
    "file_delimiter": "GRE_FILE_DELIMITER",
    "file_sheet": "GRE_FILE_SHEET",
    "max_parallel_rules": "GRE_MAX_PARALLEL_RULES",
}


_CONNECTION_KEYS = ("connection_type", "connection_id", "nas_connection_id")

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


    config = _load_runtime_variable(variable_key)
    if overrides:
        config = {**config, **overrides}


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


    nas_connection_id = config.get("nas_connection_id")
    if nas_connection_id:
        _load_nas_connection(nas_connection_id)


    for config_key, env_key in _ENV_KEYS.items():
        if config_key in config:
            _set_env(env_key, config[config_key])


    if "GRE_META_CONNECTION" not in os.environ:
        _set_env("GRE_META_CONNECTION", resolved_connection_type)


    _ensure_package_on_path()
    from run_rules import run_rules as _run_rules

    call_kwargs = {k: config.get(k) for k in _RUN_RULES_KEYS if k in config}
    call_kwargs.update({k: v for k, v in run_rules_kwargs.items() if v is not None})
    if "log_level" not in call_kwargs and "log_level" in config:
        call_kwargs["log_level"] = config["log_level"]


    outcome, exit_code = _run_rules(**call_kwargs)


    if exit_code != 0:
        raise RuntimeError(
            f"GRE rules run failed (exit_code={exit_code}). "
            f"outcome={outcome!r}"
        )
    return outcome
