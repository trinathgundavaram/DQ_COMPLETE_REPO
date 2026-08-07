import os

CONFIG = {
    "DEV": {
        "META_DB": "CMSUNIV_FILELAND_DEV_T",
        "ENV_TOKEN": "_DEV",
    },
    "QA": {
        "META_DB": "CMSUNIV_FILELAND_QA_T",
        "ENV_TOKEN": "_QA",
    },
    "UAT": {
        "META_DB": "CMSUNIV_FILELAND_T",
        "ENV_TOKEN": "",
    },
    "PROD": {
        "META_DB": "CMSUNIV_FILELAND_T",
        "ENV_TOKEN": "",
    },
}


def get_config() -> dict:
    """
    Read DQ_ENV fresh on every call (not cached at import time) -- same
    lazy-env-read principle every adapter's build() and get_meta_db() below
    already follow, so a process that sets/changes DQ_ENV after this module
    is first imported (e.g. a long-lived cron_run_scheduler() process, or
    any import that happens to occur before env vars are finalised) still
    resolves the correct environment on every call rather than silently
    pinning to whatever DQ_ENV happened to be set at import time.
    """
    env = os.getenv("DQ_ENV", "DEV").upper()
    if env not in CONFIG:
        raise EnvironmentError(
            f"Invalid DQ_ENV='{env}'. Must be one of: {list(CONFIG.keys())}"
        )
    return CONFIG[env]


def get_meta_db() -> str:
    """
    Return the metadata database name.

    DQ_META_DB env var overrides the CONFIG lookup — useful when deploying to
    a schema whose name doesn't follow the standard DEV/QA/PROD naming pattern
    without requiring a code change.

    Example:
        DQ_META_DB=MY_CUSTOM_DQ_SCHEMA_T  →  uses that name directly
    """
    override = os.getenv("DQ_META_DB", "").strip()
    if override:
        return override
    return get_config()["META_DB"]
