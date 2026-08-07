import os

# Read target environment from env var; default to DEV
ENV = os.getenv("DQ_ENV", "DEV").upper()

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
    if ENV not in CONFIG:
        raise EnvironmentError(
            f"Invalid DQ_ENV='{ENV}'. Must be one of: {list(CONFIG.keys())}"
        )
    return CONFIG[ENV]


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
