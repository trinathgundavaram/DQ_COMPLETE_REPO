from config.env_config import get_config


def resolve_db_name(db_pattern: str) -> str:
    """
    Replace the {ENV} token in a DB name pattern with the current env suffix.

    Example (DEV):
        "CMSUNIV_FILELAND{ENV}_T"  →  "CMSUNIV_FILELAND_DEV_T"
    """
    if not db_pattern:
        raise ValueError("db_pattern is empty — cannot resolve DB name.")

    if "{ENV}" not in db_pattern:
        raise ValueError(
            f"db_pattern must contain {{ENV}} token but got: '{db_pattern}'"
        )

    cfg = get_config()
    return db_pattern.replace("{ENV}", cfg["ENV_TOKEN"])
