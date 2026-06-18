from config.env_config import get_config


def resolve_db_name(db_pattern: str) -> str:
    """
    Replace the {ENV} token in a DB name pattern with the current env suffix.

    If the pattern contains no {ENV} token it is returned as-is, allowing
    rules to reference literal DB names (e.g. "PROD_CLAIMS_DB") without
    needing an environment placeholder.

    Examples (DEV env):
        "CMSUNIV_FILELAND{ENV}_T"  →  "CMSUNIV_FILELAND_DEV_T"
        "PROD_CLAIMS_DB"           →  "PROD_CLAIMS_DB"   (no token — returned as-is)
    """
    if not db_pattern:
        raise ValueError("db_pattern is empty — cannot resolve DB name.")

    if "{ENV}" not in db_pattern:
        # Literal DB name — return unchanged
        return db_pattern

    cfg = get_config()
    return db_pattern.replace("{ENV}", cfg["ENV_TOKEN"])
