"""Env-driven settings for metadata_sync only. Credentials themselves are
NOT redefined here -- db/connection_factory.py already reads
TERADATA_*/POSTGRES_* (see dev.env.example); this just adds the handful of
extra knobs this tool needs.
"""

import os


def get_teradata_meta_db() -> str:
    """Teradata database the gre_* tables live in (source side)."""
    return os.getenv("METADATA_SYNC_TERADATA_DB", "CMSUNIV_FILELAND_DEV_T")


def get_postgres_schema() -> str:
    """Postgres schema the mirror tables live in (target side)."""
    return os.getenv("METADATA_SYNC_PG_SCHEMA", "gre_mirror")


def get_batch_size() -> int:
    return max(1, int(os.getenv("METADATA_SYNC_BATCH_SIZE", "5000")))


def get_lookback_minutes() -> int:
    """Subtracted from the stored watermark before each incremental pull,
    to absorb clock skew and rows still mid-write at the last run."""
    return max(0, int(os.getenv("METADATA_SYNC_LOOKBACK_MINUTES", "60")))
