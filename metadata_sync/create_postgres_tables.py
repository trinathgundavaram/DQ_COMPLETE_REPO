"""Run once (and again any time ddl_postgres.sql changes) to create/update
the Postgres mirror schema. Idempotent -- every statement is
CREATE ... IF NOT EXISTS, safe to rerun any time.

    python -m metadata_sync.create_postgres_tables
    python -m metadata_sync.create_postgres_tables --dry-run   # print DDL, run nothing

Only imports db/connection_factory.py from the main repo (Postgres
connection, same POSTGRES_* env vars already used elsewhere).
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.connection_factory import PostgresAdapter  # noqa: E402
from metadata_sync.config import get_postgres_schema  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DDL_PATH = Path(__file__).resolve().parent / "ddl_postgres.sql"


def build_ddl(schema: str) -> str:
    return DDL_PATH.read_text().replace("{{SCHEMA}}", schema)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create/update the Postgres gre_* mirror schema.")
    parser.add_argument("--dry-run", action="store_true", help="Print the DDL; execute nothing.")
    args = parser.parse_args()

    schema = get_postgres_schema()
    ddl = build_ddl(schema)

    if args.dry_run:
        print(ddl)
        return

    logger.info("Applying DDL to Postgres schema '%s'...", schema)
    pg = PostgresAdapter.build()
    try:
        cur = pg.cursor()
        cur.execute(ddl)  # autocommit is on; a whole script runs fine via execute()
        cur.close()
        logger.info("Done -- schema '%s' is ready.", schema)
    finally:
        pg.close()


if __name__ == "__main__":
    main()
