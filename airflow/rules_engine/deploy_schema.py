import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.connection_factory import TeradataAdapter
from rules_engine.config import configure_logging, get_meta_db

logger = logging.getLogger(__name__)

SCHEMA_SQL_PATH = Path(__file__).resolve().parent / "schema.sql"
SCHEMA_DROP_SQL_PATH = Path(__file__).resolve().parent / "schema_drop.sql"


def render(sql_path: Path, meta_db: str) -> str:
    text = sql_path.read_text()
    if "{{META_DB}}" not in text:
        raise ValueError(f"{sql_path} has no {{{{META_DB}}}} placeholder -- template/renderer out of sync.")
    return text.replace("{{META_DB}}", meta_db)


def _run_statements(conn, ddl: str) -> None:


    cursor = conn.cursor()
    try:
        for statement in ddl.split(";"):
            statement = statement.strip()
            if not statement or statement.startswith("--"):
                continue

            lines = [ln for ln in statement.splitlines() if not ln.strip().startswith("--")]
            statement = "\n".join(lines).strip()
            if not statement:
                continue
            logger.info("Executing: %s...", statement.splitlines()[0][:100])
            cursor.execute(statement)
            conn.commit()
    finally:
        cursor.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Render (and optionally run) rules_engine/'s schema.sql against GRE_META_DB.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the rendered DDL; execute nothing.")
    parser.add_argument("--drop-first", action="store_true",
                        help="Also render+run schema_drop.sql immediately before schema.sql -- "
                             "this package's drop-and-recreate redeploy policy (see schema_drop.sql's "
                             "header). Destructive: discards every row in every gre_* table for this "
                             "meta_db.")
    args = parser.parse_args()
    configure_logging()

    meta_db = get_meta_db()
    create_ddl = render(SCHEMA_SQL_PATH, meta_db)
    drop_ddl = render(SCHEMA_DROP_SQL_PATH, meta_db) if args.drop_first else None

    if args.dry_run:
        if drop_ddl:
            print(drop_ddl)
        print(create_ddl)
        return

    adapter = TeradataAdapter.build()
    try:
        if drop_ddl:
            logger.info("Dropping existing gre_* tables in %s (--drop-first)...", meta_db)
            _run_statements(adapter, drop_ddl)
        logger.info("Creating gre_* tables in %s...", meta_db)
        _run_statements(adapter, create_ddl)
        logger.info("Done -- rules_engine schema is ready in %s.", meta_db)
    finally:
        adapter.close()


if __name__ == "__main__":
    main()
