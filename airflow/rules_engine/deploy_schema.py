"""
rules_engine/deploy_schema.py
--------------------------------
Render and (optionally) run schema.sql/schema_drop.sql against the
metadata database this environment actually resolves to -- so promoting
this package DEV -> QA -> INT -> UAT -> PROD is "set GRE_META_DB and run
this," never a hand-edit of schema.sql itself.

schema.sql/schema_drop.sql are templates: every gre_* table in them is
qualified with the placeholder "{{META_DB}}", not a literal schema name
(see schema.sql's header for why). This substitutes it with
rules_engine/config.py::get_meta_db()'s resolution -- the exact same
GRE_META_DB env var (default CMSUNIV_FILELAND_DEV_T) every query this
engine runs already uses -- so the deployed tables and the queries
against them can never point at two different schemas by accident,
mirroring metadata_sync/create_postgres_tables.py's "{{SCHEMA}}" template
pattern for the Postgres mirror.

Usage
-----
    # Print the rendered CREATE DDL for whatever GRE_META_DB resolves to;
    # execute nothing:
    python -m rules_engine.deploy_schema --dry-run

    # Actually create the tables in that database:
    python -m rules_engine.deploy_schema

    # Drop-and-recreate (this package's redeploy policy -- see
    # schema_drop.sql's header: no gre_* table holds live data yet, so a
    # schema change is a drop+recreate, not an ALTER TABLE migration):
    python -m rules_engine.deploy_schema --drop-first

    # Target a specific environment for this one run, without touching
    # dev.env or the shell's exported env vars:
    GRE_META_DB=CMSUNIV_FILELAND_QA_T python -m rules_engine.deploy_schema --dry-run

Only imports db/connection_factory.py from the main repo (Teradata
connection, same TERADATA_* env vars already used elsewhere) and this
package's own config.py (GRE_META_DB resolution).
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.connection_factory import TeradataAdapter  # noqa: E402
from rules_engine.config import configure_logging, get_meta_db  # noqa: E402

logger = logging.getLogger(__name__)

SCHEMA_SQL_PATH = Path(__file__).resolve().parent / "schema.sql"
SCHEMA_DROP_SQL_PATH = Path(__file__).resolve().parent / "schema_drop.sql"


def render(sql_path: Path, meta_db: str) -> str:
    """Substitute {{META_DB}} in the given template file with the resolved
    metadata database name. Raises if the placeholder is missing entirely
    (a sign the template file and this renderer have drifted apart)."""
    text = sql_path.read_text()
    if "{{META_DB}}" not in text:
        raise ValueError(f"{sql_path} has no {{{{META_DB}}}} placeholder -- template/renderer out of sync.")
    return text.replace("{{META_DB}}", meta_db)


def _run_statements(conn, ddl: str) -> None:
    # Teradata's driver (unlike Postgres's autocommit executescript-style
    # cursor.execute()) does not reliably run a multi-statement batch
    # separated by bare semicolons in one call -- split and run each
    # CREATE/DROP TABLE/INDEX statement as its own execute()+commit(),
    # same one-statement-per-round-trip model db_ops.py uses everywhere
    # else in this engine.
    cursor = conn.cursor()
    try:
        for statement in ddl.split(";"):
            statement = statement.strip()
            if not statement or statement.startswith("--"):
                continue
            # Strip any leading comment-only lines a split left behind.
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
