"""
sampling/deploy_schema.py
----------------------------
Render and (optionally) run schema.sql/schema_drop.sql against the
metadata database this environment actually resolves to -- so promoting
this package DEV -> QA -> INT -> UAT -> PROD is "set GRE_META_DB and run
this," never a hand-edit of schema.sql itself. See
rules_engine/deploy_schema.py for the identical tool on that package's
side -- the two are separate scripts (each package's own schema.sql is
fully standalone, see README.md's "Package separation"), not a shared
module, matching how the rest of this package deliberately duplicates
rules_engine/'s generic helpers rather than importing them.

schema.sql/schema_drop.sql are templates: every gre_*/gre_sample_* table
in them is qualified with the placeholder "{{META_DB}}", not a literal
schema name (see schema.sql's header for why). This substitutes it with
sampling/config.py::get_meta_db()'s resolution -- the exact same
GRE_META_DB env var (default CMSUNIV_FILELAND_DEV_T) every query this
package runs already uses -- so the deployed tables and the queries
against them can never point at two different schemas by accident.

Usage
-----
    python -m sampling.deploy_schema --dry-run       # print DDL, run nothing
    python -m sampling.deploy_schema                 # create the tables
    python -m sampling.deploy_schema --drop-first     # drop-and-recreate

    # Target a specific environment for this one run:
    GRE_META_DB=CMSUNIV_FILELAND_QA_T python -m sampling.deploy_schema --dry-run
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.connection_factory import TeradataAdapter  # noqa: E402
from sampling.config import configure_logging, get_meta_db  # noqa: E402

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
    # See rules_engine/deploy_schema.py::_run_statements() -- identical
    # one-statement-per-round-trip approach, Teradata's driver doesn't
    # reliably run a bare-semicolon-separated multi-statement batch.
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
        description="Render (and optionally run) sampling/'s schema.sql against GRE_META_DB.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the rendered DDL; execute nothing.")
    parser.add_argument("--drop-first", action="store_true",
                        help="Also render+run schema_drop.sql immediately before schema.sql -- "
                             "destructive: discards every row in every gre_sampling_*/gre_sample_* "
                             "table for this meta_db.")
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
            logger.info("Dropping existing gre_sampling_*/gre_sample_* tables in %s (--drop-first)...", meta_db)
            _run_statements(adapter, drop_ddl)
        logger.info("Creating gre_sampling_*/gre_sample_* tables in %s...", meta_db)
        _run_statements(adapter, create_ddl)
        logger.info("Done -- sampling schema is ready in %s.", meta_db)
    finally:
        adapter.close()


if __name__ == "__main__":
    main()
