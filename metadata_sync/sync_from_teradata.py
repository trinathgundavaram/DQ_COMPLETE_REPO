"""Run on a schedule (cron/Task Scheduler/Airflow) to copy/refresh every
gre_* table from Teradata (source of truth) into its Postgres mirror.
Two modes, declared per-table in tables.py: full_refresh (TRUNCATE +
reload) for small config tables, incremental (watermark upsert)
otherwise -- see README.md.

    python -m metadata_sync.sync_from_teradata
    python -m metadata_sync.sync_from_teradata --tables gre_rules,gre_exceptions
    python -m metadata_sync.sync_from_teradata --full-refresh   # force full refresh of every table
    python -m metadata_sync.sync_from_teradata --dry-run        # log row counts, write nothing

Only imports db/connection_factory.py from the main repo. Never writes to
Teradata (SELECT only). Does not import rules_engine/ or sampling/.
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from psycopg2.extras import execute_values  # noqa: E402

from db.connection_factory import PostgresAdapter, TeradataAdapter  # noqa: E402
from metadata_sync.config import (  # noqa: E402
    get_batch_size,
    get_lookback_minutes,
    get_postgres_schema,
    get_teradata_meta_db,
)
from metadata_sync.tables import TABLE_SPECS, TABLE_SPECS_BY_NAME  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FAR_PAST = datetime(1970, 1, 1)


def full_refresh_table(td, pg, spec, td_db, pg_schema, batch_size, dry_run) -> int:
    name, cols = spec["name"], spec["columns"]
    col_list = ", ".join(cols)

    td_cur = td.cursor()
    try:
        td_cur.execute(f"SELECT {col_list} FROM {td_db}.{name}")

        if dry_run:
            n = len(td_cur.fetchall())
            logger.info("[dry-run] %s: would full-refresh %d row(s).", name, n)
            return n

        pg_cur = pg.cursor()
        try:
            # Explicit BEGIN/COMMIT even though PostgresAdapter's connection
            # is autocommit=True (see db/connection_factory.py) -- psycopg2
            # honors an explicit BEGIN...COMMIT block regardless of the
            # autocommit setting for statements outside one. Without this,
            # TRUNCATE committed immediately on its own, then each
            # execute_values() batch committed separately too -- any reader
            # hitting this table between those commits saw it fully empty
            # (or only partially reloaded) mid-sync. Wrapping the whole
            # truncate-then-reload in one transaction means readers only
            # ever see the table in its old, fully-populated state or its
            # new, fully-populated state -- never a half-loaded one.
            pg_cur.execute("BEGIN")
            pg_cur.execute(f"TRUNCATE TABLE {pg_schema}.{name}")
            insert_sql = f"INSERT INTO {pg_schema}.{name} ({col_list}) VALUES %s"

            total = 0
            while True:
                batch = td_cur.fetchmany(batch_size)
                if not batch:
                    break
                execute_values(pg_cur, insert_sql, batch)
                total += len(batch)

            pg_cur.execute("COMMIT")
        except Exception:
            try:
                pg_cur.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            pg_cur.close()
    finally:
        td_cur.close()

    logger.info("%s: full-refreshed %d row(s).", name, total)
    return total


def _get_watermark(pg, pg_schema, table_name) -> datetime:
    cur = pg.cursor()
    try:
        cur.execute(
            f"SELECT last_watermark FROM {pg_schema}.metadata_sync_watermark WHERE table_name = %s",
            [table_name],
        )
        row = cur.fetchone()
        return row[0] if row and row[0] else FAR_PAST
    finally:
        cur.close()


def _set_watermark(pg, pg_schema, table_name, watermark, row_count) -> None:
    cur = pg.cursor()
    try:
        cur.execute(
            f"""
            INSERT INTO {pg_schema}.metadata_sync_watermark
                (table_name, last_watermark, last_synced_at, last_row_count)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (table_name) DO UPDATE SET
                last_watermark = EXCLUDED.last_watermark,
                last_synced_at = EXCLUDED.last_synced_at,
                last_row_count = EXCLUDED.last_row_count
            """,
            [table_name, watermark, datetime.now(), row_count],
        )
    finally:
        cur.close()


def incremental_sync_table(td, pg, spec, td_db, pg_schema, batch_size, lookback_minutes, dry_run) -> int:
    name, cols, pk = spec["name"], spec["columns"], spec["primary_key"]
    watermark_col = spec["watermark_col"]
    reopen_filter = spec.get("reopen_filter")
    col_list = ", ".join(cols)

    last_watermark = _get_watermark(pg, pg_schema, name)
    effective_watermark = last_watermark - timedelta(minutes=lookback_minutes)

    where = f"{watermark_col} >= ?"
    if reopen_filter:
        where = f"({where}) OR ({reopen_filter})"

    # leading __wm column carries the watermark value even when it's an
    # expression (e.g. gre_exceptions' COALESCE(...)), so it doesn't need
    # to be re-derived from the row afterward.
    select_sql = (
        f"SELECT {watermark_col} AS __wm, {col_list} "
        f"FROM {td_db}.{name} WHERE {where} ORDER BY {watermark_col}"
    )

    td_cur = td.cursor()
    try:
        td_cur.execute(select_sql, [effective_watermark])

        if dry_run:
            rows = td_cur.fetchall()
            logger.info("[dry-run] %s: watermark >= %s would pull %d row(s).",
                         name, effective_watermark, len(rows))
            return len(rows)

        pg_cur = pg.cursor()
        try:
            non_pk_cols = [c for c in cols if c not in pk]
            pk_list = ", ".join(pk)
            conflict_action = (
                f"DO UPDATE SET {', '.join(f'{c} = EXCLUDED.{c}' for c in non_pk_cols)}"
                if non_pk_cols else "DO NOTHING"
            )
            upsert_sql = (
                f"INSERT INTO {pg_schema}.{name} ({col_list}) VALUES %s "
                f"ON CONFLICT ({pk_list}) {conflict_action}"
            )

            total = 0
            max_wm_seen = last_watermark
            while True:
                batch = td_cur.fetchmany(batch_size)
                if not batch:
                    break
                execute_values(pg_cur, upsert_sql, [row[1:] for row in batch])
                batch_max = max((row[0] for row in batch if row[0] is not None), default=None)
                if batch_max and batch_max > max_wm_seen:
                    max_wm_seen = batch_max
                total += len(batch)

            _set_watermark(pg, pg_schema, name, max_wm_seen, total)
        finally:
            pg_cur.close()
    finally:
        td_cur.close()

    logger.info("%s: upserted %d row(s), watermark now %s.", name, total, max_wm_seen)
    return total


def parse_args():
    parser = argparse.ArgumentParser(description="Sync gre_* metadata tables from Teradata to Postgres.")
    parser.add_argument("--tables", type=str, default=None,
                         help="Comma-separated subset of table names to sync (default: all).")
    parser.add_argument("--full-refresh", action="store_true",
                         help="Force TRUNCATE + full reload for every table synced this run.")
    parser.add_argument("--dry-run", action="store_true",
                         help="Log what would be synced; write nothing to Postgres.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.tables:
        requested = [t.strip() for t in args.tables.split(",") if t.strip()]
        unknown = [t for t in requested if t not in TABLE_SPECS_BY_NAME]
        if unknown:
            logger.error("Unknown table name(s): %s. Known: %s",
                          ", ".join(unknown), ", ".join(TABLE_SPECS_BY_NAME))
            sys.exit(1)
        specs = [TABLE_SPECS_BY_NAME[t] for t in requested]
    else:
        specs = TABLE_SPECS

    td_db = get_teradata_meta_db()
    pg_schema = get_postgres_schema()
    batch_size = get_batch_size()
    lookback_minutes = get_lookback_minutes()

    logger.info(
        "Starting metadata_sync -- %d table(s), teradata_db=%s pg_schema=%s batch_size=%d "
        "lookback_minutes=%d dry_run=%s",
        len(specs), td_db, pg_schema, batch_size, lookback_minutes, args.dry_run,
    )

    td = TeradataAdapter.build()
    pg = PostgresAdapter.build()

    results = {}
    try:
        for spec in specs:
            name = spec["name"]
            mode = "full_refresh" if args.full_refresh else spec["mode"]
            try:
                if mode == "full_refresh":
                    results[name] = full_refresh_table(td, pg, spec, td_db, pg_schema, batch_size, args.dry_run)
                else:
                    results[name] = incremental_sync_table(
                        td, pg, spec, td_db, pg_schema, batch_size, lookback_minutes, args.dry_run
                    )
            except Exception:
                logger.exception("%s: sync failed -- continuing with remaining tables.", name)
                results[name] = "ERROR"
    finally:
        td.close()
        pg.close()

    logger.info("metadata_sync finished. Results: %s", results)
    if any(v == "ERROR" for v in results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
