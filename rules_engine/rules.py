"""
rules_engine/rules.py
------------------------
Rule config loader. One job: turn `gre_rules` rows for a rule_group into a
list of plain dicts, ordered the way a run needs them.

Kept separate from rules_engine/executor.py (which runs a rule) and
rules_engine/runner.py (which decides run order/sequencing behaviour) so
"what rules exist for this group" is testable in isolation from "how do we
run them."

The query itself is run through rules_engine.db_ops.execute_query() rather than
a second hand-rolled cursor->fetchall->dict-zip loop -- there is exactly
one place in this engine that knows how to turn a DB-API cursor into a
list of lowercase-keyed dicts, and this module reuses it instead of
duplicating it.
"""

import logging

from rules_engine.config import resolve_database_name
from rules_engine.db_ops import execute_query

logger = logging.getLogger(__name__)


def load_rules(meta_conn, meta_db: str, rule_group: str, rule_variant: str = None) -> list:
    """
    Load active rules for `rule_group`, ordered by seq_no then rule_id.

    rule_variant adds one generic hierarchical level on top of
    project/table (rule_group): within one rule_group, gre_rules.rule_variant
    IS NULL means "always applies" (universal), and a rule with an explicit
    rule_variant value only applies when the caller passes that exact value
    here -- e.g. a project whose rules differ by year, run_type, or any
    other criterion sets rule_variant on just the rules that differ, and
    leaves the rest NULL. A project needing more than one dimension at once
    composes a single string (e.g. "2026|MONTHLY") -- there's no separate
    hardcoded year_column/run_type_column, consistent with this engine
    having no filter_column system anywhere else (see schema.sql's header).

    Two explicit query shapes instead of one with a NULL bind parameter --
    `rule_variant = ?` never matches when the bind value is NULL (SQL NULL
    comparison semantics), so a single parameterized query can't express
    "match NULL or this value" without a database-specific NULL-safe
    operator. Branching in Python keeps this portable across every source
    dialect this engine already supports.

    seq_no ordering is applied unconditionally here -- it's a cheap, stable
    sort that only matters when sequencing_mode='sequential' for the group,
    and is harmless (a no-op ordering preference) for 'independent' groups.

    Returns a list of dicts (column names lowercased), matching the
    row-as-dict convention used throughout this engine. Every row carries
    project_name/process_name straight through from gre_rules (SELECT *) --
    they're descriptive/reporting dimensions, not part of this lookup's own
    filter (see rules_engine/schema.sql's design notes); rules_engine/
    runner.py reads them off these dicts to stamp gre_rule_audit/
    gre_exceptions/gre_results for the run.
    """
    if rule_variant:
        sql = f"""
            SELECT *
            FROM {meta_db}.gre_rules
            WHERE rule_group   = ?
              AND act_ind      = 1
              AND (rule_variant IS NULL OR rule_variant = ?)
            ORDER BY seq_no ASC, rule_id ASC
        """
        params = [rule_group, rule_variant]
    else:
        sql = f"""
            SELECT *
            FROM {meta_db}.gre_rules
            WHERE rule_group   = ?
              AND act_ind      = 1
              AND rule_variant IS NULL
            ORDER BY seq_no ASC, rule_id ASC
        """
        params = [rule_group]

    rows = execute_query(meta_conn, sql, params)

    # Resolve each rule's AUTHORED database_name to whatever this process's
    # environment (GRE_ENVIRONMENT) actually has that source data in -- see
    # rules_engine/config.py::resolve_database_name()'s docstring. A no-op
    # for any rule whose database_name has no GRE_DB_MAP_* configured, so
    # this is safe to run unconditionally for every row. Every downstream
    # consumer of these dicts (db/connection_factory.py's qualified_name(),
    # rules_engine/executor.py's total-count query and
    # build_source_tieback_sql()) reads database_name off THESE already-
    # resolved dicts, so nothing else in the engine needs to know this
    # resolution happened.
    for row in rows:
        row["database_name"] = resolve_database_name(row.get("database_name"))

    logger.info(
        "Loaded %d active rule(s) for rule_group=%s%s.",
        len(rows), rule_group, f" rule_variant={rule_variant}" if rule_variant else "",
    )
    return rows
