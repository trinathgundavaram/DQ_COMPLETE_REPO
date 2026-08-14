"""
rules_engine/rules.py
------------------------
Rule config loader. One job: turn `gre_rules` rows for a rule_group into a
list of plain dicts, ordered the way a run needs them.

Kept separate from rules_engine/executor.py (which runs a rule) and
rules_engine/runner.py (which decides run order/sequencing behaviour) so
"what rules exist for this group" is testable in isolation from "how do we
run them."

The query itself is run through shared.db_ops.execute_query() rather than
a second hand-rolled cursor->fetchall->dict-zip loop -- there is exactly
one place in this engine that knows how to turn a DB-API cursor into a
list of lowercase-keyed dicts, and this module reuses it instead of
duplicating it.
"""

import logging

from shared.db_ops import execute_query

logger = logging.getLogger(__name__)


def load_rules(meta_conn, meta_db: str, rule_group: str) -> list:
    """
    Load active rules for `rule_group`, ordered by seq_no then rule_id.

    seq_no ordering is applied unconditionally here -- it's a cheap, stable
    sort that only matters when sequencing_mode='sequential' for the group,
    and is harmless (a no-op ordering preference) for 'independent' groups.

    Returns a list of dicts (column names lowercased), matching the
    row-as-dict convention used throughout this engine.
    """
    sql = f"""
        SELECT *
        FROM {meta_db}.gre_rules
        WHERE rule_group  = ?
          AND active_flag = 1
        ORDER BY seq_no ASC, rule_id ASC
    """
    rows = execute_query(meta_conn, sql, [rule_group])
    logger.info("Loaded %d active rule(s) for rule_group=%s.", len(rows), rule_group)
    return rows
