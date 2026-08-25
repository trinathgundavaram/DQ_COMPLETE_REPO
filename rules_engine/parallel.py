"""
rules_engine/parallel.py
--------------------------
The bounded per-connection connection pool backing runner.py's parallel
execution path for sequencing_mode='independent' rule groups (see
run_rule_group()'s "PARALLEL PATH" section). Kept in its own file so
runner.py's default, single-threaded path -- still the only path that runs
unless GRE_MAX_PARALLEL_RULES is explicitly raised -- is unaffected by any
of this.

Why a pool, not just cf.get() per rule
-----------------------------------------
cf.get(name) returns ONE cached adapter shared by every caller. That's
correct and efficient for the sequential path (one rule executes at a
time, so nothing ever touches that adapter from two places at once), but
it's actively unsafe once two rules can run concurrently: most DB-API
connections/cursors are not safe for simultaneous use by multiple threads
(see db/connection_factory.py::new_connection()'s own docstring, written
for exactly this scenario -- "so concurrent writes never share state").

So the parallel path never calls cf.get() for a connection a worker thread
will run a query on. Instead, ConnectionPool below builds up to `size`
INDEPENDENT adapters for one named connection via cf.new_connection(name)
-- a real, separate connection per pool slot -- and hands them out to
whichever worker asks first, via a queue.Queue whose blocking get() is
what actually enforces "no more than `size` concurrent sessions against
this connection," with no extra locking needed.

FileAdapter/S3Adapter are the one exception: cf.new_connection() already
returns the SAME shared singleton for those every time (their per-thread
DuckDB connections are internally thread-safe already), so building `size`
copies would be pointless -- the pool just holds that one shared instance.

Lifecycle
---------
A pool is built once per distinct connection name at the start of a
parallel run_rule_group() call and closed once, in a finally block, when
every rule in that call has finished -- see build_pools()/close_pools()
below. Adapters obtained via cf.get() are never touched here and are
never closed (ConnectionFactory owns that lifecycle); only adapters THIS
pool built via cf.new_connection() get closed, and FileAdapter/S3Adapter
instances are skipped even then, for the same shared-singleton reason.
"""

import logging
import queue

from db.connection_factory import FileAdapter, S3Adapter
from rules_engine.config import get_max_parallel_for_connection

logger = logging.getLogger(__name__)


class ConnectionPool:
    """
    Up to `size` independent adapters for one named connection (built via
    cf.new_connection(name)), handed out through a blocking queue.

    `available` is False when not even one adapter could be built (e.g.
    the connection isn't configured, or every build attempt errored) --
    callers check this BEFORE submitting any rule that needs this
    connection, and treat it exactly like today's `db_conn is None` check
    in the sequential path (CONNECTION_UNAVAILABLE, logged, rule marked
    ERROR, never submitted to the pool).
    """

    def __init__(self, cf, name: str, size: int):
        self.name = name
        self._owned = []          # adapters THIS pool built -- only these get closed
        self._q = queue.Queue()

        for _ in range(max(1, size)):
            adapter = cf.new_connection(name)
            if adapter is None:
                continue
            self._owned.append(adapter)
            self._q.put(adapter)
            if isinstance(adapter, (FileAdapter, S3Adapter)):
                # new_connection() always hands back the SAME shared
                # singleton for these -- one slot is enough; queuing more
                # would just let several workers pull the identical
                # object (harmless, since it's internally thread-safe,
                # but adds nothing).
                break

        self.available = len(self._owned) > 0

    def acquire(self):
        """Block until a connection is free, then return it."""
        return self._q.get()

    def release(self, adapter) -> None:
        """Return a connection to the pool for the next waiting worker."""
        self._q.put(adapter)

    def close(self) -> None:
        """Close every adapter this pool built (skips shared File/S3 singletons)."""
        for adapter in self._owned:
            if isinstance(adapter, (FileAdapter, S3Adapter)):
                continue
            try:
                adapter.close()
            except Exception as exc:
                logger.warning("Error closing pooled connection '%s': %s", self.name, exc)


def build_pools(cf, names: set, max_workers: int, cap_override: dict = None) -> dict:
    """
    One ConnectionPool per name in `names`, each sized to the SMALLER of
    that connection's own configured cap (GRE_<TYPE>_MAX_PARALLEL, or
    cap_override[name] when given) and max_workers -- no point building
    more slots for a connection than the group-wide worker count could
    ever use concurrently anyway.

    cap_override lets a caller building TWO separate pools for the SAME
    underlying connection name (e.g. runner.py::_run_pending_parallel(),
    when a rule's sql_dialect and the metadata connection share a name)
    shrink each pool's share of that connection's cap, so the two pools'
    sizes stay additive within the real GRE_<TYPE>_MAX_PARALLEL limit
    instead of each independently maxing out and doubling real concurrent
    sessions against that one source.
    """
    pools = {}
    for name in names:
        cap = (cap_override or {}).get(name, get_max_parallel_for_connection(name))
        size = min(cap, max_workers)
        pools[name] = ConnectionPool(cf, name, size)
    return pools


def close_pools(pools: dict) -> None:
    """Close every pool built by build_pools(). Never raises -- see ConnectionPool.close()."""
    for pool in pools.values():
        pool.close()
