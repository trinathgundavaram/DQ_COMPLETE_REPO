"""
rules_engine/parallel.py tests: ConnectionPool / build_pools() / close_pools()
-- the connection-pooling primitive backing runner.py's opt-in parallel
execution path. No real DB connection required: a tiny fake
ConnectionFactory hands out counting stand-in "adapters" so pool sizing
and the unavailable-connection path can be asserted directly.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rules_engine.parallel import ConnectionPool, build_pools, close_pools
from db.connection_factory import FileAdapter


class _StubAdapter:
    """Minimal stand-in for a db.connection_factory.SourceAdapter -- just tracks close() calls."""
    def __init__(self, name, seq):
        self.name = name
        self.seq = seq
        self.closed = False

    def close(self):
        self.closed = True


class _FakeCF:
    """
    new_connection(name) hands back a FRESH _StubAdapter every call (unless
    `fail_after` caps how many succeed before returning None, simulating a
    connection that can't be opened past some capacity), and counts calls
    per name so pool-sizing behavior can be asserted directly.
    """
    def __init__(self, fail_after=None):
        self.fail_after = fail_after or {}
        self.calls = {}

    def new_connection(self, name):
        n = self.calls.get(name, 0)
        self.calls[name] = n + 1
        limit = self.fail_after.get(name)
        if limit is not None and n >= limit:
            return None
        return _StubAdapter(name, n)


# ── ConnectionPool ──────────────────────────────────────────────────────

def test_pool_builds_requested_size_and_is_available():
    cf = _FakeCF()
    pool = ConnectionPool(cf, "claims_pg", size=3)
    assert pool.available is True
    assert cf.calls["claims_pg"] == 3


def test_pool_acquire_release_round_trips_same_adapters():
    cf = _FakeCF()
    pool = ConnectionPool(cf, "claims_pg", size=2)
    a = pool.acquire()
    b = pool.acquire()
    assert a is not b
    pool.release(a)
    pool.release(b)
    # Both come back out -- nothing lost or duplicated by release().
    seen = {id(pool.acquire()), id(pool.acquire())}
    assert seen == {id(a), id(b)}


def test_pool_unavailable_when_zero_adapters_build():
    cf = _FakeCF(fail_after={"gone": 0})   # every attempt returns None
    pool = ConnectionPool(cf, "gone", size=3)
    assert pool.available is False


def test_pool_partial_build_failure_still_available_with_fewer_slots():
    cf = _FakeCF(fail_after={"flaky": 2})   # first 2 succeed, rest fail
    pool = ConnectionPool(cf, "flaky", size=5)
    assert pool.available is True
    assert cf.calls["flaky"] == 5   # every slot attempted
    # Only the 2 that actually built are in the pool.
    a = pool.acquire()
    b = pool.acquire()
    assert {a.seq, b.seq} == {0, 1}


def test_pool_close_closes_every_owned_adapter():
    cf = _FakeCF()
    pool = ConnectionPool(cf, "claims_pg", size=2)
    adapters = [pool.acquire(), pool.acquire()]
    pool.close()
    assert all(a.closed for a in adapters)


def test_pool_close_never_raises_on_adapter_close_error():
    class _BoomAdapter(_StubAdapter):
        def close(self):
            raise RuntimeError("boom")

    class _BoomCF:
        def new_connection(self, name):
            return _BoomAdapter(name, 0)

    pool = ConnectionPool(_BoomCF(), "claims_pg", size=1)
    pool.close()   # must not raise


def test_pool_shares_single_file_adapter_singleton_not_size_copies():
    file_adapter = FileAdapter.__new__(FileAdapter)   # bypass build(); identity is all that matters here

    class _FileCF:
        def __init__(self):
            self.calls = 0

        def new_connection(self, name):
            self.calls += 1
            return file_adapter

    cf = _FileCF()
    pool = ConnectionPool(cf, "raw_files", size=5)
    assert pool.available is True
    assert cf.calls == 1   # stopped after the first shared instance, not 5
    assert pool.acquire() is file_adapter


def test_pool_close_skips_file_adapter_singleton():
    file_adapter = FileAdapter.__new__(FileAdapter)
    closed = []
    file_adapter.close = lambda: closed.append(True)

    class _FileCF:
        def new_connection(self, name):
            return file_adapter

    pool = ConnectionPool(_FileCF(), "raw_files", size=3)
    pool.close()
    assert closed == []   # ConnectionFactory owns the singleton's lifecycle, not the pool


# ── build_pools / close_pools ────────────────────────────────────────────

def test_build_pools_sizes_by_min_of_configured_cap_and_max_workers(monkeypatch):
    monkeypatch.setenv("GRE_CLAIMS_PG_MAX_PARALLEL", "2")
    monkeypatch.setenv("GRE_TERADATA_MAX_PARALLEL", "10")
    cf = _FakeCF()

    pools = build_pools(cf, {"claims_pg", "teradata"}, max_workers=5)

    assert cf.calls["claims_pg"] == 2    # capped by GRE_CLAIMS_PG_MAX_PARALLEL, below max_workers
    assert cf.calls["teradata"] == 5     # capped by max_workers, below GRE_TERADATA_MAX_PARALLEL
    close_pools(pools)


def test_build_pools_one_pool_per_name():
    cf = _FakeCF()
    pools = build_pools(cf, {"a", "b", "c"}, max_workers=2)
    assert set(pools.keys()) == {"a", "b", "c"}
    close_pools(pools)


def test_close_pools_closes_every_pool(monkeypatch):
    # Default per-connection cap is 1 -- raise it so 2 acquire()s per pool
    # don't block waiting for a slot that was never released.
    monkeypatch.setenv("GRE_A_MAX_PARALLEL", "2")
    monkeypatch.setenv("GRE_B_MAX_PARALLEL", "2")
    cf = _FakeCF()
    pools = build_pools(cf, {"a", "b"}, max_workers=2)
    adapters_a = [pools["a"].acquire() for _ in range(2)]
    adapters_b = [pools["b"].acquire() for _ in range(2)]

    close_pools(pools)

    assert all(a.closed for a in adapters_a)
    assert all(a.closed for a in adapters_b)
