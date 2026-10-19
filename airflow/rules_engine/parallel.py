import logging
import queue

from db.connection_factory import FileAdapter, S3Adapter
from rules_engine.config import get_max_parallel_for_connection

logger = logging.getLogger(__name__)


class ConnectionPool:

    def __init__(self, cf, name: str, size: int):
        self.name = name
        self._owned = []
        self._q = queue.Queue()

        for _ in range(max(1, size)):
            adapter = cf.new_connection(name)
            if adapter is None:
                continue
            self._owned.append(adapter)
            self._q.put(adapter)
            if isinstance(adapter, (FileAdapter, S3Adapter)):


                break

        self.available = len(self._owned) > 0

    def acquire(self):
        return self._q.get()

    def release(self, adapter) -> None:
        self._q.put(adapter)

    def close(self) -> None:
        for adapter in self._owned:
            if isinstance(adapter, (FileAdapter, S3Adapter)):
                continue
            try:
                adapter.close()
            except Exception as exc:
                logger.warning("Error closing pooled connection '%s': %s", self.name, exc)


def build_pools(cf, names: set, max_workers: int, cap_override: dict = None) -> dict:
    pools = {}
    for name in names:
        cap = (cap_override or {}).get(name, get_max_parallel_for_connection(name))
        size = min(cap, max_workers)
        pools[name] = ConnectionPool(cf, name, size)
    return pools


def close_pools(pools: dict) -> None:
    for pool in pools.values():
        pool.close()
