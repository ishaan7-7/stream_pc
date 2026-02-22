import time
from collections import OrderedDict
from threading import Lock


class IdempotencyCache:

    def __init__(
        self,
        *,
        max_entries: int = 500_000,
        ttl_seconds: int = 3600,
    ):
        self.max_entries = max_entries
        self.ttl_seconds = ttl_seconds

        self._store = OrderedDict()
        self._lock = Lock()

    def seen_before(self, row_hash: str) -> bool:
        now = time.monotonic()

        with self._lock:
            self._evict_expired(now)

            if row_hash in self._store:
                self._store.move_to_end(row_hash)
                return True

            return False

    def mark_seen(self, row_hash: str) -> None:
        now = time.monotonic()

        with self._lock:
            self._evict_expired(now)

            self._store[row_hash] = now
            self._store.move_to_end(row_hash)

            if len(self._store) > self.max_entries:
                self._store.popitem(last=False)

    def _evict_expired(self, now: float) -> None:
        cutoff = now - self.ttl_seconds

        while self._store:
            _, ts = next(iter(self._store.items()))
            if ts >= cutoff:
                break
            self._store.popitem(last=False)
