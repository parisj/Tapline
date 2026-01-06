from __future__ import annotations

import threading
import time
from collections import deque


class DedupCache:
    """Dedup cache with O(1) membership + amortized O(1) eviction using a deque."""

    def __init__(self, ttl_sec: float = 3600.0) -> None:
        self._ttl_sec = ttl_sec
        self._lock = threading.Lock()
        self._seen: dict[str, float] = {}
        self._q: deque[tuple[float, str]] = deque()

    def seen_recently(self, key: str) -> bool:
        now = time.monotonic()
        with self._lock:
            self._evict_locked(now)
            if key in self._seen:
                return True

            self._seen[key] = now
            self._q.append((now, key))
            return False

    def _evict_locked(self, now: float) -> None:
        cutoff = now - self._ttl_sec
        while self._q and self._q[0][0] < cutoff:
            ts, k = self._q.popleft()
            # Safe only if this queue entry is the current record
            if self._seen.get(k) == ts:
                self._seen.pop(k, None)
