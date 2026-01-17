import threading
import time

import pytest

from src.ingest.dedup import DedupCache  # adjust import as needed


def test_first_seen_returns_false() -> None:
    cache = DedupCache(ttl_sec=10.0)

    assert cache.seen_recently("key") is False


def test_second_seen_returns_true() -> None:
    cache = DedupCache(ttl_sec=10.0)

    cache.seen_recently("key")
    assert cache.seen_recently("key") is True


def test_different_keys_are_tracked_independently() -> None:
    cache = DedupCache(ttl_sec=10.0)

    assert cache.seen_recently("a") is False
    assert cache.seen_recently("b") is False
    assert cache.seen_recently("a") is True
    assert cache.seen_recently("b") is True


def test_key_expires_after_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_time = 100.0

    def monotonic() -> float:
        return fake_time

    monkeypatch.setattr(time, "monotonic", monotonic)

    cache = DedupCache(ttl_sec=10.0)

    # First seen
    assert cache.seen_recently("key") is False

    # Still within TTL
    fake_time += 5.0
    assert cache.seen_recently("key") is True

    # Beyond TTL
    fake_time += 6.0
    assert cache.seen_recently("key") is False


def test_gc_removes_only_expired_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_time = 100.0

    def monotonic() -> float:
        return fake_time

    monkeypatch.setattr(time, "monotonic", monotonic)

    cache = DedupCache(ttl_sec=10.0)

    cache.seen_recently("old")
    fake_time += 5.0
    cache.seen_recently("new")

    # Advance beyond TTL for "old" only
    fake_time += 6.0

    assert cache.seen_recently("old") is False
    assert cache.seen_recently("new") is True


def test_thread_safety_basic_smoke_test() -> None:
    """Basic smoke test for thread safety."""
    cache = DedupCache(ttl_sec=10.0)
    results = []

    def worker() -> None:
        results.append(cache.seen_recently("key"))

    threads = [threading.Thread(target=worker) for _ in range(20)]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Exactly one thread should observe False, the rest True
    assert results.count(False) == 1
    assert results.count(True) == 19
