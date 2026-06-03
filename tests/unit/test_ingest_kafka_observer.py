"""Unit tests for KafkaDirectoryObserver.

The observer scans configured directories, dedups files, and publishes
JOB_CREATED events to an EventProducer. All Kafka I/O is mocked; tests run
the internal _scan_dir method directly to avoid threading.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from src.ingest import kafka_observer as kafka_observer_module
from src.ingest.kafka_observer import KafkaDirectoryObserver

if TYPE_CHECKING:
    from pathlib import Path


def _make_observer(
    directories: dict[str, Path],
    *,
    producer: MagicMock | None = None,
    dedup: MagicMock | None = None,
    allowed_exts: set[str] | None = None,
) -> tuple[KafkaDirectoryObserver, MagicMock, MagicMock]:
    if producer is None:
        producer = MagicMock()
    if dedup is None:
        dedup = MagicMock()
        dedup.seen_recently.return_value = False
    if allowed_exts is None:
        allowed_exts = {".png", ".jpg"}

    observer = KafkaDirectoryObserver(
        directories=directories,
        producer=producer,
        dedup=dedup,
        allowed_exts=allowed_exts,
        poll_interval_sec=0.01,
        max_wait_sec=0.5,
        stable_window_sec=0.0,
    )
    return observer, producer, dedup


@pytest.fixture(autouse=True)
def _bypass_readiness(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make wait_for_file_ready a no-op success for all tests in this module."""
    monkeypatch.setattr(
        kafka_observer_module,
        "wait_for_file_ready",
        lambda **_kwargs: True,
    )


class TestKafkaDirectoryObserverInit:
    """Tests for constructor & initial state."""

    def test_init_sets_attributes(self, tmp_path: Path) -> None:
        observer, _, _ = _make_observer({"inbox": tmp_path})

        # Stats start at zero
        stats = observer.stats
        assert stats["jobs_published"] == 0
        assert stats["files_skipped"] == 0
        assert stats["directories_watched"] == 1

    def test_multiple_directories_counted(self, tmp_path: Path) -> None:
        d1 = tmp_path / "a"
        d2 = tmp_path / "b"
        d1.mkdir()
        d2.mkdir()

        observer, _, _ = _make_observer({"a": d1, "b": d2})

        assert observer.stats["directories_watched"] == 2


class TestKafkaDirectoryObserverScanDir:
    """Tests for the internal _scan_dir scanning logic."""

    def test_missing_directory_logs_and_returns(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        missing = tmp_path / "missing"
        observer, producer, _ = _make_observer({"inbox": missing})

        # Should not raise
        observer._scan_dir("inbox", missing)

        producer.publish_job_created.assert_not_called()
        assert observer.stats["jobs_published"] == 0

    def test_publishes_for_new_file_with_allowed_ext(self, tmp_path: Path) -> None:
        file = tmp_path / "image.png"
        file.write_bytes(b"some image data")

        observer, producer, dedup = _make_observer({"inbox": tmp_path})

        observer._scan_dir("inbox", tmp_path)

        assert producer.publish_job_created.call_count == 1
        kwargs = producer.publish_job_created.call_args.kwargs
        assert kwargs["source_id"] == "inbox"
        assert kwargs["directory_key"] == "inbox"
        assert kwargs["path"] == str(file)
        assert isinstance(kwargs["fingerprint"], str)
        assert len(kwargs["fingerprint"]) == 64
        assert isinstance(kwargs["task_id"], str)
        assert len(kwargs["task_id"]) == 64
        assert isinstance(kwargs["file_hash"], str)
        assert len(kwargs["file_hash"]) == 64
        assert observer.stats["jobs_published"] == 1
        assert observer.stats["files_skipped"] == 0
        dedup.seen_recently.assert_called_once()

    def test_skips_files_with_disallowed_extension(self, tmp_path: Path) -> None:
        (tmp_path / "doc.txt").write_text("not an image")

        observer, producer, _ = _make_observer({"inbox": tmp_path})

        observer._scan_dir("inbox", tmp_path)

        producer.publish_job_created.assert_not_called()
        assert observer.stats["jobs_published"] == 0

    def test_skips_subdirectories(self, tmp_path: Path) -> None:
        sub = tmp_path / "sub"
        sub.mkdir()

        observer, producer, _ = _make_observer({"inbox": tmp_path})

        observer._scan_dir("inbox", tmp_path)

        producer.publish_job_created.assert_not_called()

    def test_recurses_into_subdirectories(self, tmp_path: Path) -> None:
        sub = tmp_path / "nested"
        sub.mkdir()
        nested_file = sub / "deep.png"
        nested_file.write_bytes(b"x")

        observer, producer, _ = _make_observer({"inbox": tmp_path})

        observer._scan_dir("inbox", tmp_path)

        assert producer.publish_job_created.call_count == 1
        assert producer.publish_job_created.call_args.kwargs["path"] == str(nested_file)

    def test_does_not_republish_unchanged_file_on_second_scan(
        self,
        tmp_path: Path,
    ) -> None:
        file = tmp_path / "img.png"
        file.write_bytes(b"data")

        observer, producer, _ = _make_observer({"inbox": tmp_path})

        observer._scan_dir("inbox", tmp_path)
        observer._scan_dir("inbox", tmp_path)

        # Only the first scan should publish; second scan sees unchanged mtime
        assert producer.publish_job_created.call_count == 1

    def test_dedup_hit_increments_files_skipped(self, tmp_path: Path) -> None:
        file = tmp_path / "img.png"
        file.write_bytes(b"data")

        dedup = MagicMock()
        dedup.seen_recently.return_value = True

        observer, producer, _ = _make_observer({"inbox": tmp_path}, dedup=dedup)

        observer._scan_dir("inbox", tmp_path)

        producer.publish_job_created.assert_not_called()
        assert observer.stats["files_skipped"] == 1
        assert observer.stats["jobs_published"] == 0

    def test_readiness_failure_skips_file(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        file = tmp_path / "img.png"
        file.write_bytes(b"data")

        # Override the auto fixture: simulate readiness check failing
        monkeypatch.setattr(
            kafka_observer_module,
            "wait_for_file_ready",
            lambda **_kwargs: False,
        )

        observer, producer, _ = _make_observer({"inbox": tmp_path})

        observer._scan_dir("inbox", tmp_path)

        producer.publish_job_created.assert_not_called()
        assert observer.stats["jobs_published"] == 0

    def test_file_disappears_between_glob_and_stat(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        file = tmp_path / "ghost.png"
        file.write_bytes(b"data")

        path_type = type(file)
        original_stat = path_type.stat
        ghost_stat_calls = {"n": 0}

        def fake_stat(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            if str(self).endswith("ghost.png"):
                ghost_stat_calls["n"] += 1
                # First stat is from p.is_file() — let it succeed.
                # The subsequent stat in _scan_dir should raise.
                if ghost_stat_calls["n"] >= 2:
                    raise FileNotFoundError
            return original_stat(self, *args, **kwargs)

        monkeypatch.setattr(path_type, "stat", fake_stat)

        observer, producer, _ = _make_observer({"inbox": tmp_path})

        # Should not raise; the file is silently skipped via the
        # `except FileNotFoundError: continue` path in _scan_dir.
        observer._scan_dir("inbox", tmp_path)

        producer.publish_job_created.assert_not_called()
        assert ghost_stat_calls["n"] >= 2

    def test_modified_file_is_republished(self, tmp_path: Path) -> None:
        import os
        import time as time_mod

        file = tmp_path / "img.png"
        file.write_bytes(b"data")

        observer, producer, _ = _make_observer({"inbox": tmp_path})

        observer._scan_dir("inbox", tmp_path)
        assert producer.publish_job_created.call_count == 1

        # Bump mtime forward to simulate modification
        new_time = time_mod.time() + 60.0
        os.utime(file, (new_time, new_time))

        observer._scan_dir("inbox", tmp_path)

        assert producer.publish_job_created.call_count == 2

    def test_extension_check_is_case_insensitive(self, tmp_path: Path) -> None:
        file = tmp_path / "img.PNG"
        file.write_bytes(b"data")

        observer, producer, _ = _make_observer({"inbox": tmp_path})

        observer._scan_dir("inbox", tmp_path)

        assert producer.publish_job_created.call_count == 1


class TestComputeFileHash:
    """Tests for the internal _compute_file_hash helper."""

    def test_hash_is_64_hex_chars(self, tmp_path: Path) -> None:
        file = tmp_path / "x.png"
        file.write_bytes(b"hello world")

        observer, _, _ = _make_observer({"inbox": tmp_path})

        h = observer._compute_file_hash(file)
        assert isinstance(h, str)
        assert len(h) == 64
        int(h, 16)  # Valid hex

    def test_hash_is_deterministic(self, tmp_path: Path) -> None:
        file = tmp_path / "x.png"
        file.write_bytes(b"contents")

        observer, _, _ = _make_observer({"inbox": tmp_path})

        assert observer._compute_file_hash(file) == observer._compute_file_hash(file)

    def test_different_contents_yield_different_hash(self, tmp_path: Path) -> None:
        f1 = tmp_path / "a.png"
        f2 = tmp_path / "b.png"
        f1.write_bytes(b"one")
        f2.write_bytes(b"two")

        observer, _, _ = _make_observer({"inbox": tmp_path})

        assert observer._compute_file_hash(f1) != observer._compute_file_hash(f2)


class TestKafkaDirectoryObserverLifecycle:
    """Tests for start/stop/_run lifecycle."""

    def test_stop_sets_event(self, tmp_path: Path) -> None:
        observer, _, _ = _make_observer({"inbox": tmp_path})

        assert observer._stop.is_set() is False
        observer.stop()
        assert observer._stop.is_set() is True

    def test_run_exits_when_stopped_and_flushes_producer(
        self,
        tmp_path: Path,
    ) -> None:
        observer, producer, _ = _make_observer({"inbox": tmp_path})

        observer.stop()  # signal stop before _run starts
        observer._run()  # should exit loop immediately and flush

        producer.flush.assert_called_once()

    def test_run_processes_files_then_flushes(self, tmp_path: Path) -> None:
        file = tmp_path / "x.png"
        file.write_bytes(b"data")

        observer, producer, _ = _make_observer({"inbox": tmp_path})

        # Schedule stop to occur after first scan: monkeypatch poll sleep
        scan_count = {"n": 0}
        original_scan = observer._scan_dir

        def wrapped_scan(*args, **kwargs):  # type: ignore[no-untyped-def]
            original_scan(*args, **kwargs)
            scan_count["n"] += 1
            if scan_count["n"] >= 1:
                observer.stop()

        observer._scan_dir = wrapped_scan  # type: ignore[method-assign]

        observer._run()

        assert producer.publish_job_created.call_count == 1
        producer.flush.assert_called_once()


class TestStatsProperty:
    """Tests for the stats property."""

    def test_stats_reflects_state(self, tmp_path: Path) -> None:
        observer, _, _ = _make_observer({"inbox": tmp_path})

        stats = observer.stats
        assert set(stats.keys()) == {
            "jobs_published",
            "files_skipped",
            "directories_watched",
        }
        assert all(isinstance(v, int) for v in stats.values())

    def test_stats_returns_new_dict(self, tmp_path: Path) -> None:
        observer, _, _ = _make_observer({"inbox": tmp_path})

        s1 = observer.stats
        s2 = observer.stats
        # Each call returns a fresh dict
        assert s1 == s2
        assert s1 is not s2
