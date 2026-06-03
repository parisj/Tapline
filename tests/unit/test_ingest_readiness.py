"""Unit tests for ingest readiness check.

Tests the file-readiness state machine that waits for a file's size+mtime to
stabilise and the file to be openable before declaring it ready.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pytest

from src.ingest import readiness as readiness_module
from src.ingest.readiness import wait_for_file_ready

if TYPE_CHECKING:
    from pathlib import Path


class FakeClock:
    """Drives monotonic time and tracks sleeps to fast-forward simulated time."""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def fake_clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    clock = FakeClock()
    monkeypatch.setattr(readiness_module.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(readiness_module.time, "sleep", clock.sleep)
    return clock


class TestWaitForFileReady:
    """Tests for wait_for_file_ready."""

    def test_returns_true_for_stable_existing_file(
        self,
        tmp_path: Path,
        fake_clock: FakeClock,
    ) -> None:
        path = tmp_path / "stable.txt"
        path.write_text("content")

        # stable_window=0 means the very next iteration with matching
        # size+mtime will succeed
        assert wait_for_file_ready(path, stable_window_sec=0.0, max_wait_sec=5.0) is True

    def test_returns_false_for_missing_file(
        self,
        tmp_path: Path,
        fake_clock: FakeClock,
    ) -> None:
        missing = tmp_path / "does_not_exist.txt"

        # File never appears within max_wait_sec
        assert wait_for_file_ready(missing, stable_window_sec=0.1, max_wait_sec=0.5) is False

    def test_waits_for_size_to_stabilise(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        path = tmp_path / "growing.txt"
        path.write_bytes(b"abc")

        clock = FakeClock()
        # On the first sleep call, mutate the file so the next iteration sees
        # a new size; afterwards the file is stable.
        mutation_count = {"n": 0}

        def mutating_sleep(seconds: float) -> None:
            clock.sleeps.append(seconds)
            clock.now += seconds
            mutation_count["n"] += 1
            if mutation_count["n"] == 1:
                path.write_bytes(b"abcdef")  # grow once

        monkeypatch.setattr(readiness_module.time, "monotonic", clock.monotonic)
        monkeypatch.setattr(readiness_module.time, "sleep", mutating_sleep)

        # File "grows" once, then stabilises -> should still succeed
        result = wait_for_file_ready(path, stable_window_sec=0.0, max_wait_sec=5.0)
        assert result is True
        # At least 2 iterations happened: pre-grow + post-grow
        assert mutation_count["n"] >= 1

    def test_times_out_when_file_keeps_changing(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        path = tmp_path / "changing.txt"
        path.write_bytes(b"abc")

        clock = FakeClock()
        counter = {"n": 0}

        def mutating_sleep(seconds: float) -> None:
            clock.sleeps.append(seconds)
            clock.now += seconds
            counter["n"] += 1
            # Grow file each iteration so size never matches
            path.write_bytes(b"abc" + b"x" * counter["n"])

        monkeypatch.setattr(readiness_module.time, "monotonic", clock.monotonic)
        monkeypatch.setattr(readiness_module.time, "sleep", mutating_sleep)

        # Needs stable_window > poll cadence and a tight deadline so the
        # constantly-growing file never stabilises.
        result = wait_for_file_ready(path, stable_window_sec=0.5, max_wait_sec=0.2)
        assert result is False

    def test_zero_max_wait_returns_false(
        self,
        tmp_path: Path,
        fake_clock: FakeClock,
    ) -> None:
        path = tmp_path / "x.txt"
        path.write_text("x")

        # deadline = now + 0 -> loop condition fails immediately
        assert wait_for_file_ready(path, stable_window_sec=0.1, max_wait_sec=0.0) is False

    def test_requires_stable_window_to_elapse(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        path = tmp_path / "stable_window.txt"
        path.write_bytes(b"abc")

        clock = FakeClock()
        monkeypatch.setattr(readiness_module.time, "monotonic", clock.monotonic)
        monkeypatch.setattr(readiness_module.time, "sleep", clock.sleep)

        # stable_window=1.0, sleeps of 0.01 each -> needs ~100 iterations
        # before declaring ready. With max_wait_sec=5 there's plenty of room.
        result = wait_for_file_ready(path, stable_window_sec=1.0, max_wait_sec=5.0)
        assert result is True
        # Several sleeps should have occurred while waiting for the window
        assert len(clock.sleeps) >= 2

    def test_file_appearing_after_initial_miss(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        path = tmp_path / "delayed.txt"
        # File deliberately does not exist yet

        clock = FakeClock()
        # The first sleep call corresponds to the FileNotFoundError branch
        # (sleep 0.02); on that sleep, create the file so subsequent stat
        # calls succeed.
        creation_count = {"n": 0}

        def creating_sleep(seconds: float) -> None:
            clock.sleeps.append(seconds)
            clock.now += seconds
            creation_count["n"] += 1
            if creation_count["n"] == 1:
                path.write_text("now here")

        monkeypatch.setattr(readiness_module.time, "monotonic", clock.monotonic)
        monkeypatch.setattr(readiness_module.time, "sleep", creating_sleep)

        result = wait_for_file_ready(path, stable_window_sec=0.0, max_wait_sec=5.0)
        assert result is True
        # The initial FileNotFoundError branch sleeps 0.02
        assert 0.02 in clock.sleeps

    def test_open_failure_does_not_immediately_succeed(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """If open() fails (OSError), the loop keeps trying until deadline."""
        # Use a chmod-based approach: file exists, but first open raises OSError
        # via a wrapper on the specific path's bound method.
        path = tmp_path / "stable.txt"
        path.write_text("content")

        clock = FakeClock()
        monkeypatch.setattr(readiness_module.time, "monotonic", clock.monotonic)
        monkeypatch.setattr(readiness_module.time, "sleep", clock.sleep)

        target_str = str(path)
        path_type = type(path)
        original_open = path_type.open
        open_calls = {"n": 0}

        def fake_open(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            if str(self) == target_str:
                open_calls["n"] += 1
                if open_calls["n"] == 1:
                    msg = "locked"
                    raise OSError(msg)
            return original_open(self, *args, **kwargs)

        monkeypatch.setattr(path_type, "open", fake_open)

        result = wait_for_file_ready(path, stable_window_sec=0.0, max_wait_sec=5.0)
        # Should still eventually succeed since the second open works
        assert result is True
        assert open_calls["n"] >= 2

    def test_actually_uses_time_monotonic(
        self,
        tmp_path: Path,
    ) -> None:
        """Smoke test that without monkeypatching the real clock is used."""
        path = tmp_path / "real.txt"
        path.write_text("x")

        start = time.monotonic()
        result = wait_for_file_ready(path, stable_window_sec=0.0, max_wait_sec=5.0)
        elapsed = time.monotonic() - start
        assert result is True
        # Should be fast since file already exists and is stable
        assert elapsed < 5.0
