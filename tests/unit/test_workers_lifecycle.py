"""Unit tests for AlgoLifecycle - process-scoped algorithm initialization."""

from __future__ import annotations

import contextlib
import threading
from unittest.mock import MagicMock

import pytest

from src.dispatch.dispatcher import DispatchPlan
from src.workers.lifecycle import AlgoLifecycle


def _make_plan(
    name: str = "proc",
    version: str = "1.0",
    settings: dict | None = None,
) -> DispatchPlan:
    """Helper to build a DispatchPlan with a MagicMock processor."""
    processor = MagicMock()
    processor.name = name
    processor.version = version
    return DispatchPlan(processor=processor, settings=settings or {})


class TestAlgoLifecycle:
    def test_initial_state_is_empty(self) -> None:
        lifecycle = AlgoLifecycle()

        assert lifecycle._initialized == set()

    def test_ensure_initialized_calls_initialize_first_time(self) -> None:
        lifecycle = AlgoLifecycle()
        plan = _make_plan(name="alg_a", version="1.0", settings={"k": "v"})

        lifecycle.ensure_initialized(plan)

        plan.processor.initialize.assert_called_once_with(plan.settings)

    def test_initialized_set_tracks_name_version_key(self) -> None:
        lifecycle = AlgoLifecycle()
        plan = _make_plan(name="alg_a", version="1.0")

        lifecycle.ensure_initialized(plan)

        assert ("alg_a", "1.0") in lifecycle._initialized

    def test_idempotent_does_not_initialize_twice(self) -> None:
        lifecycle = AlgoLifecycle()
        plan = _make_plan(name="alg_a", version="1.0")

        lifecycle.ensure_initialized(plan)
        lifecycle.ensure_initialized(plan)
        lifecycle.ensure_initialized(plan)

        assert plan.processor.initialize.call_count == 1

    def test_different_versions_initialized_separately(self) -> None:
        lifecycle = AlgoLifecycle()
        plan_v1 = _make_plan(name="alg_a", version="1.0")
        plan_v2 = _make_plan(name="alg_a", version="2.0")

        lifecycle.ensure_initialized(plan_v1)
        lifecycle.ensure_initialized(plan_v2)

        plan_v1.processor.initialize.assert_called_once()
        plan_v2.processor.initialize.assert_called_once()
        assert ("alg_a", "1.0") in lifecycle._initialized
        assert ("alg_a", "2.0") in lifecycle._initialized

    def test_different_names_initialized_separately(self) -> None:
        lifecycle = AlgoLifecycle()
        plan_a = _make_plan(name="alg_a", version="1.0")
        plan_b = _make_plan(name="alg_b", version="1.0")

        lifecycle.ensure_initialized(plan_a)
        lifecycle.ensure_initialized(plan_b)

        plan_a.processor.initialize.assert_called_once()
        plan_b.processor.initialize.assert_called_once()
        assert ("alg_a", "1.0") in lifecycle._initialized
        assert ("alg_b", "1.0") in lifecycle._initialized

    def test_initialize_propagates_exception(self) -> None:
        lifecycle = AlgoLifecycle()
        plan = _make_plan(name="alg_a", version="1.0")
        plan.processor.initialize.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            lifecycle.ensure_initialized(plan)

        # Failed init should NOT mark as initialized (so retry is possible)
        assert ("alg_a", "1.0") not in lifecycle._initialized

    def test_retry_after_failed_initialize(self) -> None:
        lifecycle = AlgoLifecycle()
        plan = _make_plan(name="alg_a", version="1.0")
        plan.processor.initialize.side_effect = [RuntimeError("boom"), None]

        with contextlib.suppress(RuntimeError):
            lifecycle.ensure_initialized(plan)

        # Second call should retry since first didn't mark initialized
        lifecycle.ensure_initialized(plan)

        assert plan.processor.initialize.call_count == 2
        assert ("alg_a", "1.0") in lifecycle._initialized

    def test_passes_plan_settings_to_initialize(self) -> None:
        lifecycle = AlgoLifecycle()
        settings = {"threshold": 0.5, "mode": "fast"}
        plan = _make_plan(name="alg_a", version="1.0", settings=settings)

        lifecycle.ensure_initialized(plan)

        plan.processor.initialize.assert_called_once_with(settings)

    def test_thread_safety_only_initializes_once(self) -> None:
        """Under concurrent calls, initialize() should run exactly once."""
        lifecycle = AlgoLifecycle()
        plan = _make_plan(name="alg_a", version="1.0")
        # Add small artificial wait to widen race window
        barrier = threading.Barrier(10)

        def slow_init(_settings: dict) -> None:
            # Block until all threads have entered ensure_initialized
            pass

        plan.processor.initialize.side_effect = slow_init

        def worker() -> None:
            barrier.wait()
            lifecycle.ensure_initialized(plan)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Even with 10 concurrent callers, initialize must run exactly once
        assert plan.processor.initialize.call_count == 1
        assert ("alg_a", "1.0") in lifecycle._initialized

    def test_separate_lifecycle_instances_are_independent(self) -> None:
        lifecycle_a = AlgoLifecycle()
        lifecycle_b = AlgoLifecycle()
        plan = _make_plan(name="alg_a", version="1.0")

        lifecycle_a.ensure_initialized(plan)
        lifecycle_b.ensure_initialized(plan)

        # Each lifecycle tracks initialization independently
        assert plan.processor.initialize.call_count == 2
        assert ("alg_a", "1.0") in lifecycle_a._initialized
        assert ("alg_a", "1.0") in lifecycle_b._initialized
