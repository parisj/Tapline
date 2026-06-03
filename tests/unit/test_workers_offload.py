"""Unit tests for offload strategies (LocalWorkerStrategy + dataclasses)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.domain.results import ProcessorResult
from src.domain.tasks import Task
from src.workers.offload import (
    ExecutionContext,
    ExecutionResult,
    LocalWorkerStrategy,
)


def _make_task(task_id: str = "task-123") -> Task:
    return Task(
        task_id=task_id,
        directory_key="inbox",
        path="/tmp/file.png",
        created_at_unix=1704067200.0,
        fingerprint="abc",
    )


def _make_algo(
    name: str = "proc",
    version: str = "1.0",
    run_result: object | None = None,
    run_side_effect: BaseException | None = None,
) -> MagicMock:
    algo = MagicMock()
    algo.name = name
    algo.version = version
    if run_side_effect is not None:
        algo.run.side_effect = run_side_effect
    else:
        algo.run.return_value = run_result
    return algo


class TestExecutionContext:
    def test_construction(self) -> None:
        task = _make_task()
        algo = _make_algo()
        ctx = ExecutionContext(
            job=task,
            algo=algo,
            settings={"k": "v"},
            image_bytes=b"abc",
        )

        assert ctx.job is task
        assert ctx.algo is algo
        assert ctx.settings == {"k": "v"}
        assert ctx.image_bytes == b"abc"


class TestExecutionResult:
    def test_success_property_true_when_result_set_and_no_error(self) -> None:
        result = ExecutionResult(
            task_id="t",
            processor_name="p",
            processor_version="1.0",
            result=ProcessorResult(),
            error=None,
            duration_ms=1.0,
        )

        assert result.success is True

    def test_success_property_false_when_error(self) -> None:
        result = ExecutionResult(
            task_id="t",
            processor_name="p",
            processor_version="1.0",
            result=ProcessorResult(),
            error="oh no",
            duration_ms=1.0,
        )

        assert result.success is False

    def test_success_property_false_when_result_none(self) -> None:
        result = ExecutionResult(
            task_id="t",
            processor_name="p",
            processor_version="1.0",
            result=None,
            error=None,
            duration_ms=1.0,
        )

        assert result.success is False


class TestLocalWorkerStrategy:
    def test_can_handle_returns_true(self) -> None:
        strat = LocalWorkerStrategy()
        ctx = ExecutionContext(
            job=_make_task(),
            algo=_make_algo(),
            settings={},
            image_bytes=b"",
        )

        assert strat.can_handle(ctx) is True

    def test_execute_invokes_algo_run_with_image_bytes_and_settings(self) -> None:
        proc_result = ProcessorResult()
        algo = _make_algo(run_result=proc_result)
        ctx = ExecutionContext(
            job=_make_task(),
            algo=algo,
            settings={"a": 1},
            image_bytes=b"image_data",
        )

        strat = LocalWorkerStrategy()
        strat.execute(ctx)

        algo.run.assert_called_once_with(image_bytes=b"image_data", settings={"a": 1})

    def test_execute_success_returns_filled_result(self) -> None:
        proc_result = ProcessorResult()
        algo = _make_algo(name="myproc", version="2.5", run_result=proc_result)
        ctx = ExecutionContext(
            job=_make_task(task_id="t-1"),
            algo=algo,
            settings={},
            image_bytes=b"",
        )

        result = LocalWorkerStrategy().execute(ctx)

        assert result.task_id == "t-1"
        assert result.processor_name == "myproc"
        assert result.processor_version == "2.5"
        assert result.result is proc_result
        assert result.error is None
        assert result.success is True
        assert result.duration_ms >= 0

    def test_execute_failure_captures_error_message(self) -> None:
        algo = _make_algo(run_side_effect=ValueError("bad input"))
        ctx = ExecutionContext(
            job=_make_task(task_id="t-2"),
            algo=algo,
            settings={},
            image_bytes=b"",
        )

        result = LocalWorkerStrategy().execute(ctx)

        assert result.task_id == "t-2"
        assert result.result is None
        assert result.error == "bad input"
        assert result.success is False
        assert result.duration_ms >= 0

    def test_execute_failure_uses_task_and_algo_metadata(self) -> None:
        algo = _make_algo(
            name="failing_proc",
            version="0.1",
            run_side_effect=RuntimeError("kaboom"),
        )
        ctx = ExecutionContext(
            job=_make_task(task_id="task-x"),
            algo=algo,
            settings={},
            image_bytes=b"",
        )

        result = LocalWorkerStrategy().execute(ctx)

        assert result.processor_name == "failing_proc"
        assert result.processor_version == "0.1"
        assert result.task_id == "task-x"
        assert result.error == "kaboom"

    def test_execute_duration_is_positive_for_slow_run(self) -> None:
        import time

        def slow_run(image_bytes: bytes, settings: dict) -> ProcessorResult:
            time.sleep(0.01)
            return ProcessorResult()

        algo = _make_algo()
        algo.run.side_effect = slow_run
        ctx = ExecutionContext(
            job=_make_task(),
            algo=algo,
            settings={},
            image_bytes=b"",
        )

        result = LocalWorkerStrategy().execute(ctx)

        # >= 5 ms accounts for clock jitter on slow CI; 10 ms sleep should easily exceed
        assert result.duration_ms >= 5.0
        assert result.error is None

    def test_execute_does_not_reraise_on_run_failure(self) -> None:
        """LocalWorkerStrategy should never raise from execute(); errors land in result.error."""
        algo = _make_algo(run_side_effect=Exception("any error"))
        ctx = ExecutionContext(
            job=_make_task(),
            algo=algo,
            settings={},
            image_bytes=b"",
        )

        # Should not raise
        result = LocalWorkerStrategy().execute(ctx)
        assert result.error == "any error"

    def test_execute_does_not_catch_keyboard_interrupt(self) -> None:
        """BaseException (e.g., KeyboardInterrupt) is not caught by bare `except Exception`."""
        algo = _make_algo(run_side_effect=KeyboardInterrupt())
        ctx = ExecutionContext(
            job=_make_task(),
            algo=algo,
            settings={},
            image_bytes=b"",
        )

        with pytest.raises(KeyboardInterrupt):
            LocalWorkerStrategy().execute(ctx)
