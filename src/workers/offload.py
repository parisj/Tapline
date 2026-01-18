"""Worker offload strategies for handling job execution.

Provides interface and implementations for job execution strategies:
- LocalWorkerStrategy: Execute in local thread pool (default)

The offload pattern allows for future distributed execution strategies.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from src.utils.logging import get_logger

if TYPE_CHECKING:
    from src.algorithms.base import Algorithm
    from src.domain.jobs import Job
    from src.domain.results import AlgoResult

logger = get_logger(__name__)


@dataclass
class ExecutionContext:
    """Context for job execution."""

    job: Job
    algo: Algorithm
    settings: dict[str, Any]
    image_bytes: bytes


@dataclass
class ExecutionResult:
    """Result of job execution."""

    job_id: str
    algo_name: str
    algo_version: str
    result: AlgoResult | None
    error: str | None
    duration_ms: float

    @property
    def success(self) -> bool:
        return self.result is not None and self.error is None


class OffloadStrategy(Protocol):
    """Protocol for job execution strategies."""

    def execute(self, context: ExecutionContext) -> ExecutionResult:
        """Execute a job and return the result."""
        ...

    def can_handle(self, context: ExecutionContext) -> bool:
        """Check if this strategy can handle the given job."""
        ...


class LocalWorkerStrategy:
    """Execute jobs locally in the current process.

    This is the default strategy that executes algorithms
    directly in the worker thread pool.
    """

    def execute(self, context: ExecutionContext) -> ExecutionResult:
        """Execute job locally."""
        start_time = time.perf_counter()
        error = None
        result = None

        try:
            result = context.algo.run(
                image_bytes=context.image_bytes,
                settings=context.settings,
            )
        except Exception as e:
            error = str(e)
            logger.exception(
                "Local execution failed: job=%s, algo=%s",
                context.job.job_id,
                context.algo.name,
            )

        duration_ms = (time.perf_counter() - start_time) * 1000

        return ExecutionResult(
            job_id=context.job.job_id,
            algo_name=context.algo.name,
            algo_version=context.algo.version,
            result=result,
            error=error,
            duration_ms=duration_ms,
        )

    def can_handle(self, context: ExecutionContext) -> bool:
        """Local strategy can handle any job."""
        return True
