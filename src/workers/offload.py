"""Worker offload strategies for handling slow jobs.

Provides interface and implementations for job execution strategies:
- LocalWorkerStrategy: Execute in local thread pool (current)
- Future: RemoteWorkerStrategy for distributed execution

The offload pattern allows:
- Pause/resume of Kafka partition during slow job processing
- Distributed execution for compute-heavy jobs
- Graceful degradation under load
"""

from __future__ import annotations

from abc import ABC, abstractmethod
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
    """Protocol for job execution strategies.

    Implementations determine how and where jobs are executed:
    - Locally in the current process
    - Remotely on distributed workers
    - Hybrid based on job characteristics
    """

    def execute(self, context: ExecutionContext) -> ExecutionResult:
        """Execute a job and return the result.

        Args:
            context: Execution context with job, algorithm, and data

        Returns:
            ExecutionResult with output or error
        """
        ...

    def can_handle(self, context: ExecutionContext) -> bool:
        """Check if this strategy can handle the given job.

        Args:
            context: Execution context to check

        Returns:
            True if this strategy can handle the job
        """
        ...


class LocalWorkerStrategy:
    """Execute jobs locally in the current process.

    This is the default strategy that executes algorithms
    directly in the worker thread pool.
    """

    def execute(self, context: ExecutionContext) -> ExecutionResult:
        """Execute job locally.

        Args:
            context: Execution context

        Returns:
            ExecutionResult with algorithm output
        """
        import time

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


class RemoteWorkerStrategy:
    """Placeholder for remote/distributed execution strategy.

    Future implementation would:
    - Send job to remote worker cluster (e.g., Ray, Dask, Celery)
    - Handle result retrieval and error handling
    - Support async execution with callbacks
    """

    def __init__(self, endpoint: str | None = None) -> None:
        self._endpoint = endpoint
        logger.warning(
            "RemoteWorkerStrategy is a placeholder - not implemented"
        )

    def execute(self, context: ExecutionContext) -> ExecutionResult:
        """Execute job remotely (not implemented).

        Args:
            context: Execution context

        Returns:
            ExecutionResult with error (not implemented)
        """
        return ExecutionResult(
            job_id=context.job.job_id,
            algo_name=context.algo.name,
            algo_version=context.algo.version,
            result=None,
            error="RemoteWorkerStrategy not implemented",
            duration_ms=0,
        )

    def can_handle(self, context: ExecutionContext) -> bool:
        """Remote strategy is not yet implemented."""
        return False


class HybridStrategy:
    """Hybrid strategy that chooses between local and remote execution.

    Decision criteria (future):
    - Estimated job duration
    - Current local worker load
    - Algorithm requirements (GPU, memory, etc.)
    """

    def __init__(
        self,
        local: LocalWorkerStrategy | None = None,
        remote: RemoteWorkerStrategy | None = None,
        duration_threshold_ms: float = 5000,
    ) -> None:
        self._local = local or LocalWorkerStrategy()
        self._remote = remote
        self._duration_threshold_ms = duration_threshold_ms

    def execute(self, context: ExecutionContext) -> ExecutionResult:
        """Execute using appropriate strategy.

        Currently always uses local strategy since remote is not implemented.
        """
        # Future: choose based on job characteristics
        # if self._should_use_remote(context):
        #     return self._remote.execute(context)
        return self._local.execute(context)

    def can_handle(self, context: ExecutionContext) -> bool:
        """Hybrid can handle if either strategy can."""
        return self._local.can_handle(context) or (
            self._remote is not None and self._remote.can_handle(context)
        )

    def _should_use_remote(self, context: ExecutionContext) -> bool:
        """Determine if job should be executed remotely.

        Future implementation would consider:
        - Historical execution time for this algorithm
        - Current load on local workers
        - Job-specific hints or requirements
        """
        # Placeholder - always use local for now
        return False
