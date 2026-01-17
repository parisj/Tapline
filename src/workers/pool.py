from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING

from src.utils.logging import get_logger
from src.workers.lifecycle import AlgoLifecycle

if TYPE_CHECKING:
    from src.dispatch.dispatcher import Dispatcher
    from src.domain.jobs import Job
    from src.ingest.queue import IngestQueue
    from src.persistence.service import PersistenceService

logger = get_logger("pipeline.workers.pool")


class WorkerPool:
    """Thread-based worker pool for orchestration and I/O-heavy work."""

    def __init__(
        self,
        ingest: IngestQueue,
        dispatcher: Dispatcher,
        persistence: PersistenceService,
        max_workers: int,
    ) -> None:
        self._ingest = ingest
        self._dispatcher = dispatcher
        self._persistence = persistence
        self._stop = threading.Event()
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="Worker",
        )
        self._max_workers = max_workers
        self._lifecycle = AlgoLifecycle()

    def start(self) -> None:
        for _ in range(self._max_workers):
            self._executor.submit(self._loop)

    def stop(self) -> None:
        self._stop.set()
        self._executor.shutdown(wait=True, cancel_futures=True)

    def _loop(self) -> None:
        logger.info("Worker started.")
        while not self._stop.is_set():
            job = self._ingest.get()
            if job is None:
                continue
            logger.info("Processing job: %s", job.job_id)
            self.process_job(job)

    def process_job(self, job: Job, *, raise_on_error: bool = False) -> None:
        try:
            self._persistence.upsert_job_created(job)
            plan = self._dispatcher.dispatch(job)

            self._lifecycle.ensure_initialized(plan)
            self._persistence.mark_job_started(job.job_id)

            image_bytes = Path(job.path).read_bytes()
            result = plan.algo.run(image_bytes=image_bytes, settings=plan.settings)

            self._persistence.persist_result(job.job_id, plan.algo, result)
            self._persistence.mark_job_finished(job.job_id)
            logger.info("Job completed successfully: %s", job.job_id)

        except Exception as ex:
            logger.exception("Job failed: %s", job.job_id)
            self._persistence.mark_job_failed(job.job_id, error=str(ex))
            if raise_on_error:
                raise

        finally:
            self._ingest.task_done()
