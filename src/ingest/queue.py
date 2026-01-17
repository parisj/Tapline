from __future__ import annotations

import queue
from typing import TYPE_CHECKING

from src.utils.logging import get_logger

if TYPE_CHECKING:
    from src.domain.jobs import Job

logger = get_logger("pipeline.ingest.queue")
class IngestQueue:
    """Bounded queue to enforce backpressure."""

    def __init__(self, maxsize: int, timeout_sec: float) -> None:
        self._q: queue.Queue[Job] = queue.Queue(maxsize=maxsize)
        self._timeout_sec = timeout_sec

    def try_put(self, job: Job) -> bool:
        try:
            self._q.put(job, block=False)
            return True
        except queue.Full:
            return False

    def get(self) -> Job | None:
        try:
            return self._q.get(timeout=self._timeout_sec)
        except queue.Empty:
            return None

    def task_done(self) -> None:
        self._q.task_done()
