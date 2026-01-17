from __future__ import annotations

import hashlib
import threading
import time
from typing import TYPE_CHECKING

from src.domain.jobs import Job
from src.ingest.readiness import wait_for_file_ready
from src.utils.hashing import compute_fingerprint
from src.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from src.ingest.dedup import DedupCache
    from src.ingest.queue import IngestQueue

logger = get_logger("pipeline.ingest.observer")


class DirectoryObserver:
    """Polling-based observer (dependency-free boilerplate)."""

    def __init__(
        self,
        directories: Mapping[str, Path],
        ingest: IngestQueue,
        dedup: DedupCache,
        allowed_exts: set[str],
        poll_interval_sec: float,
        max_wait_sec: float,
        stable_window_sec: float,
    ) -> None:
        self._directories = directories
        self._ingest = ingest
        self._dedup = dedup
        self._poll_interval_sec = poll_interval_sec
        self._allowed_exts = allowed_exts
        self._stop = threading.Event()
        self._seen_paths: dict[tuple[str, str], float] = {}
        self._max_wait_sec = max_wait_sec
        self._stable_window_sec = stable_window_sec

    def start(self) -> None:
        t = threading.Thread(target=self._run, name="DirectoryObserver", daemon=True)
        t.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        logger.info(
            "Observer started for %d directories: %s",
            len(self._directories),
            ", ".join(f"{k}={v}" for k, v in self._directories.items()),
        )
        while not self._stop.is_set():
            for directory_key, root in self._directories.items():
                self._scan_dir(directory_key, root)
            time.sleep(self._poll_interval_sec)
        logger.info("Observer stopped.")

    def _scan_dir(self, directory_key: str, root: Path) -> None:
        if not root.exists():
            logger.error("Directory does not exist; skipping: %s", root)
            raise Exception.DirectoryNotFound(directory=str(root))

        for p in root.rglob("*"):
            if not p.is_file():
                continue
            if p.suffix.lower() not in self._allowed_exts:
                continue

            try:
                st = p.stat()
            except FileNotFoundError:
                continue

            key = (directory_key, str(p))
            last_mtime = self._seen_paths.get(key)
            if last_mtime is not None and st.st_mtime <= last_mtime:
                continue

            if not wait_for_file_ready(
                path=p,
                stable_window_sec=self._stable_window_sec,
                max_wait_sec=self._max_wait_sec,
            ):
                continue

            logger.info("New or modified file detected: %s", p)

            fingerprint = compute_fingerprint(p)
            logger.info("Computed fingerprint %s for %s", fingerprint, p)

            if self._dedup.seen_recently(fingerprint):
                self._seen_paths[key] = st.st_mtime
                continue

            job_id = hashlib.sha256(
                f"{directory_key}|{p.resolve()}|{fingerprint}".encode(),
            ).hexdigest()

            job = Job(
                job_id=job_id,
                directory_key=directory_key,
                path=str(p),
                created_at_unix=time.time(),
                fingerprint=fingerprint,
            )

            if self._ingest.try_put(job):
                self._seen_paths[key] = st.st_mtime
                logger.info("Enqueued job %s for %s", job.job_id, p)
            else:
                logger.warning("Ingest queue full; backpressure applied for %s", p)
