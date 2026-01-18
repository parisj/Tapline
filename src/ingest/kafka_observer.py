"""Kafka-based directory observer for streaming architecture.

Replaces queue-based DirectoryObserver with Kafka producer integration.
Publishes JOB_CREATED events to Kafka instead of enqueuing to local queue.
"""

from __future__ import annotations

import hashlib
import threading
import time
from typing import TYPE_CHECKING

from src.ingest.readiness import wait_for_file_ready
from src.utils.hashing import compute_fingerprint
from src.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from src.ingest.dedup import DedupCache
    from src.streaming.producer import EventProducer

logger = get_logger("pipeline.ingest.kafka_observer")


class KafkaDirectoryObserver:
    """Kafka-based directory observer that publishes to event stream.

    Replaces queue.put() with Kafka producer.publish().
    Events are partitioned by directory_key for ordering guarantees.
    """

    def __init__(
        self,
        directories: Mapping[str, Path],
        producer: EventProducer,
        dedup: DedupCache,
        allowed_exts: set[str],
        poll_interval_sec: float,
        max_wait_sec: float,
        stable_window_sec: float,
    ) -> None:
        self._directories = directories
        self._producer = producer
        self._dedup = dedup
        self._poll_interval_sec = poll_interval_sec
        self._allowed_exts = allowed_exts
        self._stop = threading.Event()
        self._seen_paths: dict[tuple[str, str], float] = {}
        self._max_wait_sec = max_wait_sec
        self._stable_window_sec = stable_window_sec

        # Statistics
        self._jobs_published = 0
        self._files_skipped = 0

    def start(self) -> None:
        """Start observer in background thread."""
        t = threading.Thread(
            target=self._run,
            name="KafkaDirectoryObserver",
            daemon=True,
        )
        t.start()

    def stop(self) -> None:
        """Signal observer to stop."""
        self._stop.set()

    def _run(self) -> None:
        """Main observer loop."""
        logger.info(
            "KafkaDirectoryObserver started for %d directories: %s",
            len(self._directories),
            ", ".join(f"{k}={v}" for k, v in self._directories.items()),
        )

        while not self._stop.is_set():
            for directory_key, root in self._directories.items():
                self._scan_dir(directory_key, root)
            time.sleep(self._poll_interval_sec)

        # Flush any pending messages before stopping
        self._producer.flush()
        logger.info(
            "KafkaDirectoryObserver stopped. Published %d jobs, skipped %d files.",
            self._jobs_published,
            self._files_skipped,
        )

    def _scan_dir(self, directory_key: str, root: Path) -> None:
        """Scan a directory for new/modified files."""
        if not root.exists():
            logger.error("Directory does not exist; skipping: %s", root)
            return

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

            logger.debug("New or modified file detected: %s", p)

            fingerprint = compute_fingerprint(p)

            if self._dedup.seen_recently(fingerprint):
                self._seen_paths[key] = st.st_mtime
                self._files_skipped += 1
                continue

            job_id = hashlib.sha256(
                f"{directory_key}|{p.resolve()}|{fingerprint}".encode(),
            ).hexdigest()

            # Compute file hash for audit trail
            file_hash = self._compute_file_hash(p)

            # Publish JOB_CREATED event to Kafka
            # Use directory_key as source_id for partition ordering
            self._producer.publish_job_created(
                source_id=directory_key,
                job_id=job_id,
                directory_key=directory_key,
                path=str(p),
                fingerprint=fingerprint,
                file_hash=file_hash,
            )

            self._seen_paths[key] = st.st_mtime
            self._jobs_published += 1
            logger.info("Published JOB_CREATED: job_id=%s, path=%s", job_id, p)

    def _compute_file_hash(self, path: Path) -> str:
        """Compute SHA-256 hash of file contents for audit trail."""
        sha256 = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    @property
    def stats(self) -> dict[str, int]:
        """Get observer statistics."""
        return {
            "jobs_published": self._jobs_published,
            "files_skipped": self._files_skipped,
            "directories_watched": len(self._directories),
        }
