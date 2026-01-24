"""State reader for querying pipeline state.

Provides read access to:
- Job status (from Kafka topic consumption)
- Artifacts (from MinIO)
- Aggregates (from MinIO and Kafka)

This replaces PostgreSQL queries with streaming-native approaches.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from src.domain.events import EventEnvelope, EventType
from src.streaming.consumer import EventConsumer
from src.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Iterator

    from src.storage.minio_service import MinioStorageService
    from src.streaming.config import KafkaConfig

logger = get_logger(__name__)


@dataclass
class JobStatus:
    """Current status of a job."""

    job_id: str
    status: str
    directory_key: str
    path: str
    fingerprint: str
    algo_name: str | None = None
    algo_version: str | None = None
    created_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_ms: float | None = None
    error: str | None = None
    artifact_refs: list[str] | None = None


@dataclass
class ArtifactInfo:
    """Information about a stored artifact."""

    content_hash: str
    bucket: str
    key: str
    size: int
    mime: str
    stored_at: datetime


class StateReader:
    """Reader for pipeline state from streaming infrastructure.

    Provides methods to query:
    - Individual job status
    - Job history
    - Artifacts
    - Aggregates

    Uses Kafka consumer for event replay and MinIO for artifact access.
    """

    def __init__(
        self,
        kafka_config: KafkaConfig,
        storage: MinioStorageService,
    ) -> None:
        self._kafka_config = kafka_config
        self._storage = storage

        # In-memory state cache (populated by consuming events)
        self._job_states: dict[str, JobStatus] = {}
        self._artifact_index: dict[str, ArtifactInfo] = {}

    def get_job_status(self, job_id: str) -> JobStatus | None:
        """Get current status of a job.

        Args:
            job_id: Job identifier

        Returns:
            JobStatus or None if not found

        """
        return self._job_states.get(job_id)

    def list_jobs(
        self,
        status: str | None = None,
        limit: int = 100,
    ) -> list[JobStatus]:
        """List jobs with optional status filter.

        Args:
            status: Filter by status (created, started, completed, failed)
            limit: Maximum jobs to return

        Returns:
            List of JobStatus objects

        """
        jobs = list(self._job_states.values())

        if status:
            jobs = [j for j in jobs if j.status == status]

        # Sort by created_at descending
        jobs.sort(
            key=lambda j: j.created_at or datetime.min,
            reverse=True,
        )

        return jobs[:limit]

    def get_artifact(self, content_hash: str) -> bytes | None:
        """Retrieve artifact data by content hash.

        Args:
            content_hash: SHA-256 hash of artifact content

        Returns:
            Artifact bytes or None if not found

        """
        try:
            return self._storage.retrieve_by_hash(
                bucket=self._storage.buckets["artifacts"],
                content_hash=content_hash,
            )
        except FileNotFoundError:
            return None

    def get_artifact_info(self, content_hash: str) -> ArtifactInfo | None:
        """Get artifact metadata without retrieving content.

        Args:
            content_hash: SHA-256 hash of artifact content

        Returns:
            ArtifactInfo or None if not found

        """
        return self._artifact_index.get(content_hash)

    def list_artifacts(
        self,
        limit: int = 100,
    ) -> list[ArtifactInfo]:
        """List artifacts ordered by storage time.

        Args:
            limit: Maximum artifacts to return

        Returns:
            List of ArtifactInfo objects

        """
        artifacts = list(self._artifact_index.values())

        # Sort by stored_at descending
        artifacts.sort(key=lambda a: a.stored_at, reverse=True)

        return artifacts[:limit]

    def consume_events(
        self,
        topics: list[str] | None = None,
        max_events: int | None = None,
    ) -> Iterator[EventEnvelope]:
        """Consume events from Kafka topics.

        Useful for replaying history or building state.

        Args:
            topics: Topics to consume (defaults to jobs + results)
            max_events: Maximum events to consume (None = unlimited)

        Yields:
            EventEnvelope objects

        """
        if topics is None:
            topics = [
                self._kafka_config.topic_jobs,
                self._kafka_config.topic_results,
            ]

        consumer = EventConsumer(
            config=self._kafka_config,
            topics=topics,
            group_id=f"state-reader-{id(self)}",
        )

        count = 0
        try:
            for event in consumer:
                yield event
                self._process_event(event)

                count += 1
                if max_events and count >= max_events:
                    break
        finally:
            consumer.close()

    def rebuild_state(self, max_events: int | None = None) -> int:
        """Rebuild in-memory state by replaying events.

        Args:
            max_events: Maximum events to process

        Returns:
            Number of events processed

        """
        count = 0
        for _ in self.consume_events(max_events=max_events):
            count += 1
        logger.info("Rebuilt state from %d events", count)
        return count

    def _process_event(self, event: EventEnvelope) -> None:
        """Process an event and update internal state."""
        payload = event.payload

        if event.event_type == EventType.JOB_CREATED:
            self._job_states[payload["job_id"]] = JobStatus(
                job_id=payload["job_id"],
                status="created",
                directory_key=payload["directory_key"],
                path=payload["path"],
                fingerprint=payload["fingerprint"],
                created_at=event.timestamp,
            )

        elif event.event_type == EventType.JOB_STARTED:
            job_id = payload["job_id"]
            if job_id in self._job_states:
                job = self._job_states[job_id]
                self._job_states[job_id] = JobStatus(
                    job_id=job.job_id,
                    status="started",
                    directory_key=job.directory_key,
                    path=job.path,
                    fingerprint=job.fingerprint,
                    algo_name=payload.get("algo_name"),
                    algo_version=payload.get("algo_version"),
                    created_at=job.created_at,
                    started_at=event.timestamp,
                )

        elif event.event_type == EventType.JOB_COMPLETED:
            job_id = payload["job_id"]
            if job_id in self._job_states:
                job = self._job_states[job_id]
                self._job_states[job_id] = JobStatus(
                    job_id=job.job_id,
                    status="completed",
                    directory_key=job.directory_key,
                    path=job.path,
                    fingerprint=job.fingerprint,
                    algo_name=job.algo_name,
                    algo_version=job.algo_version,
                    created_at=job.created_at,
                    started_at=job.started_at,
                    completed_at=event.timestamp,
                    duration_ms=payload.get("duration_ms"),
                )

        elif event.event_type == EventType.JOB_FAILED:
            job_id = payload["job_id"]
            if job_id in self._job_states:
                job = self._job_states[job_id]
                self._job_states[job_id] = JobStatus(
                    job_id=job.job_id,
                    status="failed",
                    directory_key=job.directory_key,
                    path=job.path,
                    fingerprint=job.fingerprint,
                    algo_name=job.algo_name,
                    algo_version=job.algo_version,
                    created_at=job.created_at,
                    started_at=job.started_at,
                    completed_at=event.timestamp,
                    error=payload.get("error"),
                )

        elif event.event_type == EventType.RESULT_PRODUCED:
            job_id = payload["job_id"]
            if job_id in self._job_states:
                job = self._job_states[job_id]
                self._job_states[job_id] = JobStatus(
                    job_id=job.job_id,
                    status=job.status,
                    directory_key=job.directory_key,
                    path=job.path,
                    fingerprint=job.fingerprint,
                    algo_name=job.algo_name,
                    algo_version=job.algo_version,
                    created_at=job.created_at,
                    started_at=job.started_at,
                    completed_at=job.completed_at,
                    duration_ms=job.duration_ms,
                    artifact_refs=payload.get("artifact_refs", []),
                )

        elif event.event_type == EventType.ARTIFACT_STORED:
            self._artifact_index[payload["content_hash"]] = ArtifactInfo(
                content_hash=payload["content_hash"],
                bucket=payload["bucket"],
                key=payload["key"],
                size=payload["size"],
                mime=payload["mime"],
                stored_at=event.timestamp,
            )
