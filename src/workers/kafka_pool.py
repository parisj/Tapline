"""Kafka-based worker pool for job processing.

Workers consume from the jobs topic and execute algorithms.
Performance tuning via batch commits, I/O executor, and parallel uploads.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.domain.evaluation import AggregationType
from src.domain.events import EventType
from src.domain.results import Measurement
from src.observability.metrics import (
    TASK_DURATION,
    TASKS_COMPLETED,
    TASKS_FAILED,
    WORKERS_ACTIVE,
)
from src.utils.logging import get_logger
from src.workers.lifecycle import AlgoLifecycle
from src.workers.offload import ExecutionContext, LocalWorkerStrategy

if TYPE_CHECKING:
    from src.dispatch.dispatcher import Dispatcher
    from src.storage.minio_service import MinioStorageService
    from src.streaming.config import KafkaConfig
    from src.streaming.consumer import EventConsumer
    from src.streaming.producer import EventProducer

logger = get_logger(__name__)


class KafkaWorkerPool:
    """Kafka-based worker pool using event streaming.

    Workers consume from the jobs topic and:
    1. Process jobs using algorithms
    2. Store artifacts to MinIO
    3. Publish results and metrics to Kafka

    Features:
    - Partition-aware consumption for ordering
    - Pause/resume for slow job handling
    - Batch offset commits for performance
    - Dedicated I/O executor for file reads
    - Parallel artifact uploads
    """

    def __init__(
        self,
        kafka_config: KafkaConfig,
        dispatcher: Dispatcher,
        storage: MinioStorageService,
        producer: EventProducer,
        max_workers: int,
        commit_batch_size: int = 10,
        io_workers: int = 4,
        artifact_upload_workers: int = 4,
    ) -> None:
        self._kafka_config = kafka_config
        self._dispatcher = dispatcher
        self._storage = storage
        self._producer = producer
        self._max_workers = max_workers

        # Performance tuning
        self._commit_batch_size = max(1, commit_batch_size)
        self._io_workers = max(0, io_workers)
        self._artifact_upload_workers = max(0, artifact_upload_workers)

        self._stop = threading.Event()
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="KafkaWorker",
        )
        self._lifecycle = AlgoLifecycle()
        self._strategy = LocalWorkerStrategy()

        # Dedicated I/O executor for file reads (reduces worker thread blocking)
        self._io_executor: ThreadPoolExecutor | None = None
        if self._io_workers > 0:
            self._io_executor = ThreadPoolExecutor(
                max_workers=self._io_workers,
                thread_name_prefix="IOWorker",
            )

        # Per-worker consumers (created on start)
        self._consumers: list[EventConsumer] = []

        # Per-worker uncommitted message counters for batch commits
        self._uncommitted_counts: dict[int, int] = {}
        self._uncommitted_lock = threading.Lock()

        # Event to signal when workers are ready (have partitions assigned)
        self._ready = threading.Event()
        self._ready_count = 0
        self._ready_lock = threading.Lock()

        logger.info(
            "KafkaWorkerPool initialized: max_workers=%d, commit_batch_size=%d, "
            "io_workers=%d, artifact_upload_workers=%d",
            max_workers,
            self._commit_batch_size,
            self._io_workers,
            self._artifact_upload_workers,
        )

    def start(self, wait_for_ready: bool = True, ready_timeout: float = 30.0) -> None:
        """Start worker threads.

        Args:
            wait_for_ready: If True, wait for workers to have partitions assigned
            ready_timeout: Maximum time to wait for workers to be ready

        """
        from src.streaming.consumer import EventConsumer

        # All workers share the same consumer group for load balancing
        # Kafka will distribute partitions among consumers in the same group
        for i in range(self._max_workers):
            consumer = EventConsumer(
                config=self._kafka_config,
                topics=[self._kafka_config.topic_jobs],
                group_id=self._kafka_config.consumer_group_id,  # Same group for all
                on_assign_callback=lambda partitions: self._signal_ready(),
            )
            self._consumers.append(consumer)
            self._executor.submit(self._worker_loop, consumer, i)

        logger.info("Started %d Kafka workers", self._max_workers)

        # Wait for workers to be ready (have partitions assigned)
        if wait_for_ready:
            logger.info("Waiting for workers to join consumer group...")
            if self._ready.wait(timeout=ready_timeout):
                logger.info("Workers ready with partitions assigned")
            else:
                logger.warning(
                    "Timeout waiting for workers to be ready after %.1fs",
                    ready_timeout,
                )

    def stop(self) -> None:
        """Stop all workers gracefully."""
        logger.info("Stopping KafkaWorkerPool...")
        self._stop.set()

        # Stop all consumers
        for consumer in self._consumers:
            consumer.stop()

        # Wait for executor shutdown
        self._executor.shutdown(wait=True, cancel_futures=True)

        # Shutdown I/O executor if present
        if self._io_executor is not None:
            self._io_executor.shutdown(wait=True)

        # Close consumers
        for consumer in self._consumers:
            consumer.close()

        self._consumers.clear()
        logger.info("KafkaWorkerPool stopped")

    def _signal_ready(self) -> None:
        """Signal that a worker is ready (has partitions assigned)."""
        with self._ready_lock:
            self._ready_count += 1
            # Signal ready when at least one worker has partitions
            # (Kafka will distribute among workers based on partitions)
            if self._ready_count >= 1 and not self._ready.is_set():
                self._ready.set()

    def _worker_loop(self, consumer: EventConsumer, worker_id: int) -> None:
        """Main worker loop consuming from Kafka."""
        logger.info("Worker %d started", worker_id)

        # Initialize uncommitted counter for this worker
        with self._uncommitted_lock:
            self._uncommitted_counts[worker_id] = 0

        for event in consumer:
            if self._stop.is_set():
                break

            try:
                if event.event_type == EventType.TASK_CREATED:
                    self._process_job_event(consumer, event, worker_id)
            except Exception as e:
                logger.exception(
                    "Worker %d: Error processing event %s: %s",
                    worker_id,
                    event.event_id,
                    e,
                )

            # Batch commit: only commit after N messages
            with self._uncommitted_lock:
                self._uncommitted_counts[worker_id] += 1
                should_commit = self._uncommitted_counts[worker_id] >= self._commit_batch_size

            if should_commit:
                consumer.commit()
                with self._uncommitted_lock:
                    self._uncommitted_counts[worker_id] = 0

        # Final commit on shutdown for any remaining uncommitted messages
        with self._uncommitted_lock:
            if self._uncommitted_counts.get(worker_id, 0) > 0:
                consumer.commit()
                self._uncommitted_counts[worker_id] = 0

        logger.info("Worker %d stopped", worker_id)

    def _process_job_event(
        self,
        consumer: EventConsumer,
        event: Any,
        worker_id: int,
    ) -> None:
        """Process a JOB_CREATED event."""
        from src.domain.jobs import Job

        payload = event.payload
        source_id = event.source_id

        # Reconstruct Job from event payload
        job = Job(
            task_id=payload["task_id"],
            directory_key=payload["directory_key"],
            path=payload["path"],
            created_at_unix=event.timestamp.timestamp(),
            fingerprint=payload["fingerprint"],
        )

        logger.info(
            "Worker %d processing job: %s",
            worker_id,
            job.task_id,
        )

        WORKERS_ACTIVE.labels(worker_id=str(worker_id)).inc()
        processor_name: str | None = None
        processor_version: str | None = None
        try:
            plan = self._dispatcher.dispatch(job)
            processor_name = plan.processor.name
            processor_version = plan.processor.version

            self._lifecycle.ensure_initialized(plan)

            self._producer.publish_job_started(
                source_id=source_id,
                task_id=job.task_id,
                processor_name=processor_name,
                processor_version=processor_version,
            )

            # Read image data (using I/O executor if available for non-blocking reads)
            image_bytes = self._read_file(job.path)

            # Execute algorithm
            context = ExecutionContext(
                job=job,
                algo=plan.processor,
                settings=dict(plan.settings),
                image_bytes=image_bytes,
            )

            # For slow jobs, pause the partition
            is_slow_job = self._is_slow_job(plan)
            if is_slow_job:
                consumer.pause_current()

            try:
                exec_result = self._strategy.execute(context)
            finally:
                if is_slow_job:
                    consumer.resume_current()

            if exec_result.error:
                raise RuntimeError(exec_result.error)

            result = exec_result.result

            # Store artifacts to MinIO (parallel upload for performance)
            artifact_refs: list[str] = []
            if result and result.artifacts:
                artifact_refs = self._store_artifacts_parallel(
                    list(result.artifacts),
                    job.task_id,
                    source_id,
                )

            # Publish RESULT_PRODUCED
            self._producer.publish_result_produced(
                source_id=source_id,
                task_id=job.task_id,
                processor_name=processor_name,
                processor_version=processor_version,
                metric_count=len(result.metrics) if result and result.metrics else 0,
                artifact_refs=artifact_refs,
            )

            # Publish individual metrics for Flink aggregation
            if result and result.metrics:
                for metric_name, metric_value in result.metrics.items():
                    if isinstance(metric_value, Measurement):
                        self._producer.publish_metric_emitted(
                            source_id=source_id,
                            task_id=job.task_id,
                            processor_name=processor_name,
                            processor_version=processor_version,
                            metric_name=metric_name,
                            value=metric_value.value,
                            aggregation_mask=int(metric_value.aggregation.value),
                            meta=dict(metric_value.meta) if metric_value.meta else None,
                        )

            # Emit job_duration as a metric for dashboard display
            self._producer.publish_metric_emitted(
                source_id=source_id,
                task_id=job.task_id,
                processor_name=processor_name,
                processor_version=processor_version,
                metric_name="task_duration_ms",
                value=exec_result.duration_ms,
                aggregation_mask=int(AggregationType.STATS | AggregationType.HISTOGRAM),
                meta={"unit": "milliseconds", "description": "Total task processing time"},
            )

            # Publish JOB_COMPLETED
            self._producer.publish_job_completed(
                source_id=source_id,
                task_id=job.task_id,
                processor_name=processor_name,
                processor_version=processor_version,
                duration_ms=exec_result.duration_ms,
            )

            # Record task duration and completion metrics
            TASK_DURATION.labels(processor_name=processor_name).observe(
                exec_result.duration_ms / 1000.0,  # Convert ms to seconds
            )
            TASKS_COMPLETED.labels(
                processor_name=processor_name,
                processor_version=processor_version,
            ).inc()

            logger.info(
                "Worker %d completed job %s in %.2fms",
                worker_id,
                job.task_id,
                exec_result.duration_ms,
            )

        except Exception as e:
            logger.exception(
                "Worker %d failed job %s: %s",
                worker_id,
                job.task_id,
                e,
            )

            error_type = type(e).__name__

            self._producer.publish_job_failed(
                source_id=source_id,
                task_id=job.task_id,
                error=str(e),
                processor_name=processor_name,
                processor_version=processor_version,
                error_type=error_type,
            )

            # Record task failure metrics
            TASKS_FAILED.labels(
                processor_name=processor_name or "unknown",
                error_type=error_type,
            ).inc()
        finally:
            # Always decrement active workers when done
            WORKERS_ACTIVE.labels(worker_id=str(worker_id)).dec()

    def _is_slow_job(self, plan: Any) -> bool:
        """Check if job is expected to be slow based on processor type."""
        return bool(plan.processor.name.startswith("model_"))

    def _read_file(self, path: str, timeout: float = 30.0) -> bytes:
        """Read file contents, using I/O executor if available.

        Args:
            path: Path to file to read
            timeout: Maximum time to wait for read (seconds)

        Returns:
            File contents as bytes

        """
        if self._io_executor is not None:
            # Non-blocking read using dedicated I/O thread pool
            future = self._io_executor.submit(Path(path).read_bytes)
            return future.result(timeout=timeout)
        # Synchronous read (fallback)
        return Path(path).read_bytes()

    def _store_artifacts_parallel(
        self,
        artifacts: list[Any],
        task_id: str,
        source_id: str,
    ) -> list[str]:
        """Store artifacts to MinIO in parallel.

        Args:
            artifacts: List of Artifact objects to store
            task_id: Job ID for metadata
            source_id: Source ID for event publishing

        Returns:
            List of content hashes for stored artifacts

        """
        if not artifacts:
            return []

        # Filter artifacts with data
        to_store = [(a, a.data) for a in artifacts if a.data]
        if not to_store:
            return []

        # If no parallel workers configured, use sequential storage
        if self._artifact_upload_workers == 0:
            return self._store_artifacts_sequential(to_store, task_id, source_id)

        artifact_refs: list[str] = []
        with ThreadPoolExecutor(max_workers=self._artifact_upload_workers) as executor:
            futures = {}
            for artifact, data in to_store:
                future = executor.submit(
                    self._store_single_artifact,
                    artifact,
                    data,
                    task_id,
                    source_id,
                )
                futures[future] = artifact

            for future in as_completed(futures):
                try:
                    content_hash = future.result()
                    if content_hash:
                        artifact_refs.append(content_hash)
                except Exception as e:
                    artifact = futures[future]
                    logger.warning(
                        "Failed to store artifact %s for job %s: %s",
                        artifact.name,
                        task_id,
                        e,
                    )

        return artifact_refs

    def _store_artifacts_sequential(
        self,
        to_store: list[tuple[Any, bytes]],
        task_id: str,
        source_id: str,
    ) -> list[str]:
        """Store artifacts sequentially (fallback when parallel disabled)."""
        artifact_refs: list[str] = []
        for artifact, data in to_store:
            try:
                content_hash = self._store_single_artifact(artifact, data, task_id, source_id)
                if content_hash:
                    artifact_refs.append(content_hash)
            except Exception as e:
                logger.warning(
                    "Failed to store artifact %s for job %s: %s",
                    artifact.name,
                    task_id,
                    e,
                )
        return artifact_refs

    def _store_single_artifact(
        self,
        artifact: Any,
        data: bytes,
        task_id: str,
        source_id: str,
    ) -> str | None:
        """Store a single artifact to MinIO and publish event.

        Returns:
            Content hash if successful, None otherwise

        """
        obj_ref = self._storage.store(
            data=data,
            bucket=self._storage.buckets["artifacts"],
            mime=artifact.mime,
            metadata={"task_id": task_id, "name": artifact.name},
        )

        # Publish ARTIFACT_STORED event
        self._producer.publish_artifact_stored(
            source_id=source_id,
            content_hash=obj_ref.content_hash,
            bucket=obj_ref.bucket,
            key=obj_ref.key,
            size=obj_ref.size,
            mime=artifact.mime,
            source_task_id=task_id,
        )

        return obj_ref.content_hash
