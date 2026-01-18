"""Kafka-based worker pool for streaming architecture.

Replaces the queue-based WorkerPool with Kafka consumer-based workers.
Each worker consumes from the jobs topic and processes jobs.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.domain.events import EventType
from src.domain.results import MetricValue
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
    - Manual offset commit after processing
    """

    def __init__(
        self,
        kafka_config: KafkaConfig,
        dispatcher: Dispatcher,
        storage: MinioStorageService,
        producer: EventProducer,
        max_workers: int,
    ) -> None:
        self._kafka_config = kafka_config
        self._dispatcher = dispatcher
        self._storage = storage
        self._producer = producer
        self._max_workers = max_workers

        self._stop = threading.Event()
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="KafkaWorker",
        )
        self._lifecycle = AlgoLifecycle()
        self._strategy = LocalWorkerStrategy()

        # Per-worker consumers (created on start)
        self._consumers: list[EventConsumer] = []

        # Event to signal when workers are ready (have partitions assigned)
        self._ready = threading.Event()
        self._ready_count = 0
        self._ready_lock = threading.Lock()

        logger.info(
            "KafkaWorkerPool initialized: max_workers=%d",
            max_workers,
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

        for event in consumer:
            if self._stop.is_set():
                break

            try:
                if event.event_type == EventType.JOB_CREATED:
                    self._process_job_event(consumer, event, worker_id)
            except Exception as e:
                logger.exception(
                    "Worker %d: Error processing event %s: %s",
                    worker_id,
                    event.event_id,
                    e,
                )

            # Commit after processing
            consumer.commit()

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
            job_id=payload["job_id"],
            directory_key=payload["directory_key"],
            path=payload["path"],
            created_at_unix=event.timestamp.timestamp(),
            fingerprint=payload["fingerprint"],
        )

        logger.info(
            "Worker %d processing job: %s",
            worker_id,
            job.job_id,
        )

        try:
            # Get dispatch plan
            plan = self._dispatcher.dispatch(job)

            # Ensure algorithm is initialized
            self._lifecycle.ensure_initialized(plan)

            # Publish JOB_STARTED
            self._producer.publish_job_started(
                source_id=source_id,
                job_id=job.job_id,
                algo_name=plan.algo.name,
                algo_version=plan.algo.version,
            )

            # Read image data
            image_bytes = Path(job.path).read_bytes()

            # Execute algorithm
            context = ExecutionContext(
                job=job,
                algo=plan.algo,
                settings=plan.settings,
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

            # Store artifacts to MinIO
            artifact_refs = []
            if result and result.artifacts:
                for artifact in result.artifacts:
                    if artifact.data:
                        obj_ref = self._storage.store(
                            data=artifact.data,
                            bucket=self._storage.buckets["artifacts"],
                            mime=artifact.mime,
                            metadata={"job_id": job.job_id, "name": artifact.name},
                        )
                        artifact_refs.append(obj_ref.content_hash)

                        # Publish ARTIFACT_STORED event
                        self._producer.publish_artifact_stored(
                            source_id=source_id,
                            content_hash=obj_ref.content_hash,
                            bucket=obj_ref.bucket,
                            key=obj_ref.key,
                            size=obj_ref.size,
                            mime=artifact.mime,
                            source_job_id=job.job_id,
                        )

            # Publish RESULT_PRODUCED
            self._producer.publish_result_produced(
                source_id=source_id,
                job_id=job.job_id,
                algo_name=plan.algo.name,
                algo_version=plan.algo.version,
                metric_count=len(result.metrics) if result and result.metrics else 0,
                artifact_refs=artifact_refs,
            )

            # Publish individual metrics for Flink aggregation
            if result and result.metrics:
                for metric_name, metric_value in result.metrics.items():
                    if isinstance(metric_value, MetricValue):
                        self._producer.publish_metric_emitted(
                            source_id=source_id,
                            job_id=job.job_id,
                            algo_name=plan.algo.name,
                            algo_version=plan.algo.version,
                            metric_name=metric_name,
                            value=metric_value.value,
                            analysis_mask=int(metric_value.analysis.value),
                            meta=dict(metric_value.meta) if metric_value.meta else None,
                        )

            # Publish JOB_COMPLETED
            self._producer.publish_job_completed(
                source_id=source_id,
                job_id=job.job_id,
                algo_name=plan.algo.name,
                algo_version=plan.algo.version,
                duration_ms=exec_result.duration_ms,
            )

            logger.info(
                "Worker %d completed job %s in %.2fms",
                worker_id,
                job.job_id,
                exec_result.duration_ms,
            )

        except Exception as e:
            logger.exception(
                "Worker %d failed job %s: %s",
                worker_id,
                job.job_id,
                e,
            )

            # Publish JOB_FAILED
            algo_name = plan.algo.name if "plan" in dir() else None
            algo_version = plan.algo.version if "plan" in dir() else None

            self._producer.publish_job_failed(
                source_id=source_id,
                job_id=job.job_id,
                error=str(e),
                algo_name=algo_name,
                algo_version=algo_version,
                error_type=type(e).__name__,
            )

    def _is_slow_job(self, plan: Any) -> bool:
        """Determine if a job is expected to be slow.

        Future: Use historical timing data or algorithm hints.
        """
        # Placeholder - consider jobs slow if algorithm is ML inference
        return plan.algo.name.startswith("model_")
