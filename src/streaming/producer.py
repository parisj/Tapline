"""Kafka producer for event streaming.

The EventProducer provides:
- Automatic event envelope creation with hash chain
- Dual-write to operational topic + audit-log
- Delivery confirmation handling
- Per-partition ordering guarantees
- OpenTelemetry tracing and Prometheus metrics
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Any, Self

from confluent_kafka import Producer

from src.audit.chain import HashChainTracker
from src.domain.events import EventEnvelope, EventType
from src.utils.logging import get_logger

if TYPE_CHECKING:
    from src.streaming.config import KafkaConfig

from src.streaming.config import build_security_config

logger = get_logger(__name__)

# Import observability components
from src.observability.availability import get_correlation_id, get_tracer
from src.observability.availability import is_available as _obs_available

_OBSERVABILITY_AVAILABLE = _obs_available()

if _OBSERVABILITY_AVAILABLE:
    from src.observability.metrics import (
        KAFKA_MESSAGES_PRODUCED,
        KAFKA_PRODUCE_ERRORS,
        KAFKA_PRODUCE_LATENCY,
    )


class DeliveryReport:
    """Tracks delivery status for produced messages."""

    def __init__(self) -> None:
        self.delivered = 0
        self.failed = 0
        self._lock = threading.Lock()
        self._pending_start_times: dict[str, float] = {}

    def track_send(self, msg_id: str) -> None:
        """Track when a message was sent for latency calculation."""
        self._pending_start_times[msg_id] = time.perf_counter()

    def on_delivery(self, err: Any, msg: Any) -> None:
        """Callback for message delivery confirmation."""
        topic = msg.topic() if msg else "unknown"

        with self._lock:
            if err is not None:
                logger.error(
                    "Message delivery failed: topic=%s, error=%s",
                    topic,
                    err,
                )
                self.failed += 1

                # Record metrics if available
                if _OBSERVABILITY_AVAILABLE:
                    KAFKA_PRODUCE_ERRORS.labels(
                        topic=topic,
                        error_type=str(err.code()) if hasattr(err, "code") else "unknown",
                    ).inc()
            else:
                logger.debug(
                    "Message delivered: topic=%s, partition=%s, offset=%s",
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )
                self.delivered += 1

                # Record metrics if available
                if _OBSERVABILITY_AVAILABLE:
                    KAFKA_MESSAGES_PRODUCED.labels(topic=topic).inc()

                    # Calculate latency if we have the start time
                    key = msg.key()
                    if key and key.decode("utf-8") in self._pending_start_times:
                        key_str = key.decode("utf-8")
                        start_time = self._pending_start_times.pop(key_str, None)
                        if start_time:
                            latency = time.perf_counter() - start_time
                            KAFKA_PRODUCE_LATENCY.labels(topic=topic).observe(latency)


class EventProducer:
    """Kafka producer with hash chain support for audit trail.

    Usage:
        producer = EventProducer(config)

        # Publish a job created event
        event = producer.publish_job_created(
            source_id="directory-1",
            task_id="job-123",
            directory_key="path0",
            path="/data/image.png",
            fingerprint="abc123",
        )

        # Ensure delivery before shutdown
        producer.flush()
        producer.close()
    """

    def __init__(
        self,
        config: KafkaConfig,
        chain_tracker: HashChainTracker | None = None,
    ) -> None:
        self._config = config
        self._chain_tracker = chain_tracker or HashChainTracker()
        self._delivery_report = DeliveryReport()

        producer_config: dict[str, str | int | bool] = {
            "bootstrap.servers": config.bootstrap_servers,
            "client.id": f"{config.client_id}-producer",
            "acks": config.producer_acks,
            "retries": config.producer_retries,
            "retry.backoff.ms": config.producer_retry_backoff_ms,
            "linger.ms": config.producer_linger_ms,
            "batch.size": config.producer_batch_size,
            "compression.type": config.producer_compression_type,
            "enable.idempotence": config.producer_enable_idempotence,
        }

        # Add security config (TLS/SASL) and connection timeouts
        security_config = build_security_config(config)
        producer_config.update(security_config)

        self._producer = Producer(producer_config)  # type: ignore[arg-type]
        self._closed = False

        # Get tracer if available
        self._tracer = get_tracer(__name__) if _OBSERVABILITY_AVAILABLE else None

        logger.info(
            "EventProducer initialized: bootstrap_servers=%s",
            config.bootstrap_servers,
        )

    def publish(
        self,
        topic: str,
        event: EventEnvelope,
        *,
        key: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Publish an event to a Kafka topic.

        Args:
            topic: Target topic name
            event: Event envelope to publish
            key: Partition key (defaults to event.source_id)
            headers: Optional message headers

        """
        if self._closed:
            msg = "Producer is closed"
            raise RuntimeError(msg)

        # Create span for tracing
        span_ctx = None
        if self._tracer is not None:
            span_ctx = self._tracer.start_as_current_span(  # type: ignore[attr-defined]
                f"kafka.publish.{topic}",
                attributes={
                    "kafka.topic": topic,
                    "event.id": event.event_id,
                    "event.type": event.event_type.value,
                    "event.source_id": event.source_id,
                },
            )
            span_ctx.__enter__()

        try:
            partition_key = (key or event.source_id).encode("utf-8")
            value = event.to_json().encode("utf-8")

            # Build headers with correlation ID
            kafka_headers: list[tuple[str, bytes]] = []
            if headers:
                kafka_headers = [(k, v.encode("utf-8")) for k, v in headers.items()]

            # Inject correlation ID if available
            if _OBSERVABILITY_AVAILABLE:
                cid = get_correlation_id()
                if cid:
                    kafka_headers.append(("x-correlation-id", cid.encode("utf-8")))

            # Track send time for latency metrics
            self._delivery_report.track_send(key or event.source_id)

            self._producer.produce(
                topic=topic,
                key=partition_key,
                value=value,
                headers=kafka_headers if kafka_headers else None,  # type: ignore[arg-type]
                callback=self._delivery_report.on_delivery,
            )

            # Update chain tracker after successful enqueue
            self._chain_tracker.update(event.source_id, event.content_hash)

            # Trigger delivery of buffered messages
            self._producer.poll(0)

        finally:
            if span_ctx:
                span_ctx.__exit__(None, None, None)

    def publish_with_audit(
        self,
        topic: str,
        event: EventEnvelope,
        *,
        key: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Publish event to operational topic and audit log.

        This dual-write ensures GxP compliance by maintaining an
        immutable audit trail separate from operational data.

        Args:
            topic: Operational topic name
            event: Event envelope to publish
            key: Partition key (defaults to event.source_id)
            headers: Optional message headers

        """
        # Publish to operational topic
        self.publish(topic, event, key=key, headers=headers)

        # Dual-write to audit log (no partition key = single partition)
        audit_headers = {
            "source-topic": topic,
            "source-partition-key": key or event.source_id,
        }
        if headers:
            audit_headers.update(headers)

        # Add correlation ID to audit headers
        if _OBSERVABILITY_AVAILABLE:
            cid = get_correlation_id()
            if cid:
                audit_headers["x-correlation-id"] = cid

        self._producer.produce(
            topic=self._config.topic_audit_log,
            value=event.to_json().encode("utf-8"),
            headers=[(k, v.encode("utf-8")) for k, v in audit_headers.items()],
            callback=self._delivery_report.on_delivery,
        )

    def create_event(
        self,
        event_type: EventType,
        source_id: str,
        payload: dict[str, Any],
    ) -> EventEnvelope:
        """Create a new event with proper chain linkage.

        Args:
            event_type: Type of event
            source_id: Partition key for ordering
            payload: Event data

        Returns:
            EventEnvelope with computed hashes and chain linkage

        """
        prev_hash = self._chain_tracker.get_prev_hash(source_id)
        return EventEnvelope.create(
            event_type=event_type,
            source_id=source_id,
            payload=payload,
            prev_hash=prev_hash,
        )

    # Convenience methods for common event types

    def publish_job_created(
        self,
        source_id: str,
        task_id: str,
        directory_key: str,
        path: str,
        fingerprint: str,
        file_hash: str | None = None,
    ) -> EventEnvelope:
        """Publish TASK_CREATED event."""
        from src.domain.events import task_created_payload

        payload = task_created_payload(
            task_id=task_id,
            directory_key=directory_key,
            path=path,
            fingerprint=fingerprint,
            file_hash=file_hash,
        )
        event = self.create_event(EventType.TASK_CREATED, source_id, payload)
        self.publish_with_audit(self._config.topic_jobs, event)
        return event

    def publish_job_started(
        self,
        source_id: str,
        task_id: str,
        processor_name: str,
        processor_version: str,
    ) -> EventEnvelope:
        """Publish TASK_STARTED event."""
        from src.domain.events import task_started_payload

        payload = task_started_payload(
            task_id=task_id,
            processor_name=processor_name,
            processor_version=processor_version,
        )
        event = self.create_event(EventType.TASK_STARTED, source_id, payload)
        self.publish_with_audit(self._config.topic_jobs, event)
        return event

    def publish_job_completed(
        self,
        source_id: str,
        task_id: str,
        processor_name: str,
        processor_version: str,
        duration_ms: float,
    ) -> EventEnvelope:
        """Publish TASK_COMPLETED event."""
        from src.domain.events import task_completed_payload

        payload = task_completed_payload(
            task_id=task_id,
            processor_name=processor_name,
            processor_version=processor_version,
            duration_ms=duration_ms,
        )
        event = self.create_event(EventType.TASK_COMPLETED, source_id, payload)
        self.publish_with_audit(self._config.topic_jobs, event)
        return event

    def publish_job_failed(
        self,
        source_id: str,
        task_id: str,
        error: str,
        processor_name: str | None = None,
        processor_version: str | None = None,
        error_type: str | None = None,
    ) -> EventEnvelope:
        """Publish TASK_FAILED event."""
        from src.domain.events import task_failed_payload

        payload = task_failed_payload(
            task_id=task_id,
            processor_name=processor_name,
            processor_version=processor_version,
            error=error,
            error_type=error_type,
        )
        event = self.create_event(EventType.TASK_FAILED, source_id, payload)
        self.publish_with_audit(self._config.topic_jobs, event)
        return event

    def publish_result_produced(
        self,
        source_id: str,
        task_id: str,
        processor_name: str,
        processor_version: str,
        metric_count: int,
        artifact_refs: list[str],
    ) -> EventEnvelope:
        """Publish RESULT_PRODUCED event."""
        from src.domain.events import result_produced_payload

        payload = result_produced_payload(
            task_id=task_id,
            processor_name=processor_name,
            processor_version=processor_version,
            metric_count=metric_count,
            artifact_refs=artifact_refs,
        )
        event = self.create_event(EventType.RESULT_PRODUCED, source_id, payload)
        self.publish_with_audit(self._config.topic_results, event)
        return event

    def publish_metric_emitted(
        self,
        source_id: str,
        task_id: str,
        processor_name: str,
        processor_version: str,
        metric_name: str,
        value: Any,
        aggregation_mask: int,
        meta: dict[str, Any] | None = None,
    ) -> EventEnvelope:
        """Publish METRIC_EMITTED event to metrics topic.

        Metrics are partitioned by processor_name for efficient aggregation.
        """
        from src.domain.events import metric_emitted_payload

        payload = metric_emitted_payload(
            task_id=task_id,
            processor_name=processor_name,
            processor_version=processor_version,
            metric_name=metric_name,
            value=value,
            aggregation_mask=aggregation_mask,
            meta=meta,
        )
        # Use processor_name as partition key for metric aggregation
        event = self.create_event(EventType.METRIC_EMITTED, processor_name, payload)
        self.publish(self._config.topic_metrics, event, key=processor_name)
        return event

    def publish_aggregate_produced(
        self,
        source_id: str,
        processor_name: str,
        processor_version: str,
        metric_name: str,
        window_start: str,
        window_end: str,
        count: int,
        summary: dict[str, Any],
        object_ref: str | None = None,
    ) -> EventEnvelope:
        """Publish AGGREGATE_COMPUTED event for Flink aggregation results."""
        from datetime import datetime

        from src.domain.events import aggregate_computed_payload

        # Parse ISO timestamps to unix for consistency
        try:
            start_unix = datetime.fromisoformat(window_start).timestamp()
            end_unix = datetime.fromisoformat(window_end).timestamp()
        except ValueError:
            start_unix = 0.0
            end_unix = 0.0

        payload = aggregate_computed_payload(
            processor_name=processor_name,
            processor_version=processor_version,
            metric_name=metric_name,
            aggregation_type="SUMMARY",
            window_start_unix=start_unix,
            window_end_unix=end_unix,
            summary=summary,
            artifact_ref=object_ref,
        )
        event = self.create_event(EventType.AGGREGATE_COMPUTED, source_id, payload)
        self.publish(self._config.topic_aggregates, event, key=processor_name)
        return event

    def publish_artifact_stored(
        self,
        source_id: str,
        content_hash: str,
        bucket: str,
        key: str,
        size: int,
        mime: str,
        source_task_id: str | None = None,
    ) -> EventEnvelope:
        """Publish ARTIFACT_STORED event."""
        from src.domain.events import artifact_stored_payload

        payload = artifact_stored_payload(
            content_hash=content_hash,
            bucket=bucket,
            key=key,
            size=size,
            mime=mime,
            source_task_id=source_task_id,
        )
        event = self.create_event(EventType.ARTIFACT_STORED, source_id, payload)
        self.publish_with_audit(self._config.topic_results, event)
        return event

    def flush(self, timeout: float = 30.0) -> int:
        """Flush all buffered messages.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            Number of messages still in queue after flush

        """
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning(
                "Flush timeout: %d messages still pending",
                remaining,
            )
        return remaining

    def poll(self, timeout: float = 0) -> int:
        """Poll for delivery callbacks.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            Number of events processed

        """
        return self._producer.poll(timeout=timeout)

    def close(self) -> None:
        """Close the producer, flushing pending messages."""
        if self._closed:
            return

        self._closed = True
        remaining = self.flush(timeout=30.0)

        if remaining > 0:
            logger.error(
                "Producer closed with %d messages not delivered",
                remaining,
            )

        logger.info(
            "EventProducer closed: delivered=%d, failed=%d",
            self._delivery_report.delivered,
            self._delivery_report.failed,
        )

    @property
    def delivery_stats(self) -> tuple[int, int]:
        """Get delivery statistics (delivered, failed)."""
        return (
            self._delivery_report.delivered,
            self._delivery_report.failed,
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
