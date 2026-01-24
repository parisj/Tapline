"""Kafka consumer for event streaming.

The EventConsumer provides:
- Pause/resume for slow job handling
- Manual offset commit after processing
- Partition-aware consumption
- Backpressure integration
- OpenTelemetry tracing and Prometheus metrics
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Self

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition

from src.domain.events import EventEnvelope
from src.observability.availability import get_tracer
from src.observability.availability import is_available as _obs_available
from src.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from types import TracebackType

    from src.streaming.config import KafkaConfig

from src.streaming.config import build_security_config

logger = get_logger(__name__)

_OBSERVABILITY_AVAILABLE = _obs_available()

if _OBSERVABILITY_AVAILABLE:
    from src.observability.correlation import propagate_from_kafka_headers
    from src.observability.metrics import (
        KAFKA_CONSUME_ERRORS,
        KAFKA_MESSAGES_CONSUMED,
    )


class EventConsumer:
    """Kafka consumer with pause/resume support for slow jobs.

    Features:
    - Manual offset management for exactly-once processing
    - Partition pause/resume for backpressure control
    - Graceful shutdown with offset commit
    - Iterator interface for message consumption

    Usage:
        consumer = EventConsumer(config, topics=["visio.jobs"])

        for event in consumer:
            try:
                process(event)
                consumer.commit()
            except SlowJobError:
                consumer.pause_current()
                # Handle slow job asynchronously
                consumer.resume_current()

        consumer.close()
    """

    def __init__(
        self,
        config: KafkaConfig,
        topics: list[str],
        group_id: str | None = None,
        on_assign_callback: Callable[[list[TopicPartition]], None] | None = None,
    ) -> None:
        self._config = config
        self._topics = topics
        self._group_id = group_id or config.consumer_group_id
        self._on_assign_callback = on_assign_callback

        consumer_config: dict[str, str | int | bool] = {
            "bootstrap.servers": config.bootstrap_servers,
            "client.id": f"{config.client_id}-consumer",
            "group.id": self._group_id,
            "auto.offset.reset": config.consumer_auto_offset_reset,
            "enable.auto.commit": config.consumer_enable_auto_commit,
            "session.timeout.ms": config.consumer_session_timeout_ms,
            "heartbeat.interval.ms": config.consumer_heartbeat_interval_ms,
            "max.poll.interval.ms": config.consumer_max_poll_interval_ms,
            # Cooperative-sticky: workers keep their partitions during rebalance,
            # only orphaned partitions get reassigned (prevents stop-the-world)
            "partition.assignment.strategy": config.consumer_partition_assignment_strategy,
        }

        # Add security config (TLS/SASL) and connection timeouts
        security_config = build_security_config(config)
        consumer_config.update(security_config)

        self._consumer = Consumer(consumer_config)  # type: ignore[arg-type]
        self._consumer.subscribe(topics, on_assign=self._on_assign, on_revoke=self._on_revoke)

        self._closed = False
        self._stop = threading.Event()
        self._current_message: Message | None = None
        self._paused_partitions: set[tuple[str, int]] = set()
        self._partitions_assigned = threading.Event()

        # Statistics
        self._messages_consumed = 0
        self._messages_committed = 0
        self._errors = 0

        # Get tracer if available
        self._tracer = get_tracer(__name__) if _OBSERVABILITY_AVAILABLE else None

        logger.info(
            "EventConsumer initialized: group=%s, topics=%s",
            self._group_id,
            topics,
        )

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Callback when partitions are assigned."""
        partition_info = [(p.topic, p.partition) for p in partitions]
        logger.info("Partitions assigned: %s", partition_info)

        # Signal that partitions have been assigned
        self._partitions_assigned.set()

        # Call external callback if provided
        if self._on_assign_callback:
            try:
                self._on_assign_callback(partitions)
            except Exception as e:
                logger.warning("on_assign_callback failed: %s", e)

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Callback when partitions are revoked."""
        # Only commit if we have consumed messages (avoids "No offset stored" warning)
        if self._messages_consumed > self._messages_committed:
            try:
                consumer.commit(asynchronous=False)
                self._messages_committed = self._messages_consumed
            except KafkaException as e:
                # Ignore "no offset stored" errors - they're expected when no messages consumed
                if "_NO_OFFSET" not in str(e):
                    logger.warning("Failed to commit during revoke: %s", e)

        partition_info = [(p.topic, p.partition) for p in partitions]
        logger.info("Partitions revoked: %s", partition_info)

    def poll(self, timeout: float = 1.0) -> EventEnvelope | None:
        """Poll for a single message.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            EventEnvelope if message received, None otherwise

        """
        if self._closed:
            msg = "Consumer is closed"
            raise RuntimeError(msg)

        polled_msg = self._consumer.poll(timeout=timeout)
        if polled_msg is None:
            return None

        error = polled_msg.error()
        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:  # type: ignore[attr-defined]
                logger.debug(
                    "Reached end of partition: topic=%s, partition=%s",
                    polled_msg.topic(),
                    polled_msg.partition(),
                )
                return None
            # Handle unknown topic/partition gracefully - topic may not exist yet
            if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                logger.warning(
                    "Topic not available yet: %s (will retry)",
                    polled_msg.topic() or "unknown",
                )
                return None
            self._errors += 1
            logger.error("Consumer error: %s", error)

            # Record error metrics
            if _OBSERVABILITY_AVAILABLE:
                KAFKA_CONSUME_ERRORS.labels(
                    topic=polled_msg.topic() or "unknown",
                    error_type=str(error.code()),
                ).inc()

            raise KafkaException(error)

        self._current_message = polled_msg
        self._messages_consumed += 1

        # Extract and propagate correlation ID from headers
        if _OBSERVABILITY_AVAILABLE:
            propagate_from_kafka_headers(polled_msg.headers())  # type: ignore[arg-type]

        # Record metrics
        if _OBSERVABILITY_AVAILABLE:
            KAFKA_MESSAGES_CONSUMED.labels(
                topic=polled_msg.topic(),
                consumer_group=self._group_id,
            ).inc()

        try:
            msg_value = polled_msg.value()
            if msg_value is None:
                return None
            value = msg_value.decode("utf-8")
            return EventEnvelope.from_json(value)
        except Exception as e:
            logger.exception(
                "Failed to deserialize message: topic=%s, partition=%s, error=%s",
                polled_msg.topic(),
                polled_msg.partition(),
                e,
            )
            self._errors += 1

            # Record error metrics
            if _OBSERVABILITY_AVAILABLE:
                KAFKA_CONSUME_ERRORS.labels(
                    topic=polled_msg.topic() or "unknown",
                    error_type="deserialization_error",
                ).inc()

            raise

    def __iter__(self) -> Iterator[EventEnvelope]:
        """Iterate over messages until stopped."""
        while not self._stop.is_set() and not self._closed:
            event = self.poll(timeout=1.0)
            if event is not None:
                yield event

    def commit(self, asynchronous: bool = False) -> None:
        """Commit current offset.

        Args:
            asynchronous: If True, commit asynchronously

        """
        if self._current_message is None:
            return

        try:
            if asynchronous:
                self._consumer.commit(message=self._current_message, asynchronous=True)
            else:
                self._consumer.commit(message=self._current_message, asynchronous=False)
            self._messages_committed += 1
            self._current_message = None
        except KafkaException as e:
            logger.exception("Failed to commit offset: %s", e)
            raise

    def commit_offsets(
        self,
        offsets: list[TopicPartition],
        asynchronous: bool = False,
    ) -> None:
        """Commit specific offsets.

        Args:
            offsets: List of TopicPartition with offsets
            asynchronous: If True, commit asynchronously

        """
        try:
            if asynchronous:
                self._consumer.commit(offsets=offsets, asynchronous=True)
            else:
                self._consumer.commit(offsets=offsets, asynchronous=False)
        except KafkaException as e:
            logger.exception("Failed to commit offsets: %s", e)
            raise

    def pause_current(self) -> None:
        """Pause consumption from current message's partition.

        Use this when processing a slow job to prevent consumer
        timeout while still allowing other partitions to be consumed.
        """
        if self._current_message is None:
            return

        topic = self._current_message.topic()
        partition = self._current_message.partition()
        if topic is None or partition is None:
            return

        tp = TopicPartition(topic, partition)
        self._consumer.pause([tp])
        self._paused_partitions.add((tp.topic, tp.partition))

        logger.info(
            "Paused partition: topic=%s, partition=%s",
            tp.topic,
            tp.partition,
        )

    def resume_current(self) -> None:
        """Resume consumption from paused partition."""
        if self._current_message is None:
            return

        topic = self._current_message.topic()
        partition = self._current_message.partition()
        if topic is None or partition is None:
            return

        tp = TopicPartition(topic, partition)
        self._consumer.resume([tp])
        self._paused_partitions.discard((tp.topic, tp.partition))

        logger.info(
            "Resumed partition: topic=%s, partition=%s",
            tp.topic,
            tp.partition,
        )

    def pause_partition(self, topic: str, partition: int) -> None:
        """Pause a specific partition."""
        tp = TopicPartition(topic, partition)
        self._consumer.pause([tp])
        self._paused_partitions.add((topic, partition))
        logger.info("Paused partition: topic=%s, partition=%s", topic, partition)

    def resume_partition(self, topic: str, partition: int) -> None:
        """Resume a specific partition."""
        tp = TopicPartition(topic, partition)
        self._consumer.resume([tp])
        self._paused_partitions.discard((topic, partition))
        logger.info("Resumed partition: topic=%s, partition=%s", topic, partition)

    def resume_all(self) -> None:
        """Resume all paused partitions."""
        if not self._paused_partitions:
            return

        tps = [TopicPartition(topic, partition) for topic, partition in self._paused_partitions]
        self._consumer.resume(tps)
        self._paused_partitions.clear()
        logger.info("Resumed all paused partitions")

    def get_assignment(self) -> list[tuple[str, int]]:
        """Get current partition assignment."""
        partitions = self._consumer.assignment()
        return [(p.topic, p.partition) for p in partitions]

    def wait_for_assignment(self, timeout: float = 30.0) -> bool:
        """Wait for partition assignment.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            True if partitions were assigned, False if timeout

        """
        return self._partitions_assigned.wait(timeout=timeout)

    def get_paused_partitions(self) -> set[tuple[str, int]]:
        """Get set of paused partitions."""
        return self._paused_partitions.copy()

    def stop(self) -> None:
        """Signal consumer to stop iteration."""
        self._stop.set()

    def close(self) -> None:
        """Close consumer gracefully."""
        if self._closed:
            return

        self._closed = True
        self._stop.set()

        # Final commit only if we have uncommitted messages
        if self._messages_consumed > self._messages_committed:
            try:
                self._consumer.commit(asynchronous=False)
                self._messages_committed = self._messages_consumed
            except KafkaException as e:
                # Ignore "no offset stored" errors
                if "_NO_OFFSET" not in str(e):
                    logger.warning("Failed final commit on close: %s", e)

        self._consumer.close()

        logger.info(
            "EventConsumer closed: consumed=%d, committed=%d, errors=%d",
            self._messages_consumed,
            self._messages_committed,
            self._errors,
        )

    @property
    def stats(self) -> dict[str, int]:
        """Get consumer statistics."""
        return {
            "consumed": self._messages_consumed,
            "committed": self._messages_committed,
            "errors": self._errors,
            "paused_partitions": len(self._paused_partitions),
        }

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()


class BatchEventConsumer:
    """Batch-oriented consumer for high-throughput scenarios.

    Consumes messages in batches and provides batch-level commit.
    """

    def __init__(
        self,
        config: KafkaConfig,
        topics: list[str],
        batch_size: int = 100,
        batch_timeout: float = 1.0,
        group_id: str | None = None,
    ) -> None:
        self._config = config
        self._topics = topics
        self._batch_size = batch_size
        self._batch_timeout = batch_timeout
        self._group_id = group_id or config.consumer_group_id

        consumer_config: dict[str, str | int | bool] = {
            "bootstrap.servers": config.bootstrap_servers,
            "client.id": f"{config.client_id}-batch-consumer",
            "group.id": self._group_id,
            "auto.offset.reset": config.consumer_auto_offset_reset,
            "enable.auto.commit": False,
            "session.timeout.ms": config.consumer_session_timeout_ms,
            "heartbeat.interval.ms": config.consumer_heartbeat_interval_ms,
            "max.poll.interval.ms": config.consumer_max_poll_interval_ms,
            "partition.assignment.strategy": config.consumer_partition_assignment_strategy,
        }

        # Add security config (TLS/SASL) and connection timeouts
        security_config = build_security_config(config)
        consumer_config.update(security_config)

        self._consumer = Consumer(consumer_config)  # type: ignore[arg-type]
        self._consumer.subscribe(topics)
        self._closed = False
        self._stop = threading.Event()

        logger.info(
            "BatchEventConsumer initialized: group=%s, topics=%s, batch_size=%d",
            self._group_id,
            topics,
            batch_size,
        )

    def poll_batch(self) -> list[EventEnvelope]:
        """Poll for a batch of messages.

        Returns:
            List of EventEnvelopes (may be empty if timeout)

        """
        if self._closed:
            msg = "Consumer is closed"
            raise RuntimeError(msg)

        batch: list[EventEnvelope] = []
        start_time = time.time()

        while len(batch) < self._batch_size:
            remaining = self._batch_timeout - (time.time() - start_time)
            if remaining <= 0:
                break

            polled_msg = self._consumer.poll(timeout=min(remaining, 0.1))
            if polled_msg is None:
                continue

            err = polled_msg.error()
            if err is not None:
                if err.code() != KafkaError._PARTITION_EOF:  # type: ignore[attr-defined]
                    logger.error("Consumer error: %s", err)
                continue

            try:
                msg_value = polled_msg.value()
                if msg_value is None:
                    continue
                value = msg_value.decode("utf-8")
                event = EventEnvelope.from_json(value)
                batch.append(event)
            except Exception as e:
                logger.exception("Failed to deserialize message: %s", e)

        return batch

    def commit(self) -> None:
        """Commit all consumed offsets."""
        try:
            self._consumer.commit(asynchronous=False)
        except KafkaException as e:
            logger.exception("Failed to commit: %s", e)
            raise

    def stop(self) -> None:
        """Signal consumer to stop."""
        self._stop.set()

    def close(self) -> None:
        """Close consumer gracefully."""
        if self._closed:
            return

        self._closed = True
        self._stop.set()

        # Only commit if we have consumed messages
        try:
            positions = self._consumer.position(self._consumer.assignment())
            has_positions = any(p.offset >= 0 for p in positions)
            if has_positions:
                self._consumer.commit(asynchronous=False)
        except KafkaException:
            pass

        self._consumer.close()
        logger.info("BatchEventConsumer closed")

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
