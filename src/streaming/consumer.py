"""Kafka consumer for event streaming.

The EventConsumer provides:
- Pause/resume for slow job handling
- Manual offset commit after processing
- Partition-aware consumption
- Backpressure integration
"""

from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Any, Callable, Iterator

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from src.domain.events import EventEnvelope
from src.utils.logging import get_logger

if TYPE_CHECKING:
    from src.streaming.config import KafkaConfig

logger = get_logger(__name__)


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
    ) -> None:
        self._config = config
        self._topics = topics
        self._group_id = group_id or config.consumer_group_id

        consumer_config = {
            "bootstrap.servers": config.bootstrap_servers,
            "client.id": f"{config.client_id}-consumer",
            "group.id": self._group_id,
            "auto.offset.reset": config.consumer_auto_offset_reset,
            "enable.auto.commit": config.consumer_enable_auto_commit,
            "session.timeout.ms": config.consumer_session_timeout_ms,
            "heartbeat.interval.ms": config.consumer_heartbeat_interval_ms,
            "max.poll.interval.ms": config.consumer_max_poll_interval_ms,
        }

        self._consumer = Consumer(consumer_config)
        self._consumer.subscribe(topics, on_assign=self._on_assign, on_revoke=self._on_revoke)

        self._closed = False
        self._stop = threading.Event()
        self._current_message = None
        self._paused_partitions: set[tuple[str, int]] = set()

        # Statistics
        self._messages_consumed = 0
        self._messages_committed = 0
        self._errors = 0

        logger.info(
            "EventConsumer initialized: group=%s, topics=%s",
            self._group_id,
            topics,
        )

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Callback when partitions are assigned."""
        partition_info = [(p.topic, p.partition) for p in partitions]
        logger.info("Partitions assigned: %s", partition_info)

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        """Callback when partitions are revoked."""
        # Commit pending offsets before revocation
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as e:
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
            raise RuntimeError("Consumer is closed")

        msg = self._consumer.poll(timeout=timeout)
        if msg is None:
            return None

        if msg.error():
            error = msg.error()
            if error.code() == KafkaError._PARTITION_EOF:
                logger.debug(
                    "Reached end of partition: topic=%s, partition=%s",
                    msg.topic(),
                    msg.partition(),
                )
                return None
            else:
                self._errors += 1
                logger.error("Consumer error: %s", error)
                raise KafkaException(error)

        self._current_message = msg
        self._messages_consumed += 1

        try:
            value = msg.value().decode("utf-8")
            event = EventEnvelope.from_json(value)
            return event
        except Exception as e:
            logger.error(
                "Failed to deserialize message: topic=%s, partition=%s, error=%s",
                msg.topic(),
                msg.partition(),
                e,
            )
            self._errors += 1
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
            self._consumer.commit(message=self._current_message, asynchronous=asynchronous)
            self._messages_committed += 1
            self._current_message = None
        except KafkaException as e:
            logger.error("Failed to commit offset: %s", e)
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
            self._consumer.commit(offsets=offsets, asynchronous=asynchronous)
        except KafkaException as e:
            logger.error("Failed to commit offsets: %s", e)
            raise

    def pause_current(self) -> None:
        """Pause consumption from current message's partition.

        Use this when processing a slow job to prevent consumer
        timeout while still allowing other partitions to be consumed.
        """
        if self._current_message is None:
            return

        tp = TopicPartition(
            self._current_message.topic(),
            self._current_message.partition(),
        )
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

        tp = TopicPartition(
            self._current_message.topic(),
            self._current_message.partition(),
        )
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

        # Final commit
        try:
            self._consumer.commit(asynchronous=False)
        except KafkaException as e:
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

    def __enter__(self) -> EventConsumer:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
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

        consumer_config = {
            "bootstrap.servers": config.bootstrap_servers,
            "client.id": f"{config.client_id}-batch-consumer",
            "group.id": self._group_id,
            "auto.offset.reset": config.consumer_auto_offset_reset,
            "enable.auto.commit": False,
            "session.timeout.ms": config.consumer_session_timeout_ms,
            "heartbeat.interval.ms": config.consumer_heartbeat_interval_ms,
            "max.poll.interval.ms": config.consumer_max_poll_interval_ms,
        }

        self._consumer = Consumer(consumer_config)
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
            raise RuntimeError("Consumer is closed")

        batch: list[EventEnvelope] = []
        start_time = time.time()

        while len(batch) < self._batch_size:
            remaining = self._batch_timeout - (time.time() - start_time)
            if remaining <= 0:
                break

            msg = self._consumer.poll(timeout=min(remaining, 0.1))
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Consumer error: %s", msg.error())
                continue

            try:
                value = msg.value().decode("utf-8")
                event = EventEnvelope.from_json(value)
                batch.append(event)
            except Exception as e:
                logger.error("Failed to deserialize message: %s", e)

        return batch

    def commit(self) -> None:
        """Commit all consumed offsets."""
        try:
            self._consumer.commit(asynchronous=False)
        except KafkaException as e:
            logger.error("Failed to commit: %s", e)
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

        try:
            self._consumer.commit(asynchronous=False)
        except KafkaException:
            pass

        self._consumer.close()
        logger.info("BatchEventConsumer closed")

    def __enter__(self) -> BatchEventConsumer:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
