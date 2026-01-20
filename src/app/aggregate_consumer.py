"""Kafka aggregate consumer that writes Flink output to MinIO.

This consumer reads aggregate records from the Kafka aggregates topic
(produced by the Flink SQL job) and persists them to MinIO for dashboard access.

Note: The aggregates topic contains raw JSON records (not EventEnvelope),
since they are produced directly by Flink SQL.

Optimizations:
- Uses orjson for fast JSON parsing/serialization (falls back to json)
- Batches messages for efficient MinIO writes
- Processes messages in parallel within batches
- Compact JSON output (no whitespace)

Usage:
    Integrated into main pipeline via run_aggregate_consumer()
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from confluent_kafka import Consumer, KafkaError, KafkaException

from src.utils.logging import get_logger

# Try to use orjson for better performance, fall back to standard json
try:
    import orjson

    def json_loads(data: bytes | str) -> dict:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

    def json_dumps(obj: dict) -> bytes:
        return orjson.dumps(obj)

    _USING_ORJSON = True
except ImportError:
    import json

    def json_loads(data: bytes | str) -> dict:
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return json.loads(data)

    def json_dumps(obj: dict) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")

    _USING_ORJSON = False

if TYPE_CHECKING:
    import threading

    from src.flink.config import FlinkConfig
    from src.storage.minio_service import MinioStorageService
    from src.streaming.config import KafkaConfig

logger = get_logger("pipeline.app.aggregate_consumer")

# Constants
_LOG_INTERVAL_SEC = 30.0
_TOPIC_RETRY_INTERVAL_SEC = 5.0  # Retry interval when topic doesn't exist
_BATCH_SIZE = 100  # Max messages per batch
_BATCH_TIMEOUT_SEC = 1.0  # Max time to wait for batch to fill
_STORAGE_WORKERS = 4  # Parallel MinIO writers


def _create_consumer(kafka_config: KafkaConfig, group_id: str, topic: str) -> Consumer:
    """Create a Kafka consumer for raw JSON messages."""
    consumer_config = {
        "bootstrap.servers": kafka_config.bootstrap_servers,
        "client.id": f"{kafka_config.client_id}-aggregate-consumer",
        "group.id": group_id,
        "auto.offset.reset": kafka_config.consumer_auto_offset_reset,
        "enable.auto.commit": False,  # Manual commit after batch processing
        "session.timeout.ms": kafka_config.consumer_session_timeout_ms,
        "heartbeat.interval.ms": kafka_config.consumer_heartbeat_interval_ms,
        "max.poll.interval.ms": kafka_config.consumer_max_poll_interval_ms,
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])
    return consumer


class _ConsumerState:
    """Mutable state for consumer reconnection handling."""

    def __init__(self, consumer: Consumer) -> None:
        self.consumer = consumer
        self.needs_reconnect = False


def _handle_kafka_error(
    error: KafkaError,
    topic: str,
    state: _ConsumerState,
) -> bool:
    """Handle Kafka errors, return True if should continue polling."""
    if error.code() == KafkaError._PARTITION_EOF:
        return True
    if error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
        logger.info(
            "Topic %s not available yet, will retry...",
            topic,
        )
        state.needs_reconnect = True
        return False
    logger.error("Consumer error: %s", error)
    return True


def _poll_batch(
    state: _ConsumerState,
    topic: str,
    kafka_config: KafkaConfig,
    group_id: str,
) -> list[dict]:
    """Poll for a batch of messages.

    Args:
        state: Consumer state (may be updated for reconnection)
        topic: Kafka topic
        kafka_config: Kafka configuration
        group_id: Consumer group ID

    Returns:
        List of parsed aggregate records

    """
    batch: list[dict] = []
    batch_start = time.time()

    while len(batch) < _BATCH_SIZE:
        elapsed = time.time() - batch_start
        if elapsed >= _BATCH_TIMEOUT_SEC and batch:
            break

        poll_timeout = min(0.1, _BATCH_TIMEOUT_SEC - elapsed)
        if poll_timeout <= 0:
            break

        # Handle reconnection if needed
        if state.needs_reconnect:
            state.consumer.close()
            time.sleep(_TOPIC_RETRY_INTERVAL_SEC)
            state.consumer = _create_consumer(kafka_config, group_id, topic)
            state.needs_reconnect = False
            batch_start = time.time()
            continue

        try:
            msg = state.consumer.poll(timeout=poll_timeout)
        except KafkaException as e:
            if "UNKNOWN_TOPIC_OR_PART" in str(e):
                logger.info(
                    "Topic %s not available yet (Flink job may not be running), retrying in %ds...",
                    topic,
                    int(_TOPIC_RETRY_INTERVAL_SEC),
                )
                state.needs_reconnect = True
                continue
            raise

        if msg is None:
            continue

        if msg.error():
            if not _handle_kafka_error(msg.error(), topic, state):
                continue
            continue

        try:
            aggregate = json_loads(msg.value())
            batch.append(aggregate)
        except Exception as e:
            logger.warning(
                "Failed to parse aggregate message: partition=%s, offset=%s, error=%s",
                msg.partition(),
                msg.offset(),
                e,
            )

    return batch


def run_aggregate_consumer(
    flink_config: FlinkConfig,
    kafka_config: KafkaConfig,
    storage: MinioStorageService,
    stop_event: threading.Event,
) -> None:
    """Run the aggregate consumer that writes Flink output to MinIO.

    Args:
        flink_config: Flink configuration (for consumer group)
        kafka_config: Kafka configuration
        storage: MinIO storage service
        stop_event: Event to signal shutdown

    """
    group_id = f"{flink_config.kafka_consumer_group}-minio-sink"
    topic = kafka_config.topic_aggregates

    state = _ConsumerState(_create_consumer(kafka_config, group_id, topic))

    logger.info(
        "Aggregate consumer started: topic=%s, group=%s, batch_size=%d, workers=%d, orjson=%s",
        topic,
        group_id,
        _BATCH_SIZE,
        _STORAGE_WORKERS,
        _USING_ORJSON,
    )

    events_processed = 0
    aggregates_stored = 0
    batches_processed = 0
    last_log_time = time.time()

    executor = ThreadPoolExecutor(max_workers=_STORAGE_WORKERS, thread_name_prefix="minio-writer")

    try:
        while not stop_event.is_set():
            batch = _poll_batch(state, topic, kafka_config, group_id)

            if stop_event.is_set():
                break

            if batch:
                stored = _process_batch(batch, storage, executor)
                aggregates_stored += stored
                events_processed += len(batch)
                batches_processed += 1

                try:
                    state.consumer.commit(asynchronous=False)
                except KafkaException as e:
                    logger.warning("Failed to commit offsets: %s", e)

            now = time.time()
            if now - last_log_time >= _LOG_INTERVAL_SEC:
                logger.info(
                    "Aggregate consumer status: events=%d, stored=%d, batches=%d",
                    events_processed,
                    aggregates_stored,
                    batches_processed,
                )
                last_log_time = now

    finally:
        executor.shutdown(wait=True)
        state.consumer.close()

        logger.info(
            "Aggregate consumer stopped: events_processed=%d, aggregates_stored=%d, batches=%d",
            events_processed,
            aggregates_stored,
            batches_processed,
        )


def _process_batch(
    batch: list[dict],
    storage: MinioStorageService,
    executor: ThreadPoolExecutor,
) -> int:
    """Process a batch of aggregates in parallel.

    Args:
        batch: List of aggregate records
        storage: MinIO storage service
        executor: Thread pool for parallel writes

    Returns:
        Number of successfully stored aggregates

    """
    futures = [executor.submit(_store_aggregate, agg, storage) for agg in batch]

    stored = 0
    for future in as_completed(futures):
        try:
            if future.result():
                stored += 1
        except Exception as e:
            logger.warning("Failed to store aggregate: %s", e)

    return stored


_FLINK_TS_FORMAT = "%Y-%m-%d %H:%M:%S"


def _parse_flink_timestamp(ts_str: str) -> int:
    """Parse Flink timestamp string to unix timestamp."""
    if not ts_str:
        return 0
    try:
        dt = datetime.strptime(ts_str, _FLINK_TS_FORMAT)  # noqa: DTZ007
        return int(dt.replace(tzinfo=UTC).timestamp())
    except ValueError:
        return 0


def _store_aggregate(aggregate: dict, storage: MinioStorageService) -> bool:
    """Store an aggregate record to MinIO.

    Args:
        aggregate: Aggregate record from Flink (raw JSON)
        storage: MinIO storage service

    Returns:
        True if stored successfully

    """
    window_start = aggregate.get("window_start", "")
    window_end = aggregate.get("window_end", "")

    aggregate_doc = {
        "algo_name": aggregate.get("algo_name", "unknown"),
        "algo_version": aggregate.get("algo_version", "0.0.0"),
        "metric_name": aggregate.get("metric_name", "unknown"),
        "analysis_mask": aggregate.get("analysis_mask", 0),
        "window_start": window_start,
        "window_end": window_end,
        "window_start_unix": _parse_flink_timestamp(window_start),
        "window_end_unix": _parse_flink_timestamp(window_end),
        "summary": {
            "count": aggregate.get("metric_count", 0),
            "sum": aggregate.get("metric_sum", 0.0),
            "avg": aggregate.get("metric_avg", 0.0),
            "min": aggregate.get("metric_min", 0.0),
            "max": aggregate.get("metric_max", 0.0),
        },
    }

    aggregate_bytes = json_dumps(aggregate_doc)
    object_ref = storage.store(
        data=aggregate_bytes,
        bucket="aggregates",
        mime="application/json",
    )

    logger.debug(
        "Stored aggregate: %s/%s/%s [%s - %s] -> %s",
        aggregate_doc["algo_name"],
        aggregate_doc["algo_version"],
        aggregate_doc["metric_name"],
        window_start,
        window_end,
        object_ref.full_path,
    )

    return True
