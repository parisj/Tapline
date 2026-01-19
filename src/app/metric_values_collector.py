"""Metric values collector that stores raw values for analysis.

This collector runs alongside the aggregate consumer and stores raw metric
values to MinIO. These values are used for AnalysisKind-specific computations
(histograms, ellipses, contours, rates, counters) that require raw data.

The values are stored in time-windowed buckets matching the Flink aggregation
windows, enabling the dashboard to retrieve raw values for any aggregate.

Usage:
    Integrated into main pipeline via run_metric_values_collector()
"""

from __future__ import annotations

import hashlib
import time
from collections import defaultdict
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from confluent_kafka import Consumer, KafkaError, KafkaException

from src.utils.logging import get_logger

# Try to use orjson for better performance
try:
    import orjson

    def json_loads(data: bytes | str) -> dict:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)

    def json_dumps(obj: dict) -> bytes:
        return orjson.dumps(obj)

except ImportError:
    import json

    def json_loads(data: bytes | str) -> dict:
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return json.loads(data)

    def json_dumps(obj: dict) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")


if TYPE_CHECKING:
    import threading

    from src.flink.config import FlinkConfig
    from src.storage.minio_service import MinioStorageService
    from src.streaming.config import KafkaConfig

logger = get_logger("pipeline.app.metric_values_collector")

# Constants
_LOG_INTERVAL_SEC = 30.0
_TOPIC_RETRY_INTERVAL_SEC = 5.0
_FLUSH_INTERVAL_SEC = 60.0  # Flush values every minute (match Flink window)


def _create_consumer(kafka_config: KafkaConfig, group_id: str, topic: str) -> Consumer:
    """Create a Kafka consumer for metric events."""
    consumer_config = {
        "bootstrap.servers": kafka_config.bootstrap_servers,
        "client.id": f"{kafka_config.client_id}-values-collector",
        "group.id": group_id,
        "auto.offset.reset": kafka_config.consumer_auto_offset_reset,
        "enable.auto.commit": False,
        "session.timeout.ms": kafka_config.consumer_session_timeout_ms,
        "heartbeat.interval.ms": kafka_config.consumer_heartbeat_interval_ms,
        "max.poll.interval.ms": kafka_config.consumer_max_poll_interval_ms,
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])
    return consumer


class _CollectorState:
    """Mutable state for collector reconnection handling."""

    def __init__(self, consumer: Consumer) -> None:
        self.consumer = consumer
        self.needs_reconnect = False


class MetricValuesWindow:
    """Collects metric values within a time window."""

    def __init__(self, window_start: datetime, window_end: datetime) -> None:
        self.window_start = window_start
        self.window_end = window_end
        self.metrics: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)

    def add(
        self,
        algo_name: str,
        algo_version: str,
        metric_name: str,
        value: float | bool | dict | list,
        analysis_mask: int,
        meta: dict | None,
    ) -> None:
        """Add a metric value to the window."""
        key = (algo_name, algo_version, metric_name)
        self.metrics[key].append({
            "value": value,
            "analysis_mask": analysis_mask,
            "meta": meta,
        })


def _get_window_boundaries(timestamp: datetime, window_size_sec: int) -> tuple[datetime, datetime]:
    """Calculate window boundaries for a given timestamp."""
    epoch_sec = int(timestamp.timestamp())
    window_start_sec = (epoch_sec // window_size_sec) * window_size_sec
    window_start = datetime.fromtimestamp(window_start_sec, tz=UTC)
    window_end = datetime.fromtimestamp(window_start_sec + window_size_sec, tz=UTC)
    return window_start, window_end


def _store_window_values(
    window: MetricValuesWindow,
    storage: MinioStorageService,
) -> int:
    """Store values from a window to MinIO.

    Returns
    -------
        Number of metric groups stored

    """
    stored = 0

    for (algo_name, algo_version, metric_name), entries in window.metrics.items():
        if not entries:
            continue

        # Extract just the values and the analysis_mask (should be same for all)
        values = [e["value"] for e in entries]
        analysis_mask = entries[0]["analysis_mask"] if entries else 0
        meta = entries[0].get("meta")

        # Create the values document
        doc = {
            "algo_name": algo_name,
            "algo_version": algo_version,
            "metric_name": metric_name,
            "analysis_mask": analysis_mask,
            "meta": meta,
            "window_start": window.window_start.isoformat(),
            "window_end": window.window_end.isoformat(),
            "window_start_unix": int(window.window_start.timestamp()),
            "window_end_unix": int(window.window_end.timestamp()),
            "count": len(values),
            "values": values,
        }

        try:
            doc_bytes = json_dumps(doc)

            # Create a deterministic key based on metric identity and window
            key_str = f"{algo_name}|{algo_version}|{metric_name}|{window.window_start.isoformat()}"
            content_hash = hashlib.sha256(key_str.encode()).hexdigest()

            # Store with sharded path
            bucket = storage.buckets.get("metric-values", "metric-values")
            key = f"{content_hash[:2]}/{content_hash[2:4]}/{content_hash}"

            storage.store_with_key(
                data=doc_bytes,
                bucket=bucket,
                key=key,
                mime="application/json",
            )

            logger.debug(
                "Stored values: %s/%s/%s [%s], count=%d",
                algo_name,
                algo_version,
                metric_name,
                window.window_start.isoformat(),
                len(values),
            )
            stored += 1

        except Exception as e:
            logger.warning(
                "Failed to store values for %s/%s/%s: %s",
                algo_name,
                algo_version,
                metric_name,
                e,
            )

    return stored


def run_metric_values_collector(  # noqa: PLR0912
    flink_config: FlinkConfig,
    kafka_config: KafkaConfig,
    storage: MinioStorageService,
    stop_event: threading.Event,
) -> None:
    """Run the metric values collector.

    Args:
        flink_config: Flink configuration (for window size)
        kafka_config: Kafka configuration
        storage: MinIO storage service
        stop_event: Event to signal shutdown

    """
    group_id = f"{flink_config.kafka_consumer_group}-values-collector"
    topic = kafka_config.topic_metrics
    window_size_sec = flink_config.tumbling_size_sec

    state = _CollectorState(_create_consumer(kafka_config, group_id, topic))

    logger.info(
        "Metric values collector started: topic=%s, group=%s, window_size=%ds",
        topic,
        group_id,
        window_size_sec,
    )

    # Current window
    current_window: MetricValuesWindow | None = None

    events_processed = 0
    values_stored = 0
    windows_flushed = 0
    last_log_time = time.time()

    try:
        while not stop_event.is_set():
            # Handle reconnection
            if state.needs_reconnect:
                state.consumer.close()
                time.sleep(_TOPIC_RETRY_INTERVAL_SEC)
                state.consumer = _create_consumer(kafka_config, group_id, topic)
                state.needs_reconnect = False
                continue

            try:
                msg = state.consumer.poll(timeout=1.0)
            except KafkaException as e:
                if "UNKNOWN_TOPIC_OR_PART" in str(e):
                    logger.info(
                        "Topic %s not available yet, retrying in %ds...",
                        topic,
                        int(_TOPIC_RETRY_INTERVAL_SEC),
                    )
                    state.needs_reconnect = True
                    continue
                raise

            now = datetime.now(UTC)

            # Check if we need to flush the current window
            if current_window is not None and now >= current_window.window_end:
                stored = _store_window_values(current_window, storage)
                values_stored += stored
                windows_flushed += 1
                current_window = None

                try:
                    state.consumer.commit(asynchronous=False)
                except KafkaException as e:
                    logger.warning("Failed to commit offsets: %s", e)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    logger.info("Topic %s not available yet...", topic)
                    state.needs_reconnect = True
                    continue
                logger.error("Consumer error: %s", msg.error())
                continue

            try:
                event = json_loads(msg.value())

                # Only process METRIC_EMITTED events
                if event.get("event_type") != "METRIC_EMITTED":
                    continue

                payload = event.get("payload", {})
                algo_name = payload.get("algo_name", "unknown")
                algo_version = payload.get("algo_version", "0.0.0")
                metric_name = payload.get("metric_name", "unknown")
                value = payload.get("value")
                analysis_mask = payload.get("analysis_mask", 0)
                meta = payload.get("meta")

                # Skip if no value
                if value is None:
                    continue

                # Initialize window if needed
                if current_window is None:
                    window_start, window_end = _get_window_boundaries(now, window_size_sec)
                    current_window = MetricValuesWindow(window_start, window_end)

                # Add value to current window
                current_window.add(
                    algo_name=algo_name,
                    algo_version=algo_version,
                    metric_name=metric_name,
                    value=value,
                    analysis_mask=analysis_mask,
                    meta=meta,
                )

                events_processed += 1

            except Exception as e:
                logger.warning(
                    "Failed to process message: partition=%s, offset=%s, error=%s",
                    msg.partition(),
                    msg.offset(),
                    e,
                )

            # Periodic logging
            current_time = time.time()
            if current_time - last_log_time >= _LOG_INTERVAL_SEC:
                logger.info(
                    "Values collector status: events=%d, stored=%d, windows=%d",
                    events_processed,
                    values_stored,
                    windows_flushed,
                )
                last_log_time = current_time

    finally:
        # Flush any remaining window
        if current_window is not None:
            stored = _store_window_values(current_window, storage)
            values_stored += stored
            windows_flushed += 1

        state.consumer.close()

        logger.info(
            "Values collector stopped: events=%d, stored=%d, windows=%d",
            events_processed,
            values_stored,
            windows_flushed,
        )
