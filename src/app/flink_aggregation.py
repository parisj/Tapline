"""Flink metric aggregation entry point.

This job runs separately from the main streaming pipeline.
It consumes METRIC_EMITTED events from Kafka, aggregates them
in time windows, and stores results to MinIO.

Usage:
    pixi run run-flink-agg
    # or
    python -m src.app.flink_aggregation
"""

from __future__ import annotations

import atexit
import json
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

from src.domain.events import EventType
from src.flink.config import FlinkConfig, load_flink_config
from src.observability.config import load_observability_config
from src.observability.metrics import (
    FLINK_AGGREGATIONS_PRODUCED,
    configure_metrics,
)
from src.observability.tracing import configure_tracing, shutdown_tracing
from src.storage.config import load_minio_config
from src.storage.minio_service import MinioStorageService
from src.streaming.config import load_kafka_config
from src.streaming.consumer import EventConsumer
from src.streaming.producer import EventProducer
from src.utils.logging import configure_logging, get_logger

logger = get_logger("pipeline.app.flink_aggregation")


@dataclass
class MetricAggregate:
    """Accumulated metrics for a single key within a window."""

    algo_name: str
    algo_version: str
    metric_name: str
    analysis_mask: int
    values: list[float] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.values)

    @property
    def sum(self) -> float:
        return sum(self.values) if self.values else 0.0

    @property
    def mean(self) -> float:
        return self.sum / self.count if self.count > 0 else 0.0

    @property
    def min(self) -> float | None:
        return min(self.values) if self.values else None

    @property
    def max(self) -> float | None:
        return max(self.values) if self.values else None

    @property
    def std(self) -> float:
        if self.count < 2:
            return 0.0
        mean = self.mean
        variance = sum((v - mean) ** 2 for v in self.values) / self.count
        return variance**0.5

    def to_summary(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "sum": self.sum,
            "mean": self.mean,
            "std": self.std,
            "min": self.min,
            "max": self.max,
            "median": self._median(),
            "p95": self._percentile(95),
            "p99": self._percentile(99),
        }

    def _median(self) -> float | None:
        if not self.values:
            return None
        sorted_vals = sorted(self.values)
        mid = len(sorted_vals) // 2
        if len(sorted_vals) % 2 == 0:
            return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2
        return sorted_vals[mid]

    def _percentile(self, p: int) -> float | None:
        if not self.values:
            return None
        sorted_vals = sorted(self.values)
        idx = int(len(sorted_vals) * p / 100)
        return sorted_vals[min(idx, len(sorted_vals) - 1)]


class TumblingWindowAggregator:
    """Simple tumbling window aggregator for metrics.

    This is a lightweight Python implementation of what PyFlink would do.
    For production, consider using actual PyFlink with windowed operations.
    """

    def __init__(
        self,
        window_size_sec: int,
        storage: MinioStorageService,
        producer: EventProducer,
    ) -> None:
        self._window_size_sec = window_size_sec
        self._storage = storage
        self._producer = producer

        # Current window state
        # Key: (algo_name, algo_version, metric_name)
        self._current_window: dict[tuple[str, str, str], MetricAggregate] = {}
        self._window_start: datetime | None = None
        self._window_end: datetime | None = None

        self._lock = threading.Lock()
        self._aggregates_produced = 0

    def add_metric(self, event_payload: dict[str, Any]) -> None:
        """Add a metric event to the current window."""
        try:
            algo_name = event_payload.get("algo_name", "unknown")
            algo_version = event_payload.get("algo_version", "0.0.0")
            metric_name = event_payload.get("metric_name", "unknown")
            value = event_payload.get("value")
            analysis_mask = event_payload.get("analysis_mask", 0)

            # Skip non-numeric values
            if not isinstance(value, (int, float)):
                return

            key = (algo_name, algo_version, metric_name)
            now = datetime.now(timezone.utc)

            with self._lock:
                # Initialize window if needed
                if self._window_start is None:
                    self._init_window(now)

                # Check if we need to flush and start new window
                if now >= self._window_end:
                    self._flush_window()
                    self._init_window(now)

                # Add to accumulator
                if key not in self._current_window:
                    self._current_window[key] = MetricAggregate(
                        algo_name=algo_name,
                        algo_version=algo_version,
                        metric_name=metric_name,
                        analysis_mask=analysis_mask,
                    )

                self._current_window[key].values.append(float(value))

        except Exception as e:
            logger.warning("Failed to add metric: %s", e)

    def _init_window(self, now: datetime) -> None:
        """Initialize a new window starting at the given time."""
        # Align to window boundary
        epoch_sec = int(now.timestamp())
        window_start_sec = (epoch_sec // self._window_size_sec) * self._window_size_sec

        self._window_start = datetime.fromtimestamp(window_start_sec, tz=timezone.utc)
        self._window_end = datetime.fromtimestamp(
            window_start_sec + self._window_size_sec, tz=timezone.utc
        )
        self._current_window = {}

        logger.debug(
            "New window: %s to %s",
            self._window_start.isoformat(),
            self._window_end.isoformat(),
        )

    def _flush_window(self) -> None:
        """Flush current window aggregates to storage and Kafka."""
        if not self._current_window or self._window_start is None:
            return

        window_start = self._window_start
        window_end = self._window_end

        for key, aggregate in self._current_window.items():
            if aggregate.count == 0:
                continue

            # Create aggregate document
            aggregate_doc = {
                "algo_name": aggregate.algo_name,
                "algo_version": aggregate.algo_version,
                "metric_name": aggregate.metric_name,
                "analysis_mask": aggregate.analysis_mask,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "window_size_sec": self._window_size_sec,
                "summary": aggregate.to_summary(),
                "values": aggregate.values,  # Include raw values for detailed analysis
            }

            # Store to MinIO
            try:
                aggregate_bytes = json.dumps(aggregate_doc, indent=2).encode("utf-8")
                object_ref = self._storage.store(
                    data=aggregate_bytes,
                    bucket="aggregates",
                    mime="application/json",
                )

                logger.info(
                    "Stored aggregate: %s/%s/%s, count=%d, path=%s",
                    aggregate.algo_name,
                    aggregate.algo_version,
                    aggregate.metric_name,
                    aggregate.count,
                    object_ref.full_path,
                )

                # Publish AGGREGATE_PRODUCED event
                self._producer.publish_aggregate_produced(
                    source_id="flink_aggregation",
                    algo_name=aggregate.algo_name,
                    algo_version=aggregate.algo_version,
                    metric_name=aggregate.metric_name,
                    window_start=window_start.isoformat(),
                    window_end=window_end.isoformat(),
                    count=aggregate.count,
                    summary=aggregate.to_summary(),
                    object_ref=object_ref.full_path,
                )

                self._aggregates_produced += 1
                FLINK_AGGREGATIONS_PRODUCED.inc()

            except Exception as e:
                logger.error("Failed to store aggregate: %s", e)

        logger.info(
            "Flushed window %s: %d aggregates",
            window_start.isoformat(),
            len(self._current_window),
        )

    def flush(self) -> None:
        """Force flush current window (for graceful shutdown)."""
        with self._lock:
            self._flush_window()

    @property
    def aggregates_produced(self) -> int:
        return self._aggregates_produced


def run_aggregation_job(
    flink_config: FlinkConfig,
    kafka_config: Any,
    minio_config: Any,
    stop_event: threading.Event,
) -> None:
    """Run the metric aggregation job."""
    # Initialize storage and producer
    storage = MinioStorageService(minio_config)
    producer = EventProducer(kafka_config)

    # Create aggregator
    aggregator = TumblingWindowAggregator(
        window_size_sec=flink_config.tumbling_size_sec,
        storage=storage,
        producer=producer,
    )

    # Create consumer for metrics topic
    consumer = EventConsumer(
        config=kafka_config,
        topics=[kafka_config.topic_metrics],
        group_id=flink_config.kafka_consumer_group,
    )

    logger.info(
        "Aggregation job started: window_size=%ds, topic=%s, group=%s",
        flink_config.tumbling_size_sec,
        kafka_config.topic_metrics,
        flink_config.kafka_consumer_group,
    )

    events_processed = 0
    last_log_time = time.time()

    try:
        while not stop_event.is_set():
            event = consumer.poll(timeout=1.0)

            if event is None:
                continue

            if event.event_type == EventType.METRIC_EMITTED:
                aggregator.add_metric(event.payload)
                events_processed += 1

            # Periodic logging
            now = time.time()
            if now - last_log_time >= 30.0:
                logger.info(
                    "Aggregation status: events_processed=%d, aggregates_produced=%d",
                    events_processed,
                    aggregator.aggregates_produced,
                )
                last_log_time = now

    finally:
        # Flush remaining data
        aggregator.flush()
        consumer.close()
        producer.close()

        logger.info(
            "Aggregation job stopped: events_processed=%d, aggregates_produced=%d",
            events_processed,
            aggregator.aggregates_produced,
        )


def main() -> None:
    """Main entry point for Flink aggregation job."""
    # Load configs
    obs_config = load_observability_config(Path("src/config/observability.toml"))
    flink_config = load_flink_config(Path("src/config/flink.toml"))
    kafka_config = load_kafka_config(Path("src/config/kafka.toml"))
    minio_config = load_minio_config(Path("src/config/minio.toml"))

    # Configure logging and observability
    configure_logging(obs_config)
    configure_tracing(obs_config)
    atexit.register(shutdown_tracing)

    # Start metrics server on different port to avoid conflict
    obs_config_metrics = obs_config
    if obs_config.metrics_enabled:
        # Use port 8001 for aggregation job
        from dataclasses import replace
        obs_config_metrics = replace(obs_config, metrics_port=8001)
        configure_metrics(obs_config_metrics)
        logger.info("Metrics server started on port %d", obs_config_metrics.metrics_port)

    logger.info("Starting Flink aggregation job...")

    stop_event = threading.Event()

    # Install signal handlers
    import signal

    def handle_signal(signum: int, frame: Any) -> None:
        logger.info("Received signal %d, stopping...", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Run aggregation
    run_aggregation_job(flink_config, kafka_config, minio_config, stop_event)

    logger.info("Flink aggregation job stopped.")


if __name__ == "__main__":
    main()
