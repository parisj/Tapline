"""Python-based metric aggregation (default aggregation method).

This job runs separately from the main streaming pipeline.
It consumes METRIC_EMITTED events from Kafka, aggregates them
in time windows, and stores results directly to MinIO.

Key features:
- Preserves raw values (numeric, boolean, 2D points) in aggregates
- Stores aggregation_mask for AggregationType-aware processing
- Supports all value types for COUNTER, RATE, ELLIPSE_2D, CONTOUR_2D, etc.
- Writes directly to MinIO aggregates bucket

Note: This is the default aggregation method. For Flink SQL aggregation,
set TAPLINE_USE_FLINK_SQL=true environment variable.

Usage:
    pixi run run-flink-agg
    # or
    python -m src.app.flink_aggregation
"""

from __future__ import annotations

import atexit
import json
import signal
import threading
import time
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dotenv import load_dotenv

from src.domain.events import EventType
from src.flink.config import FlinkConfig, load_flink_config
from src.observability.config import load_observability_config
from src.observability.metrics import (
    FLINK_AGGREGATIONS_PRODUCED,
    configure_metrics,
)
from src.observability.tracing import configure_tracing, shutdown_tracing
from src.storage.config import MinioConfig, load_minio_config
from src.storage.minio_service import MinioStorageService
from src.streaming.config import KafkaConfig, load_kafka_config
from src.streaming.consumer import EventConsumer
from src.streaming.producer import EventProducer
from src.utils.logging import configure_logging, get_logger

if TYPE_CHECKING:
    from types import FrameType

logger = get_logger("pipeline.app.flink_aggregation")

# Constants
_MIN_VALUES_FOR_STD = 2
_LOG_INTERVAL_SEC = 30.0


@dataclass
class MetricAggregate:
    """Accumulated metrics for a single key within a window.

    Handles both numeric values (for SUMMARY, DISTRIBUTION_1D, etc.) and
    structured values like {x, y} dicts (for ELLIPSE_2D, CONTOUR_2D).
    """

    processor_name: str
    processor_version: str
    metric_name: str
    aggregation_mask: int
    meta: dict[str, Any] | None = None
    values: list[Any] = field(default_factory=list)  # Can be float, bool, dict, etc.

    @property
    def count(self) -> int:
        return len(self.values)

    @property
    def numeric_values(self) -> list[float]:
        """Extract only numeric values for statistical calculations."""
        return [float(v) for v in self.values if isinstance(v, (int, float))]

    @property
    def point_values(self) -> list[dict[str, float]]:
        """Extract 2D point values ({x, y} dicts) for ELLIPSE_2D/CONTOUR_2D."""
        return [
            {"x": float(v["x"]), "y": float(v["y"])}
            for v in self.values
            if isinstance(v, dict) and "x" in v and "y" in v
        ]

    @property
    def has_2d_data(self) -> bool:
        """Check if this aggregate contains 2D point data."""
        return len(self.point_values) > 0

    @property
    def sum(self) -> float:
        nums = self.numeric_values
        return sum(nums) if nums else 0.0

    @property
    def mean(self) -> float:
        nums = self.numeric_values
        return self.sum / len(nums) if nums else 0.0

    @property
    def min(self) -> float | None:
        nums = self.numeric_values
        return min(nums) if nums else None

    @property
    def max(self) -> float | None:
        nums = self.numeric_values
        return max(nums) if nums else None

    @property
    def std(self) -> float:
        nums = self.numeric_values
        if len(nums) < _MIN_VALUES_FOR_STD:
            return 0.0
        mean = self.mean
        variance = sum((v - mean) ** 2 for v in nums) / len(nums)
        return float(variance**0.5)

    def to_summary(self) -> dict[str, Any]:
        """Generate summary statistics.

        Includes numeric stats and 2D point stats when applicable.
        """
        nums = self.numeric_values
        points = self.point_values

        summary: dict[str, Any] = {
            "count": self.count,
            "numeric_count": len(nums),
            "sum": self.sum,
            "mean": self.mean,
            "std": self.std,
            "min": self.min,
            "max": self.max,
            "median": self._median(),
            "p95": self._percentile(95),
            "p99": self._percentile(99),
        }

        # Add 2D point summary if we have point data
        if points:
            xs = [p["x"] for p in points]
            ys = [p["y"] for p in points]
            summary["point_count"] = len(points)
            summary["x_mean"] = sum(xs) / len(xs) if xs else None
            summary["y_mean"] = sum(ys) / len(ys) if ys else None
            summary["x_min"] = min(xs) if xs else None
            summary["x_max"] = max(xs) if xs else None
            summary["y_min"] = min(ys) if ys else None
            summary["y_max"] = max(ys) if ys else None

        return summary

    def _median(self) -> float | None:
        nums = self.numeric_values
        if not nums:
            return None
        sorted_vals = sorted(nums)
        mid = len(sorted_vals) // 2
        if len(sorted_vals) % 2 == 0:
            return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2
        return sorted_vals[mid]

    def _percentile(self, p: int) -> float | None:
        nums = self.numeric_values
        if not nums:
            return None
        sorted_vals = sorted(nums)
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

        # Current window state keyed by (processor_name, processor_version, metric_name)
        self._current_window: dict[tuple[str, str, str], MetricAggregate] = {}
        self._window_start: datetime | None = None
        self._window_end: datetime | None = None

        self._lock = threading.Lock()
        self._aggregates_produced = 0

    def add_metric(self, event_payload: dict[str, Any]) -> None:
        """Add a metric event to the current window.

        Handles all value types:
        - Numeric (int, float): For SUMMARY, DISTRIBUTION_1D, COUNTER, RATE
        - Boolean: For COUNTER (converted to 0/1)
        - Dict with x,y: For ELLIPSE_2D, CONTOUR_2D (2D point data)
        """
        try:
            processor_name = event_payload.get("processor_name", "unknown")
            processor_version = event_payload.get("processor_version", "0.0.0")
            metric_name = event_payload.get("metric_name", "unknown")
            value = event_payload.get("value")
            aggregation_mask = event_payload.get("aggregation_mask", 0)
            meta = event_payload.get("meta")

            # Skip None values
            if value is None:
                return

            key = (processor_name, processor_version, metric_name)
            now = datetime.now(UTC)

            with self._lock:
                # Initialize window if needed
                if self._window_start is None:
                    self._init_window(now)

                # Check if we need to flush and start new window
                if self._window_end is not None and now >= self._window_end:
                    self._flush_window()
                    self._init_window(now)

                # Add to accumulator
                if key not in self._current_window:
                    self._current_window[key] = MetricAggregate(
                        processor_name=processor_name,
                        processor_version=processor_version,
                        metric_name=metric_name,
                        aggregation_mask=aggregation_mask,
                        meta=meta,
                    )

                # Store value as appropriate type
                if isinstance(value, bool):
                    # Convert boolean to int for counting
                    self._current_window[key].values.append(1 if value else 0)
                elif isinstance(value, (int, float)):
                    self._current_window[key].values.append(float(value))
                elif isinstance(value, dict):
                    # Preserve dict values (e.g., {x, y} for 2D points)
                    self._current_window[key].values.append(value)
                else:
                    # Store other types as-is (strings, etc.)
                    self._current_window[key].values.append(value)

        except Exception as e:
            logger.warning("Failed to add metric: %s", e)

    def _init_window(self, now: datetime) -> None:
        """Initialize a new window starting at the given time."""
        # Align to window boundary
        epoch_sec = int(now.timestamp())
        window_start_sec = (epoch_sec // self._window_size_sec) * self._window_size_sec

        self._window_start = datetime.fromtimestamp(window_start_sec, tz=UTC)
        self._window_end = datetime.fromtimestamp(
            window_start_sec + self._window_size_sec,
            tz=UTC,
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

        # Ensure both window bounds are set
        if window_start is None or window_end is None:
            return

        for aggregate in self._current_window.values():
            if aggregate.count == 0:
                continue

            # Create aggregate document
            aggregate_doc = {
                "processor_name": aggregate.processor_name,
                "processor_version": aggregate.processor_version,
                "metric_name": aggregate.metric_name,
                "aggregation_mask": aggregate.aggregation_mask,
                "meta": aggregate.meta,
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "window_start_unix": int(window_start.timestamp()),
                "window_end_unix": int(window_end.timestamp()),
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
                    aggregate.processor_name,
                    aggregate.processor_version,
                    aggregate.metric_name,
                    aggregate.count,
                    object_ref.full_path,
                )

                # Publish AGGREGATE_PRODUCED event
                self._producer.publish_aggregate_produced(
                    source_id="flink_aggregation",
                    processor_name=aggregate.processor_name,
                    processor_version=aggregate.processor_version,
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
                logger.exception("Failed to store aggregate: %s", e)

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
    kafka_config: KafkaConfig,
    minio_config: MinioConfig,
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
            if now - last_log_time >= _LOG_INTERVAL_SEC:
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
    load_dotenv()

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
    metrics_port = 8001
    if obs_config.metrics_enabled:
        # Use port 8001 for aggregation job
        obs_config_metrics = replace(obs_config, metrics_port=metrics_port)
        configure_metrics(obs_config_metrics)
        logger.info("Metrics server started on port %d", obs_config_metrics.metrics_port)

    logger.info("Starting Flink aggregation job...")

    stop_event = threading.Event()

    # Install signal handlers
    def handle_signal(signum: int, _frame: FrameType | None) -> None:
        logger.info("Received signal %d, stopping...", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Run aggregation
    run_aggregation_job(flink_config, kafka_config, minio_config, stop_event)

    logger.info("Flink aggregation job stopped.")


if __name__ == "__main__":
    main()
