"""Metric aggregation Flink job.

Consumes metrics from Kafka, applies time-windowed aggregation,
and produces aggregates to output topic and MinIO storage.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Iterator

from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.common.time import Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import (
    AggregateFunction,
    ProcessWindowFunction,
    RuntimeContext,
)
from pyflink.datastream.window import TumblingEventTimeWindows, Time

from src.utils.logging import get_logger

if TYPE_CHECKING:
    from pyflink.datastream.window import TimeWindow

    from src.flink.config import FlinkConfig
    from src.streaming.config import KafkaConfig

logger = get_logger(__name__)


@dataclass
class MetricAccumulator:
    """Accumulator for metric aggregation."""

    algo_name: str
    algo_version: str
    metric_name: str
    analysis_mask: int
    values: list[float]
    count: int
    sum_value: float
    min_value: float
    max_value: float
    meta: dict[str, Any]


class MetricAggregateFunction(AggregateFunction):
    """Aggregate function for metric values."""

    def create_accumulator(self) -> MetricAccumulator:
        return MetricAccumulator(
            algo_name="",
            algo_version="",
            metric_name="",
            analysis_mask=0,
            values=[],
            count=0,
            sum_value=0.0,
            min_value=float("inf"),
            max_value=float("-inf"),
            meta={},
        )

    def add(self, value: Row, accumulator: MetricAccumulator) -> MetricAccumulator:
        # Extract fields from Row
        metric_value = float(value.value)

        if accumulator.count == 0:
            accumulator.algo_name = value.algo_name
            accumulator.algo_version = value.algo_version
            accumulator.metric_name = value.metric_name
            accumulator.analysis_mask = value.analysis_mask

        accumulator.values.append(metric_value)
        accumulator.count += 1
        accumulator.sum_value += metric_value
        accumulator.min_value = min(accumulator.min_value, metric_value)
        accumulator.max_value = max(accumulator.max_value, metric_value)

        return accumulator

    def get_result(self, accumulator: MetricAccumulator) -> MetricAccumulator:
        return accumulator

    def merge(
        self,
        acc1: MetricAccumulator,
        acc2: MetricAccumulator,
    ) -> MetricAccumulator:
        acc1.values.extend(acc2.values)
        acc1.count += acc2.count
        acc1.sum_value += acc2.sum_value
        acc1.min_value = min(acc1.min_value, acc2.min_value)
        acc1.max_value = max(acc1.max_value, acc2.max_value)
        return acc1


class MetricWindowProcessor(ProcessWindowFunction):
    """Process window results and compute final statistics."""

    def process(
        self,
        key: str,
        context: ProcessWindowFunction.Context,
        elements: Iterator[MetricAccumulator],
    ) -> Iterator[Row]:
        window: TimeWindow = context.window()
        window_start = window.start
        window_end = window.end

        for acc in elements:
            if acc.count == 0:
                continue

            # Compute statistics
            mean = acc.sum_value / acc.count
            values = sorted(acc.values)
            median = values[len(values) // 2] if values else 0.0

            # Compute variance and std
            variance = sum((v - mean) ** 2 for v in values) / acc.count if acc.count > 1 else 0.0
            std = variance ** 0.5

            summary = {
                "count": acc.count,
                "sum": acc.sum_value,
                "mean": mean,
                "median": median,
                "std": std,
                "variance": variance,
                "min": acc.min_value if acc.min_value != float("inf") else None,
                "max": acc.max_value if acc.max_value != float("-inf") else None,
            }

            yield Row(
                algo_name=acc.algo_name,
                algo_version=acc.algo_version,
                metric_name=acc.metric_name,
                analysis_mask=acc.analysis_mask,
                window_start_ms=window_start,
                window_end_ms=window_end,
                summary=json.dumps(summary),
                values=json.dumps(values),
            )


class MetricAggregationJob:
    """Flink job for metric aggregation with time windows.

    Consumes METRIC_EMITTED events from Kafka, aggregates by:
    - algo_name
    - algo_version
    - metric_name
    - analysis_kind

    Produces aggregated results with statistics to output topic
    and stores artifacts to MinIO.
    """

    def __init__(
        self,
        flink_config: FlinkConfig,
        kafka_config: KafkaConfig,
    ) -> None:
        self._flink_config = flink_config
        self._kafka_config = kafka_config
        self._env: StreamExecutionEnvironment | None = None

    def setup_environment(self) -> StreamExecutionEnvironment:
        """Create and configure Flink execution environment."""
        env = StreamExecutionEnvironment.get_execution_environment()

        # Set parallelism
        env.set_parallelism(self._flink_config.parallelism)
        env.set_max_parallelism(self._flink_config.max_parallelism)
        env.set_buffer_timeout(self._flink_config.buffer_timeout_ms)

        # Enable checkpointing
        if self._flink_config.checkpoint_enabled:
            env.enable_checkpointing(self._flink_config.checkpoint_interval_ms)
            checkpoint_config = env.get_checkpoint_config()
            checkpoint_config.set_min_pause_between_checkpoints(
                self._flink_config.checkpoint_min_pause_ms
            )
            checkpoint_config.set_checkpoint_timeout(
                self._flink_config.checkpoint_timeout_ms
            )
            checkpoint_config.set_max_concurrent_checkpoints(
                self._flink_config.checkpoint_max_concurrent
            )

        self._env = env
        return env

    def build_job(self) -> None:
        """Build the Flink job graph."""
        if self._env is None:
            self.setup_environment()

        env = self._env

        # Define watermark strategy
        watermark_strategy = (
            WatermarkStrategy.for_bounded_out_of_orderness(
                Duration.of_seconds(self._flink_config.max_out_of_orderness_sec)
            )
            .with_idleness(Duration.of_seconds(self._flink_config.idle_timeout_sec))
            .with_timestamp_assigner(self._extract_timestamp)
        )

        # Create Kafka source
        # Note: In production, use KafkaSource from pyflink.datastream.connectors
        # This is a simplified version for illustration
        kafka_props = {
            "bootstrap.servers": self._kafka_config.bootstrap_servers,
            "group.id": self._flink_config.kafka_consumer_group,
        }

        # Define row type for metrics
        metric_row_type = Types.ROW_NAMED(
            ["job_id", "algo_name", "algo_version", "metric_name", "value", "analysis_mask", "timestamp_ms"],
            [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.INT(), Types.LONG()],
        )

        # For a complete implementation, add KafkaSource here:
        # source = KafkaSource.builder() \
        #     .set_bootstrap_servers(kafka_props["bootstrap.servers"]) \
        #     .set_topics(self._kafka_config.topic_metrics) \
        #     .set_group_id(kafka_props["group.id"]) \
        #     .set_value_only_deserializer(JsonRowDeserializationSchema.builder().type_info(metric_row_type).build()) \
        #     .build()

        logger.info(
            "MetricAggregationJob configured: parallelism=%d, window=%ds",
            self._flink_config.parallelism,
            self._flink_config.tumbling_size_sec,
        )

    def _extract_timestamp(self, element: Row, record_timestamp: int) -> int:
        """Extract event timestamp for watermarking."""
        return element.timestamp_ms if hasattr(element, "timestamp_ms") else record_timestamp

    def execute(self, job_name: str = "MetricAggregation") -> None:
        """Execute the Flink job."""
        if self._env is None:
            self.build_job()
        self._env.execute(job_name)


def parse_metric_event(json_str: str) -> Row | None:
    """Parse METRIC_EMITTED event JSON to Row."""
    try:
        data = json.loads(json_str)
        payload = data.get("payload", {})

        return Row(
            job_id=payload.get("job_id", ""),
            algo_name=payload.get("algo_name", ""),
            algo_version=payload.get("algo_version", ""),
            metric_name=payload.get("metric_name", ""),
            value=float(payload.get("value", 0)),
            analysis_mask=int(payload.get("analysis_mask", 0)),
            timestamp_ms=int(
                datetime.fromisoformat(data.get("timestamp", "")).timestamp() * 1000
            ),
        )
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        logger.warning("Failed to parse metric event: %s", e)
        return None
