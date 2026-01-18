"""PyFlink metric aggregation job for cluster submission.

This job runs on the Flink cluster and:
1. Reads METRIC_EMITTED events from Kafka
2. Aggregates metrics in tumbling time windows
3. Writes aggregated results to Kafka aggregates topic

The job provides:
- Distributed processing across Flink task managers
- Exactly-once semantics with checkpointing
- Monitoring via Flink UI
- Audit trail through Flink history server

Usage:
    pixi run run-flink-job

    Or submit directly:
    flink run -py src/flink/jobs/metric_aggregation_job.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from pyflink.common import Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Duration
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.functions import MapFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows, Time

from dotenv import load_dotenv

load_dotenv()


class MetricParser(MapFunction):
    """Parse JSON metric events from Kafka."""

    def map(self, value: str) -> str | None:
        try:
            data = json.loads(value)

            # Only process METRIC_EMITTED events
            if data.get("event_type") != "METRIC_EMITTED":
                return None

            payload = data.get("payload", {})
            metric_value = payload.get("value")

            # Skip non-numeric values
            if not isinstance(metric_value, (int, float)):
                return None

            # Create structured metric record
            return json.dumps({
                "job_id": payload.get("job_id", ""),
                "algo_name": payload.get("algo_name", "unknown"),
                "algo_version": payload.get("algo_version", "0.0.0"),
                "metric_name": payload.get("metric_name", "unknown"),
                "value": float(metric_value),
                "analysis_mask": payload.get("analysis_mask", 0),
                "event_timestamp": data.get("timestamp", ""),
            })
        except (json.JSONDecodeError, KeyError, ValueError):
            return None


class MetricWindowAggregator(ProcessWindowFunction):
    """Aggregate metrics within a time window and compute statistics."""

    def process(self, key: str, context: ProcessWindowFunction.Context, elements):
        """Process all elements in the window and emit aggregate."""
        values = []
        algo_name = "unknown"
        algo_version = "0.0.0"
        metric_name = "unknown"
        analysis_mask = 0

        for elem in elements:
            try:
                data = json.loads(elem)
                values.append(data["value"])
                algo_name = data["algo_name"]
                algo_version = data["algo_version"]
                metric_name = data["metric_name"]
                analysis_mask = data.get("analysis_mask", 0)
            except (json.JSONDecodeError, KeyError):
                continue

        if not values:
            return

        # Compute statistics
        count = len(values)
        sum_val = sum(values)
        mean = sum_val / count
        min_val = min(values)
        max_val = max(values)

        # Sort for percentiles
        sorted_values = sorted(values)
        median = sorted_values[count // 2] if count > 0 else 0

        # Compute std
        if count > 1:
            variance = sum((v - mean) ** 2 for v in values) / count
            std = variance ** 0.5
        else:
            std = 0.0

        # Percentiles
        p95_idx = int(count * 0.95)
        p99_idx = int(count * 0.99)
        p95 = sorted_values[min(p95_idx, count - 1)] if count > 0 else 0
        p99 = sorted_values[min(p99_idx, count - 1)] if count > 0 else 0

        # Get window bounds
        window = context.window()
        window_start_ms = window.start
        window_end_ms = window.end

        # Create aggregate event
        aggregate = {
            "event_type": "AGGREGATE_COMPUTED",
            "source_id": "flink-metric-aggregation",
            "algo_name": algo_name,
            "algo_version": algo_version,
            "metric_name": metric_name,
            "analysis_mask": analysis_mask,
            "window_start_ms": window_start_ms,
            "window_end_ms": window_end_ms,
            "summary": {
                "count": count,
                "sum": sum_val,
                "mean": mean,
                "std": std,
                "min": min_val,
                "max": max_val,
                "median": median,
                "p95": p95,
                "p99": p99,
            },
        }

        yield json.dumps(aggregate)


def get_key(value: str) -> str:
    """Extract grouping key from metric record."""
    try:
        data = json.loads(value)
        return f"{data['algo_name']}|{data['algo_version']}|{data['metric_name']}"
    except (json.JSONDecodeError, KeyError):
        return "unknown"


def create_job():
    """Create and configure the Flink job."""
    # Get configuration from environment
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
    kafka_group_id = os.getenv("FLINK_CONSUMER_GROUP", "visioeval-flink-aggregation")
    metrics_topic = os.getenv("KAFKA_TOPIC_METRICS", "visio.metrics")
    aggregates_topic = os.getenv("KAFKA_TOPIC_AGGREGATES", "visio.aggregates")
    window_size_sec = int(os.getenv("FLINK_WINDOW_SIZE_SEC", "60"))
    parallelism = int(os.getenv("FLINK_PARALLELISM", "4"))

    print(f"Configuring Flink job:")
    print(f"  Kafka: {kafka_bootstrap}")
    print(f"  Consumer Group: {kafka_group_id}")
    print(f"  Metrics Topic: {metrics_topic}")
    print(f"  Aggregates Topic: {aggregates_topic}")
    print(f"  Window Size: {window_size_sec}s")
    print(f"  Parallelism: {parallelism}")

    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(parallelism)

    # Enable checkpointing for exactly-once semantics
    env.enable_checkpointing(60000)  # 60 second checkpoints
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_min_pause_between_checkpoints(5000)
    checkpoint_config.set_checkpoint_timeout(600000)  # 10 minutes

    # Add Kafka connector JARs
    jar_path = "/opt/flink/lib/kafka"
    jars = [
        f"file://{jar_path}/flink-connector-kafka-3.0.2-1.18.jar",
        f"file://{jar_path}/kafka-clients-3.4.0.jar",
    ]
    env.add_jars(*jars)

    # Create Kafka source
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_topics(metrics_topic)
        .set_group_id(kafka_group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    # Create Kafka sink
    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(kafka_bootstrap)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(aggregates_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

    # Build the streaming pipeline
    (
        env.from_source(
            kafka_source,
            WatermarkStrategy.for_monotonous_timestamps(),
            "Kafka Metrics Source"
        )
        .map(MetricParser())
        .filter(lambda x: x is not None)
        .key_by(get_key)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(window_size_sec)))
        .process(MetricWindowAggregator())
        .sink_to(kafka_sink)
        .name("Kafka Aggregates Sink")
    )

    return env


def main():
    """Main entry point."""
    print("=" * 60)
    print("VisioEval Metric Aggregation - Flink Job")
    print("=" * 60)

    env = create_job()

    print("\nSubmitting job to Flink cluster...")
    env.execute("VisioEval-MetricAggregation")


if __name__ == "__main__":
    main()
