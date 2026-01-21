"""Kafka configuration loader."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.config.base import get_section, load_toml

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class KafkaConfig:
    """Kafka configuration settings."""

    # Broker settings
    bootstrap_servers: str
    client_id: str

    # Producer settings
    producer_acks: str
    producer_retries: int
    producer_retry_backoff_ms: int
    producer_linger_ms: int
    producer_batch_size: int
    producer_compression_type: str
    producer_enable_idempotence: bool

    # Consumer settings
    consumer_group_id: str
    consumer_auto_offset_reset: str
    consumer_enable_auto_commit: bool
    consumer_session_timeout_ms: int
    consumer_heartbeat_interval_ms: int
    consumer_max_poll_interval_ms: int
    consumer_partition_assignment_strategy: str  # cooperative-sticky recommended
    # Note: max_poll_records is not supported by librdkafka (confluent-kafka)

    # Topic names
    topic_jobs: str
    topic_results: str
    topic_metrics: str
    topic_audit_log: str
    topic_aggregates: str

    # Topic partitions
    jobs_partitions: int
    results_partitions: int
    metrics_partitions: int
    audit_log_partitions: int
    aggregates_partitions: int

    # Schema registry
    schema_registry_url: str


def load_kafka_config(path: Path) -> KafkaConfig:
    """Load Kafka configuration from TOML file."""
    doc = load_toml(path)

    broker = get_section(doc, "broker")
    producer = get_section(doc, "producer")
    consumer = get_section(doc, "consumer")
    topics = get_section(doc, "topics")
    schema_registry = get_section(doc, "schema_registry")

    return KafkaConfig(
        # Broker
        bootstrap_servers=broker.get("bootstrap_servers", "localhost:9092"),
        client_id=broker.get("client_id", "visioeval"),
        # Producer
        producer_acks=producer.get("acks", "all"),
        producer_retries=producer.get("retries", 3),
        producer_retry_backoff_ms=producer.get("retry_backoff_ms", 100),
        producer_linger_ms=producer.get("linger_ms", 5),
        producer_batch_size=producer.get("batch_size", 16384),
        producer_compression_type=producer.get("compression_type", "lz4"),
        producer_enable_idempotence=producer.get("enable_idempotence", True),
        # Consumer
        consumer_group_id=consumer.get("group_id", "visioeval-workers"),
        consumer_auto_offset_reset=consumer.get("auto_offset_reset", "earliest"),
        consumer_enable_auto_commit=consumer.get("enable_auto_commit", False),
        consumer_session_timeout_ms=consumer.get("session_timeout_ms", 30000),
        consumer_heartbeat_interval_ms=consumer.get("heartbeat_interval_ms", 10000),
        consumer_max_poll_interval_ms=consumer.get("max_poll_interval_ms", 300000),
        consumer_partition_assignment_strategy=consumer.get(
            "partition_assignment_strategy", "cooperative-sticky",
        ),
        # Topics
        topic_jobs=topics.get("jobs", "visio.jobs"),
        topic_results=topics.get("results", "visio.results"),
        topic_metrics=topics.get("metrics", "visio.metrics"),
        topic_audit_log=topics.get("audit_log", "visio.audit-log"),
        topic_aggregates=topics.get("aggregates", "visio.aggregates"),
        # Partitions
        jobs_partitions=topics.get("jobs_partitions", 32),
        results_partitions=topics.get("results_partitions", 32),
        metrics_partitions=topics.get("metrics_partitions", 32),
        audit_log_partitions=topics.get("audit_log_partitions", 1),
        aggregates_partitions=topics.get("aggregates_partitions", 16),
        # Schema Registry
        schema_registry_url=schema_registry.get("url", "http://localhost:8085"),
    )
