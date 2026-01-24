"""Kafka configuration loader."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

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

    # Security settings (TLS/SASL)
    security_protocol: str  # PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
    ssl_ca_location: str | None  # Path to CA certificate
    ssl_certificate_location: str | None  # Path to client certificate
    ssl_key_location: str | None  # Path to client private key
    ssl_key_password: str | None  # Password for private key (if encrypted)
    sasl_mechanism: str | None  # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI, OAUTHBEARER
    sasl_username: str | None  # SASL username
    sasl_password: str | None  # SASL password

    # Connection settings
    socket_timeout_ms: int  # Socket timeout in milliseconds
    socket_connection_setup_timeout_ms: int  # Connection setup timeout


def load_kafka_config(path: Path) -> KafkaConfig:
    """Load Kafka configuration from TOML file."""
    doc = load_toml(path)

    broker = get_section(doc, "broker")
    producer = get_section(doc, "producer")
    consumer = get_section(doc, "consumer")
    topics = get_section(doc, "topics")
    schema_registry = get_section(doc, "schema_registry")

    # Security section is optional
    security = doc.get("security", {})
    if not isinstance(security, dict):
        security = {}

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
            "partition_assignment_strategy",
            "cooperative-sticky",
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
        # Security (TLS/SASL)
        security_protocol=security.get("protocol", "PLAINTEXT"),
        ssl_ca_location=security.get("ssl_ca_location"),
        ssl_certificate_location=security.get("ssl_certificate_location"),
        ssl_key_location=security.get("ssl_key_location"),
        ssl_key_password=security.get("ssl_key_password"),
        sasl_mechanism=security.get("sasl_mechanism"),
        sasl_username=security.get("sasl_username"),
        sasl_password=security.get("sasl_password"),
        # Connection settings
        socket_timeout_ms=consumer.get("socket_timeout_ms", 30000),
        socket_connection_setup_timeout_ms=consumer.get(
            "socket_connection_setup_timeout_ms",
            10000,
        ),
    )


def build_security_config(config: KafkaConfig) -> dict[str, Any]:
    """Build security configuration dict for Kafka clients.

    This creates the security-related config entries needed by
    confluent-kafka Producer and Consumer instances.

    Args:
        config: KafkaConfig instance

    Returns:
        Dict with security config entries (empty if PLAINTEXT)

    """
    security_config: dict[str, Any] = {
        "security.protocol": config.security_protocol,
        "socket.timeout.ms": config.socket_timeout_ms,
        "socket.connection.setup.timeout.ms": config.socket_connection_setup_timeout_ms,
    }

    # SSL settings (for SSL and SASL_SSL protocols)
    if config.security_protocol in ("SSL", "SASL_SSL"):
        if config.ssl_ca_location:
            security_config["ssl.ca.location"] = config.ssl_ca_location
        if config.ssl_certificate_location:
            security_config["ssl.certificate.location"] = config.ssl_certificate_location
        if config.ssl_key_location:
            security_config["ssl.key.location"] = config.ssl_key_location
        if config.ssl_key_password:
            security_config["ssl.key.password"] = config.ssl_key_password

    # SASL settings (for SASL_PLAINTEXT and SASL_SSL protocols)
    if config.security_protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
        if config.sasl_mechanism:
            security_config["sasl.mechanism"] = config.sasl_mechanism
        if config.sasl_username:
            security_config["sasl.username"] = config.sasl_username
        if config.sasl_password:
            security_config["sasl.password"] = config.sasl_password

    return security_config
