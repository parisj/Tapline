"""Unit tests for Kafka streaming configuration loader.

Tests for:
- KafkaConfig dataclass
- load_kafka_config function
- Default values
- Custom values from TOML
"""

from __future__ import annotations

import tomllib
from typing import TYPE_CHECKING

import pytest

from src.streaming.config import KafkaConfig, load_kafka_config

if TYPE_CHECKING:
    from pathlib import Path


def _write_toml(tmp_path: Path, text: str) -> Path:
    """Helper to write TOML content to a temp file."""
    p = tmp_path / "kafka.toml"
    p.write_text(text, encoding="utf-8")
    return p


class TestKafkaConfig:
    """Tests for KafkaConfig dataclass."""

    def test_kafka_config_creation(self) -> None:
        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            client_id="test-client",
            producer_acks="all",
            producer_retries=3,
            producer_retry_backoff_ms=100,
            producer_linger_ms=5,
            producer_batch_size=16384,
            producer_compression_type="lz4",
            producer_enable_idempotence=True,
            consumer_group_id="test-group",
            consumer_auto_offset_reset="earliest",
            consumer_enable_auto_commit=False,
            consumer_session_timeout_ms=30000,
            consumer_heartbeat_interval_ms=10000,
            consumer_max_poll_interval_ms=300000,
            consumer_partition_assignment_strategy="cooperative-sticky",
            topic_jobs="tapline.tasks",
            topic_results="tapline.results",
            topic_metrics="tapline.metrics",
            topic_audit_log="tapline.audit",
            topic_aggregates="tapline.aggregates",
            jobs_partitions=32,
            results_partitions=32,
            metrics_partitions=32,
            audit_log_partitions=1,
            aggregates_partitions=16,
            schema_registry_url="http://localhost:8085",
            security_protocol="PLAINTEXT",
            ssl_ca_location=None,
            ssl_certificate_location=None,
            ssl_key_location=None,
            ssl_key_password=None,
            sasl_mechanism=None,
            sasl_username=None,
            sasl_password=None,
            socket_timeout_ms=30000,
            socket_connection_setup_timeout_ms=10000,
        )

        assert config.bootstrap_servers == "localhost:9092"
        assert config.client_id == "test-client"
        assert config.producer_acks == "all"
        assert config.producer_enable_idempotence is True
        assert config.consumer_group_id == "test-group"
        assert config.topic_jobs == "tapline.tasks"
        assert config.security_protocol == "PLAINTEXT"
        assert config.socket_timeout_ms == 30000

    def test_kafka_config_is_frozen(self) -> None:
        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            client_id="test-client",
            producer_acks="all",
            producer_retries=3,
            producer_retry_backoff_ms=100,
            producer_linger_ms=5,
            producer_batch_size=16384,
            producer_compression_type="lz4",
            producer_enable_idempotence=True,
            consumer_group_id="test-group",
            consumer_auto_offset_reset="earliest",
            consumer_enable_auto_commit=False,
            consumer_session_timeout_ms=30000,
            consumer_heartbeat_interval_ms=10000,
            consumer_max_poll_interval_ms=300000,
            consumer_partition_assignment_strategy="cooperative-sticky",
            topic_jobs="tapline.tasks",
            topic_results="tapline.results",
            topic_metrics="tapline.metrics",
            topic_audit_log="tapline.audit",
            topic_aggregates="tapline.aggregates",
            jobs_partitions=32,
            results_partitions=32,
            metrics_partitions=32,
            audit_log_partitions=1,
            aggregates_partitions=16,
            schema_registry_url="http://localhost:8085",
            security_protocol="PLAINTEXT",
            ssl_ca_location=None,
            ssl_certificate_location=None,
            ssl_key_location=None,
            ssl_key_password=None,
            sasl_mechanism=None,
            sasl_username=None,
            sasl_password=None,
            socket_timeout_ms=30000,
            socket_connection_setup_timeout_ms=10000,
        )

        with pytest.raises(AttributeError):
            config.bootstrap_servers = "new-server:9092"  # type: ignore[misc]


class TestLoadKafkaConfig:
    """Tests for load_kafka_config function."""

    def test_load_minimal_config_uses_defaults(self, tmp_path: Path) -> None:
        path = _write_toml(
            tmp_path,
            """
            [broker]
            [producer]
            [consumer]
            [topics]
            [schema_registry]
            """,
        )

        config = load_kafka_config(path)

        # Check defaults
        assert config.bootstrap_servers == "localhost:9092"
        assert config.client_id == "tapline"
        assert config.producer_acks == "all"
        assert config.producer_retries == 3
        assert config.producer_compression_type == "lz4"
        assert config.producer_enable_idempotence is True
        assert config.consumer_group_id == "tapline-workers"
        assert config.consumer_auto_offset_reset == "earliest"
        assert config.consumer_enable_auto_commit is False
        assert config.topic_jobs == "tapline.tasks"
        assert config.jobs_partitions == 32
        assert config.schema_registry_url == "http://localhost:8085"

    def test_load_custom_broker_settings(self, tmp_path: Path) -> None:
        path = _write_toml(
            tmp_path,
            """
            [broker]
            bootstrap_servers = "kafka.example.com:9092"
            client_id = "custom-client"

            [producer]
            [consumer]
            [topics]
            [schema_registry]
            """,
        )

        config = load_kafka_config(path)

        assert config.bootstrap_servers == "kafka.example.com:9092"
        assert config.client_id == "custom-client"

    def test_load_custom_producer_settings(self, tmp_path: Path) -> None:
        path = _write_toml(
            tmp_path,
            """
            [broker]

            [producer]
            acks = "1"
            retries = 5
            retry_backoff_ms = 200
            linger_ms = 10
            batch_size = 32768
            compression_type = "gzip"
            enable_idempotence = false

            [consumer]
            [topics]
            [schema_registry]
            """,
        )

        config = load_kafka_config(path)

        assert config.producer_acks == "1"
        assert config.producer_retries == 5
        assert config.producer_retry_backoff_ms == 200
        assert config.producer_linger_ms == 10
        assert config.producer_batch_size == 32768
        assert config.producer_compression_type == "gzip"
        assert config.producer_enable_idempotence is False

    def test_load_custom_consumer_settings(self, tmp_path: Path) -> None:
        path = _write_toml(
            tmp_path,
            """
            [broker]
            [producer]

            [consumer]
            group_id = "custom-workers"
            auto_offset_reset = "latest"
            enable_auto_commit = true
            session_timeout_ms = 45000
            heartbeat_interval_ms = 15000
            max_poll_interval_ms = 600000

            [topics]
            [schema_registry]
            """,
        )

        config = load_kafka_config(path)

        assert config.consumer_group_id == "custom-workers"
        assert config.consumer_auto_offset_reset == "latest"
        assert config.consumer_enable_auto_commit is True
        assert config.consumer_session_timeout_ms == 45000
        assert config.consumer_heartbeat_interval_ms == 15000
        assert config.consumer_max_poll_interval_ms == 600000

    def test_load_custom_topic_settings(self, tmp_path: Path) -> None:
        path = _write_toml(
            tmp_path,
            """
            [broker]
            [producer]
            [consumer]

            [topics]
            jobs = "custom.jobs"
            results = "custom.results"
            metrics = "custom.metrics"
            audit_log = "custom.audit"
            aggregates = "custom.aggregates"
            jobs_partitions = 64
            results_partitions = 64
            metrics_partitions = 64
            audit_log_partitions = 4
            aggregates_partitions = 32

            [schema_registry]
            """,
        )

        config = load_kafka_config(path)

        assert config.topic_jobs == "custom.jobs"
        assert config.topic_results == "custom.results"
        assert config.topic_metrics == "custom.metrics"
        assert config.topic_audit_log == "custom.audit"
        assert config.topic_aggregates == "custom.aggregates"
        assert config.jobs_partitions == 64
        assert config.results_partitions == 64
        assert config.metrics_partitions == 64
        assert config.audit_log_partitions == 4
        assert config.aggregates_partitions == 32

    def test_load_custom_schema_registry(self, tmp_path: Path) -> None:
        path = _write_toml(
            tmp_path,
            """
            [broker]
            [producer]
            [consumer]
            [topics]

            [schema_registry]
            url = "http://schema.example.com:8081"
            """,
        )

        config = load_kafka_config(path)

        assert config.schema_registry_url == "http://schema.example.com:8081"

    def test_load_file_not_found(self, tmp_path: Path) -> None:
        path = tmp_path / "nonexistent.toml"

        with pytest.raises(FileNotFoundError):
            load_kafka_config(path)

    def test_load_invalid_toml(self, tmp_path: Path) -> None:
        path = _write_toml(tmp_path, "invalid toml [[[")

        with pytest.raises(tomllib.TOMLDecodeError):
            load_kafka_config(path)

    def test_load_missing_sections_uses_empty_defaults(self, tmp_path: Path) -> None:
        # File with no sections at all - should still work with defaults
        path = _write_toml(tmp_path, "")

        config = load_kafka_config(path)

        # Should use all defaults
        assert config.bootstrap_servers == "localhost:9092"
        assert config.topic_jobs == "tapline.tasks"
