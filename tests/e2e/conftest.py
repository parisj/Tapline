"""Fixtures for end-to-end tests.

Provides infrastructure for testing the complete pipeline flow:
- File ingestion -> Kafka -> Worker processing -> MinIO storage
"""

from __future__ import annotations

import os
import struct
import zlib
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path


@dataclass
class E2ETestConfig:
    """E2E test configuration from environment."""

    kafka_bootstrap_servers: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str


@pytest.fixture(scope="session")
def e2e_config() -> E2ETestConfig:
    """Load E2E test configuration from environment."""
    return E2ETestConfig(
        kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        minio_endpoint=os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
        minio_access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin123"),
    )


@pytest.fixture(scope="session")
def infrastructure_available(e2e_config: E2ETestConfig) -> bool:
    """Check if all required infrastructure is available."""
    kafka_ok = False
    minio_ok = False

    # Check Kafka
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": e2e_config.kafka_bootstrap_servers})
        metadata = admin.list_topics(timeout=5)
        kafka_ok = metadata is not None
    except Exception:
        pass

    # Check MinIO
    try:
        from minio import Minio

        client = Minio(
            endpoint=e2e_config.minio_endpoint,
            access_key=e2e_config.minio_access_key,
            secret_key=e2e_config.minio_secret_key,
            secure=False,
        )
        client.list_buckets()
        minio_ok = True
    except Exception:
        pass

    return kafka_ok and minio_ok


@pytest.fixture
def skip_without_infrastructure(infrastructure_available: bool) -> None:
    """Skip test if infrastructure is not available."""
    if not infrastructure_available:
        pytest.skip("Required infrastructure (Kafka, MinIO) not available")


def generate_test_png(width: int = 100, height: int = 100, color: tuple = (255, 0, 0)) -> bytes:
    """Generate a valid PNG image for testing."""

    def png_chunk(chunk_type: bytes, data: bytes) -> bytes:
        chunk = chunk_type + data
        return struct.pack(">I", len(data)) + chunk + struct.pack(">I", zlib.crc32(chunk) & 0xFFFFFFFF)

    r, g, b = color

    # IHDR chunk
    ihdr_data = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)

    # IDAT chunk (compressed image data)
    raw_data = b""
    for _ in range(height):
        raw_data += b"\x00"  # Filter byte (none)
        raw_data += bytes([r, g, b] * width)

    compressed = zlib.compress(raw_data)

    # Build PNG
    png = b"\x89PNG\r\n\x1a\n"
    png += png_chunk(b"IHDR", ihdr_data)
    png += png_chunk(b"IDAT", compressed)
    png += png_chunk(b"IEND", b"")

    return png


@pytest.fixture
def test_image_bytes() -> bytes:
    """Generate a test PNG image."""
    return generate_test_png(width=100, height=100, color=(255, 128, 0))


@pytest.fixture
def test_image_file(tmp_path: Path, test_image_bytes: bytes) -> Path:
    """Create a test image file."""
    image_path = tmp_path / "test_image.png"
    image_path.write_bytes(test_image_bytes)
    return image_path


@pytest.fixture
def kafka_config(e2e_config: E2ETestConfig):
    """Create KafkaConfig for E2E tests."""
    from src.streaming.config import KafkaConfig

    return KafkaConfig(
        bootstrap_servers=e2e_config.kafka_bootstrap_servers,
        client_id="e2e-test-client",
        producer_acks="all",
        producer_retries=3,
        producer_retry_backoff_ms=100,
        producer_linger_ms=5,
        producer_batch_size=16384,
        producer_compression_type="none",
        producer_enable_idempotence=True,
        consumer_group_id="e2e-test-group",
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
        # Security settings (PLAINTEXT for local testing)
        security_protocol="PLAINTEXT",
        ssl_ca_location=None,
        ssl_certificate_location=None,
        ssl_key_location=None,
        ssl_key_password=None,
        sasl_mechanism=None,
        sasl_username=None,
        sasl_password=None,
        # Connection timeouts
        socket_timeout_ms=30000,
        socket_connection_setup_timeout_ms=10000,
    )


@pytest.fixture
def minio_config(e2e_config: E2ETestConfig):
    """Create MinioConfig for E2E tests."""
    from src.storage.config import MinioConfig

    return MinioConfig(
        endpoint=e2e_config.minio_endpoint,
        access_key=e2e_config.minio_access_key,
        secret_key=e2e_config.minio_secret_key,
        secure=False,
        region="us-east-1",
        bucket_artifacts="artifacts",
        bucket_inputs="inputs",
        bucket_aggregates="aggregates",
        bucket_metric_values="metric-values",
        path_prefix_length=4,
        max_object_size=104857600,
        max_retries=3,
        retry_delay_sec=0.5,
        artifacts_retention_days=0,
        inputs_retention_days=30,
        aggregates_retention_days=90,
        # Connection timeouts
        connect_timeout=10.0,
        read_timeout=30.0,
    )


@pytest.fixture
def minio_storage(minio_config, skip_without_infrastructure):
    """Create MinioStorageService for E2E tests."""
    from src.storage.minio_service import MinioStorageService

    storage = MinioStorageService(minio_config)
    yield storage


@pytest.fixture
def event_producer(kafka_config, skip_without_infrastructure):
    """Create EventProducer for E2E tests."""
    from src.streaming.producer import EventProducer

    producer = EventProducer(kafka_config)
    yield producer
    producer.close()


@pytest.fixture
def dashboard_available(e2e_config: E2ETestConfig) -> bool:
    """Check if the dashboard API is available."""
    import urllib.request

    try:
        with urllib.request.urlopen("http://localhost:5007/api/health", timeout=2):
            return True
    except Exception:
        return False


@pytest.fixture
def skip_without_dashboard(dashboard_available: bool) -> None:
    """Skip test if dashboard is not available."""
    if not dashboard_available:
        pytest.skip("Dashboard API not available at localhost:5007")
