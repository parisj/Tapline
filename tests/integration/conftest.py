"""Fixtures for integration tests.

Provides test infrastructure:
- Kafka topic management
- MinIO bucket setup
- Test data generation
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path


@dataclass
class TestConfig:
    """Test configuration from environment."""

    kafka_bootstrap_servers: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    flink_jobmanager: str


@pytest.fixture(scope="session")
def test_config() -> TestConfig:
    """Load test configuration from environment."""
    return TestConfig(
        kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        minio_endpoint=os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
        minio_access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin123"),
        flink_jobmanager=os.environ.get("FLINK_JOBMANAGER", "localhost:8081"),
    )


@pytest.fixture(scope="session")
def kafka_available(test_config: TestConfig) -> bool:
    """Check if Kafka is available."""
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": test_config.kafka_bootstrap_servers})
        metadata = admin.list_topics(timeout=5)
        return metadata is not None
    except Exception:
        return False


@pytest.fixture(scope="session")
def minio_available(test_config: TestConfig) -> bool:
    """Check if MinIO is available."""
    try:
        from minio import Minio

        client = Minio(
            endpoint=test_config.minio_endpoint,
            access_key=test_config.minio_access_key,
            secret_key=test_config.minio_secret_key,
            secure=False,
        )
        client.list_buckets()
        return True
    except Exception:
        return False


@pytest.fixture
def unique_topic_name() -> str:
    """Generate a unique topic name for test isolation."""
    import uuid
    return f"test-topic-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def unique_bucket_name() -> str:
    """Generate a unique bucket name for test isolation."""
    import uuid
    return f"test-bucket-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def kafka_producer(test_config: TestConfig, kafka_available: bool):
    """Create a Kafka producer for tests."""
    if not kafka_available:
        pytest.skip("Kafka not available")

    from src.streaming.config import KafkaConfig
    from src.streaming.producer import EventProducer

    config = KafkaConfig(
        bootstrap_servers=test_config.kafka_bootstrap_servers,
        client_id="test-client",
        producer_acks="all",
        producer_retries=3,
        producer_retry_backoff_ms=100,
        producer_linger_ms=5,
        producer_batch_size=16384,
        producer_compression_type="none",
        producer_enable_idempotence=True,
        consumer_group_id="test-group",
        consumer_auto_offset_reset="earliest",
        consumer_enable_auto_commit=False,
        consumer_session_timeout_ms=30000,
        consumer_heartbeat_interval_ms=10000,
        consumer_max_poll_interval_ms=300000,
        topic_jobs="test.jobs",
        topic_results="test.results",
        topic_metrics="test.metrics",
        topic_audit_log="test.audit-log",
        topic_aggregates="test.aggregates",
        jobs_partitions=4,
        results_partitions=4,
        metrics_partitions=4,
        audit_log_partitions=1,
        aggregates_partitions=4,
        schema_registry_url="http://localhost:8085",
    )

    producer = EventProducer(config)
    yield producer
    producer.close()


@pytest.fixture
def minio_storage(test_config: TestConfig, minio_available: bool, unique_bucket_name: str):
    """Create a MinIO storage service for tests."""
    if not minio_available:
        pytest.skip("MinIO not available")

    from src.storage.config import MinioConfig
    from src.storage.minio_service import MinioStorageService

    config = MinioConfig(
        endpoint=test_config.minio_endpoint,
        access_key=test_config.minio_access_key,
        secret_key=test_config.minio_secret_key,
        secure=False,
        region="us-east-1",
        bucket_artifacts=f"{unique_bucket_name}-artifacts",
        bucket_inputs=f"{unique_bucket_name}-inputs",
        bucket_aggregates=f"{unique_bucket_name}-aggregates",
        path_prefix_length=4,
        max_object_size=104857600,
        max_retries=3,
        retry_delay_sec=0.5,
        artifacts_retention_days=0,
        inputs_retention_days=30,
        aggregates_retention_days=90,
    )

    service = MinioStorageService(config)
    yield service

    # Cleanup buckets after test
    try:
        from minio import Minio

        client = Minio(
            endpoint=test_config.minio_endpoint,
            access_key=test_config.minio_access_key,
            secret_key=test_config.minio_secret_key,
            secure=False,
        )
        for bucket in [config.bucket_artifacts, config.bucket_inputs, config.bucket_aggregates]:
            try:
                objects = client.list_objects(bucket, recursive=True)
                for obj in objects:
                    client.remove_object(bucket, obj.object_name)
                client.remove_bucket(bucket)
            except Exception:
                pass
    except Exception:
        pass


@pytest.fixture
def sample_image_bytes() -> bytes:
    """Generate sample image-like bytes for testing."""
    # PNG header followed by random data
    png_header = b"\x89PNG\r\n\x1a\n"
    return png_header + os.urandom(1024)


@pytest.fixture
def temp_directory(tmp_path: Path) -> Path:
    """Create a temporary directory for file-based tests."""
    test_dir = tmp_path / "test_input"
    test_dir.mkdir()
    return test_dir
