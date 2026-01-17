"""MinIO configuration loader."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


@dataclass(frozen=True)
class MinioConfig:
    """MinIO configuration settings."""

    # Connection settings
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool
    region: str

    # Bucket names
    bucket_artifacts: str
    bucket_inputs: str
    bucket_aggregates: str

    # Storage settings
    path_prefix_length: int
    max_object_size: int
    max_retries: int
    retry_delay_sec: float

    # Lifecycle settings
    artifacts_retention_days: int
    inputs_retention_days: int
    aggregates_retention_days: int


def load_minio_config(path: Path) -> MinioConfig:
    """Load MinIO configuration from TOML file."""
    doc = tomllib.loads(path.read_text(encoding="utf-8"))

    connection = doc.get("connection", {})
    buckets = doc.get("buckets", {})
    storage = doc.get("storage", {})
    lifecycle = doc.get("lifecycle", {})

    return MinioConfig(
        # Connection
        endpoint=connection.get("endpoint", "localhost:9000"),
        access_key=connection.get("access_key", "minioadmin"),
        secret_key=connection.get("secret_key", "minioadmin123"),
        secure=connection.get("secure", False),
        region=connection.get("region", "us-east-1"),
        # Buckets
        bucket_artifacts=buckets.get("artifacts", "artifacts"),
        bucket_inputs=buckets.get("inputs", "inputs"),
        bucket_aggregates=buckets.get("aggregates", "aggregates"),
        # Storage
        path_prefix_length=storage.get("path_prefix_length", 4),
        max_object_size=storage.get("max_object_size", 104857600),
        max_retries=storage.get("max_retries", 3),
        retry_delay_sec=storage.get("retry_delay_sec", 0.5),
        # Lifecycle
        artifacts_retention_days=lifecycle.get("artifacts_retention_days", 0),
        inputs_retention_days=lifecycle.get("inputs_retention_days", 30),
        aggregates_retention_days=lifecycle.get("aggregates_retention_days", 90),
    )
