"""MinIO configuration loader."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.config.base import get_section, load_toml

if TYPE_CHECKING:
    from pathlib import Path


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
    bucket_metric_values: str

    # Storage settings
    path_prefix_length: int
    max_object_size: int
    max_retries: int
    retry_delay_sec: float

    # Lifecycle settings
    artifacts_retention_days: int
    inputs_retention_days: int
    aggregates_retention_days: int

    # Connection timeouts (in seconds)
    connect_timeout: float
    read_timeout: float


def load_minio_config(path: Path) -> MinioConfig:
    """Load MinIO configuration from TOML file."""
    doc = load_toml(path)

    connection = get_section(doc, "connection")
    buckets = get_section(doc, "buckets")
    storage = get_section(doc, "storage")
    lifecycle = get_section(doc, "lifecycle")

    # Load credentials from environment variables (preferred) or config file
    # Environment variables take precedence for security
    endpoint = os.environ.get("MINIO_ENDPOINT") or connection.get("endpoint", "localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY") or connection.get("access_key", "")
    secret_key = os.environ.get("MINIO_SECRET_KEY") or connection.get("secret_key", "")

    # Validate that credentials are provided
    if not access_key or not secret_key:
        msg = "MinIO credentials not configured. Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables."
        raise ValueError(msg)

    return MinioConfig(
        # Connection - environment variables take precedence
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=connection.get("secure", False),
        region=connection.get("region", "us-east-1"),
        # Buckets
        bucket_artifacts=buckets.get("artifacts", "artifacts"),
        bucket_inputs=buckets.get("inputs", "inputs"),
        bucket_aggregates=buckets.get("aggregates", "aggregates"),
        bucket_metric_values=buckets.get("metric_values", "metric-values"),
        # Storage
        path_prefix_length=storage.get("path_prefix_length", 4),
        max_object_size=storage.get("max_object_size", 104857600),
        max_retries=storage.get("max_retries", 3),
        retry_delay_sec=storage.get("retry_delay_sec", 0.5),
        # Lifecycle
        artifacts_retention_days=lifecycle.get("artifacts_retention_days", 0),
        inputs_retention_days=lifecycle.get("inputs_retention_days", 30),
        aggregates_retention_days=lifecycle.get("aggregates_retention_days", 90),
        # Connection timeouts
        connect_timeout=float(connection.get("connect_timeout", 10.0)),
        read_timeout=float(connection.get("read_timeout", 30.0)),
    )
