"""Shared constants and validation functions for the visualization module."""

from __future__ import annotations

import re

# Allowed buckets for artifact access (security: prevent bucket enumeration)
ALLOWED_BUCKETS = frozenset({"artifacts", "inputs", "aggregates", "metric-values"})

# Rate limiting
CLEANUP_CUTOFF_SECONDS = 300
RATE_LIMIT_TOKENS_PER_SEC = 10.0
RATE_LIMIT_BUCKET_CAPACITY = 50

# Pagination limits
DEFAULT_LIST_LIMIT = 1000
MAX_TOPICS_DISPLAY = 20
DEFAULT_TASK_LIMIT = 50
MAX_TASK_LIMIT = 200
DEFAULT_ARTIFACT_LIMIT = 100
MAX_ARTIFACT_LIMIT = 1000

# MinIO
MINIO_MAX_OBJECTS = 10000

# Time ranges
MAX_TIME_RANGE_MINUTES = 43200  # 30 days

# Default URLs
PROMETHEUS_URL = "http://localhost:9091"

# Input validation patterns
METRIC_NAME_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,63}$")
ALGORITHM_NAME_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9_-]{0,63}$")
VERSION_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,31}$")
TASK_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")

# MIME type mapping
EXTENSION_TO_MIME = {
    "png": "image/png",
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "gif": "image/gif",
    "json": "application/json",
    "npz": "application/x-npz",
    "csv": "text/csv",
    "txt": "text/plain",
    "html": "text/html",
}


def validate_metric_name(name: str) -> bool:
    """Validate metric name format.

    Args:
        name: Metric name to validate

    Returns:
        True if valid, False otherwise

    """
    return bool(METRIC_NAME_PATTERN.match(name))


def validate_algorithm_name(name: str) -> bool:
    """Validate algorithm name format.

    Args:
        name: Algorithm name to validate

    Returns:
        True if valid, False otherwise

    """
    return bool(ALGORITHM_NAME_PATTERN.match(name))


def validate_version(version: str) -> bool:
    """Validate version string format.

    Args:
        version: Version string to validate

    Returns:
        True if valid, False otherwise

    """
    return bool(VERSION_PATTERN.match(version))


def validate_task_id(task_id: str) -> bool:
    """Validate task ID format.

    Args:
        task_id: Task ID to validate

    Returns:
        True if valid, False otherwise

    """
    return bool(TASK_ID_PATTERN.match(task_id))


def get_mime_from_extension(filename: str) -> str:
    """Get MIME type from filename extension.

    Args:
        filename: Filename with extension

    Returns:
        MIME type string

    """
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    return EXTENSION_TO_MIME.get(ext, "application/octet-stream")
