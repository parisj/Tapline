"""Unit tests for MinIO storage configuration loader.

Tests for:
- MinioConfig dataclass
- load_minio_config function
- Environment variable handling
- Credential validation
"""

from __future__ import annotations

import os
import tomllib
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest

from src.storage.config import MinioConfig, load_minio_config

if TYPE_CHECKING:
    from pathlib import Path


def _write_toml(tmp_path: Path, text: str) -> Path:
    """Helper to write TOML content to a temp file."""
    p = tmp_path / "minio.toml"
    p.write_text(text, encoding="utf-8")
    return p


class TestMinioConfig:
    """Tests for MinioConfig dataclass."""

    def test_minio_config_creation(self) -> None:
        # Test credentials - not real secrets
        test_key = "minioadmin"
        config = MinioConfig(
            endpoint="localhost:9000",
            access_key=test_key,
            secret_key=test_key,
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
            connect_timeout=10.0,
            read_timeout=30.0,
        )

        assert config.endpoint == "localhost:9000"
        assert config.access_key == test_key
        assert config.secret_key == test_key
        assert config.secure is False
        assert config.bucket_artifacts == "artifacts"
        assert config.path_prefix_length == 4
        assert config.connect_timeout == 10.0
        assert config.read_timeout == 30.0

    def test_minio_config_is_frozen(self) -> None:
        # Test credentials - not real secrets
        test_key = "minioadmin"
        config = MinioConfig(
            endpoint="localhost:9000",
            access_key=test_key,
            secret_key=test_key,
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
            connect_timeout=10.0,
            read_timeout=30.0,
        )

        with pytest.raises(AttributeError):
            config.endpoint = "new-endpoint:9000"  # type: ignore[misc]


class TestLoadMinioConfig:
    """Tests for load_minio_config function."""

    @pytest.fixture(autouse=True)
    def clear_env_vars(self) -> None:
        """Clear MinIO environment variables before each test."""
        for var in ["MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"]:
            os.environ.pop(var, None)

    def test_load_config_with_env_credentials(self, tmp_path: Path) -> None:
        """Test that environment variables take precedence."""
        path = _write_toml(
            tmp_path,
            """
            [connection]
            endpoint = "config-endpoint:9000"
            access_key = "config-access"
            secret_key = "config-secret"

            [buckets]
            [storage]
            [lifecycle]
            """,
        )

        with patch.dict(
            os.environ,
            {
                "MINIO_ENDPOINT": "env-endpoint:9000",
                "MINIO_ACCESS_KEY": "env-access",
                "MINIO_SECRET_KEY": "env-secret",
            },
        ):
            config = load_minio_config(path)

        # Env vars should override config file
        assert config.endpoint == "env-endpoint:9000"
        assert config.access_key == "env-access"
        env_secret = "env-secret"  # noqa: S105
        assert config.secret_key == env_secret

    def test_load_config_with_config_file_credentials(self, tmp_path: Path) -> None:
        """Test that config file credentials work when no env vars set."""
        path = _write_toml(
            tmp_path,
            """
            [connection]
            endpoint = "config-endpoint:9000"
            access_key = "config-access"
            secret_key = "config-secret"

            [buckets]
            [storage]
            [lifecycle]
            """,
        )

        config = load_minio_config(path)

        assert config.endpoint == "config-endpoint:9000"
        assert config.access_key == "config-access"
        config_secret = "config-secret"  # noqa: S105
        assert config.secret_key == config_secret

    def test_load_config_requires_credentials(self, tmp_path: Path) -> None:
        """Test that missing credentials raises ValueError."""
        path = _write_toml(
            tmp_path,
            """
            [connection]
            endpoint = "localhost:9000"
            # No credentials

            [buckets]
            [storage]
            [lifecycle]
            """,
        )

        with pytest.raises(ValueError, match="credentials not configured"):
            load_minio_config(path)

    def test_load_config_requires_access_key(self, tmp_path: Path) -> None:
        """Test that missing access_key raises ValueError."""
        path = _write_toml(
            tmp_path,
            """
            [connection]
            secret_key = "secret"

            [buckets]
            [storage]
            [lifecycle]
            """,
        )

        with pytest.raises(ValueError, match="credentials not configured"):
            load_minio_config(path)

    def test_load_config_requires_secret_key(self, tmp_path: Path) -> None:
        """Test that missing secret_key raises ValueError."""
        path = _write_toml(
            tmp_path,
            """
            [connection]
            access_key = "access"

            [buckets]
            [storage]
            [lifecycle]
            """,
        )

        with pytest.raises(ValueError, match="credentials not configured"):
            load_minio_config(path)

    def test_load_config_default_values(self, tmp_path: Path) -> None:
        """Test default values when sections are empty."""
        path = _write_toml(
            tmp_path,
            """
            [connection]
            access_key = "access"
            secret_key = "secret"

            [buckets]
            [storage]
            [lifecycle]
            """,
        )

        config = load_minio_config(path)

        # Connection defaults
        assert config.endpoint == "localhost:9000"
        assert config.secure is False
        assert config.region == "us-east-1"

        # Bucket defaults
        assert config.bucket_artifacts == "artifacts"
        assert config.bucket_inputs == "inputs"
        assert config.bucket_aggregates == "aggregates"
        assert config.bucket_metric_values == "metric-values"

        # Storage defaults
        assert config.path_prefix_length == 4
        assert config.max_object_size == 104857600
        assert config.max_retries == 3
        assert config.retry_delay_sec == 0.5

        # Lifecycle defaults
        assert config.artifacts_retention_days == 0
        assert config.inputs_retention_days == 30
        assert config.aggregates_retention_days == 90

    def test_load_config_custom_connection(self, tmp_path: Path) -> None:
        """Test custom connection settings."""
        path = _write_toml(
            tmp_path,
            """
            [connection]
            endpoint = "minio.example.com:9000"
            access_key = "access"
            secret_key = "secret"
            secure = true
            region = "eu-west-1"

            [buckets]
            [storage]
            [lifecycle]
            """,
        )

        config = load_minio_config(path)

        assert config.endpoint == "minio.example.com:9000"
        assert config.secure is True
        assert config.region == "eu-west-1"

    def test_load_config_custom_buckets(self, tmp_path: Path) -> None:
        """Test custom bucket names."""
        path = _write_toml(
            tmp_path,
            """
            [connection]
            access_key = "access"
            secret_key = "secret"

            [buckets]
            artifacts = "custom-artifacts"
            inputs = "custom-inputs"
            aggregates = "custom-aggregates"
            metric_values = "custom-metrics"

            [storage]
            [lifecycle]
            """,
        )

        config = load_minio_config(path)

        assert config.bucket_artifacts == "custom-artifacts"
        assert config.bucket_inputs == "custom-inputs"
        assert config.bucket_aggregates == "custom-aggregates"
        assert config.bucket_metric_values == "custom-metrics"

    def test_load_config_custom_storage(self, tmp_path: Path) -> None:
        """Test custom storage settings."""
        path = _write_toml(
            tmp_path,
            """
            [connection]
            access_key = "access"
            secret_key = "secret"

            [buckets]

            [storage]
            path_prefix_length = 8
            max_object_size = 52428800
            max_retries = 5
            retry_delay_sec = 1.0

            [lifecycle]
            """,
        )

        config = load_minio_config(path)

        assert config.path_prefix_length == 8
        assert config.max_object_size == 52428800
        assert config.max_retries == 5
        assert config.retry_delay_sec == 1.0

    def test_load_config_custom_lifecycle(self, tmp_path: Path) -> None:
        """Test custom lifecycle settings."""
        path = _write_toml(
            tmp_path,
            """
            [connection]
            access_key = "access"
            secret_key = "secret"

            [buckets]
            [storage]

            [lifecycle]
            artifacts_retention_days = 365
            inputs_retention_days = 7
            aggregates_retention_days = 180
            """,
        )

        config = load_minio_config(path)

        assert config.artifacts_retention_days == 365
        assert config.inputs_retention_days == 7
        assert config.aggregates_retention_days == 180

    def test_load_file_not_found(self, tmp_path: Path) -> None:
        """Test that FileNotFoundError is raised for missing file."""
        path = tmp_path / "nonexistent.toml"

        with pytest.raises(FileNotFoundError):
            load_minio_config(path)

    def test_load_invalid_toml(self, tmp_path: Path) -> None:
        """Test that invalid TOML raises an error."""
        path = _write_toml(tmp_path, "invalid toml [[[")

        with pytest.raises(tomllib.TOMLDecodeError):
            load_minio_config(path)

    def test_env_vars_override_partial(self, tmp_path: Path) -> None:
        """Test that env vars can partially override config."""
        path = _write_toml(
            tmp_path,
            """
            [connection]
            endpoint = "config-endpoint:9000"
            access_key = "config-access"
            secret_key = "config-secret"

            [buckets]
            [storage]
            [lifecycle]
            """,
        )

        # Only override endpoint, use config for credentials
        with patch.dict(os.environ, {"MINIO_ENDPOINT": "env-endpoint:9000"}):
            config = load_minio_config(path)

        assert config.endpoint == "env-endpoint:9000"
        assert config.access_key == "config-access"
        config_secret = "config-secret"  # noqa: S105
        assert config.secret_key == config_secret
