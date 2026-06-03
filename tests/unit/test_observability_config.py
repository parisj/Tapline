"""Unit tests for src.observability.config.

Covers:
- ObservabilityConfig dataclass (frozen)
- load_observability_config from TOML
- Default fallback values when sections / fields missing
- File-not-exists path
- Tuple conversion for histogram_buckets
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from src.observability.config import ObservabilityConfig, load_observability_config

if TYPE_CHECKING:
    from pathlib import Path


def _write_toml(tmp_path: Path, text: str) -> Path:
    """Helper to write TOML content to a temp file."""
    p = tmp_path / "observability.toml"
    p.write_text(text, encoding="utf-8")
    return p


class TestObservabilityConfig:
    """Tests for the ObservabilityConfig dataclass itself."""

    def _make(self) -> ObservabilityConfig:
        return ObservabilityConfig(
            tracing_enabled=True,
            service_name="tapline",
            tracing_exporter="otlp",
            otlp_endpoint="http://localhost:4317",
            sample_rate=1.0,
            metrics_enabled=True,
            metrics_port=8000,
            metrics_path="/metrics",
            include_runtime_metrics=True,
            histogram_buckets=(0.1, 1.0),
            log_format="json",
            include_trace_context=True,
            include_correlation_id=True,
            include_caller_info=False,
            correlation_header_name="X-Correlation-ID",
            kafka_correlation_header="x-correlation-id",
            propagate_correlation_to_kafka=True,
        )

    def test_construction_round_trip(self) -> None:
        cfg = self._make()
        assert cfg.tracing_enabled is True
        assert cfg.service_name == "tapline"
        assert cfg.metrics_port == 8000
        assert cfg.histogram_buckets == (0.1, 1.0)
        assert cfg.log_format == "json"

    def test_is_frozen(self) -> None:
        cfg = self._make()
        with pytest.raises((AttributeError, Exception)):
            cfg.tracing_enabled = False  # type: ignore[misc]


class TestLoadObservabilityConfigDefaults:
    """Tests that defaults are returned when TOML is empty or path doesn't exist."""

    def test_missing_file_returns_defaults(self, tmp_path: Path) -> None:
        # Path that doesn't exist
        nonexistent = tmp_path / "absent.toml"

        cfg = load_observability_config(nonexistent)

        # All defaults
        assert cfg.tracing_enabled is True
        assert cfg.service_name == "tapline"
        assert cfg.tracing_exporter == "otlp"
        assert cfg.otlp_endpoint == "http://localhost:4317"
        assert cfg.sample_rate == 1.0
        assert cfg.metrics_enabled is True
        assert cfg.metrics_port == 8000
        assert cfg.metrics_path == "/metrics"
        assert cfg.include_runtime_metrics is True
        assert cfg.log_format == "json"
        assert cfg.include_trace_context is True
        assert cfg.include_correlation_id is True
        assert cfg.include_caller_info is False
        assert cfg.correlation_header_name == "X-Correlation-ID"
        assert cfg.kafka_correlation_header == "x-correlation-id"
        assert cfg.propagate_correlation_to_kafka is True

    def test_default_histogram_buckets(self, tmp_path: Path) -> None:
        nonexistent = tmp_path / "absent.toml"
        cfg = load_observability_config(nonexistent)

        assert isinstance(cfg.histogram_buckets, tuple)
        assert cfg.histogram_buckets == (
            0.005,
            0.01,
            0.025,
            0.05,
            0.1,
            0.25,
            0.5,
            1.0,
            2.5,
            5.0,
            10.0,
        )

    def test_empty_toml_returns_defaults(self, tmp_path: Path) -> None:
        path = _write_toml(tmp_path, "")

        cfg = load_observability_config(path)

        assert cfg.service_name == "tapline"
        assert cfg.metrics_port == 8000

    def test_empty_sections_return_defaults(self, tmp_path: Path) -> None:
        path = _write_toml(
            tmp_path,
            """
            [tracing]
            [metrics]
            [logging]
            [correlation]
            """,
        )

        cfg = load_observability_config(path)

        assert cfg.tracing_enabled is True
        assert cfg.metrics_enabled is True
        assert cfg.log_format == "json"
        assert cfg.correlation_header_name == "X-Correlation-ID"


class TestLoadObservabilityConfigOverrides:
    """Tests that explicit TOML values override defaults."""

    def test_tracing_overrides(self, tmp_path: Path) -> None:
        path = _write_toml(
            tmp_path,
            """
            [tracing]
            enabled = false
            service_name = "my-svc"
            exporter = "console"
            otlp_endpoint = "http://collector:4317"
            sample_rate = 0.25
            """,
        )

        cfg = load_observability_config(path)

        assert cfg.tracing_enabled is False
        assert cfg.service_name == "my-svc"
        assert cfg.tracing_exporter == "console"
        assert cfg.otlp_endpoint == "http://collector:4317"
        assert cfg.sample_rate == 0.25

    def test_metrics_overrides(self, tmp_path: Path) -> None:
        path = _write_toml(
            tmp_path,
            """
            [metrics]
            enabled = false
            port = 9090
            path = "/m"
            include_runtime_metrics = false
            histogram_buckets = [0.1, 0.5, 1.0, 5.0]
            """,
        )

        cfg = load_observability_config(path)

        assert cfg.metrics_enabled is False
        assert cfg.metrics_port == 9090
        assert cfg.metrics_path == "/m"
        assert cfg.include_runtime_metrics is False
        assert cfg.histogram_buckets == (0.1, 0.5, 1.0, 5.0)
        assert isinstance(cfg.histogram_buckets, tuple)

    def test_logging_overrides(self, tmp_path: Path) -> None:
        path = _write_toml(
            tmp_path,
            """
            [logging]
            format = "text"
            include_trace_context = false
            include_correlation_id = false
            include_caller_info = true
            """,
        )

        cfg = load_observability_config(path)

        assert cfg.log_format == "text"
        assert cfg.include_trace_context is False
        assert cfg.include_correlation_id is False
        assert cfg.include_caller_info is True

    def test_correlation_overrides(self, tmp_path: Path) -> None:
        path = _write_toml(
            tmp_path,
            """
            [correlation]
            header_name = "X-Trace-ID"
            kafka_header_name = "kafka-cid"
            propagate_to_kafka = false
            """,
        )

        cfg = load_observability_config(path)

        assert cfg.correlation_header_name == "X-Trace-ID"
        assert cfg.kafka_correlation_header == "kafka-cid"
        assert cfg.propagate_correlation_to_kafka is False

    def test_full_overrides(self, tmp_path: Path) -> None:
        path = _write_toml(
            tmp_path,
            """
            [tracing]
            enabled = false
            service_name = "svc"
            exporter = "none"
            sample_rate = 0.5

            [metrics]
            enabled = false
            port = 1234

            [logging]
            format = "text"

            [correlation]
            header_name = "X-Cid"
            """,
        )

        cfg = load_observability_config(path)

        assert cfg.tracing_enabled is False
        assert cfg.service_name == "svc"
        assert cfg.tracing_exporter == "none"
        assert cfg.sample_rate == 0.5
        assert cfg.metrics_enabled is False
        assert cfg.metrics_port == 1234
        assert cfg.log_format == "text"
        assert cfg.correlation_header_name == "X-Cid"


class TestLoadObservabilityConfigDefaultPath:
    def test_none_path_uses_default(self) -> None:
        """When called with path=None it should load from the default file."""
        # The default observability.toml ships with the project — should load.
        cfg = load_observability_config(None)

        assert isinstance(cfg, ObservabilityConfig)
        # Default file declares service_name = "tapline"
        assert cfg.service_name == "tapline"
