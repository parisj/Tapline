"""Unit tests for src.flink.config.

Tests for:
- FlinkConfig dataclass
- load_flink_config function
- Default values
- Custom values from TOML
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.flink.config import FlinkConfig, load_flink_config

if TYPE_CHECKING:
    from pathlib import Path


def _write_toml(tmp_path: Path, text: str) -> Path:
    p = tmp_path / "flink.toml"
    p.write_text(text, encoding="utf-8")
    return p


class TestFlinkConfig:
    """Tests for FlinkConfig dataclass."""

    def test_flink_config_creation(self) -> None:
        config = FlinkConfig(
            jobmanager_address="jm:8081",
            rest_port=8081,
            parallelism=4,
            max_parallelism=128,
            buffer_timeout_ms=100,
            checkpoint_enabled=True,
            checkpoint_interval_ms=60000,
            checkpoint_mode="exactly_once",
            checkpoint_min_pause_ms=5000,
            checkpoint_timeout_ms=600000,
            checkpoint_max_concurrent=1,
            checkpoint_unaligned=False,
            state_backend="rocksdb",
            state_incremental=True,
            state_ttl_ms=86400000,
            tumbling_size_sec=60,
            sliding_size_sec=60,
            sliding_step_sec=10,
            late_event_tolerance_sec=30,
            allowed_lateness_sec=60,
            max_out_of_orderness_sec=10,
            idle_timeout_sec=60,
            kafka_consumer_group="grp",
            kafka_commit_on_checkpoints=True,
            kafka_start_from_earliest=True,
        )

        assert config.jobmanager_address == "jm:8081"
        assert config.parallelism == 4
        assert config.checkpoint_enabled is True
        assert config.state_backend == "rocksdb"

    def test_flink_config_is_frozen(self) -> None:
        import dataclasses

        config = FlinkConfig(
            jobmanager_address="jm:8081",
            rest_port=8081,
            parallelism=4,
            max_parallelism=128,
            buffer_timeout_ms=100,
            checkpoint_enabled=True,
            checkpoint_interval_ms=60000,
            checkpoint_mode="exactly_once",
            checkpoint_min_pause_ms=5000,
            checkpoint_timeout_ms=600000,
            checkpoint_max_concurrent=1,
            checkpoint_unaligned=False,
            state_backend="rocksdb",
            state_incremental=True,
            state_ttl_ms=86400000,
            tumbling_size_sec=60,
            sliding_size_sec=60,
            sliding_step_sec=10,
            late_event_tolerance_sec=30,
            allowed_lateness_sec=60,
            max_out_of_orderness_sec=10,
            idle_timeout_sec=60,
            kafka_consumer_group="grp",
            kafka_commit_on_checkpoints=True,
            kafka_start_from_earliest=True,
        )

        try:
            config.parallelism = 8  # type: ignore[misc]
        except dataclasses.FrozenInstanceError:
            return
        msg = "Expected FrozenInstanceError"
        raise AssertionError(msg)


class TestLoadFlinkConfig:
    """Tests for load_flink_config."""

    def test_load_uses_all_defaults_when_sections_empty(self, tmp_path: Path) -> None:
        path = _write_toml(
            tmp_path,
            """
            [connection]
            [execution]
            [checkpointing]
            [state]
            [windows]
            [watermarks]
            [kafka]
            """,
        )

        cfg = load_flink_config(path)

        # connection
        assert cfg.jobmanager_address == "localhost:8081"
        assert cfg.rest_port == 8081
        # execution
        assert cfg.parallelism == 4
        assert cfg.max_parallelism == 128
        assert cfg.buffer_timeout_ms == 100
        # checkpointing
        assert cfg.checkpoint_enabled is True
        assert cfg.checkpoint_interval_ms == 60000
        assert cfg.checkpoint_mode == "exactly_once"
        assert cfg.checkpoint_min_pause_ms == 5000
        assert cfg.checkpoint_timeout_ms == 600000
        assert cfg.checkpoint_max_concurrent == 1
        assert cfg.checkpoint_unaligned is False
        # state
        assert cfg.state_backend == "rocksdb"
        assert cfg.state_incremental is True
        assert cfg.state_ttl_ms == 86400000
        # windows
        assert cfg.tumbling_size_sec == 60
        assert cfg.sliding_size_sec == 60
        assert cfg.sliding_step_sec == 10
        assert cfg.late_event_tolerance_sec == 30
        assert cfg.allowed_lateness_sec == 60
        # watermarks
        assert cfg.max_out_of_orderness_sec == 10
        assert cfg.idle_timeout_sec == 60
        # kafka
        assert cfg.kafka_consumer_group == "tapline-flink"
        assert cfg.kafka_commit_on_checkpoints is True
        assert cfg.kafka_start_from_earliest is True

    def test_load_custom_values(self, tmp_path: Path) -> None:
        path = _write_toml(
            tmp_path,
            """
            [connection]
            jobmanager_address = "flink-jm:9999"
            rest_port = 9999

            [execution]
            parallelism = 8
            max_parallelism = 256
            buffer_timeout_ms = 50

            [checkpointing]
            enabled = false
            interval_ms = 30000
            mode = "at_least_once"
            min_pause_ms = 1000
            timeout_ms = 300000
            max_concurrent = 2
            unaligned = true

            [state]
            backend = "hashmap"
            incremental = false
            ttl_ms = 3600000

            [windows]
            tumbling_size_sec = 30
            sliding_size_sec = 120
            sliding_step_sec = 5
            late_event_tolerance_sec = 15
            allowed_lateness_sec = 90

            [watermarks]
            max_out_of_orderness_sec = 20
            idle_timeout_sec = 120

            [kafka]
            consumer_group = "custom-grp"
            commit_on_checkpoints = false
            start_from_earliest = false
            """,
        )

        cfg = load_flink_config(path)

        assert cfg.jobmanager_address == "flink-jm:9999"
        assert cfg.rest_port == 9999
        assert cfg.parallelism == 8
        assert cfg.max_parallelism == 256
        assert cfg.buffer_timeout_ms == 50
        assert cfg.checkpoint_enabled is False
        assert cfg.checkpoint_interval_ms == 30000
        assert cfg.checkpoint_mode == "at_least_once"
        assert cfg.checkpoint_min_pause_ms == 1000
        assert cfg.checkpoint_timeout_ms == 300000
        assert cfg.checkpoint_max_concurrent == 2
        assert cfg.checkpoint_unaligned is True
        assert cfg.state_backend == "hashmap"
        assert cfg.state_incremental is False
        assert cfg.state_ttl_ms == 3600000
        assert cfg.tumbling_size_sec == 30
        assert cfg.sliding_size_sec == 120
        assert cfg.sliding_step_sec == 5
        assert cfg.late_event_tolerance_sec == 15
        assert cfg.allowed_lateness_sec == 90
        assert cfg.max_out_of_orderness_sec == 20
        assert cfg.idle_timeout_sec == 120
        assert cfg.kafka_consumer_group == "custom-grp"
        assert cfg.kafka_commit_on_checkpoints is False
        assert cfg.kafka_start_from_earliest is False

    def test_load_partial_override(self, tmp_path: Path) -> None:
        path = _write_toml(
            tmp_path,
            """
            [connection]
            jobmanager_address = "host:1234"

            [execution]
            parallelism = 16

            [checkpointing]
            [state]
            [windows]
            [watermarks]
            [kafka]
            """,
        )

        cfg = load_flink_config(path)

        assert cfg.jobmanager_address == "host:1234"
        # rest_port keeps default
        assert cfg.rest_port == 8081
        assert cfg.parallelism == 16
        # max_parallelism keeps default
        assert cfg.max_parallelism == 128
