"""Flink configuration loader."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.config.base import get_section, load_toml

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class FlinkConfig:
    """PyFlink configuration settings."""

    # Connection
    jobmanager_address: str
    rest_port: int

    # Execution
    parallelism: int
    max_parallelism: int
    buffer_timeout_ms: int

    # Checkpointing
    checkpoint_enabled: bool
    checkpoint_interval_ms: int
    checkpoint_mode: str
    checkpoint_min_pause_ms: int
    checkpoint_timeout_ms: int
    checkpoint_max_concurrent: int
    checkpoint_unaligned: bool

    # State
    state_backend: str
    state_incremental: bool
    state_ttl_ms: int

    # Windows
    tumbling_size_sec: int
    sliding_size_sec: int
    sliding_step_sec: int
    late_event_tolerance_sec: int
    allowed_lateness_sec: int

    # Watermarks
    max_out_of_orderness_sec: int
    idle_timeout_sec: int

    # Kafka connector
    kafka_consumer_group: str
    kafka_commit_on_checkpoints: bool
    kafka_start_from_earliest: bool


def load_flink_config(path: Path) -> FlinkConfig:
    """Load Flink configuration from TOML file."""
    doc = load_toml(path)

    connection = get_section(doc, "connection")
    execution = get_section(doc, "execution")
    checkpointing = get_section(doc, "checkpointing")
    state = get_section(doc, "state")
    windows = get_section(doc, "windows")
    watermarks = get_section(doc, "watermarks")
    kafka = get_section(doc, "kafka")

    return FlinkConfig(
        # Connection
        jobmanager_address=connection.get("jobmanager_address", "localhost:8081"),
        rest_port=connection.get("rest_port", 8081),
        # Execution
        parallelism=execution.get("parallelism", 4),
        max_parallelism=execution.get("max_parallelism", 128),
        buffer_timeout_ms=execution.get("buffer_timeout_ms", 100),
        # Checkpointing
        checkpoint_enabled=checkpointing.get("enabled", True),
        checkpoint_interval_ms=checkpointing.get("interval_ms", 60000),
        checkpoint_mode=checkpointing.get("mode", "exactly_once"),
        checkpoint_min_pause_ms=checkpointing.get("min_pause_ms", 5000),
        checkpoint_timeout_ms=checkpointing.get("timeout_ms", 600000),
        checkpoint_max_concurrent=checkpointing.get("max_concurrent", 1),
        checkpoint_unaligned=checkpointing.get("unaligned", False),
        # State
        state_backend=state.get("backend", "rocksdb"),
        state_incremental=state.get("incremental", True),
        state_ttl_ms=state.get("ttl_ms", 86400000),
        # Windows
        tumbling_size_sec=windows.get("tumbling_size_sec", 60),
        sliding_size_sec=windows.get("sliding_size_sec", 60),
        sliding_step_sec=windows.get("sliding_step_sec", 10),
        late_event_tolerance_sec=windows.get("late_event_tolerance_sec", 30),
        allowed_lateness_sec=windows.get("allowed_lateness_sec", 60),
        # Watermarks
        max_out_of_orderness_sec=watermarks.get("max_out_of_orderness_sec", 10),
        idle_timeout_sec=watermarks.get("idle_timeout_sec", 60),
        # Kafka
        kafka_consumer_group=kafka.get("consumer_group", "tapline-flink"),
        kafka_commit_on_checkpoints=kafka.get("commit_on_checkpoints", True),
        kafka_start_from_earliest=kafka.get("start_from_earliest", True),
    )
