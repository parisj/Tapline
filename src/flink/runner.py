"""Flink job runner and environment setup.

Provides utilities for:
- PyFlink environment configuration
- Checkpoint configuration (RocksDB state backend)
- Job submission helpers
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration

from src.utils.logging import get_logger

if TYPE_CHECKING:
    from src.flink.config import FlinkConfig

logger = get_logger(__name__)


class FlinkRunner:
    """Runner for PyFlink jobs with managed environment.

    Handles:
    - Environment creation and configuration
    - State backend setup (RocksDB or HashMap)
    - Checkpoint configuration
    - JAR dependency management
    """

    def __init__(self, config: FlinkConfig) -> None:
        self._config = config
        self._env: StreamExecutionEnvironment | None = None

    def create_environment(
        self,
        *,
        local: bool = True,
        jars: list[str] | None = None,
    ) -> StreamExecutionEnvironment:
        """Create and configure Flink execution environment.

        Args:
            local: If True, create local environment for development
            jars: Additional JAR files to include (Kafka connectors, etc.)

        Returns:
            Configured StreamExecutionEnvironment
        """
        if local:
            configuration = Configuration()

            # Configure parallelism
            configuration.set_integer(
                "parallelism.default",
                self._config.parallelism,
            )

            # Configure state backend
            if self._config.state_backend == "rocksdb":
                configuration.set_string(
                    "state.backend",
                    "rocksdb",
                )
                configuration.set_boolean(
                    "state.backend.incremental",
                    self._config.state_incremental,
                )

            env = StreamExecutionEnvironment.get_execution_environment(configuration)
        else:
            env = StreamExecutionEnvironment.get_execution_environment()

        # Set parallelism
        env.set_parallelism(self._config.parallelism)
        env.set_max_parallelism(self._config.max_parallelism)
        env.set_buffer_timeout(self._config.buffer_timeout_ms)

        # Configure checkpointing
        self._configure_checkpointing(env)

        # Add JARs if provided
        if jars:
            for jar_path in jars:
                if Path(jar_path).exists():
                    env.add_jars(f"file://{jar_path}")
                    logger.info("Added JAR: %s", jar_path)

        self._env = env
        logger.info(
            "Flink environment created: parallelism=%d, state_backend=%s",
            self._config.parallelism,
            self._config.state_backend,
        )

        return env

    def _configure_checkpointing(self, env: StreamExecutionEnvironment) -> None:
        """Configure checkpointing for fault tolerance."""
        if not self._config.checkpoint_enabled:
            return

        # Enable checkpointing
        env.enable_checkpointing(self._config.checkpoint_interval_ms)

        checkpoint_config = env.get_checkpoint_config()

        # Set checkpoint mode
        if self._config.checkpoint_mode == "exactly_once":
            from pyflink.datastream import CheckpointingMode
            checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

        # Configure timing
        checkpoint_config.set_min_pause_between_checkpoints(
            self._config.checkpoint_min_pause_ms
        )
        checkpoint_config.set_checkpoint_timeout(
            self._config.checkpoint_timeout_ms
        )
        checkpoint_config.set_max_concurrent_checkpoints(
            self._config.checkpoint_max_concurrent
        )

        # Enable unaligned checkpoints if configured
        if self._config.checkpoint_unaligned:
            checkpoint_config.enable_unaligned_checkpoints()

        logger.info(
            "Checkpointing configured: interval=%dms, mode=%s",
            self._config.checkpoint_interval_ms,
            self._config.checkpoint_mode,
        )

    def get_environment(self) -> StreamExecutionEnvironment:
        """Get or create the execution environment."""
        if self._env is None:
            return self.create_environment()
        return self._env

    def submit_job(
        self,
        job_name: str,
        *,
        detached: bool = False,
    ) -> str | None:
        """Submit job for execution.

        Args:
            job_name: Name for the job
            detached: If True, submit without waiting for completion

        Returns:
            Job ID if detached, None otherwise
        """
        if self._env is None:
            raise RuntimeError("Environment not created. Call create_environment first.")

        if detached:
            # For detached execution, would use REST API submission
            # This is a simplified synchronous execution
            logger.info("Submitting job: %s", job_name)

        result = self._env.execute(job_name)
        logger.info("Job completed: %s", job_name)

        return None


def get_kafka_connector_jars() -> list[str]:
    """Get paths to Kafka connector JARs.

    Returns list of JAR paths needed for Kafka integration.
    These should be downloaded separately or provided via maven coordinates.
    """
    # Common locations for Flink Kafka connector JARs
    potential_paths = [
        "/opt/flink/lib/flink-connector-kafka-*.jar",
        "./lib/flink-connector-kafka-*.jar",
        os.environ.get("FLINK_KAFKA_CONNECTOR_JAR", ""),
    ]

    jars = []
    for pattern in potential_paths:
        if pattern:
            matches = list(Path(pattern).parent.glob(Path(pattern).name))
            jars.extend(str(m) for m in matches)

    return jars


def create_kafka_source_sql(
    topic: str,
    bootstrap_servers: str,
    group_id: str,
    *,
    format_type: str = "json",
    scan_startup_mode: str = "earliest-offset",
) -> str:
    """Generate SQL for creating a Kafka source table.

    This can be used with Flink SQL for table-based processing.

    Args:
        topic: Kafka topic name
        bootstrap_servers: Kafka bootstrap servers
        group_id: Consumer group ID
        format_type: Message format (json, avro, etc.)
        scan_startup_mode: earliest-offset or latest-offset

    Returns:
        SQL CREATE TABLE statement
    """
    return f"""
    CREATE TABLE kafka_source (
        event_id STRING,
        event_type STRING,
        source_id STRING,
        `timestamp` TIMESTAMP(3),
        payload STRING,
        content_hash STRING,
        prev_hash STRING,
        WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '10' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{topic}',
        'properties.bootstrap.servers' = '{bootstrap_servers}',
        'properties.group.id' = '{group_id}',
        'format' = '{format_type}',
        'scan.startup.mode' = '{scan_startup_mode}'
    )
    """


def create_kafka_sink_sql(
    topic: str,
    bootstrap_servers: str,
    *,
    format_type: str = "json",
) -> str:
    """Generate SQL for creating a Kafka sink table.

    Args:
        topic: Kafka topic name
        bootstrap_servers: Kafka bootstrap servers
        format_type: Message format (json, avro, etc.)

    Returns:
        SQL CREATE TABLE statement
    """
    return f"""
    CREATE TABLE kafka_sink (
        algo_name STRING,
        algo_version STRING,
        metric_name STRING,
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        summary STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{topic}',
        'properties.bootstrap.servers' = '{bootstrap_servers}',
        'format' = '{format_type}'
    )
    """
