"""Flink job runner and environment setup.

Provides utilities for:
- PyFlink environment configuration
- Checkpoint configuration (RocksDB state backend)
- Job execution

Note: This module requires apache-flink to be installed separately.
PyFlink is an optional dependency due to its pyarrow version constraints.
Install with: pip install "apache-flink>=2.0"
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

try:
    from pyflink.common import Configuration
    from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment

    PYFLINK_AVAILABLE = True
except ImportError:
    PYFLINK_AVAILABLE = False
    Configuration = None
    CheckpointingMode = None
    StreamExecutionEnvironment = None

from src.utils.logging import get_logger

if TYPE_CHECKING:
    from pyflink.datastream import StreamExecutionEnvironment as FlinkEnv

    from src.flink.config import FlinkConfig
else:
    # Runtime type alias when PyFlink may not be available
    FlinkEnv = object

logger = get_logger(__name__)


def check_pyflink_available() -> bool:
    """Check if PyFlink is available.

    Returns:
        True if apache-flink is installed and importable.

    """
    return PYFLINK_AVAILABLE


class FlinkRunner:
    """Runner for PyFlink jobs with managed environment.

    Handles:
    - Environment creation and configuration
    - State backend setup (RocksDB or HashMap)
    - Checkpoint configuration
    - JAR dependency management

    Requires apache-flink to be installed separately:
        pip install "apache-flink>=2.0"
    """

    def __init__(self, config: FlinkConfig) -> None:
        if not PYFLINK_AVAILABLE:
            msg = (
                "PyFlink is not installed. Install with: pip install 'apache-flink>=2.0'\n"
                "Note: PyFlink is optional. Python aggregation (default) works without it."
            )
            raise ImportError(msg)
        self._config = config
        self._env: FlinkEnv | None = None

    def create_environment(
        self,
        *,
        local: bool = True,
        jars: list[str] | None = None,
    ) -> FlinkEnv:
        """Create and configure Flink execution environment.

        Args:
            local: If True, create local environment for development
            jars: Additional JAR files to include (Kafka connectors, etc.)

        Returns:
            Configured StreamExecutionEnvironment

        """
        if local:
            configuration = Configuration()

            configuration.set_integer(
                "parallelism.default",
                self._config.parallelism,
            )

            if self._config.state_backend == "rocksdb":
                configuration.set_string("state.backend", "rocksdb")
                configuration.set_boolean(
                    "state.backend.incremental",
                    self._config.state_incremental,
                )

            env = StreamExecutionEnvironment.get_execution_environment(configuration)
        else:
            env = StreamExecutionEnvironment.get_execution_environment()

        env.set_parallelism(self._config.parallelism)
        env.set_max_parallelism(self._config.max_parallelism)
        env.set_buffer_timeout(self._config.buffer_timeout_ms)

        self._configure_checkpointing(env)

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

    def _configure_checkpointing(self, env: FlinkEnv) -> None:
        """Configure checkpointing for fault tolerance."""
        if not self._config.checkpoint_enabled:
            return

        env.enable_checkpointing(self._config.checkpoint_interval_ms)

        checkpoint_config = env.get_checkpoint_config()

        if self._config.checkpoint_mode == "exactly_once":
            checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

        checkpoint_config.set_min_pause_between_checkpoints(
            self._config.checkpoint_min_pause_ms,
        )
        checkpoint_config.set_checkpoint_timeout(
            self._config.checkpoint_timeout_ms,
        )
        checkpoint_config.set_max_concurrent_checkpoints(
            self._config.checkpoint_max_concurrent,
        )

        if self._config.checkpoint_unaligned:
            checkpoint_config.enable_unaligned_checkpoints()

        logger.info(
            "Checkpointing configured: interval=%dms, mode=%s",
            self._config.checkpoint_interval_ms,
            self._config.checkpoint_mode,
        )

    def get_environment(self) -> FlinkEnv:
        """Get or create the execution environment."""
        if self._env is None:
            return self.create_environment()
        return self._env

    def execute(self, job_name: str) -> None:
        """Execute the job synchronously.

        Args:
            job_name: Name for the job

        """
        if self._env is None:
            msg = "Environment not created. Call create_environment first."
            raise RuntimeError(msg)

        logger.info("Executing job: %s", job_name)
        self._env.execute(job_name)
        logger.info("Job completed: %s", job_name)
