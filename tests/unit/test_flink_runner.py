"""Unit tests for src.flink.runner.

PyFlink is an optional dependency. In environments without pyflink:
- module imports successfully with PYFLINK_AVAILABLE=False
- FlinkRunner() raises ImportError

These tests cover both cases: the import guard and (when available) the
FlinkRunner internal behaviour using mocked Flink primitives.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.flink import runner as runner_mod
from src.flink.config import FlinkConfig
from src.flink.runner import FlinkRunner, check_pyflink_available


def _make_config(**overrides: object) -> FlinkConfig:
    defaults: dict[str, object] = {
        "jobmanager_address": "localhost:8081",
        "rest_port": 8081,
        "parallelism": 4,
        "max_parallelism": 128,
        "buffer_timeout_ms": 100,
        "checkpoint_enabled": True,
        "checkpoint_interval_ms": 60000,
        "checkpoint_mode": "exactly_once",
        "checkpoint_min_pause_ms": 5000,
        "checkpoint_timeout_ms": 600000,
        "checkpoint_max_concurrent": 1,
        "checkpoint_unaligned": False,
        "state_backend": "rocksdb",
        "state_incremental": True,
        "state_ttl_ms": 86400000,
        "tumbling_size_sec": 60,
        "sliding_size_sec": 60,
        "sliding_step_sec": 10,
        "late_event_tolerance_sec": 30,
        "allowed_lateness_sec": 60,
        "max_out_of_orderness_sec": 10,
        "idle_timeout_sec": 60,
        "kafka_consumer_group": "grp",
        "kafka_commit_on_checkpoints": True,
        "kafka_start_from_earliest": True,
    }
    defaults.update(overrides)
    return FlinkConfig(**defaults)  # type: ignore[arg-type]


class TestCheckPyflinkAvailable:
    def test_returns_pyflink_available_constant(self) -> None:
        # Mirrors the module-level constant set at import time.
        assert check_pyflink_available() is runner_mod.PYFLINK_AVAILABLE


class TestFlinkRunnerImportGuard:
    def test_raises_import_error_when_pyflink_missing(self) -> None:
        """If pyflink is not installed, constructing FlinkRunner must raise."""
        cfg = _make_config()
        with (
            patch.object(runner_mod, "PYFLINK_AVAILABLE", new=False),
            pytest.raises(ImportError, match="PyFlink is not installed"),
        ):
            FlinkRunner(cfg)


class TestFlinkRunnerWithMockedPyflink:
    """Tests for FlinkRunner with pyflink primitives mocked.

    We forcibly mark PYFLINK_AVAILABLE=True and inject mocks for the
    Configuration and StreamExecutionEnvironment classes so we can verify
    the runner's interactions with PyFlink without needing the real library.
    """

    def _patch_pyflink(self) -> tuple[MagicMock, MagicMock, MagicMock]:
        cfg_cls = MagicMock(name="ConfigurationClass")
        env_cls = MagicMock(name="StreamExecutionEnvironmentClass")
        ckpt_mode = MagicMock(name="CheckpointingMode")
        ckpt_mode.EXACTLY_ONCE = "EXACTLY_ONCE"
        return cfg_cls, env_cls, ckpt_mode

    def test_create_environment_local_rocksdb(self) -> None:
        cfg = _make_config(parallelism=8, state_backend="rocksdb", state_incremental=True)
        cfg_cls, env_cls, ckpt_mode = self._patch_pyflink()

        with (
            patch.object(runner_mod, "PYFLINK_AVAILABLE", new=True),
            patch.object(runner_mod, "Configuration", cfg_cls),
            patch.object(runner_mod, "StreamExecutionEnvironment", env_cls),
            patch.object(runner_mod, "CheckpointingMode", ckpt_mode),
        ):
            runner = FlinkRunner(cfg)
            env = runner.create_environment(local=True)

        # Configuration was constructed and given parallelism + rocksdb settings
        cfg_cls.assert_called_once_with()
        cfg_instance = cfg_cls.return_value
        cfg_instance.set_integer.assert_any_call("parallelism.default", 8)
        cfg_instance.set_string.assert_any_call("state.backend", "rocksdb")
        cfg_instance.set_boolean.assert_any_call("state.backend.incremental", True)  # noqa: FBT003

        # local path uses get_execution_environment(configuration)
        env_cls.get_execution_environment.assert_called_once_with(cfg_instance)

        env.set_parallelism.assert_called_with(8)
        env.set_max_parallelism.assert_called_with(128)
        env.set_buffer_timeout.assert_called_with(100)
        # checkpointing path
        env.enable_checkpointing.assert_called_with(60000)

    def test_create_environment_non_local_no_configuration(self) -> None:
        cfg = _make_config()
        cfg_cls, env_cls, ckpt_mode = self._patch_pyflink()

        with (
            patch.object(runner_mod, "PYFLINK_AVAILABLE", new=True),
            patch.object(runner_mod, "Configuration", cfg_cls),
            patch.object(runner_mod, "StreamExecutionEnvironment", env_cls),
            patch.object(runner_mod, "CheckpointingMode", ckpt_mode),
        ):
            runner = FlinkRunner(cfg)
            runner.create_environment(local=False)

        # Configuration not used in non-local branch
        cfg_cls.assert_not_called()
        env_cls.get_execution_environment.assert_called_once_with()

    def test_create_environment_hashmap_backend_skips_rocksdb_calls(self) -> None:
        cfg = _make_config(state_backend="hashmap")
        cfg_cls, env_cls, ckpt_mode = self._patch_pyflink()

        with (
            patch.object(runner_mod, "PYFLINK_AVAILABLE", new=True),
            patch.object(runner_mod, "Configuration", cfg_cls),
            patch.object(runner_mod, "StreamExecutionEnvironment", env_cls),
            patch.object(runner_mod, "CheckpointingMode", ckpt_mode),
        ):
            runner = FlinkRunner(cfg)
            runner.create_environment(local=True)

        cfg_instance = cfg_cls.return_value
        # No rocksdb-specific set_string call
        for call in cfg_instance.set_string.call_args_list:
            assert call.args[0] != "state.backend"

    def test_create_environment_adds_existing_jars(self, tmp_path) -> None:
        cfg = _make_config()
        cfg_cls, env_cls, ckpt_mode = self._patch_pyflink()

        existing_jar = tmp_path / "real.jar"
        existing_jar.write_text("jar")
        missing_jar = tmp_path / "missing.jar"

        with (
            patch.object(runner_mod, "PYFLINK_AVAILABLE", new=True),
            patch.object(runner_mod, "Configuration", cfg_cls),
            patch.object(runner_mod, "StreamExecutionEnvironment", env_cls),
            patch.object(runner_mod, "CheckpointingMode", ckpt_mode),
        ):
            runner = FlinkRunner(cfg)
            env = runner.create_environment(jars=[str(existing_jar), str(missing_jar)])

        # Only the existing jar gets added
        env.add_jars.assert_called_once_with(f"file://{existing_jar}")

    def test_create_environment_no_jars_when_arg_omitted(self) -> None:
        cfg = _make_config()
        cfg_cls, env_cls, ckpt_mode = self._patch_pyflink()

        with (
            patch.object(runner_mod, "PYFLINK_AVAILABLE", new=True),
            patch.object(runner_mod, "Configuration", cfg_cls),
            patch.object(runner_mod, "StreamExecutionEnvironment", env_cls),
            patch.object(runner_mod, "CheckpointingMode", ckpt_mode),
        ):
            runner = FlinkRunner(cfg)
            env = runner.create_environment()

        env.add_jars.assert_not_called()

    def test_configure_checkpointing_exactly_once(self) -> None:
        cfg = _make_config(
            checkpoint_enabled=True,
            checkpoint_interval_ms=15000,
            checkpoint_mode="exactly_once",
            checkpoint_min_pause_ms=2000,
            checkpoint_timeout_ms=120000,
            checkpoint_max_concurrent=3,
            checkpoint_unaligned=True,
        )
        cfg_cls, env_cls, ckpt_mode = self._patch_pyflink()

        with (
            patch.object(runner_mod, "PYFLINK_AVAILABLE", new=True),
            patch.object(runner_mod, "Configuration", cfg_cls),
            patch.object(runner_mod, "StreamExecutionEnvironment", env_cls),
            patch.object(runner_mod, "CheckpointingMode", ckpt_mode),
        ):
            runner = FlinkRunner(cfg)
            env = runner.create_environment()

        env.enable_checkpointing.assert_called_with(15000)
        ckpt = env.get_checkpoint_config.return_value
        ckpt.set_checkpointing_mode.assert_called_with("EXACTLY_ONCE")
        ckpt.set_min_pause_between_checkpoints.assert_called_with(2000)
        ckpt.set_checkpoint_timeout.assert_called_with(120000)
        ckpt.set_max_concurrent_checkpoints.assert_called_with(3)
        ckpt.enable_unaligned_checkpoints.assert_called_once()

    def test_configure_checkpointing_disabled_short_circuits(self) -> None:
        cfg = _make_config(checkpoint_enabled=False)
        cfg_cls, env_cls, ckpt_mode = self._patch_pyflink()

        with (
            patch.object(runner_mod, "PYFLINK_AVAILABLE", new=True),
            patch.object(runner_mod, "Configuration", cfg_cls),
            patch.object(runner_mod, "StreamExecutionEnvironment", env_cls),
            patch.object(runner_mod, "CheckpointingMode", ckpt_mode),
        ):
            runner = FlinkRunner(cfg)
            env = runner.create_environment()

        env.enable_checkpointing.assert_not_called()
        env.get_checkpoint_config.assert_not_called()

    def test_configure_checkpointing_non_exactly_once_skips_mode_setter(self) -> None:
        cfg = _make_config(checkpoint_mode="at_least_once", checkpoint_unaligned=False)
        cfg_cls, env_cls, ckpt_mode = self._patch_pyflink()

        with (
            patch.object(runner_mod, "PYFLINK_AVAILABLE", new=True),
            patch.object(runner_mod, "Configuration", cfg_cls),
            patch.object(runner_mod, "StreamExecutionEnvironment", env_cls),
            patch.object(runner_mod, "CheckpointingMode", ckpt_mode),
        ):
            runner = FlinkRunner(cfg)
            env = runner.create_environment()

        ckpt = env.get_checkpoint_config.return_value
        ckpt.set_checkpointing_mode.assert_not_called()
        ckpt.enable_unaligned_checkpoints.assert_not_called()

    def test_get_environment_creates_if_none(self) -> None:
        cfg = _make_config()
        cfg_cls, env_cls, ckpt_mode = self._patch_pyflink()

        with (
            patch.object(runner_mod, "PYFLINK_AVAILABLE", new=True),
            patch.object(runner_mod, "Configuration", cfg_cls),
            patch.object(runner_mod, "StreamExecutionEnvironment", env_cls),
            patch.object(runner_mod, "CheckpointingMode", ckpt_mode),
        ):
            runner = FlinkRunner(cfg)
            env = runner.get_environment()

        assert env is env_cls.get_execution_environment.return_value

    def test_get_environment_returns_existing(self) -> None:
        cfg = _make_config()
        cfg_cls, env_cls, ckpt_mode = self._patch_pyflink()

        with (
            patch.object(runner_mod, "PYFLINK_AVAILABLE", new=True),
            patch.object(runner_mod, "Configuration", cfg_cls),
            patch.object(runner_mod, "StreamExecutionEnvironment", env_cls),
            patch.object(runner_mod, "CheckpointingMode", ckpt_mode),
        ):
            runner = FlinkRunner(cfg)
            env1 = runner.create_environment()
            env2 = runner.get_environment()

        assert env1 is env2
        # get_execution_environment should have been called only once
        assert env_cls.get_execution_environment.call_count == 1

    def test_execute_without_environment_raises(self) -> None:
        cfg = _make_config()
        with patch.object(runner_mod, "PYFLINK_AVAILABLE", new=True):
            runner = FlinkRunner(cfg)
            with pytest.raises(RuntimeError, match="Environment not created"):
                runner.execute("job")

    def test_execute_calls_env_execute(self) -> None:
        cfg = _make_config()
        cfg_cls, env_cls, ckpt_mode = self._patch_pyflink()

        with (
            patch.object(runner_mod, "PYFLINK_AVAILABLE", new=True),
            patch.object(runner_mod, "Configuration", cfg_cls),
            patch.object(runner_mod, "StreamExecutionEnvironment", env_cls),
            patch.object(runner_mod, "CheckpointingMode", ckpt_mode),
        ):
            runner = FlinkRunner(cfg)
            env = runner.create_environment()
            runner.execute("my-job")

        env.execute.assert_called_once_with("my-job")
