"""Unit tests for src/app/main.py.

Characterization tests: mocks every external boundary (Kafka, MinIO,
config loaders, signal handlers) and asserts the orchestration logic
of build_app, run, and main.
"""

from __future__ import annotations

import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import src.app.main as main_mod


def _make_runtime_config(**overrides: object) -> MagicMock:
    """Build a stand-in RuntimeConfig MagicMock with sensible defaults."""
    cfg = MagicMock()
    cfg.directories = {"inbox": Path("/tmp/inbox")}
    cfg.ingest_poll_interval_sec = 0.1
    cfg.allowed_image_exts = [".png", ".jpg"]
    cfg.readiness_max_wait_sec = 5.0
    cfg.readiness_stable_window_sec = 1.0
    cfg.workers_max = 3
    cfg.commit_batch_size = 10
    cfg.io_workers = 4
    cfg.artifact_upload_workers = 4
    for key, value in overrides.items():
        setattr(cfg, key, value)
    return cfg


def _make_obs_config() -> MagicMock:
    obs = MagicMock()
    obs.tracing_enabled = True
    obs.metrics_enabled = True
    obs.metrics_port = 9000
    return obs


class TestBuildApp:
    def test_build_app_returns_four_tuple(self) -> None:
        cfg = _make_runtime_config()

        with (
            patch.object(main_mod, "load_kafka_config") as mock_load_kafka,
            patch.object(main_mod, "load_minio_config") as mock_load_minio,
            patch.object(main_mod, "MinioStorageService") as mock_storage_cls,
            patch.object(main_mod, "EventProducer") as mock_producer_cls,
            patch.object(main_mod, "build_default_registry") as mock_registry,
            patch.object(main_mod, "load_routes_toml") as mock_load_routes,
            patch.object(main_mod, "build_dispatch_plans") as mock_build_plans,
            patch.object(main_mod, "Dispatcher") as mock_dispatcher_cls,
            patch.object(main_mod, "DedupCache") as mock_dedup_cls,
            patch.object(main_mod, "KafkaDirectoryObserver") as mock_observer_cls,
            patch.object(main_mod, "KafkaWorkerPool") as mock_pool_cls,
        ):
            result = main_mod.build_app(cfg=cfg)

        assert len(result) == 4
        observer, worker_pool, producer, storage = result
        assert observer is mock_observer_cls.return_value
        assert worker_pool is mock_pool_cls.return_value
        assert producer is mock_producer_cls.return_value
        assert storage is mock_storage_cls.return_value

        # Config loaders called with expected paths
        mock_load_kafka.assert_called_once_with(Path("src/config/kafka.toml"))
        mock_load_minio.assert_called_once_with(Path("src/config/minio.toml"))
        mock_load_routes.assert_called_once_with(
            Path("src/config/routes.toml"),
            config_root=Path("src/config"),
        )
        mock_build_plans.assert_called_once_with(
            mock_load_routes.return_value,
            registry=mock_registry.return_value,
        )
        mock_dispatcher_cls.assert_called_once_with(routes=mock_build_plans.return_value)
        mock_dedup_cls.assert_called_once_with(ttl_sec=3600.0)

    def test_build_app_forwards_cfg_to_observer(self) -> None:
        cfg = _make_runtime_config(
            directories={"x": Path("/dir-x")},
            ingest_poll_interval_sec=2.5,
            allowed_image_exts=[".png"],
            readiness_max_wait_sec=99.0,
            readiness_stable_window_sec=7.0,
        )

        with (
            patch.object(main_mod, "load_kafka_config"),
            patch.object(main_mod, "load_minio_config"),
            patch.object(main_mod, "MinioStorageService"),
            patch.object(main_mod, "EventProducer") as mock_producer_cls,
            patch.object(main_mod, "build_default_registry"),
            patch.object(main_mod, "load_routes_toml"),
            patch.object(main_mod, "build_dispatch_plans"),
            patch.object(main_mod, "Dispatcher"),
            patch.object(main_mod, "DedupCache") as mock_dedup_cls,
            patch.object(main_mod, "KafkaDirectoryObserver") as mock_observer_cls,
            patch.object(main_mod, "KafkaWorkerPool"),
        ):
            main_mod.build_app(cfg=cfg)

        mock_observer_cls.assert_called_once_with(
            directories={"x": Path("/dir-x")},
            producer=mock_producer_cls.return_value,
            dedup=mock_dedup_cls.return_value,
            poll_interval_sec=2.5,
            allowed_exts={".png"},
            max_wait_sec=99.0,
            stable_window_sec=7.0,
        )

    def test_build_app_forwards_cfg_to_worker_pool(self) -> None:
        cfg = _make_runtime_config(
            workers_max=8,
            commit_batch_size=20,
            io_workers=2,
            artifact_upload_workers=6,
        )

        with (
            patch.object(main_mod, "load_kafka_config") as mock_load_kafka,
            patch.object(main_mod, "load_minio_config"),
            patch.object(main_mod, "MinioStorageService") as mock_storage_cls,
            patch.object(main_mod, "EventProducer") as mock_producer_cls,
            patch.object(main_mod, "build_default_registry"),
            patch.object(main_mod, "load_routes_toml"),
            patch.object(main_mod, "build_dispatch_plans"),
            patch.object(main_mod, "Dispatcher") as mock_dispatcher_cls,
            patch.object(main_mod, "DedupCache"),
            patch.object(main_mod, "KafkaDirectoryObserver"),
            patch.object(main_mod, "KafkaWorkerPool") as mock_pool_cls,
        ):
            main_mod.build_app(cfg=cfg)

        mock_pool_cls.assert_called_once_with(
            kafka_config=mock_load_kafka.return_value,
            dispatcher=mock_dispatcher_cls.return_value,
            storage=mock_storage_cls.return_value,
            producer=mock_producer_cls.return_value,
            max_workers=8,
            commit_batch_size=20,
            io_workers=2,
            artifact_upload_workers=6,
        )

    def test_build_app_propagates_routes_config_error(self) -> None:
        cfg = _make_runtime_config()

        with (
            patch.object(main_mod, "load_kafka_config"),
            patch.object(main_mod, "load_minio_config"),
            patch.object(main_mod, "MinioStorageService"),
            patch.object(main_mod, "EventProducer"),
            patch.object(main_mod, "build_default_registry"),
            patch.object(
                main_mod,
                "load_routes_toml",
                side_effect=main_mod.RoutesConfigError("bad routes"),
            ),
            patch.object(main_mod, "build_dispatch_plans"),
            patch.object(main_mod, "Dispatcher"),
            patch.object(main_mod, "DedupCache"),
            patch.object(main_mod, "KafkaDirectoryObserver"),
            patch.object(main_mod, "KafkaWorkerPool"),
            pytest.raises(main_mod.RoutesConfigError),
        ):
            main_mod.build_app(cfg=cfg)


class TestRun:
    def test_run_starts_and_stops_components_in_order(self) -> None:
        cfg = _make_runtime_config()
        obs = _make_obs_config()

        mock_observer = MagicMock()
        mock_worker_pool = MagicMock()
        mock_producer = MagicMock()
        mock_storage = MagicMock()

        # Stop event flips True after the first sleep
        stop_event = threading.Event()

        def fake_install(event: threading.Event) -> None:
            # capture reference; event itself will be set by fake_sleep
            fake_install.captured = event  # type: ignore[attr-defined]

        sleep_calls: list[float] = []

        def fake_sleep(duration: float) -> None:
            sleep_calls.append(duration)
            # First call is the 0.5s consumer-group stabilization sleep.
            # Subsequent 1.0s sleeps are inside the while loop; set the event
            # on the second 1.0s sleep to break the loop.
            if duration == 1.0:
                fake_install.captured.set()  # type: ignore[attr-defined]

        with (
            patch.object(
                main_mod,
                "build_app",
                return_value=(mock_observer, mock_worker_pool, mock_producer, mock_storage),
            ),
            patch.object(main_mod, "install_signal_handlers", side_effect=fake_install),
            patch.object(main_mod, "PIPELINE_UP") as mock_pipeline_up,
            patch.object(main_mod, "WORKER_POOL_SIZE") as mock_pool_size,
            patch.object(main_mod.time, "sleep", side_effect=fake_sleep),
        ):
            main_mod.run(cfg, obs)

        # Lifecycle order
        mock_worker_pool.start.assert_called_once()
        mock_observer.start.assert_called_once()
        mock_observer.stop.assert_called_once()
        mock_worker_pool.stop.assert_called_once()
        mock_producer.close.assert_called_once()

        # Metrics updates: PIPELINE_UP set to 1 then 0
        set_calls = [c.args[0] for c in mock_pipeline_up.set.call_args_list]
        assert set_calls == [1, 0]
        mock_pool_size.set.assert_called_once_with(cfg.workers_max)

        # Producer polled at least once in the loop
        mock_producer.poll.assert_called_with(0)
        # Used at least one 0.5 stabilization sleep + one 1.0 loop sleep
        assert 0.5 in sleep_calls
        assert 1.0 in sleep_calls

    def test_run_stops_immediately_when_event_preset(self) -> None:
        cfg = _make_runtime_config()
        obs = _make_obs_config()

        mock_observer = MagicMock()
        mock_worker_pool = MagicMock()
        mock_producer = MagicMock()

        def install_and_preset(event: threading.Event) -> None:
            event.set()

        with (
            patch.object(
                main_mod,
                "build_app",
                return_value=(mock_observer, mock_worker_pool, mock_producer, MagicMock()),
            ),
            patch.object(main_mod, "install_signal_handlers", side_effect=install_and_preset),
            patch.object(main_mod, "PIPELINE_UP"),
            patch.object(main_mod, "WORKER_POOL_SIZE"),
            patch.object(main_mod.time, "sleep"),
        ):
            main_mod.run(cfg, obs)

        # Loop body never ran -> producer.poll never called inside loop
        mock_producer.poll.assert_not_called()
        # Shutdown still happens
        mock_observer.stop.assert_called_once()
        mock_worker_pool.stop.assert_called_once()
        mock_producer.close.assert_called_once()


class TestMain:
    def test_main_wires_config_loaders_and_invokes_run(self) -> None:
        runtime_cfg = _make_runtime_config()
        obs_cfg = _make_obs_config()

        with (
            patch.object(main_mod, "load_dotenv") as mock_load_dotenv,
            patch.object(main_mod, "load_observability_config", return_value=obs_cfg) as mock_load_obs,
            patch.object(main_mod, "configure_logging") as mock_configure_logging,
            patch.object(main_mod, "configure_tracing") as mock_configure_tracing,
            patch.object(main_mod, "atexit") as mock_atexit,
            patch.object(main_mod, "configure_metrics", return_value=True) as mock_configure_metrics,
            patch.object(main_mod, "set_build_info") as mock_set_build_info,
            patch.object(main_mod, "load_runtime_config", return_value=runtime_cfg) as mock_load_runtime,
            patch.object(main_mod, "run") as mock_run,
        ):
            main_mod.main()

        mock_load_dotenv.assert_called_once_with()
        mock_load_obs.assert_called_once_with(Path("src/config/observability.toml"))
        mock_configure_logging.assert_called_once_with(obs_cfg)
        mock_configure_tracing.assert_called_once_with(obs_cfg)
        mock_atexit.register.assert_called_once_with(main_mod.shutdown_tracing)
        mock_configure_metrics.assert_called_once_with(obs_cfg)
        mock_set_build_info.assert_called_once_with(version=main_mod.__version__, mode="streaming")
        mock_load_runtime.assert_called_once_with(Path("src/config/pipeline.toml"))
        mock_run.assert_called_once_with(runtime_cfg, obs_cfg)

    def test_main_handles_metrics_not_started(self) -> None:
        """If configure_metrics returns False, main should still complete."""
        runtime_cfg = _make_runtime_config()
        obs_cfg = _make_obs_config()

        with (
            patch.object(main_mod, "load_dotenv"),
            patch.object(main_mod, "load_observability_config", return_value=obs_cfg),
            patch.object(main_mod, "configure_logging"),
            patch.object(main_mod, "configure_tracing"),
            patch.object(main_mod, "atexit"),
            patch.object(main_mod, "configure_metrics", return_value=False),
            patch.object(main_mod, "set_build_info"),
            patch.object(main_mod, "load_runtime_config", return_value=runtime_cfg),
            patch.object(main_mod, "run") as mock_run,
        ):
            main_mod.main()

        mock_run.assert_called_once_with(runtime_cfg, obs_cfg)


class TestVersion:
    def test_version_string(self) -> None:
        assert isinstance(main_mod.__version__, str)
        assert main_mod.__version__ == "0.1.0"
