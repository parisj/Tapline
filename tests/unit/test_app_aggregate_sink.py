"""Unit tests for src/app/aggregate_sink.py."""

from __future__ import annotations

import signal
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import src.app.aggregate_sink as sink_mod


class _StoppedThread:
    """Stand-in for threading.Thread that records args and supports start/join."""

    def __init__(
        self,
        target: object | None = None,
        args: tuple[object, ...] = (),
        name: str = "",
        daemon: bool = False,
        **kwargs: object,
    ) -> None:
        self.target = target
        self.args = args
        self.name = name
        self.daemon = daemon
        self.started = False
        self.joined_with: float | None = None

    def start(self) -> None:
        self.started = True

    def join(self, timeout: float | None = None) -> None:
        self.joined_with = timeout


class TestMain:
    def test_python_aggregation_mode_default(self) -> None:
        """Without TAPLINE_USE_FLINK_SQL=true, just runs the values collector."""
        stop_event_holder: dict[str, threading.Event] = {}
        created_threads: list[_StoppedThread] = []

        def fake_thread_cls(**kwargs: object) -> _StoppedThread:
            t = _StoppedThread(**kwargs)
            created_threads.append(t)
            return t

        # When the while loop runs, set the stop event on first wait
        original_event_cls = threading.Event

        def event_factory() -> threading.Event:
            event = original_event_cls()
            stop_event_holder["event"] = event
            # Pre-set so wait returns immediately and loop exits
            event.set()
            return event

        with (
            patch.object(sink_mod, "load_dotenv"),
            patch.dict(sink_mod.os.environ, {}, clear=False),
            patch.object(sink_mod, "load_observability_config"),
            patch.object(sink_mod, "load_flink_config"),
            patch.object(sink_mod, "load_kafka_config"),
            patch.object(sink_mod, "load_minio_config"),
            patch.object(sink_mod, "configure_logging"),
            patch.object(sink_mod, "configure_tracing"),
            patch.object(sink_mod, "atexit") as mock_atexit,
            patch.object(sink_mod, "MinioStorageService"),
            patch.object(sink_mod.threading, "Event", side_effect=event_factory),
            patch.object(sink_mod.threading, "Thread", side_effect=fake_thread_cls),
            patch.object(sink_mod.signal, "signal") as mock_signal,
        ):
            sink_mod.os.environ.pop("TAPLINE_USE_FLINK_SQL", None)
            sink_mod.main()

        # atexit registered shutdown_tracing
        mock_atexit.register.assert_called_once_with(sink_mod.shutdown_tracing)

        # Two signal handlers (SIGINT/SIGTERM)
        signums = [c.args[0] for c in mock_signal.call_args_list]
        assert signal.SIGINT in signums
        assert signal.SIGTERM in signums

        # One thread created — the metric values collector
        assert len(created_threads) == 1
        thread = created_threads[0]
        assert thread.name == "metric-values-collector"
        assert thread.daemon is True
        assert thread.started is True
        assert thread.joined_with == 5.0
        # The target is run_metric_values_collector
        assert thread.target is sink_mod.run_metric_values_collector

    def test_flink_sql_mode_invokes_aggregate_consumer(self) -> None:
        """With TAPLINE_USE_FLINK_SQL=true, runs the aggregate consumer."""
        created_threads: list[_StoppedThread] = []

        def fake_thread_cls(**kwargs: object) -> _StoppedThread:
            t = _StoppedThread(**kwargs)
            created_threads.append(t)
            return t

        mock_consumer_module = MagicMock()

        with (
            patch.object(sink_mod, "load_dotenv"),
            patch.dict(sink_mod.os.environ, {"TAPLINE_USE_FLINK_SQL": "true"}),
            patch.object(sink_mod, "load_observability_config") as mock_obs,
            patch.object(sink_mod, "load_flink_config") as mock_flink,
            patch.object(sink_mod, "load_kafka_config") as mock_kafka,
            patch.object(sink_mod, "load_minio_config"),
            patch.object(sink_mod, "configure_logging"),
            patch.object(sink_mod, "configure_tracing"),
            patch.object(sink_mod, "atexit"),
            patch.object(sink_mod, "MinioStorageService") as mock_storage_cls,
            patch.object(sink_mod.threading, "Thread", side_effect=fake_thread_cls),
            patch.object(sink_mod.signal, "signal"),
            patch.dict(
                "sys.modules",
                {"src.app.aggregate_consumer": mock_consumer_module},
            ),
        ):
            sink_mod.main()

        # Consumer was called with expected args
        mock_consumer_module.run_aggregate_consumer.assert_called_once()
        call_args = mock_consumer_module.run_aggregate_consumer.call_args
        assert call_args.args[0] is mock_flink.return_value
        assert call_args.args[1] is mock_kafka.return_value
        assert call_args.args[2] is mock_storage_cls.return_value
        # stop_event is a threading.Event
        assert isinstance(call_args.args[3], threading.Event)

    def test_flink_sql_mode_case_insensitive(self) -> None:
        """TAPLINE_USE_FLINK_SQL='TRUE' should also enable Flink mode."""
        created_threads: list[_StoppedThread] = []

        def fake_thread_cls(**kwargs: object) -> _StoppedThread:
            t = _StoppedThread(**kwargs)
            created_threads.append(t)
            return t

        mock_consumer_module = MagicMock()

        with (
            patch.object(sink_mod, "load_dotenv"),
            patch.dict(sink_mod.os.environ, {"TAPLINE_USE_FLINK_SQL": "TRUE"}),
            patch.object(sink_mod, "load_observability_config"),
            patch.object(sink_mod, "load_flink_config"),
            patch.object(sink_mod, "load_kafka_config"),
            patch.object(sink_mod, "load_minio_config"),
            patch.object(sink_mod, "configure_logging"),
            patch.object(sink_mod, "configure_tracing"),
            patch.object(sink_mod, "atexit"),
            patch.object(sink_mod, "MinioStorageService"),
            patch.object(sink_mod.threading, "Thread", side_effect=fake_thread_cls),
            patch.object(sink_mod.signal, "signal"),
            patch.dict(
                "sys.modules",
                {"src.app.aggregate_consumer": mock_consumer_module},
            ),
        ):
            sink_mod.main()

        mock_consumer_module.run_aggregate_consumer.assert_called_once()

    def test_falsy_env_uses_python_mode(self) -> None:
        """TAPLINE_USE_FLINK_SQL=false should use Python mode."""
        stop_event_holder: dict[str, threading.Event] = {}
        created_threads: list[_StoppedThread] = []

        def fake_thread_cls(**kwargs: object) -> _StoppedThread:
            t = _StoppedThread(**kwargs)
            created_threads.append(t)
            return t

        original_event_cls = threading.Event

        def event_factory() -> threading.Event:
            event = original_event_cls()
            stop_event_holder["event"] = event
            event.set()
            return event

        mock_consumer_module = MagicMock()

        with (
            patch.object(sink_mod, "load_dotenv"),
            patch.dict(sink_mod.os.environ, {"TAPLINE_USE_FLINK_SQL": "false"}),
            patch.object(sink_mod, "load_observability_config"),
            patch.object(sink_mod, "load_flink_config"),
            patch.object(sink_mod, "load_kafka_config"),
            patch.object(sink_mod, "load_minio_config"),
            patch.object(sink_mod, "configure_logging"),
            patch.object(sink_mod, "configure_tracing"),
            patch.object(sink_mod, "atexit"),
            patch.object(sink_mod, "MinioStorageService"),
            patch.object(sink_mod.threading, "Event", side_effect=event_factory),
            patch.object(sink_mod.threading, "Thread", side_effect=fake_thread_cls),
            patch.object(sink_mod.signal, "signal"),
            patch.dict(
                "sys.modules",
                {"src.app.aggregate_consumer": mock_consumer_module},
            ),
        ):
            sink_mod.main()

        mock_consumer_module.run_aggregate_consumer.assert_not_called()

    def test_loads_configs_from_expected_paths(self) -> None:
        stop_event_holder: dict[str, threading.Event] = {}
        original_event_cls = threading.Event

        def event_factory() -> threading.Event:
            event = original_event_cls()
            stop_event_holder["event"] = event
            event.set()
            return event

        with (
            patch.object(sink_mod, "load_dotenv"),
            patch.dict(sink_mod.os.environ, {}, clear=False),
            patch.object(sink_mod, "load_observability_config") as mock_obs,
            patch.object(sink_mod, "load_flink_config") as mock_flink,
            patch.object(sink_mod, "load_kafka_config") as mock_kafka,
            patch.object(sink_mod, "load_minio_config") as mock_minio,
            patch.object(sink_mod, "configure_logging"),
            patch.object(sink_mod, "configure_tracing"),
            patch.object(sink_mod, "atexit"),
            patch.object(sink_mod, "MinioStorageService"),
            patch.object(sink_mod.threading, "Event", side_effect=event_factory),
            patch.object(sink_mod.threading, "Thread", side_effect=_StoppedThread),
            patch.object(sink_mod.signal, "signal"),
        ):
            sink_mod.os.environ.pop("TAPLINE_USE_FLINK_SQL", None)
            sink_mod.main()

        mock_obs.assert_called_once_with(Path("src/config/observability.toml"))
        mock_flink.assert_called_once_with(Path("src/config/flink.toml"))
        mock_kafka.assert_called_once_with(Path("src/config/kafka.toml"))
        mock_minio.assert_called_once_with(Path("src/config/minio.toml"))

    def test_signal_handler_sets_stop_event(self) -> None:
        """The handler installed by main() should set the stop event."""
        stop_event_holder: dict[str, threading.Event] = {}
        captured: dict[int, object] = {}

        original_event_cls = threading.Event

        def event_factory() -> threading.Event:
            event = original_event_cls()
            stop_event_holder["event"] = event
            event.set()  # break out of the while loop in python-aggregation mode
            return event

        def capture_signal(signum: int, handler: object) -> None:
            captured[signum] = handler

        with (
            patch.object(sink_mod, "load_dotenv"),
            patch.dict(sink_mod.os.environ, {}, clear=False),
            patch.object(sink_mod, "load_observability_config"),
            patch.object(sink_mod, "load_flink_config"),
            patch.object(sink_mod, "load_kafka_config"),
            patch.object(sink_mod, "load_minio_config"),
            patch.object(sink_mod, "configure_logging"),
            patch.object(sink_mod, "configure_tracing"),
            patch.object(sink_mod, "atexit"),
            patch.object(sink_mod, "MinioStorageService"),
            patch.object(sink_mod.threading, "Event", side_effect=event_factory),
            patch.object(sink_mod.threading, "Thread", side_effect=_StoppedThread),
            patch.object(sink_mod.signal, "signal", side_effect=capture_signal),
        ):
            sink_mod.os.environ.pop("TAPLINE_USE_FLINK_SQL", None)
            sink_mod.main()

        # Invoke the captured SIGINT handler; since stop_event was already set,
        # calling .set() again is a no-op and should still leave it set.
        event = stop_event_holder["event"]
        # Clear so we can verify the handler re-sets it
        event.clear()
        assert event.is_set() is False

        captured[signal.SIGINT](signal.SIGINT, None)  # type: ignore[operator]
        assert event.is_set() is True
