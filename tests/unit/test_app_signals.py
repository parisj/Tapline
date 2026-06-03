"""Unit tests for src/app/signals.py."""

from __future__ import annotations

import os
import signal
import threading
from unittest.mock import patch

from src.app.signals import install_signal_handlers


class TestInstallSignalHandlers:
    def test_registers_sigint_handler(self) -> None:
        stop_event = threading.Event()

        with patch("src.app.signals.signal.signal") as mock_signal:
            install_signal_handlers(stop_event)

        # Should register at least SIGINT and SIGTERM
        registered_signals = [call.args[0] for call in mock_signal.call_args_list]
        assert signal.SIGINT in registered_signals
        assert signal.SIGTERM in registered_signals

    def test_handler_sets_stop_event_on_sigint(self) -> None:
        stop_event = threading.Event()
        captured_handlers: dict[int, object] = {}

        def fake_signal(signum: int, handler: object) -> None:
            captured_handlers[signum] = handler

        with patch("src.app.signals.signal.signal", side_effect=fake_signal):
            install_signal_handlers(stop_event)

        assert stop_event.is_set() is False

        # Invoke the registered SIGINT handler directly
        captured_handlers[signal.SIGINT](signal.SIGINT, None)  # type: ignore[operator]

        assert stop_event.is_set() is True

    def test_handler_sets_stop_event_on_sigterm(self) -> None:
        stop_event = threading.Event()
        captured_handlers: dict[int, object] = {}

        def fake_signal(signum: int, handler: object) -> None:
            captured_handlers[signum] = handler

        with patch("src.app.signals.signal.signal", side_effect=fake_signal):
            install_signal_handlers(stop_event)

        captured_handlers[signal.SIGTERM](signal.SIGTERM, None)  # type: ignore[operator]

        assert stop_event.is_set() is True

    def test_handler_ignores_signum_and_frame_args(self) -> None:
        """Handler signature accepts signum/frame but ignores their values."""
        stop_event = threading.Event()
        captured_handlers: dict[int, object] = {}

        def fake_signal(signum: int, handler: object) -> None:
            captured_handlers[signum] = handler

        with patch("src.app.signals.signal.signal", side_effect=fake_signal):
            install_signal_handlers(stop_event)

        # Arbitrary signum and frame values should still set the event
        captured_handlers[signal.SIGINT](999, "not-a-frame")  # type: ignore[operator]

        assert stop_event.is_set() is True

    def test_real_sigterm_sets_stop_event(self) -> None:
        """End-to-end: send a real SIGTERM and confirm the stop_event gets set."""
        stop_event = threading.Event()

        # Save existing handlers so we can restore them after the test
        original_sigint = signal.getsignal(signal.SIGINT)
        original_sigterm = signal.getsignal(signal.SIGTERM)

        try:
            install_signal_handlers(stop_event)
            os.kill(os.getpid(), signal.SIGTERM)
            # Signal delivery is essentially synchronous on the same thread for kill(self),
            # but allow a short wait just in case.
            assert stop_event.wait(timeout=2.0) is True
        finally:
            signal.signal(signal.SIGINT, original_sigint)
            signal.signal(signal.SIGTERM, original_sigterm)

    def test_separate_events_are_independent(self) -> None:
        """install_signal_handlers binds to the specific event passed in."""
        event_a = threading.Event()
        event_b = threading.Event()
        captured_handlers: dict[int, object] = {}

        def fake_signal(signum: int, handler: object) -> None:
            captured_handlers[signum] = handler

        with patch("src.app.signals.signal.signal", side_effect=fake_signal):
            install_signal_handlers(event_a)

        # Replace handler with one bound to event_b
        captured_handlers_b: dict[int, object] = {}

        def fake_signal_b(signum: int, handler: object) -> None:
            captured_handlers_b[signum] = handler

        with patch("src.app.signals.signal.signal", side_effect=fake_signal_b):
            install_signal_handlers(event_b)

        # Calling event_a's handler should not affect event_b
        captured_handlers[signal.SIGINT](signal.SIGINT, None)  # type: ignore[operator]
        assert event_a.is_set() is True
        assert event_b.is_set() is False

        # And vice versa
        captured_handlers_b[signal.SIGINT](signal.SIGINT, None)  # type: ignore[operator]
        assert event_b.is_set() is True
