from __future__ import annotations

import signal
import threading
from typing import Any, Callable


def install_signal_handlers(stop_event: threading.Event) -> None:
    """Set stop_event on SIGINT/SIGTERM."""
    def _handler(*_: Any) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
