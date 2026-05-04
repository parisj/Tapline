from __future__ import annotations

import signal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import threading
    from types import FrameType


def install_signal_handlers(stop_event: threading.Event) -> None:
    """Set stop_event on SIGINT/SIGTERM."""

    def _handler(_signum: int, _frame: FrameType | None) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
