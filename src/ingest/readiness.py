from __future__ import annotations

import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def wait_for_file_ready(
    path: Path,
    stable_window_sec: float,
    max_wait_sec: float,
) -> bool:
    """Wait until file size+mtime are stable and file can be opened for reading."""
    deadline = time.monotonic() + max_wait_sec
    last_size = None
    last_mtime_ns = None
    stable_since = None

    while time.monotonic() < deadline:
        try:
            st = path.stat()
        except FileNotFoundError:
            time.sleep(0.02)
            continue

        size = st.st_size
        mtime_ns = st.st_mtime_ns

        if size == last_size and mtime_ns == last_mtime_ns:
            if stable_since is None:
                stable_since = time.monotonic()
            elif (time.monotonic() - stable_since) >= stable_window_sec:
                try:
                    with path.open("rb"):
                        return True
                except OSError:
                    pass
        else:
            stable_since = None
            last_size = size
            last_mtime_ns = mtime_ns

        time.sleep(0.01)

    return False
