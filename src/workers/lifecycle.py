from __future__ import annotations

import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.dispatch.dispatcher import DispatchPlan


class AlgoLifecycle:
    """Ensures algorithms are initialized once per (name, version) in this process."""

    def __init__(self) -> None:
        self._init_lock = threading.Lock()
        self._initialized: set[tuple[str, str]] = set()

    def ensure_initialized(self, plan: DispatchPlan) -> None:
        key = (plan.algo.name, plan.algo.version)
        if key in self._initialized:
            return

        with self._init_lock:
            if key in self._initialized:
                return
            plan.algo.initialize(plan.settings)
            self._initialized.add(key)
