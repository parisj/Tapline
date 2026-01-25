from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

    from src.domain.results import ProcessorResult


class Processor(abc.ABC):
    """Shared interface for both traditional CV processors and trained-model inference.

    Contract:
    - initialize(...) is called once per worker instance (not per file).
    - run(...) is pure compute (no DB, no threading, no filesystem writes).
    - Return ProcessorResult only.
    """

    @property
    @abc.abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def version(self) -> str:
        raise NotImplementedError

    def initialize(self, _settings: Mapping[str, Any]) -> None:
        """Load model weights, allocate resources, build CV kernels, etc.

        Called once per worker instance.
        """
        return

    @abc.abstractmethod
    def run(self, image_bytes: bytes, settings: Mapping[str, Any]) -> ProcessorResult:
        """Pure compute: takes bytes and settings, returns metrics + optional artifacts."""
        raise NotImplementedError


# Backwards compatibility alias (deprecated)
Algorithm = Processor
