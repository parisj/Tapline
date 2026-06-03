"""Unit tests for the processor base class.

Locks in the abstract contract of `Processor` and the deprecated alias.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from src.algorithms.base import Algorithm, Processor
from src.domain.evaluation import AggregationType
from src.domain.results import Measurement, ProcessorResult


class _MinimalProcessor(Processor):
    """Concrete subclass for instantiation tests."""

    @property
    def name(self) -> str:
        return "minimal"

    @property
    def version(self) -> str:
        return "0.0.1"

    def run(self, image_bytes: bytes, settings: Mapping[str, Any]) -> ProcessorResult:
        return ProcessorResult(
            metrics={
                "size": Measurement(
                    value=len(image_bytes),
                    aggregation=AggregationType.STATS,
                    meta=dict(settings),
                ),
            },
        )


class TestProcessorAbstractContract:
    """Verifies that Processor enforces an abstract surface."""

    def test_cannot_instantiate_processor_directly(self) -> None:
        with pytest.raises(TypeError):
            Processor()  # type: ignore[abstract]

    def test_subclass_without_name_cannot_instantiate(self) -> None:
        class NoName(Processor):
            @property
            def version(self) -> str:
                return "1"

            def run(self, image_bytes: bytes, settings: Mapping[str, Any]) -> ProcessorResult:
                return ProcessorResult()

        with pytest.raises(TypeError):
            NoName()  # type: ignore[abstract]

    def test_subclass_without_version_cannot_instantiate(self) -> None:
        class NoVersion(Processor):
            @property
            def name(self) -> str:
                return "n"

            def run(self, image_bytes: bytes, settings: Mapping[str, Any]) -> ProcessorResult:
                return ProcessorResult()

        with pytest.raises(TypeError):
            NoVersion()  # type: ignore[abstract]

    def test_subclass_without_run_cannot_instantiate(self) -> None:
        class NoRun(Processor):
            @property
            def name(self) -> str:
                return "n"

            @property
            def version(self) -> str:
                return "1"

        with pytest.raises(TypeError):
            NoRun()  # type: ignore[abstract]


class TestProcessorDefaults:
    """The default initialize() implementation should be a safe no-op."""

    def test_initialize_returns_none(self) -> None:
        proc = _MinimalProcessor()
        assert proc.initialize({}) is None

    def test_initialize_accepts_arbitrary_mapping(self) -> None:
        proc = _MinimalProcessor()
        proc.initialize({"any": "thing", "n": 1})

    def test_concrete_subclass_exposes_name_and_version(self) -> None:
        proc = _MinimalProcessor()
        assert proc.name == "minimal"
        assert proc.version == "0.0.1"

    def test_run_returns_processor_result(self) -> None:
        proc = _MinimalProcessor()
        result = proc.run(b"abcd", {"k": "v"})
        assert isinstance(result, ProcessorResult)
        assert result.metrics is not None
        assert result.metrics["size"].value == 4


class TestBackwardsCompatibilityAlias:
    """The deprecated `Algorithm` alias must still point at `Processor`."""

    def test_algorithm_is_processor(self) -> None:
        assert Algorithm is Processor

    def test_subclass_via_alias_is_processor(self) -> None:
        class ViaAlias(Algorithm):  # type: ignore[misc, valid-type]
            @property
            def name(self) -> str:
                return "x"

            @property
            def version(self) -> str:
                return "1"

            def run(self, image_bytes: bytes, settings: Mapping[str, Any]) -> ProcessorResult:
                return ProcessorResult()

        instance = ViaAlias()
        assert isinstance(instance, Processor)
