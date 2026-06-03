"""Unit tests for the processor registry."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from src.algorithms.base import Processor
from src.algorithms.registry import (
    AlgorithmFactory,
    AlgorithmRegistry,
    ProcessorRegistry,
    build_default_registry,
)
from src.domain.results import ProcessorResult


class _StubProcessor(Processor):
    @property
    def name(self) -> str:
        return "stub"

    @property
    def version(self) -> str:
        return "1.0.0"

    def run(self, image_bytes: bytes, settings: Mapping[str, Any]) -> ProcessorResult:
        return ProcessorResult()


class TestProcessorRegistryRegister:
    def test_register_adds_factory(self) -> None:
        reg = ProcessorRegistry()
        reg.register("stub", "1.0.0", _StubProcessor)

        instance = reg.create("stub", "1.0.0")
        assert isinstance(instance, _StubProcessor)

    def test_register_duplicate_raises_value_error(self) -> None:
        reg = ProcessorRegistry()
        reg.register("stub", "1.0.0", _StubProcessor)

        with pytest.raises(ValueError, match="already registered"):
            reg.register("stub", "1.0.0", _StubProcessor)

    def test_same_name_different_version_allowed(self) -> None:
        reg = ProcessorRegistry()
        reg.register("stub", "1.0.0", _StubProcessor)
        reg.register("stub", "2.0.0", _StubProcessor)

        a = reg.create("stub", "1.0.0")
        b = reg.create("stub", "2.0.0")
        assert isinstance(a, _StubProcessor)
        assert isinstance(b, _StubProcessor)

    def test_factory_called_each_create(self) -> None:
        reg = ProcessorRegistry()
        calls = {"n": 0}

        def factory() -> Processor:
            calls["n"] += 1
            return _StubProcessor()

        reg.register("stub", "1.0.0", factory)
        reg.create("stub", "1.0.0")
        reg.create("stub", "1.0.0")
        assert calls["n"] == 2


class TestProcessorRegistryCreate:
    def test_create_unknown_raises_key_error(self) -> None:
        reg = ProcessorRegistry()
        with pytest.raises(KeyError):
            reg.create("missing", "1.0.0")

    def test_create_returns_fresh_instance(self) -> None:
        reg = ProcessorRegistry()
        reg.register("stub", "1.0.0", _StubProcessor)

        a = reg.create("stub", "1.0.0")
        b = reg.create("stub", "1.0.0")
        assert a is not b


class TestBuildDefaultRegistry:
    def test_returns_processor_registry(self) -> None:
        reg = build_default_registry()
        assert isinstance(reg, ProcessorRegistry)

    def test_contains_expected_cv_processors(self) -> None:
        reg = build_default_registry()
        expected = [
            ("analysis_probe", "1.0.0"),
            ("blob_detection", "1.0.0"),
            ("image_quality", "1.0.0"),
            ("edge_detection", "1.0.0"),
            ("histogram_analysis", "1.0.0"),
            ("contour_analysis", "1.0.0"),
        ]
        for name, version in expected:
            instance = reg.create(name, version)
            assert isinstance(instance, Processor)
            assert instance.name == name
            assert instance.version == version

    def test_contains_model_processor_keys(self) -> None:
        # The model inference processors are registered but importing them
        # requires heavy dependencies; just verify the keys exist by looking
        # in the internal registry mapping.
        reg = build_default_registry()
        assert ("model_inference", "0.1.0") in reg._factories
        assert ("model_yolo_segmentation", "1.0.0") in reg._factories

    def test_unknown_processor_raises_key_error(self) -> None:
        reg = build_default_registry()
        with pytest.raises(KeyError):
            reg.create("does_not_exist", "1.0.0")


class TestBackwardsCompatibilityAliases:
    def test_algorithm_registry_is_processor_registry(self) -> None:
        assert AlgorithmRegistry is ProcessorRegistry

    def test_algorithm_factory_matches_processor_factory(self) -> None:
        # AlgorithmFactory is an alias for ProcessorFactory (Callable[[], Processor]).
        from src.algorithms.registry import ProcessorFactory

        assert AlgorithmFactory is ProcessorFactory
