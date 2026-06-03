"""Unit tests for ModelInferenceProcessor.

Characterization tests that lock in the current behaviour of the
deterministic placeholder inference processor in
``src/algorithms/models/inference.py``.
"""

from __future__ import annotations

import hashlib

import pytest

from src.algorithms.models.inference import (
    ModelInferenceAlgo,
    ModelInferenceProcessor,
)
from src.domain.evaluation import AggregationType
from src.domain.results import Measurement, ProcessorResult


class TestModelInferenceIdentity:
    """Tests for processor identifiers (name/version/aliases)."""

    def test_name(self) -> None:
        proc = ModelInferenceProcessor()

        assert proc.name == "model_inference"

    def test_version(self) -> None:
        proc = ModelInferenceProcessor()

        assert proc.version == "0.1.0"

    def test_backwards_compat_alias_is_same_class(self) -> None:
        assert ModelInferenceAlgo is ModelInferenceProcessor


class TestModelInferenceInitialize:
    """Tests for initialize() behaviour."""

    def test_initialize_stores_model_path(self) -> None:
        proc = ModelInferenceProcessor()

        proc.initialize({"model_path": "/tmp/custom.onnx"})

        assert proc._model_path == "/tmp/custom.onnx"

    def test_initialize_default_model_path(self) -> None:
        proc = ModelInferenceProcessor()

        proc.initialize({})

        assert proc._model_path == "model.onnx"

    def test_initialize_coerces_model_path_to_string(self) -> None:
        proc = ModelInferenceProcessor()

        proc.initialize({"model_path": 12345})

        assert proc._model_path == "12345"
        assert isinstance(proc._model_path, str)

    def test_initialize_ignores_unknown_settings(self) -> None:
        proc = ModelInferenceProcessor()

        proc.initialize({"model_path": "x.onnx", "extra": "ignored"})

        assert proc._model_path == "x.onnx"


class TestModelInferenceRun:
    """Tests for run() output structure."""

    def test_run_returns_processor_result(self) -> None:
        proc = ModelInferenceProcessor()
        proc.initialize({"model_path": "m.onnx"})

        result = proc.run(b"hello world bytes", settings={})

        assert isinstance(result, ProcessorResult)
        assert result.metrics is not None
        assert "score" in result.metrics
        assert "model_path" in result.metrics

    def test_run_score_is_measurement_in_unit_interval(self) -> None:
        proc = ModelInferenceProcessor()
        proc.initialize({"model_path": "m.onnx"})

        result = proc.run(b"some image bytes", settings={})

        score = result.metrics["score"]
        assert isinstance(score, Measurement)
        assert 0.0 <= score.value <= 1.0

    def test_run_score_aggregation_is_stats_plus_histogram(self) -> None:
        proc = ModelInferenceProcessor()
        proc.initialize({"model_path": "m.onnx"})

        result = proc.run(b"data", settings={})

        agg = result.metrics["score"].aggregation
        assert AggregationType.STATS in agg
        assert AggregationType.HISTOGRAM in agg

    def test_run_model_path_aggregation_is_raw(self) -> None:
        proc = ModelInferenceProcessor()
        proc.initialize({"model_path": "m.onnx"})

        result = proc.run(b"data", settings={})

        mp = result.metrics["model_path"]
        assert isinstance(mp, Measurement)
        assert mp.aggregation == AggregationType.RAW
        assert mp.value == "m.onnx"

    def test_run_is_deterministic_for_same_bytes(self) -> None:
        proc = ModelInferenceProcessor()
        proc.initialize({"model_path": "m.onnx"})

        r1 = proc.run(b"deterministic input", settings={})
        r2 = proc.run(b"deterministic input", settings={})

        assert r1.metrics["score"].value == r2.metrics["score"].value

    def test_run_score_uses_first_byte_of_sha1_over_first_64_bytes(self) -> None:
        proc = ModelInferenceProcessor()
        proc.initialize({"model_path": "m.onnx"})

        payload = b"abcdefg" * 20  # > 64 bytes
        expected = hashlib.sha1(payload[:64], usedforsecurity=False).digest()[0] / 255.0

        result = proc.run(payload, settings={})

        assert result.metrics["score"].value == pytest.approx(expected)

    def test_run_score_only_depends_on_first_64_bytes(self) -> None:
        proc = ModelInferenceProcessor()
        proc.initialize({"model_path": "m.onnx"})

        head = b"A" * 64
        r1 = proc.run(head, settings={})
        r2 = proc.run(head + b"trailing-different-bytes", settings={})

        assert r1.metrics["score"].value == r2.metrics["score"].value

    def test_run_score_differs_for_different_first_byte_input(self) -> None:
        # Two payloads whose SHA1 prefixes likely differ in first byte
        proc = ModelInferenceProcessor()
        proc.initialize({"model_path": "m.onnx"})

        r1 = proc.run(b"input one", settings={})
        r2 = proc.run(b"input two completely different", settings={})

        # The scores should differ given different inputs hash differently
        assert r1.metrics["score"].value != r2.metrics["score"].value

    def test_run_with_empty_bytes_returns_zero_score(self) -> None:
        proc = ModelInferenceProcessor()
        proc.initialize({"model_path": "m.onnx"})

        result = proc.run(b"", settings={})

        assert result.metrics["score"].value == 0.0

    def test_run_with_bytes_shorter_than_64(self) -> None:
        proc = ModelInferenceProcessor()
        proc.initialize({"model_path": "m.onnx"})

        # SHA1 of short bytes still has 20-byte digest
        payload = b"short"
        expected = hashlib.sha1(payload[:64], usedforsecurity=False).digest()[0] / 255.0

        result = proc.run(payload, settings={})

        assert result.metrics["score"].value == pytest.approx(expected)

    def test_run_without_initialize_uses_none_model_path(self) -> None:
        # Characterizes behaviour: run() uses getattr(self, "_model_path", None)
        # so it works even if initialize() was never called.
        proc = ModelInferenceProcessor()

        result = proc.run(b"data", settings={})

        assert result.metrics["model_path"].value is None

    def test_run_ignores_settings_argument(self) -> None:
        proc = ModelInferenceProcessor()
        proc.initialize({"model_path": "m.onnx"})

        r_empty = proc.run(b"data", settings={})
        r_full = proc.run(b"data", settings={"threshold": 0.9, "anything": True})

        assert r_empty.metrics["score"].value == r_full.metrics["score"].value
        assert r_empty.metrics["model_path"].value == r_full.metrics["model_path"].value

    def test_run_returns_no_artifacts(self) -> None:
        proc = ModelInferenceProcessor()
        proc.initialize({"model_path": "m.onnx"})

        result = proc.run(b"data", settings={})

        assert result.artifacts == ()

    def test_run_metrics_exactly_two_keys(self) -> None:
        proc = ModelInferenceProcessor()
        proc.initialize({"model_path": "m.onnx"})

        result = proc.run(b"data", settings={})

        assert set(result.metrics.keys()) == {"score", "model_path"}
