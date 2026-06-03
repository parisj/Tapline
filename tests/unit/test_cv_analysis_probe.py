"""Unit tests for the analysis probe processor."""

from __future__ import annotations

import json
import math

import pytest

from src.algorithms.cv.analysis_probe import (
    AnalysisProbeAlgo,
    AnalysisProbeProcessor,
)
from src.domain.evaluation import AggregationType
from src.domain.results import Measurement, ProcessorResult


class TestAnalysisProbeMetadata:
    def test_name(self) -> None:
        assert AnalysisProbeProcessor().name == "analysis_probe"

    def test_version(self) -> None:
        assert AnalysisProbeProcessor().version == "1.0.0"

    def test_alias_points_to_processor(self) -> None:
        assert AnalysisProbeAlgo is AnalysisProbeProcessor


class TestAnalysisProbeRun:
    def test_returns_processor_result(self) -> None:
        proc = AnalysisProbeProcessor()
        result = proc.run(b"hello", {})

        assert isinstance(result, ProcessorResult)
        assert result.metrics is not None

    def test_emits_all_expected_metrics(self) -> None:
        proc = AnalysisProbeProcessor()
        result = proc.run(b"x" * 100, {})

        assert result.metrics is not None
        expected = {"score", "passed", "event_count", "intensity", "center_xy"}
        assert set(result.metrics) == expected

    def test_score_is_deterministic_in_unit_range(self) -> None:
        proc = AnalysisProbeProcessor()
        n = 100
        result = proc.run(b"a" * n, {})
        assert result.metrics is not None
        expected = (n % 1000) / 1000.0
        score: Measurement = result.metrics["score"]
        assert score.value == pytest.approx(expected)
        assert 0.0 <= score.value <= 1.0

    def test_intensity_in_byte_range(self) -> None:
        proc = AnalysisProbeProcessor()
        result = proc.run(b"y" * 333, {})
        assert result.metrics is not None
        intensity = result.metrics["intensity"].value
        assert intensity == 333 % 256

    def test_center_xy_uses_sin_cos(self) -> None:
        proc = AnalysisProbeProcessor()
        n = 42
        result = proc.run(b"q" * n, {})
        assert result.metrics is not None
        xy = result.metrics["center_xy"].value
        assert xy["x"] == pytest.approx(math.sin(n) * 10.0)
        assert xy["y"] == pytest.approx(math.cos(n) * 10.0)

    def test_passed_true_when_score_above_threshold(self) -> None:
        # n=750 -> score=0.750, default threshold=0.5 -> True
        proc = AnalysisProbeProcessor()
        result = proc.run(b"z" * 750, {})
        assert result.metrics is not None
        assert result.metrics["passed"].value is True

    def test_passed_false_when_score_at_or_below_threshold(self) -> None:
        # n=200 -> score=0.2, default threshold=0.5 -> False
        proc = AnalysisProbeProcessor()
        result = proc.run(b"z" * 200, {})
        assert result.metrics is not None
        assert result.metrics["passed"].value is False

    def test_pass_threshold_setting_respected(self) -> None:
        # score=0.2 with threshold=0.1 should pass
        proc = AnalysisProbeProcessor()
        result = proc.run(b"z" * 200, {"pass_threshold": 0.1})
        assert result.metrics is not None
        assert result.metrics["passed"].value is True

    def test_event_count_default_is_one(self) -> None:
        proc = AnalysisProbeProcessor()
        result = proc.run(b"a", {})
        assert result.metrics is not None
        assert result.metrics["event_count"].value == 1

    def test_event_count_setting_respected(self) -> None:
        proc = AnalysisProbeProcessor()
        result = proc.run(b"a", {"event_count": 7})
        assert result.metrics is not None
        assert result.metrics["event_count"].value == 7

    def test_aggregation_flags_match_spec(self) -> None:
        proc = AnalysisProbeProcessor()
        result = proc.run(b"abc", {})
        assert result.metrics is not None
        score_agg = result.metrics["score"].aggregation
        assert AggregationType.STATS in score_agg
        assert AggregationType.HISTOGRAM in score_agg
        assert AggregationType.OUTLIERS in score_agg

        passed_agg = result.metrics["passed"].aggregation
        assert AggregationType.TALLY in passed_agg
        assert AggregationType.RATE in passed_agg

        center_agg = result.metrics["center_xy"].aggregation
        assert AggregationType.SCATTER_ELLIPSE in center_agg
        assert AggregationType.DENSITY_MAP in center_agg

    def test_produces_json_artifact(self) -> None:
        proc = AnalysisProbeProcessor()
        result = proc.run(b"xxxxx", {})
        assert len(result.artifacts) == 1
        artifact = result.artifacts[0]
        assert artifact.name == "analysis_report.json"
        assert artifact.mime == "application/json"
        assert artifact.data is not None

        payload = json.loads(artifact.data.decode("utf-8"))
        assert payload["processor"] == "analysis_probe"
        assert payload["version"] == "1.0.0"
        assert payload["input_size_bytes"] == 5
        assert "score" in payload
        assert "center" in payload

    def test_empty_input_still_produces_metrics(self) -> None:
        proc = AnalysisProbeProcessor()
        result = proc.run(b"", {})
        assert result.metrics is not None
        assert result.metrics["score"].value == 0.0
        assert result.metrics["intensity"].value == 0
