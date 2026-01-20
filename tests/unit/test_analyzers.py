"""Unit tests for evaluation analyzers.

Tests all analyzer implementations for:
- Correct statistical calculations
- Edge cases (empty input, all missing, single value)
- Artifact generation
- Meta parameter handling
"""

from __future__ import annotations

import io

import numpy as np
import pytest

from src.evaluation.analyzers.contour2d import ContourAnalyzer
from src.evaluation.analyzers.counter import CounterAnalyzer
from src.evaluation.analyzers.ellipse2d import CovEllipseAnalyzer
from src.evaluation.analyzers.histogram import HistogramAnalyzer
from src.evaluation.analyzers.info import InfoAnalyzer
from src.evaluation.analyzers.outliers1d import OutliersAnalyzer
from src.evaluation.analyzers.rate import RateAnalyzer, _parse_bool, _wilson_interval
from src.evaluation.analyzers.summary import SummaryAnalyzer

# =============================================================================
# SummaryAnalyzer Tests
# =============================================================================


class TestSummaryAnalyzer:
    """Tests for SummaryAnalyzer."""

    def test_run_with_valid_numeric_values(self) -> None:
        analyzer = SummaryAnalyzer()
        result = analyzer.run(values=[1.0, 2.0, 3.0, 4.0, 5.0], meta={})

        summary = result["summary"]
        assert summary["count"] == 5
        assert summary["missing"] == 0
        assert summary["mean"] == pytest.approx(3.0)
        assert summary["median"] == pytest.approx(3.0)
        assert summary["min"] == pytest.approx(1.0)
        assert summary["max"] == pytest.approx(5.0)
        assert summary["std"] > 0
        assert summary["variance"] > 0
        assert result["artifact"] is not None

    def test_run_with_empty_list_returns_zeros(self) -> None:
        analyzer = SummaryAnalyzer()
        result = analyzer.run(values=[], meta={})

        summary = result["summary"]
        assert summary["count"] == 0
        assert summary["missing"] == 0
        assert summary["mean"] == 0
        assert summary["median"] == 0
        assert result["artifact"] is None

    def test_run_filters_none_values(self) -> None:
        analyzer = SummaryAnalyzer()
        result = analyzer.run(values=[1.0, None, 2.0, None, 3.0], meta={})

        summary = result["summary"]
        assert summary["count"] == 3
        assert summary["missing"] == 2
        assert summary["mean"] == pytest.approx(2.0)

    def test_run_filters_boolean_values(self) -> None:
        analyzer = SummaryAnalyzer()
        result = analyzer.run(values=[1.0, True, 2.0, False, 3.0], meta={})

        summary = result["summary"]
        assert summary["count"] == 3
        assert summary["missing"] == 2

    def test_run_filters_non_finite_values(self) -> None:
        analyzer = SummaryAnalyzer()
        result = analyzer.run(values=[1.0, float("inf"), 2.0, float("-inf"), float("nan"), 3.0], meta={})

        summary = result["summary"]
        assert summary["count"] == 3
        assert summary["missing"] == 3

    def test_run_single_value_has_zero_variance(self) -> None:
        analyzer = SummaryAnalyzer()
        result = analyzer.run(values=[42.0], meta={})

        summary = result["summary"]
        assert summary["count"] == 1
        assert summary["std"] == 0.0
        assert summary["variance"] == 0.0

    def test_run_handles_integers(self) -> None:
        analyzer = SummaryAnalyzer()
        result = analyzer.run(values=[1, 2, 3, 4, 5], meta={})

        summary = result["summary"]
        assert summary["count"] == 5
        assert summary["mean"] == pytest.approx(3.0)

    def test_run_all_missing_returns_zeros(self) -> None:
        analyzer = SummaryAnalyzer()
        result = analyzer.run(values=[None, None, None], meta={})

        summary = result["summary"]
        assert summary["count"] == 0
        assert result["artifact"] is None

    def test_artifact_contains_valid_npz(self) -> None:
        analyzer = SummaryAnalyzer()
        result = analyzer.run(values=[1.0, 2.0, 3.0], meta={})

        artifact = result["artifact"]
        assert artifact is not None
        assert artifact.name == "summary.npz"

        # Verify NPZ content
        buf = io.BytesIO(artifact.data)
        data = np.load(buf)
        assert "value" in data
        np.testing.assert_array_equal(data["value"], [1.0, 2.0, 3.0])


# =============================================================================
# HistogramAnalyzer Tests
# =============================================================================


class TestHistogramAnalyzer:
    """Tests for HistogramAnalyzer."""

    def test_run_with_valid_values(self) -> None:
        analyzer = HistogramAnalyzer()
        result = analyzer.run(values=[1.0, 2.0, 3.0, 4.0, 5.0], meta={})

        summary = result["summary"]
        assert summary["count"] == 5
        assert summary["bins"] == 50  # default
        assert "mean" in summary
        assert "std" in summary
        assert result["artifact"] is not None

    def test_run_with_empty_list(self) -> None:
        analyzer = HistogramAnalyzer()
        result = analyzer.run(values=[], meta={})

        summary = result["summary"]
        assert summary["count"] == 0
        assert "artifact" not in result or result.get("artifact") is None

    def test_run_with_custom_bins(self) -> None:
        analyzer = HistogramAnalyzer()
        result = analyzer.run(values=[1.0, 2.0, 3.0, 4.0, 5.0], meta={"bins": 10})

        summary = result["summary"]
        assert summary["bins"] == 10

    def test_run_filters_non_numeric(self) -> None:
        analyzer = HistogramAnalyzer()
        result = analyzer.run(values=[1.0, "text", 2.0, None, 3.0], meta={})

        summary = result["summary"]
        assert summary["count"] == 3
        assert summary["missing"] == 2

    def test_run_single_value(self) -> None:
        analyzer = HistogramAnalyzer()
        result = analyzer.run(values=[42.0], meta={})

        summary = result["summary"]
        assert summary["count"] == 1
        assert summary["std"] == 0.0

    def test_artifact_contains_edges_and_counts(self) -> None:
        analyzer = HistogramAnalyzer()
        result = analyzer.run(values=[1.0, 2.0, 3.0, 4.0, 5.0], meta={"bins": 5})

        artifact = result["artifact"]
        buf = io.BytesIO(artifact.data)
        data = np.load(buf)
        assert "edges" in data
        assert "counts" in data
        assert len(data["edges"]) == 6  # bins + 1


# =============================================================================
# CounterAnalyzer Tests
# =============================================================================


class TestCounterAnalyzer:
    """Tests for CounterAnalyzer."""

    def test_run_with_categorical_values(self) -> None:
        analyzer = CounterAnalyzer()
        result = analyzer.run(values=["a", "b", "a", "c", "a", "b"], meta={})

        summary = result["summary"]
        assert summary["count"] == 6
        assert summary["unique"] == 3
        assert summary["missing"] == 0
        assert result["artifact"] is not None

    def test_run_with_empty_list(self) -> None:
        analyzer = CounterAnalyzer()
        result = analyzer.run(values=[], meta={})

        summary = result["summary"]
        assert summary["count"] == 0
        assert summary["unique"] == 0
        assert result["artifact"] is None

    def test_run_filters_none_values(self) -> None:
        analyzer = CounterAnalyzer()
        result = analyzer.run(values=["a", None, "b", None], meta={})

        summary = result["summary"]
        assert summary["count"] == 2
        assert summary["missing"] == 2

    def test_run_with_top_k(self) -> None:
        analyzer = CounterAnalyzer()
        values = ["a"] * 10 + ["b"] * 5 + ["c"] * 3 + ["d"] * 1
        result = analyzer.run(values=values, meta={"top_k": 2})

        summary = result["summary"]
        assert summary["top_k"] == 2
        # Check artifact only has top 2
        buf = io.BytesIO(result["artifact"].data)
        data = np.load(buf, allow_pickle=True)
        assert len(data["labels"]) == 2

    def test_run_with_sort_by_key(self) -> None:
        analyzer = CounterAnalyzer()
        result = analyzer.run(values=["c", "a", "b"], meta={"sort": "key"})

        summary = result["summary"]
        assert summary["sort"] == "key"
        # Check alphabetical order
        buf = io.BytesIO(result["artifact"].data)
        data = np.load(buf, allow_pickle=True)
        assert list(data["labels"]) == ["a", "b", "c"]

    def test_run_converts_values_to_string(self) -> None:
        analyzer = CounterAnalyzer()
        result = analyzer.run(values=[1, 2, 1, "1"], meta={})

        summary = result["summary"]
        # "1" and 1 become same key
        assert summary["unique"] == 2  # "1" and "2"

    def test_all_none_values(self) -> None:
        analyzer = CounterAnalyzer()
        result = analyzer.run(values=[None, None, None], meta={})

        summary = result["summary"]
        assert summary["count"] == 0
        assert summary["missing"] == 3
        assert result["artifact"] is None


# =============================================================================
# RateAnalyzer Tests
# =============================================================================


class TestRateAnalyzer:
    """Tests for RateAnalyzer."""

    def test_run_with_boolean_values(self) -> None:
        analyzer = RateAnalyzer()
        result = analyzer.run(values=[True, True, False, True, False], meta={})

        summary = result["summary"]
        assert summary["yes"] == 3
        assert summary["no"] == 2
        assert summary["rate"] == pytest.approx(0.6)
        assert summary["missing"] == 0
        assert "wilson_low" in summary
        assert "wilson_high" in summary

    def test_run_with_int_01_values(self) -> None:
        analyzer = RateAnalyzer()
        result = analyzer.run(values=[1, 0, 1, 1, 0], meta={})

        summary = result["summary"]
        assert summary["yes"] == 3
        assert summary["no"] == 2

    def test_run_with_string_values(self) -> None:
        analyzer = RateAnalyzer()
        result = analyzer.run(values=["yes", "no", "true", "false", "Y", "N"], meta={})

        summary = result["summary"]
        assert summary["yes"] == 3
        assert summary["no"] == 3

    def test_run_with_empty_list(self) -> None:
        analyzer = RateAnalyzer()
        result = analyzer.run(values=[], meta={})

        summary = result["summary"]
        assert summary["count"] == 0
        assert summary["rate"] is None

    def test_run_filters_invalid_values(self) -> None:
        analyzer = RateAnalyzer()
        result = analyzer.run(values=[True, "invalid", False, 42, None], meta={})

        summary = result["summary"]
        assert summary["yes"] == 1
        assert summary["no"] == 1
        assert summary["missing"] == 3

    def test_run_with_custom_confidence(self) -> None:
        analyzer = RateAnalyzer()
        result_95 = analyzer.run(values=[True] * 80 + [False] * 20, meta={"confidence": 0.95})
        result_99 = analyzer.run(values=[True] * 80 + [False] * 20, meta={"confidence": 0.99})

        # 99% CI should be wider
        width_95 = result_95["summary"]["wilson_high"] - result_95["summary"]["wilson_low"]
        width_99 = result_99["summary"]["wilson_high"] - result_99["summary"]["wilson_low"]
        assert width_99 > width_95

    def test_run_all_yes(self) -> None:
        analyzer = RateAnalyzer()
        result = analyzer.run(values=[True, True, True], meta={})

        summary = result["summary"]
        assert summary["rate"] == pytest.approx(1.0)
        assert summary["wilson_high"] == pytest.approx(1.0)

    def test_run_all_no(self) -> None:
        analyzer = RateAnalyzer()
        result = analyzer.run(values=[False, False, False], meta={})

        summary = result["summary"]
        assert summary["rate"] == pytest.approx(0.0)
        assert summary["wilson_low"] == pytest.approx(0.0)


class TestParseBool:
    """Tests for _parse_bool helper function."""

    def test_parse_bool_true_values(self) -> None:
        assert _parse_bool(True) is True  # noqa: FBT003
        assert _parse_bool(1) is True
        assert _parse_bool("true") is True
        assert _parse_bool("TRUE") is True
        assert _parse_bool("yes") is True
        assert _parse_bool("Y") is True
        assert _parse_bool("t") is True
        assert _parse_bool("1") is True
        assert _parse_bool("ok") is True
        assert _parse_bool("pass") is True

    def test_parse_bool_false_values(self) -> None:
        assert _parse_bool(False) is False  # noqa: FBT003
        assert _parse_bool(0) is False
        assert _parse_bool("false") is False
        assert _parse_bool("FALSE") is False
        assert _parse_bool("no") is False
        assert _parse_bool("N") is False
        assert _parse_bool("f") is False
        assert _parse_bool("0") is False
        assert _parse_bool("fail") is False

    def test_parse_bool_none_for_invalid(self) -> None:
        assert _parse_bool(None) is None
        assert _parse_bool(42) is None
        assert _parse_bool("maybe") is None
        assert _parse_bool([]) is None
        assert _parse_bool({}) is None


class TestWilsonInterval:
    """Tests for _wilson_interval helper function."""

    def test_wilson_interval_50_percent(self) -> None:
        low, high = _wilson_interval(yes=50, n=100, confidence=0.95)
        assert 0.35 < low < 0.45
        assert 0.55 < high < 0.65

    def test_wilson_interval_all_yes(self) -> None:
        low, high = _wilson_interval(yes=100, n=100, confidence=0.95)
        assert low > 0.9
        assert high == pytest.approx(1.0)

    def test_wilson_interval_all_no(self) -> None:
        low, high = _wilson_interval(yes=0, n=100, confidence=0.95)
        assert low == pytest.approx(0.0)
        assert high < 0.1


# =============================================================================
# CovEllipseAnalyzer Tests
# =============================================================================


class TestCovEllipseAnalyzer:
    """Tests for CovEllipseAnalyzer (2D covariance ellipse)."""

    def test_run_with_valid_points(self) -> None:
        analyzer = CovEllipseAnalyzer()
        points = [(1.0, 2.0), (2.0, 3.0), (3.0, 4.0), (4.0, 5.0)]
        result = analyzer.run(values=points, meta={})

        summary = result["summary"]
        assert summary["count"] == 4
        assert summary["missing"] == 0
        assert "lambda1" in summary
        assert "lambda2" in summary
        assert "angle_rad" in summary
        assert result["artifact"] is not None

    def test_run_with_empty_list(self) -> None:
        analyzer = CovEllipseAnalyzer()
        result = analyzer.run(values=[], meta={})

        summary = result["summary"]
        assert summary["count"] == 0
        assert result["artifact"] is None

    def test_run_filters_invalid_points(self) -> None:
        analyzer = CovEllipseAnalyzer()
        points = [(1.0, 2.0), None, (3.0, 4.0), "invalid", (5.0, 6.0)]
        result = analyzer.run(values=points, meta={})

        summary = result["summary"]
        assert summary["count"] == 3
        assert summary["missing"] == 2

    def test_run_with_dict_points_xy(self) -> None:
        analyzer = CovEllipseAnalyzer()
        points = [{"x": 1.0, "y": 2.0}, {"x": 3.0, "y": 4.0}]
        result = analyzer.run(values=points, meta={})

        summary = result["summary"]
        assert summary["count"] == 2

    def test_run_with_dict_points_nested(self) -> None:
        analyzer = CovEllipseAnalyzer()
        points = [{"xy": [1.0, 2.0]}, {"xy": [3.0, 4.0]}]
        result = analyzer.run(values=points, meta={})

        summary = result["summary"]
        assert summary["count"] == 2

    def test_run_with_custom_sigmas(self) -> None:
        analyzer = CovEllipseAnalyzer()
        points = [(1.0, 2.0), (2.0, 3.0), (3.0, 4.0)]
        result = analyzer.run(values=points, meta={"sigmas": [1.0, 2.0, 3.0]})

        summary = result["summary"]
        assert summary["sigmas"] == [1.0, 2.0, 3.0]

    def test_run_with_single_point_degenerate(self) -> None:
        analyzer = CovEllipseAnalyzer()
        result = analyzer.run(values=[(1.0, 2.0)], meta={"allow_degenerate": True})

        summary = result["summary"]
        assert summary["count"] == 1
        # Degenerate case should still produce artifact
        assert result["artifact"] is not None

    def test_run_filters_non_finite_coordinates(self) -> None:
        analyzer = CovEllipseAnalyzer()
        points = [(1.0, 2.0), (float("inf"), 3.0), (4.0, float("nan"))]
        result = analyzer.run(values=points, meta={})

        summary = result["summary"]
        assert summary["count"] == 1
        assert summary["missing"] == 2


# =============================================================================
# ContourAnalyzer Tests
# =============================================================================


class TestContourAnalyzer:
    """Tests for ContourAnalyzer (2D density grid)."""

    def test_run_with_valid_points(self) -> None:
        analyzer = ContourAnalyzer()
        # Generate points in a grid pattern
        points = [(float(x), float(y)) for x in range(10) for y in range(10)]
        result = analyzer.run(values=points, meta={})

        summary = result["summary"]
        assert summary["count"] == 100
        assert summary["missing"] == 0
        assert summary["grid_size"] == 64  # default
        assert result["artifact"] is not None

    def test_run_with_empty_list(self) -> None:
        analyzer = ContourAnalyzer()
        result = analyzer.run(values=[], meta={})

        summary = result["summary"]
        assert summary["count"] == 0
        assert result["artifact"] is None

    def test_run_with_custom_grid_size(self) -> None:
        analyzer = ContourAnalyzer()
        points = [(float(x), float(y)) for x in range(10) for y in range(10)]
        result = analyzer.run(values=points, meta={"grid_size": 32})

        summary = result["summary"]
        assert summary["grid_size"] == 32

    def test_run_with_custom_range(self) -> None:
        analyzer = ContourAnalyzer()
        points = [(1.0, 1.0), (2.0, 2.0)]
        result = analyzer.run(values=points, meta={"range_x": [0, 10], "range_y": [0, 10]})

        summary = result["summary"]
        assert summary["xmin"] == 0
        assert summary["xmax"] == 10
        assert summary["ymin"] == 0
        assert summary["ymax"] == 10

    def test_run_with_degenerate_range_returns_no_artifact(self) -> None:
        analyzer = ContourAnalyzer()
        # All points at same location
        points = [(5.0, 5.0), (5.0, 5.0), (5.0, 5.0)]
        result = analyzer.run(values=points, meta={})

        summary = result["summary"]
        assert summary.get("degenerate") is True
        assert result["artifact"] is None

    def test_run_filters_invalid_points(self) -> None:
        analyzer = ContourAnalyzer()
        points = [(1.0, 2.0), None, "invalid", {"x": 3.0, "y": 4.0}]
        result = analyzer.run(values=points, meta={})

        summary = result["summary"]
        assert summary["count"] == 2  # tuple and dict
        assert summary["missing"] == 2

    def test_artifact_contains_grid_data(self) -> None:
        analyzer = ContourAnalyzer()
        points = [(float(x), float(y)) for x in range(5) for y in range(5)]
        result = analyzer.run(values=points, meta={"grid_size": 16})

        buf = io.BytesIO(result["artifact"].data)
        data = np.load(buf)
        assert "x" in data
        assert "y" in data
        assert "z" in data
        assert "levels" in data
        assert data["z"].shape == (16, 16)


# =============================================================================
# InfoAnalyzer Tests
# =============================================================================


class TestInfoAnalyzer:
    """Tests for InfoAnalyzer."""

    def test_run_returns_meta_as_info(self) -> None:
        analyzer = InfoAnalyzer()
        meta = {"key1": "value1", "key2": 42}
        result = analyzer.run(values=["anything"], meta=meta)

        assert result["summary"]["info"] == meta
        assert result["artifact"] is None

    def test_run_with_empty_meta(self) -> None:
        analyzer = InfoAnalyzer()
        result = analyzer.run(values=[], meta={})

        assert result["summary"]["info"] == {}
        assert result["artifact"] is None

    def test_run_ignores_values(self) -> None:
        analyzer = InfoAnalyzer()
        # Values are ignored by InfoAnalyzer
        result = analyzer.run(values=[1, 2, 3, None, "test"], meta={"description": "test"})

        assert result["summary"]["info"] == {"description": "test"}


# =============================================================================
# OutliersAnalyzer Tests
# =============================================================================


class TestOutliersAnalyzer:
    """Tests for OutliersAnalyzer (IQR-based outlier detection)."""

    def test_run_with_normal_distribution_no_outliers(self) -> None:
        analyzer = OutliersAnalyzer()
        # Values within normal range
        values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        result = analyzer.run(values=values, meta={})

        summary = result["summary"]
        assert summary["count"] == 10
        assert summary["missing"] == 0
        assert summary["outlier_count"] == 0
        assert result["artifact"] is not None

    def test_run_detects_outliers(self) -> None:
        analyzer = OutliersAnalyzer()
        # Values with clear outliers
        values = [1.0, 2.0, 3.0, 4.0, 5.0, 100.0]  # 100 is an outlier
        result = analyzer.run(values=values, meta={})

        summary = result["summary"]
        assert summary["count"] == 6
        assert summary["outlier_count"] >= 1

    def test_run_with_empty_list(self) -> None:
        analyzer = OutliersAnalyzer()
        result = analyzer.run(values=[], meta={})

        summary = result["summary"]
        assert summary["count"] == 0
        assert summary.get("too_few_values") is True
        assert result["artifact"] is None

    def test_run_with_few_values_returns_too_few(self) -> None:
        analyzer = OutliersAnalyzer()
        result = analyzer.run(values=[1.0, 2.0, 3.0], meta={})

        summary = result["summary"]
        assert summary.get("too_few_values") is True
        assert result["artifact"] is None

    def test_run_filters_none_values(self) -> None:
        analyzer = OutliersAnalyzer()
        values = [1.0, None, 2.0, None, 3.0, 4.0, 5.0, 6.0]
        result = analyzer.run(values=values, meta={})

        summary = result["summary"]
        assert summary["count"] == 6
        assert summary["missing"] == 2

    def test_run_filters_boolean_values(self) -> None:
        analyzer = OutliersAnalyzer()
        values = [1.0, True, 2.0, False, 3.0, 4.0, 5.0, 6.0]
        result = analyzer.run(values=values, meta={})

        summary = result["summary"]
        assert summary["count"] == 6
        assert summary["missing"] == 2

    def test_run_filters_non_finite_values(self) -> None:
        analyzer = OutliersAnalyzer()
        values = [1.0, float("inf"), 2.0, float("-inf"), float("nan"), 3.0, 4.0, 5.0]
        result = analyzer.run(values=values, meta={})

        summary = result["summary"]
        assert summary["count"] == 5
        assert summary["missing"] == 3

    def test_run_with_custom_iqr_multiplier(self) -> None:
        analyzer = OutliersAnalyzer()
        values = [1.0, 2.0, 3.0, 4.0, 5.0, 10.0]

        # With default multiplier (1.5)
        result_default = analyzer.run(values=values, meta={})

        # With higher multiplier (3.0 - fewer outliers)
        result_high = analyzer.run(values=values, meta={"iqr_multiplier": 3.0})

        assert result_default["summary"]["iqr_multiplier"] == 1.5
        assert result_high["summary"]["iqr_multiplier"] == 3.0
        # Higher multiplier should find fewer or equal outliers
        assert result_high["summary"]["outlier_count"] <= result_default["summary"]["outlier_count"]

    def test_run_returns_iqr_bounds(self) -> None:
        analyzer = OutliersAnalyzer()
        values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        result = analyzer.run(values=values, meta={})

        summary = result["summary"]
        assert "q1" in summary
        assert "q3" in summary
        assert "iqr" in summary
        assert "lower_bound" in summary
        assert "upper_bound" in summary
        assert summary["q3"] > summary["q1"]
        assert summary["iqr"] == summary["q3"] - summary["q1"]

    def test_artifact_contains_outlier_data(self) -> None:
        analyzer = OutliersAnalyzer()
        values = [1.0, 2.0, 3.0, 4.0, 5.0, 100.0]  # 100 is outlier
        result = analyzer.run(values=values, meta={})

        artifact = result["artifact"]
        assert artifact is not None
        assert artifact.name == "outliers.npz"

        buf = io.BytesIO(artifact.data)
        data = np.load(buf)
        assert "values" in data
        assert "outlier_mask" in data
        assert "outlier_values" in data
        assert "outlier_indices" in data

    def test_run_handles_integers(self) -> None:
        analyzer = OutliersAnalyzer()
        values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        result = analyzer.run(values=values, meta={})

        summary = result["summary"]
        assert summary["count"] == 10

    def test_outlier_fraction_calculated(self) -> None:
        analyzer = OutliersAnalyzer()
        values = [1.0, 2.0, 3.0, 4.0, 5.0, 100.0]
        result = analyzer.run(values=values, meta={})

        summary = result["summary"]
        assert "outlier_fraction" in summary
        if summary["outlier_count"] > 0:
            assert summary["outlier_fraction"] == summary["outlier_count"] / summary["count"]
