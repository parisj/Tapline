"""Unit tests for ArtifactComputer service.

Tests for:
- Histogram computation
- Category counting
- Rate calculation with Wilson CI
- Ellipse computation
- Contour computation
"""

from __future__ import annotations

import pytest

from src.visualization.services.artifact_computer import ArtifactComputer


class TestComputeHistogram:
    """Tests for histogram computation."""

    def test_compute_histogram_with_valid_values(self) -> None:
        computer = ArtifactComputer()
        values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]

        result = computer.compute_histogram(values, bins=5)

        assert result is not None
        assert len(result["counts"]) == 5
        assert len(result["edges"]) == 6
        assert result["bins"] == 5
        assert sum(result["counts"]) == 10

    def test_compute_histogram_with_default_bins(self) -> None:
        computer = ArtifactComputer(default_histogram_bins=20)
        values = list(range(100))

        result = computer.compute_histogram(values)

        assert result is not None
        assert result["bins"] == 20

    def test_compute_histogram_with_single_value_returns_none(self) -> None:
        computer = ArtifactComputer()

        result = computer.compute_histogram([1.0])

        assert result is None

    def test_compute_histogram_with_empty_list_returns_none(self) -> None:
        computer = ArtifactComputer()

        result = computer.compute_histogram([])

        assert result is None

    def test_compute_histogram_filters_non_numeric(self) -> None:
        computer = ArtifactComputer()
        values = [1.0, "text", 2.0, None, 3.0, [], {}]

        result = computer.compute_histogram(values, bins=3)

        assert result is not None
        assert sum(result["counts"]) == 3

    def test_compute_histogram_excludes_booleans(self) -> None:
        computer = ArtifactComputer()
        values = [1.0, True, 2.0, False, 3.0]

        result = computer.compute_histogram(values, bins=3)

        assert result is not None
        assert sum(result["counts"]) == 3  # Only 1.0, 2.0, 3.0

    def test_compute_histogram_handles_integers(self) -> None:
        computer = ArtifactComputer()
        values = [1, 2, 3, 4, 5]

        result = computer.compute_histogram(values, bins=3)

        assert result is not None
        assert sum(result["counts"]) == 5


class TestComputeCategories:
    """Tests for category counting."""

    def test_compute_categories_with_strings(self) -> None:
        computer = ArtifactComputer()
        values = ["a", "b", "a", "c", "a", "b"]

        result = computer.compute_categories(values)

        assert result is not None
        assert result["a"] == 3
        assert result["b"] == 2
        assert result["c"] == 1

    def test_compute_categories_with_booleans(self) -> None:
        computer = ArtifactComputer()
        values = [True, False, True, True, False]

        result = computer.compute_categories(values)

        assert result is not None
        assert result["true"] == 3
        assert result["false"] == 2

    def test_compute_categories_with_01_as_boolean(self) -> None:
        computer = ArtifactComputer()
        values = [1, 0, 1, 0, 1]

        result = computer.compute_categories(values)

        assert result is not None
        assert result["true"] == 3
        assert result["false"] == 2

    def test_compute_categories_with_numeric_non_boolean(self) -> None:
        computer = ArtifactComputer()
        values = [1, 2, 3, 2, 1]

        result = computer.compute_categories(values)

        assert result is not None
        assert result["true"] == 2  # 1s become "true"
        assert result["2"] == 2
        assert result["3"] == 1

    def test_compute_categories_with_empty_list(self) -> None:
        computer = ArtifactComputer()

        result = computer.compute_categories([])

        assert result is None

    def test_compute_categories_filters_none(self) -> None:
        computer = ArtifactComputer()
        values = ["a", None, "b", None, "a"]

        result = computer.compute_categories(values)

        assert result is not None
        assert result["a"] == 2
        assert result["b"] == 1
        assert "None" not in result


class TestComputeRate:
    """Tests for rate computation with Wilson CI."""

    def test_compute_rate_with_booleans(self) -> None:
        computer = ArtifactComputer()
        values = [True, True, False, True, False]

        result = computer.compute_rate(values)

        assert result is not None
        assert result["value"] == pytest.approx(0.6)
        assert result["yes"] == 3
        assert result["no"] == 2
        assert result["n"] == 5
        assert 0 <= result["ci_lower"] <= result["value"]
        assert result["value"] <= result["ci_upper"] <= 1

    def test_compute_rate_with_01_integers(self) -> None:
        computer = ArtifactComputer()
        values = [1, 0, 1, 1, 0, 0, 1]

        result = computer.compute_rate(values)

        assert result is not None
        assert result["yes"] == 4
        assert result["no"] == 3

    def test_compute_rate_with_empty_list_returns_none(self) -> None:
        computer = ArtifactComputer()

        result = computer.compute_rate([])

        assert result is None

    def test_compute_rate_with_all_true(self) -> None:
        computer = ArtifactComputer()
        values = [True, True, True, True]

        result = computer.compute_rate(values)

        assert result is not None
        assert result["value"] == pytest.approx(1.0)
        assert result["ci_upper"] == pytest.approx(1.0)
        assert result["ci_lower"] < 1.0

    def test_compute_rate_with_all_false(self) -> None:
        computer = ArtifactComputer()
        values = [False, False, False, False]

        result = computer.compute_rate(values)

        assert result is not None
        assert result["value"] == pytest.approx(0.0)
        assert result["ci_lower"] == pytest.approx(0.0)
        assert result["ci_upper"] > 0.0

    def test_compute_rate_filters_non_boolean(self) -> None:
        computer = ArtifactComputer()
        values = [True, "maybe", False, 42, None, True]

        result = computer.compute_rate(values)

        assert result is not None
        assert result["n"] == 3  # Only True, False, True
        assert result["yes"] == 2
        assert result["no"] == 1


class TestComputeEllipse:
    """Tests for ellipse computation."""

    def test_compute_ellipse_with_valid_points(self) -> None:
        computer = ArtifactComputer()
        points = [(1.0, 2.0), (2.0, 3.0), (3.0, 4.0), (4.0, 5.0)]

        result = computer.compute_ellipse(points)

        assert result is not None
        assert "center_x" in result
        assert "center_y" in result
        assert "semi_major" in result
        assert "semi_minor" in result
        assert "angle" in result
        assert "points" in result
        assert result["center_x"] == pytest.approx(2.5)
        assert result["center_y"] == pytest.approx(3.5)

    def test_compute_ellipse_with_dict_points(self) -> None:
        computer = ArtifactComputer()
        points = [{"x": 1.0, "y": 2.0}, {"x": 3.0, "y": 4.0}, {"x": 5.0, "y": 6.0}]

        result = computer.compute_ellipse(points)

        assert result is not None
        assert result["center_x"] == pytest.approx(3.0)
        assert result["center_y"] == pytest.approx(4.0)

    def test_compute_ellipse_with_list_points(self) -> None:
        computer = ArtifactComputer()
        points = [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]

        result = computer.compute_ellipse(points)

        assert result is not None
        assert result["center_x"] == pytest.approx(3.0)

    def test_compute_ellipse_with_single_point_returns_none(self) -> None:
        computer = ArtifactComputer()

        result = computer.compute_ellipse([(1.0, 2.0)])

        assert result is None

    def test_compute_ellipse_with_empty_list_returns_none(self) -> None:
        computer = ArtifactComputer()

        result = computer.compute_ellipse([])

        assert result is None

    def test_compute_ellipse_filters_invalid_points(self) -> None:
        computer = ArtifactComputer()
        points = [(1.0, 2.0), "invalid", (3.0, 4.0), None, (5.0, 6.0)]

        result = computer.compute_ellipse(points)

        assert result is not None
        assert len(result["points"]) == 3


class TestComputeContour:
    """Tests for contour computation."""

    def test_compute_contour_with_valid_points(self) -> None:
        computer = ArtifactComputer()
        # Generate a grid of points
        points = [(float(x), float(y)) for x in range(5) for y in range(5)]

        result = computer.compute_contour(points, grid_size=10)

        assert result is not None
        assert "density" in result
        assert "x_edges" in result
        assert "y_edges" in result
        assert "x_min" in result
        assert "x_max" in result
        assert "points" in result
        assert len(result["density"]) == 10
        assert len(result["density"][0]) == 10

    def test_compute_contour_with_default_grid_size(self) -> None:
        computer = ArtifactComputer(default_grid_size=20)
        points = [(float(x), float(y)) for x in range(10) for y in range(10)]

        result = computer.compute_contour(points)

        assert result is not None
        assert len(result["density"]) == 20

    def test_compute_contour_with_dict_points(self) -> None:
        computer = ArtifactComputer()
        points = [{"x": float(x), "y": float(y)} for x in range(5) for y in range(5)]

        result = computer.compute_contour(points, grid_size=5)

        assert result is not None
        assert len(result["points"]) == 25

    def test_compute_contour_with_few_points_returns_none(self) -> None:
        computer = ArtifactComputer()

        result = computer.compute_contour([(1.0, 2.0), (3.0, 4.0)])

        assert result is None

    def test_compute_contour_with_empty_list_returns_none(self) -> None:
        computer = ArtifactComputer()

        result = computer.compute_contour([])

        assert result is None

    def test_compute_contour_adds_padding(self) -> None:
        computer = ArtifactComputer()
        points = [(0.0, 0.0), (1.0, 1.0), (2.0, 2.0)]

        result = computer.compute_contour(points, grid_size=5)

        assert result is not None
        # Should have padding beyond the data range
        assert result["x_min"] < 0.0
        assert result["x_max"] > 2.0
        assert result["y_min"] < 0.0
        assert result["y_max"] > 2.0


class TestParse2DPoints:
    """Tests for _parse_2d_points helper."""

    def test_parse_tuple_points(self) -> None:
        computer = ArtifactComputer()

        result = computer._parse_2d_points([(1.0, 2.0), (3.0, 4.0)])

        assert result == [(1.0, 2.0), (3.0, 4.0)]

    def test_parse_list_points(self) -> None:
        computer = ArtifactComputer()

        result = computer._parse_2d_points([[1.0, 2.0], [3.0, 4.0]])

        assert result == [(1.0, 2.0), (3.0, 4.0)]

    def test_parse_dict_points(self) -> None:
        computer = ArtifactComputer()

        result = computer._parse_2d_points([{"x": 1.0, "y": 2.0}, {"x": 3.0, "y": 4.0}])

        assert result == [(1.0, 2.0), (3.0, 4.0)]

    def test_parse_mixed_points(self) -> None:
        computer = ArtifactComputer()

        result = computer._parse_2d_points([(1.0, 2.0), {"x": 3.0, "y": 4.0}, [5.0, 6.0]])

        assert result == [(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]

    def test_parse_filters_invalid(self) -> None:
        computer = ArtifactComputer()

        result = computer._parse_2d_points(
            [(1.0, 2.0), "invalid", (3.0,), {"x": 4.0}, {"y": 5.0}, None, (6.0, 7.0)],
        )

        assert result == [(1.0, 2.0), (6.0, 7.0)]

    def test_parse_handles_string_conversion_errors(self) -> None:
        computer = ArtifactComputer()

        result = computer._parse_2d_points([{"x": "not_a_number", "y": 2.0}, (1.0, 2.0)])

        assert result == [(1.0, 2.0)]
