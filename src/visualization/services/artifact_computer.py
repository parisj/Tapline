"""Artifact computation service.

Computes derived artifacts (histograms, categories, rates, ellipses, contours)
from raw metric values. Extracted from api_server.py for better testability
and separation of concerns.
"""

from __future__ import annotations

import logging
import math
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)


class ArtifactComputer:
    """Computes derived artifacts from raw metric values."""

    def __init__(self, default_histogram_bins: int = 30, default_grid_size: int = 50) -> None:
        """Initialize artifact computer.

        Args:
            default_histogram_bins: Default number of bins for histograms.
            default_grid_size: Default grid size for contour plots.

        """
        self.default_histogram_bins = default_histogram_bins
        self.default_grid_size = default_grid_size

    def compute_histogram(self, values: list[Any], bins: int | None = None) -> dict[str, Any] | None:
        """Compute histogram from raw values.

        Args:
            values: List of numeric values.
            bins: Number of histogram bins (uses default if not specified).

        Returns:
            Dictionary with counts, edges, and bins, or None if insufficient data.

        """
        if bins is None:
            bins = self.default_histogram_bins

        try:
            nums = [v for v in values if isinstance(v, (int, float)) and not isinstance(v, bool)]
            if len(nums) < 2:
                return None

            arr = np.array(nums)
            counts, edges = np.histogram(arr, bins=bins)
            return {
                "counts": counts.tolist(),
                "bin_edges": edges.tolist(),
                "bins": bins,
            }
        except (ValueError, TypeError) as e:
            logger.debug("Failed to compute histogram: %s", e)
            return None

    def compute_categories(self, values: list[Any]) -> dict[str, int] | None:
        """Compute category counts from values.

        Args:
            values: List of categorical values (strings, bools, or small integers).

        Returns:
            Dictionary mapping category names to counts, or None if empty.

        """
        try:
            categories: dict[str, int] = {}
            for v in values:
                if v is None:
                    continue
                if isinstance(v, bool):
                    key = "true" if v else "false"
                elif isinstance(v, (int, float)):
                    # For numeric values, treat as boolean if 0/1
                    key = ("true" if v else "false") if v in (0, 1, 0.0, 1.0) else str(v)
                else:
                    key = str(v)
                categories[key] = categories.get(key, 0) + 1

            return categories if categories else None
        except (ValueError, TypeError) as e:
            logger.debug("Failed to compute categories: %s", e)
            return None

    def compute_rate(self, values: list[Any], confidence: float = 0.95) -> dict[str, Any] | None:
        """Compute rate with Wilson confidence interval.

        Args:
            values: List of boolean-like values (bool, 0/1).
            confidence: Confidence level for interval (default 0.95).

        Returns:
            Dictionary with rate, confidence interval, and counts, or None if empty.

        """
        try:
            yes = 0
            n = 0

            for v in values:
                if v is None:
                    continue
                if isinstance(v, bool) or (isinstance(v, (int, float)) and v in (0, 1, 0.0, 1.0)):
                    n += 1
                    if v:
                        yes += 1

            if n == 0:
                return None

            rate = yes / n
            # Wilson confidence interval
            z = self._z_score_for_confidence(confidence)
            denom = 1.0 + (z * z) / n
            center = (rate + (z * z) / (2.0 * n)) / denom
            margin = (z / denom) * math.sqrt((rate * (1.0 - rate) / n) + (z * z) / (4.0 * n * n))

            return {
                "value": rate,
                "ci_lower": max(0.0, center - margin),
                "ci_upper": min(1.0, center + margin),
                "yes": yes,
                "no": n - yes,
                "n": n,
            }
        except (ValueError, ZeroDivisionError) as e:
            logger.debug("Failed to compute rate: %s", e)
            return None

    def compute_ellipse(self, values: list[Any]) -> dict[str, Any] | None:
        """Compute covariance ellipse parameters from 2D point values.

        Args:
            values: List of 2D points (dicts with x/y or tuples/lists of length 2).

        Returns:
            Dictionary with ellipse parameters and points, or None if insufficient data.

        """
        try:
            pts = self._parse_2d_points(values)
            if len(pts) < 2:
                return None

            arr = np.array(pts)
            mx, my = arr.mean(axis=0)
            cov = np.cov(arr.T)

            # Eigendecomposition for ellipse parameters
            vals, vecs = np.linalg.eigh(cov)
            order = np.argsort(vals)[::-1]
            vals = vals[order]
            vecs = vecs[:, order]

            angle = math.atan2(vecs[1, 0], vecs[0, 0])
            semi_major = math.sqrt(max(vals[0], 0)) * 2  # 2 sigma
            semi_minor = math.sqrt(max(vals[1], 0)) * 2

            return {
                "center_x": float(mx),
                "center_y": float(my),
                "semi_major": float(semi_major),
                "semi_minor": float(semi_minor),
                "angle": float(math.degrees(angle)),
                "points": arr.tolist(),
            }
        except (ValueError, np.linalg.LinAlgError) as e:
            logger.debug("Failed to compute ellipse: %s", e)
            return None

    def compute_contour(self, values: list[Any], grid_size: int | None = None) -> dict[str, Any] | None:
        """Compute 2D density grid for contour plotting from 2D point values.

        Args:
            values: List of 2D points (dicts with x/y or tuples/lists of length 2).
            grid_size: Size of the density grid (uses default if not specified).

        Returns:
            Dictionary with density grid and metadata, or None if insufficient data.

        """
        if grid_size is None:
            grid_size = self.default_grid_size

        try:
            pts = self._parse_2d_points(values)
            if len(pts) < 3:
                return None

            arr = np.array(pts)
            x_vals = arr[:, 0]
            y_vals = arr[:, 1]

            # Create grid
            x_min, x_max = float(x_vals.min()), float(x_vals.max())
            y_min, y_max = float(y_vals.min()), float(y_vals.max())

            # Add some padding
            x_pad = (x_max - x_min) * 0.1 or 1.0
            y_pad = (y_max - y_min) * 0.1 or 1.0
            x_min -= x_pad
            x_max += x_pad
            y_min -= y_pad
            y_max += y_pad

            x_edges = np.linspace(x_min, x_max, grid_size + 1)
            y_edges = np.linspace(y_min, y_max, grid_size + 1)

            # Compute 2D histogram as density grid
            density, _, _ = np.histogram2d(x_vals, y_vals, bins=[x_edges, y_edges])

            return {
                "density": density.T.tolist(),  # Transpose for proper orientation
                "x_edges": x_edges.tolist(),
                "y_edges": y_edges.tolist(),
                "x_min": x_min,
                "x_max": x_max,
                "y_min": y_min,
                "y_max": y_max,
                "points": arr.tolist(),
            }
        except (ValueError, RuntimeError) as e:
            logger.debug("Failed to compute contour: %s", e)
            return None

    def _parse_2d_points(self, values: list[Any]) -> list[tuple[float, float]]:
        """Parse 2D points from various formats.

        Args:
            values: List of potential 2D points.

        Returns:
            List of (x, y) tuples.

        """
        pts: list[tuple[float, float]] = []
        for v in values:
            if isinstance(v, dict) and "x" in v and "y" in v:
                try:
                    pts.append((float(v["x"]), float(v["y"])))
                except (TypeError, ValueError):
                    continue
            elif isinstance(v, (list, tuple)) and len(v) == 2:
                try:
                    pts.append((float(v[0]), float(v[1])))
                except (TypeError, ValueError):
                    continue
        return pts

    def _z_score_for_confidence(self, confidence: float) -> float:
        """Get z-score for confidence level.

        Args:
            confidence: Confidence level (e.g., 0.95 for 95%).

        Returns:
            Corresponding z-score.

        """
        # Common z-scores for Wilson interval
        if confidence >= 0.99:
            return 2.576
        if confidence >= 0.975:
            return 1.96
        if confidence >= 0.95:
            return 1.96
        if confidence >= 0.90:
            return 1.645
        return 1.28  # ~80%
