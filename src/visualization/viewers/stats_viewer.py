"""Stats viewer for summary statistics NPZ artifacts.

Displays summary statistics (count, mean, median, etc.) from STATS aggregation.
"""

from __future__ import annotations

import io
from typing import Any

import numpy as np

from src.domain.evaluation import AggregationType
from src.visualization.viewers.base import ViewerResult


class StatsViewer:
    """Viewer for summary statistics NPZ artifacts.

    Handles artifacts with MIME type 'summary/x-npz' and STATS aggregation type.
    Produces stats display configuration with optional raw values.
    """

    @property
    def viewer_type(self) -> str:
        return "stats"

    def can_view(self, *, mime: str, aggregation_mask: int) -> bool:
        """Check if this is a stats artifact."""
        is_summary_npz = mime == "summary/x-npz"
        has_stats = bool(aggregation_mask & AggregationType.STATS.value)
        return is_summary_npz or (has_stats and mime == "application/x-npz")

    def view(
        self,
        *,
        data: bytes,
        mime: str,
        aggregation_mask: int,
        metadata: dict[str, Any],
    ) -> ViewerResult:
        """Render stats data for display."""
        try:
            buf = io.BytesIO(data)
            npz = np.load(buf)

            # Get raw values if present
            values = npz.get("value", npz.get("values"))

            stats_data: dict[str, Any] = {"metadata": metadata}

            if values is not None:
                values_list = values.tolist()
                stats_data["values"] = values_list
                stats_data["count"] = len(values_list)

                # Calculate stats from raw values
                if values_list:
                    arr = np.array(values_list)
                    stats_data["mean"] = float(np.mean(arr))
                    stats_data["median"] = float(np.median(arr))
                    stats_data["min"] = float(np.min(arr))
                    stats_data["max"] = float(np.max(arr))
                    if len(arr) > 1:
                        stats_data["std"] = float(np.std(arr, ddof=1))
                    else:
                        stats_data["std"] = 0.0

            return ViewerResult(
                viewer_type=self.viewer_type,
                render_type="stats",
                data=stats_data,
                metadata=metadata,
            )
        except Exception as e:
            return ViewerResult(
                viewer_type=self.viewer_type,
                render_type="error",
                data=None,
                error=f"Failed to parse stats: {e}",
            )
