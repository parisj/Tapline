from __future__ import annotations

import io
from typing import Any

import numpy as np

from src.domain.results import Artifact
from src.evaluation.analyzers.base import Analyzer, AnalyzerResult

# Constants
_POINT_LENGTH = 2  # Expected length for (x, y) coordinate pairs


class ContourAnalyzer(Analyzer):
    """2D density grid for contour plotting (stored as NPZ artifact)."""

    def run(self, *, values: list[Any], meta: dict[str, Any]) -> AnalyzerResult:
        pts: list[tuple[float, float]] = []
        missing = 0

        for v in values:
            pt = _parse_point(v)
            if pt is None:
                missing += 1
            else:
                pts.append(pt)

        if not pts:
            return {"summary": {"count": 0, "missing": missing}, "artifact": None}

        arr = np.asarray(pts, dtype=float)
        xs = arr[:, 0]
        ys = arr[:, 1]

        size = max(8, int(meta.get("grid_size", 64)))

        rx = meta.get("range_x")
        ry = meta.get("range_y")

        if isinstance(rx, (list, tuple)) and len(rx) == _POINT_LENGTH:
            xmin, xmax = float(rx[0]), float(rx[1])
        else:
            xmin, xmax = float(xs.min()), float(xs.max())

        if isinstance(ry, (list, tuple)) and len(ry) == _POINT_LENGTH:
            ymin, ymax = float(ry[0]), float(ry[1])
        else:
            ymin, ymax = float(ys.min()), float(ys.max())

        # Handle degenerate ranges
        if xmin == xmax or ymin == ymax:
            return {
                "summary": {"count": int(xs.size), "missing": missing, "grid_size": size, "degenerate": True},
                "artifact": None,
            }

        x = np.linspace(xmin, xmax, size)
        y = np.linspace(ymin, ymax, size)
        xx, yy = np.meshgrid(x, y)

        z, _, _ = np.histogram2d(
            xs, ys,
            bins=size,
            range=[[xmin, xmax], [ymin, ymax]],
            density=bool(meta.get("density", True)),
        )
        z = z.T

        frac_levels = meta.get("levels", [0.5, 0.8, 0.95])
        zmax = float(z.max()) if z.size else 0.0
        levels = np.asarray(
            [float(f) * zmax for f in frac_levels if isinstance(f, (int, float)) and 0.0 < float(f) < 1.0],
            dtype=float,
        )

        buf = io.BytesIO()
        np.savez_compressed(buf, x=xx, y=yy, z=z, levels=levels)

        return {
            "summary": {
                "count": int(xs.size),
                "missing": missing,
                "grid_size": size,
                "xmin": xmin,
                "xmax": xmax,
                "ymin": ymin,
                "ymax": ymax,
                "levels_n": int(levels.size),
                "density": bool(meta.get("density", True)),
            },
            "artifact":Artifact(
                name="contour.npz",
                mime="application/x-npz",
                data=buf.getvalue(),
            ),
        }


def _parse_point(v: object) -> tuple[float, float] | None:
    try:
        if isinstance(v, (list, tuple)) and len(v) == _POINT_LENGTH:
            return float(v[0]), float(v[1])
        if isinstance(v, dict) and "x" in v and "y" in v:
            return float(v["x"]), float(v["y"])
    except (TypeError, ValueError):
        return None
    return None
