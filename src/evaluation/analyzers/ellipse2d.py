from __future__ import annotations

import io
import math
from typing import Any

import numpy as np

from src.domain.results import Artifact
from src.evaluation.analyzers.base import Analyzer, AnalyzerResult


class CovEllipseAnalyzer(Analyzer):
    """2D covariance ellipse stored as NPZ artifact."""

    def run(self, *, values: list[Any], meta: dict[str, Any]) -> AnalyzerResult:
        pts: list[tuple[float, float]] = []
        missing = 0

        sigmas = meta.get("sigmas")
        ks = tuple(float(s) for s in sigmas) if isinstance(sigmas, (list, tuple)) and sigmas else (1.0, 2.0)

        npoints = max(16, int(meta.get("points", 200)))
        ddof = 0 if int(meta.get("ddof", 1)) <= 0 else 1
        include_center = bool(meta.get("include_center", True))
        allow_degenerate = bool(meta.get("allow_degenerate", True))

        for v in values:
            pt = _parse_point(v)
            if pt is None:
                missing += 1
                continue
            pts.append(pt)

        if not pts:
            return {"summary": {"count": 0, "missing": missing}, "artifact": None}

        arr = np.asarray(pts, dtype=float)
        n = int(arr.shape[0])

        mx = float(arr[:, 0].mean())
        my = float(arr[:, 1].mean())

        cov = np.cov(arr.T, ddof=ddof) if n >= 2 else np.zeros((2, 2), dtype=float)

        vals, vecs = np.linalg.eigh(cov)
        order = np.argsort(vals)[::-1]
        vals = vals[order]
        vecs = vecs[:, order]

        lam1 = float(vals[0])
        lam2 = float(vals[1])
        angle = float(math.atan2(vecs[1, 0], vecs[0, 0]))

        r1_base = math.sqrt(max(lam1, 0.0))
        r2_base = math.sqrt(max(lam2, 0.0))
        if (r1_base == 0.0 and r2_base == 0.0) and allow_degenerate:
            r1_base = r2_base = 1e-9

        t = np.linspace(0.0, 2.0 * math.pi, npoints, endpoint=True)
        unit = np.vstack((np.cos(t), np.sin(t)))
        R = vecs

        xs_list: list[np.ndarray] = []
        ys_list: list[np.ndarray] = []
        k_list: list[float] = []

        for k in ks:
            a = r1_base * k
            b = r2_base * k
            scale = np.array([[a, 0.0], [0.0, b]], dtype=float)
            pts_k = (R @ scale @ unit).T
            xs_list.append(pts_k[:, 0] + mx)
            ys_list.append(pts_k[:, 1] + my)
            k_list.append(float(k))

        # Store ragged polylines as object arrays (NPZ supports it)
        xs_obj = np.asarray([x.astype(float) for x in xs_list], dtype=object)
        ys_obj = np.asarray([y.astype(float) for y in ys_list], dtype=object)
        ks_arr = np.asarray(k_list, dtype=float)

        buf = io.BytesIO()
        if include_center:
            np.savez_compressed(buf, xs=xs_obj, ys=ys_obj, k=ks_arr, center=np.asarray([mx, my], dtype=float))
        else:
            np.savez_compressed(buf, xs=xs_obj, ys=ys_obj, k=ks_arr)

        return {
            "summary": {
                "count": n,
                "missing": missing,
                "lambda1": lam1,
                "lambda2": lam2,
                "angle_rad": angle,
                "sigmas": list(ks),
                "points": npoints,
                "has_center": include_center,
            },
            "artifact": Artifact(
                name="ellipse.npz",
                mime="application/x-npz",
                data=buf.getvalue(),
            ),
        }


def _parse_point(v: Any) -> tuple[float, float] | None:
    if v is None:
        return None
    try:
        if isinstance(v, (list, tuple)) and len(v) == 2:
            x = float(v[0]); y = float(v[1])
            return (x, y) if math.isfinite(x) and math.isfinite(y) else None
        if isinstance(v, dict):
            if "x" in v and "y" in v:
                x = float(v["x"]); y = float(v["y"])
                return (x, y) if math.isfinite(x) and math.isfinite(y) else None
            if "xy" in v and isinstance(v["xy"], (list, tuple)) and len(v["xy"]) == 2:
                x = float(v["xy"][0]); y = float(v["xy"][1])
                return (x, y) if math.isfinite(x) and math.isfinite(y) else None
    except (TypeError, ValueError):
        return None
    return None
