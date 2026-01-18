from __future__ import annotations

import io
import math
from typing import Any

import numpy as np

from src.domain.results import Artifact
from src.evaluation.analyzers.base import Analyzer

# Confidence level thresholds for z-score lookup
_CONFIDENCE_99 = 0.99
_CONFIDENCE_975 = 0.975
_CONFIDENCE_95 = 0.95
_CONFIDENCE_90 = 0.90


class RateAnalyzer(Analyzer):
    """Boolean / yes-no rate computation.

    Accepts values:
      - bool
      - 0/1 ints
      - strings like "yes"/"no"/"true"/"false" (case-insensitive)

    Meta:
      confidence: float (default 0.95)
    """

    def run(self, *, values: list[Any], meta: dict[str, Any]) -> dict[str, Any]:
        missing = 0
        n = 0
        yes = 0
        conf = meta.get("confidence", 0.95)

        for v in values:
            parsed = _parse_bool(v)
            if parsed is None:
                missing += 1
                continue
            n += 1
            yes += 1 if parsed else 0

        if n == 0:
            return {
                "count": 0,
                "missing": missing,
                "yes": 0,
                "no": 0,
                "rate": None,
                "wilson_low": None,
                "wilson_high": None,
            }

        rate = yes / n
        low, high = _wilson_interval(yes=yes, n=n, confidence=conf)

        buf = io.BytesIO()
        np.savez_compressed(
            buf,
            yes=yes,
            n=n,
        )

        return {
            "summary": {
                "missing": missing,
                "yes": yes,
                "no": n - yes,
                "rate": rate,
                "wilson_low": low,
                "wilson_high": high,
            },
            "artifact": Artifact(name="rate.npz", mime="rate/x-npz", data=buf.getvalue()),
        }


def _parse_bool(v: object) -> bool | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, int) and v in (0, 1):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "t", "yes", "y", "1", "ok", "pass"):
            return True
        if s in ("false", "f", "no", "n", "0", "fail"):
            return False
    return None


def _wilson_interval(*, yes: int, n: int, confidence: float) -> tuple[float, float]:
    # Normal approximation z for common confidences. (stdlib only)
    # If you need arbitrary confidence, swap to scipy later.
    z = _z_for_confidence(confidence)
    phat = yes / n
    denom = 1.0 + (z * z) / n
    center = (phat + (z * z) / (2.0 * n)) / denom
    margin = (z / denom) * math.sqrt((phat * (1.0 - phat) / n) + (z * z) / (4.0 * n * n))
    return (max(0.0, center - margin), min(1.0, center + margin))


def _z_for_confidence(confidence: float) -> float:
    # Small lookup table; good enough for operational dashboards.
    if confidence >= _CONFIDENCE_99:
        return 2.575829
    if confidence >= _CONFIDENCE_975:
        return 1.959964
    if confidence >= _CONFIDENCE_95:
        return 1.959964
    if confidence >= _CONFIDENCE_90:
        return 1.644854
    return 1.281552  # ~80%
