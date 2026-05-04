from __future__ import annotations

import io
from collections import Counter
from typing import Any

import numpy as np

from src.domain.results import Artifact
from src.evaluation.analyzers.base import Analyzer, AnalyzerResult


class CounterAnalyzer(Analyzer):
    """Categorical counts stored as NPZ artifact."""

    def run(self, *, values: list[Any], meta: dict[str, Any]) -> AnalyzerResult:
        missing = 0
        counter: Counter[str] = Counter()

        top_k = max(1, int(meta.get("top_k", 20)))
        sort_mode = meta.get("sort", "count")

        for v in values:
            if v is None:
                missing += 1
                continue
            counter[str(v)] += 1

        if not counter:
            return {"summary": {"count": 0, "missing": missing, "unique": 0}, "artifact": None}

        items = sorted(counter.items(), key=lambda kv: kv[0]) if sort_mode == "key" else counter.most_common()
        items = items[:top_k]

        labels = np.asarray([k for k, _ in items], dtype=object)
        counts = np.asarray([int(v) for _, v in items], dtype=np.int64)

        buf = io.BytesIO()
        np.savez_compressed(buf, labels=labels, counts=counts)

        return {
            "summary": {
                "count": int(sum(counter.values())),
                "missing": missing,
                "unique": len(counter),
                "top_k": top_k,
                "sort": str(sort_mode),
            },
            "artifact": Artifact(
                name="counter.npz",
                mime="application/x-npz",
                data=buf.getvalue(),
            ),
        }
