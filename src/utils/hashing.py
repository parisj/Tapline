from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def compute_fingerprint(path: Path) -> str:
    """Compute a fast dedup fingerprint (NOT a content hash).

    Uses path + size + mtime_ns.
    """
    st = path.stat()
    payload = f"{path.resolve()}|{st.st_size}|{int(st.st_mtime_ns)}".encode()
    return hashlib.sha256(payload).hexdigest()


def make_aggregate_artifact_hash(
    *,
    algo_name: str,
    algo_version: str,
    metric_name: str,
    analysis_kind: str,
    window_start_unix: float,
    window_end_unix: float,
    artifact_name: str,
) -> str:
    payload = (
        f"{algo_name}|{algo_version}|{metric_name}|{analysis_kind}|"
        f"{window_start_unix}|{window_end_unix}|{artifact_name}"
    ).encode()
    return hashlib.sha256(payload).hexdigest()
