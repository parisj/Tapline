"""Configuration loader for VisioEval pipeline.

Supports loading configuration from TOML files for:
- Runtime settings (pipeline.toml)
- Kafka settings (kafka.toml) - via streaming.config
- MinIO settings (minio.toml) - via storage.config
- Flink settings (flink.toml) - via flink.config
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.config.base import load_toml

if TYPE_CHECKING:
    from collections.abc import Mapping


class ConfigError(ValueError):
    """Configuration validation error."""


@dataclass(frozen=True)
class RuntimeConfig:
    """Runtime configuration for the VisioEval pipeline."""

    ingest_queue_maxsize: int
    ingest_poll_interval_sec: float
    allowed_image_exts: list[str]
    readiness_stable_window_sec: float
    readiness_max_wait_sec: float
    workers_max: int
    evaluation_interval_sec: float
    directories: dict[str, Path]
    # Performance tuning
    commit_batch_size: int  # Commit offsets after N messages (1 = per-message)
    io_workers: int  # Number of I/O threads for file reads (0 = sync)
    artifact_upload_workers: int  # Threads for parallel artifact uploads (0 = sequential)


def load_runtime_config(path: Path) -> RuntimeConfig:
    doc = load_toml(path)

    ingest = _require_table(doc, "ingest")
    readiness = _require_table(doc, "readiness")
    workers = _require_table(doc, "workers")
    evaluation = _require_table(doc, "evaluation")
    directories = _require_table(doc, "directories")

    # ingest
    queue_maxsize = _require_int(ingest, "queue_maxsize")
    poll_interval = _require_float(ingest, "poll_interval_sec")
    exts = _require_str_list(ingest, "allowed_image_exts")

    # readiness
    stable = _require_float(readiness, "stable_window_sec")
    max_wait = _require_float(readiness, "max_wait_sec")

    # workers (auto policy)
    workers_max = _parse_workers_max(workers)
    commit_batch_size = max(1, int(workers.get("commit_batch_size", 10)))
    io_workers = max(0, int(workers.get("io_workers", 4)))
    artifact_upload_workers = max(0, int(workers.get("artifact_upload_workers", 4)))

    # evaluation
    eval_interval = _require_float(evaluation, "interval_sec")

    dirs = {k: Path(v.strip()) for k, v in directories.items() if isinstance(v, str) and v.strip()}
    if not dirs:
        msg = "directories table must contain at least one directory_key path mapping"
        raise ConfigError(msg)

    return RuntimeConfig(
        ingest_queue_maxsize=queue_maxsize,
        ingest_poll_interval_sec=poll_interval,
        allowed_image_exts=exts,
        readiness_stable_window_sec=stable,
        readiness_max_wait_sec=max_wait,
        workers_max=workers_max,
        evaluation_interval_sec=eval_interval,
        directories=dirs,
        commit_batch_size=commit_batch_size,
        io_workers=io_workers,
        artifact_upload_workers=artifact_upload_workers,
    )


def _parse_workers_max(workers: Mapping[str, Any]) -> int:
    max_workers = workers.get("max_workers", "auto")
    if isinstance(max_workers, int):
        return max(1, max_workers)

    if max_workers != "auto":
        msg = "workers.max_workers must be 'auto' or an integer"
        raise ConfigError(msg)

    divisor = int(workers.get("auto_divisor", 2))
    min_workers = int(workers.get("min_workers", 2))
    cpu = os.cpu_count() or 4
    return max(min_workers, 1, cpu // max(1, divisor))


def _require_table(doc: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    val = doc.get(key)
    if not isinstance(val, dict):
        msg = f"Missing or invalid table [{key}]"
        raise ConfigError(msg)
    return val


def _require_int(doc: Mapping[str, Any], key: str) -> int:
    v = doc.get(key)
    if not isinstance(v, int) or int(v) < 0:
        msg = f"{key} must be an integer"
        raise ConfigError(msg)
    return v


def _require_float(doc: Mapping[str, Any], key: str) -> float:
    v = doc.get(key)
    if not isinstance(v, (int, float)) or float(v) < 0:
        msg = f"{key} must be a number"
        raise ConfigError(msg)
    return float(v)


def _require_str_list(doc: Mapping[str, Any], key: str) -> list[str]:
    v = doc.get(key)
    if not isinstance(v, list) or not all(isinstance(x, str) for x in v):
        msg = f"{key} must be a list of strings"
        raise ConfigError(msg)
    return [x.strip() for x in v if x.strip()]
