"""Unit tests for visualization discovery service."""

from __future__ import annotations

import dataclasses
import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.visualization.discovery import (
    AlgorithmInfo,
    DirectoryInfo,
    DiscoveryService,
    MetricInfo,
)


def _make_loaded_route(algorithm: str, version: str, settings_relpath: str = "s.toml") -> SimpleNamespace:
    return SimpleNamespace(
        algorithm=algorithm,
        version=version,
        settings_relpath=settings_relpath,
        settings={},
    )


def _make_runtime_config(directories: dict[str, Path]) -> SimpleNamespace:
    return SimpleNamespace(directories=directories)


def _make_storage(
    bucket_map: dict[str, str] | None = None,
    objects_by_bucket: dict[str, list[dict]] | None = None,
    docs_by_key: dict[str, dict] | None = None,
    stat_map: dict[str, MagicMock] | None = None,
) -> MagicMock:
    storage = MagicMock()
    storage.buckets = bucket_map or {
        "artifacts": "artifacts",
        "aggregates": "aggregates",
        "metric-values": "metric-values",
        "inputs": "inputs",
    }

    docs_by_key = docs_by_key or {}
    objects_by_bucket = objects_by_bucket or {}

    def _list(bucket, prefix="", limit=10000):
        return objects_by_bucket.get(bucket, [])

    def _retrieve(bucket, key):
        if key in docs_by_key:
            return json.dumps(docs_by_key[key]).encode("utf-8")
        return None

    storage.list_objects.side_effect = _list
    storage.retrieve_by_key.side_effect = _retrieve

    # Provide a fake _client with stat_object
    client = MagicMock()
    if stat_map is None:
        stat_map = {}

    def _stat(bucket, key):
        if (bucket, key) in stat_map:
            return stat_map[(bucket, key)]
        # Default empty stat
        stat = MagicMock()
        stat.metadata = {}
        stat.content_type = "application/octet-stream"
        stat.size = 0
        stat.last_modified = None
        return stat

    client.stat_object.side_effect = _stat
    storage._client = client
    return storage


class TestDirectoryInfo:
    def test_dataclass_is_frozen(self) -> None:
        info = DirectoryInfo(key="k", path=Path("/a"))
        with pytest.raises(dataclasses.FrozenInstanceError):
            info.key = "x"  # type: ignore[misc]

    def test_attrs(self) -> None:
        info = DirectoryInfo(key="path0", path=Path("/data"))
        assert info.key == "path0"
        assert info.path == Path("/data")


class TestAlgorithmInfo:
    def test_attrs(self) -> None:
        info = AlgorithmInfo(name="algoA", version="1.0.0", settings_path="s.toml")
        assert info.name == "algoA"
        assert info.version == "1.0.0"
        assert info.settings_path == "s.toml"


class TestMetricInfo:
    def test_post_init_fills_aggregation_types(self) -> None:
        m = MetricInfo(
            metric_name="m",
            processor_name="p",
            processor_version="1",
            aggregation_mask=1,
        )
        # Should be populated by mask_to_kind_names(1)
        assert isinstance(m.aggregation_types, list)

    def test_explicit_aggregation_types_preserved(self) -> None:
        m = MetricInfo(
            metric_name="m",
            processor_name="p",
            processor_version="1",
            aggregation_mask=0,
            aggregation_types=["CUSTOM"],
        )
        assert m.aggregation_types == ["CUSTOM"]


class TestListDirectories:
    def test_returns_dir_info_list(self) -> None:
        runtime = _make_runtime_config({"path0": Path("/data/a"), "path1": Path("/data/b")})
        storage = _make_storage()
        svc = DiscoveryService(runtime, {}, storage)

        result = svc.list_directories()

        assert len(result) == 2
        keys = {d.key for d in result}
        assert keys == {"path0", "path1"}
        assert all(isinstance(d, DirectoryInfo) for d in result)

    def test_empty_when_no_directories(self) -> None:
        runtime = _make_runtime_config({})
        storage = _make_storage()
        svc = DiscoveryService(runtime, {}, storage)
        assert svc.list_directories() == []


class TestGetAlgorithmForDirectory:
    def test_returns_algo_info_for_known_directory(self) -> None:
        runtime = _make_runtime_config({})
        storage = _make_storage()
        routes = {"path0": _make_loaded_route("algoA", "1.0.0", "settings0.toml")}
        svc = DiscoveryService(runtime, routes, storage)

        algo = svc.get_algorithm_for_directory("path0")

        assert algo is not None
        assert algo.name == "algoA"
        assert algo.version == "1.0.0"
        assert algo.settings_path == "settings0.toml"

    def test_returns_none_for_unknown_directory(self) -> None:
        runtime = _make_runtime_config({})
        storage = _make_storage()
        svc = DiscoveryService(runtime, {}, storage)

        assert svc.get_algorithm_for_directory("missing") is None


class TestListAlgorithms:
    def test_returns_unique_algorithms(self) -> None:
        runtime = _make_runtime_config({})
        storage = _make_storage()
        routes = {
            "path0": _make_loaded_route("algoA", "1.0.0"),
            "path1": _make_loaded_route("algoA", "1.0.0"),  # duplicate
            "path2": _make_loaded_route("algoB", "2.0.0"),
        }
        svc = DiscoveryService(runtime, routes, storage)

        algos = svc.list_algorithms()

        assert len(algos) == 2
        pairs = {(a.name, a.version) for a in algos}
        assert pairs == {("algoA", "1.0.0"), ("algoB", "2.0.0")}

    def test_empty_when_no_routes(self) -> None:
        runtime = _make_runtime_config({})
        storage = _make_storage()
        svc = DiscoveryService(runtime, {}, storage)
        assert svc.list_algorithms() == []


class TestDiscoverMetricsFromMinio:
    def test_returns_metric_info_objects(self) -> None:
        runtime = _make_runtime_config({})
        objects = [{"key": "a/b/agg1"}, {"key": "a/b/agg2"}]
        docs = {
            "a/b/agg1": {
                "metric_name": "accuracy",
                "processor_name": "p1",
                "processor_version": "1",
                "aggregation_mask": 1,
                "summary": {"count": 5, "sum": 10, "min": 1, "max": 3},
            },
            "a/b/agg2": {
                "metric_name": "accuracy",
                "processor_name": "p1",
                "processor_version": "1",
                "aggregation_mask": 1,
                "summary": {"count": 5, "sum": 5, "min": 0, "max": 4},
            },
        }
        storage = _make_storage(
            objects_by_bucket={"aggregates": objects},
            docs_by_key=docs,
        )
        svc = DiscoveryService(runtime, {}, storage)

        metrics = svc.discover_metrics_from_minio()

        assert len(metrics) == 1
        m = metrics[0]
        assert m.metric_name == "accuracy"
        assert m.summary["count"] == 10
        assert m.summary["sum"] == 15
        assert m.summary["min"] == 0
        assert m.summary["max"] == 4
        assert m.summary["avg"] == 1.5

    def test_filters_by_processor_name(self) -> None:
        runtime = _make_runtime_config({})
        objects = [{"key": "k1"}, {"key": "k2"}]
        docs = {
            "k1": {
                "metric_name": "m1",
                "processor_name": "p1",
                "processor_version": "1",
                "aggregation_mask": 0,
                "summary": {"count": 1, "sum": 1},
            },
            "k2": {
                "metric_name": "m2",
                "processor_name": "p2",
                "processor_version": "1",
                "aggregation_mask": 0,
                "summary": {"count": 1, "sum": 1},
            },
        }
        storage = _make_storage(objects_by_bucket={"aggregates": objects}, docs_by_key=docs)
        svc = DiscoveryService(runtime, {}, storage)

        metrics = svc.discover_metrics_from_minio(processor_name="p1")

        assert len(metrics) == 1
        assert metrics[0].processor_name == "p1"

    def test_filters_by_processor_version(self) -> None:
        runtime = _make_runtime_config({})
        objects = [{"key": "k1"}, {"key": "k2"}]
        docs = {
            "k1": {
                "metric_name": "m1",
                "processor_name": "p",
                "processor_version": "1.0.0",
                "aggregation_mask": 0,
                "summary": {"count": 1, "sum": 1},
            },
            "k2": {
                "metric_name": "m2",
                "processor_name": "p",
                "processor_version": "2.0.0",
                "aggregation_mask": 0,
                "summary": {"count": 1, "sum": 1},
            },
        }
        storage = _make_storage(objects_by_bucket={"aggregates": objects}, docs_by_key=docs)
        svc = DiscoveryService(runtime, {}, storage)

        metrics = svc.discover_metrics_from_minio(processor_version="2.0.0")

        assert len(metrics) == 1
        assert metrics[0].processor_version == "2.0.0"

    def test_skips_objects_without_metric_name(self) -> None:
        runtime = _make_runtime_config({})
        objects = [{"key": "k1"}]
        docs = {
            "k1": {
                "processor_name": "p",
                "processor_version": "1",
                "aggregation_mask": 0,
                "summary": {},
            },
        }
        storage = _make_storage(objects_by_bucket={"aggregates": objects}, docs_by_key=docs)
        svc = DiscoveryService(runtime, {}, storage)

        metrics = svc.discover_metrics_from_minio()
        assert metrics == []

    def test_time_range_filter_excludes_old_windows(self) -> None:
        import time

        runtime = _make_runtime_config({})
        now = time.time()
        objects = [{"key": "old"}, {"key": "new"}]
        docs = {
            "old": {
                "metric_name": "m",
                "processor_name": "p",
                "processor_version": "1",
                "aggregation_mask": 0,
                "window_end_unix": now - (60 * 60 * 24),  # 1 day ago
                "summary": {"count": 1, "sum": 1},
            },
            "new": {
                "metric_name": "m",
                "processor_name": "p",
                "processor_version": "1",
                "aggregation_mask": 0,
                "window_end_unix": now - 10,  # 10s ago
                "summary": {"count": 3, "sum": 3},
            },
        }
        storage = _make_storage(objects_by_bucket={"aggregates": objects}, docs_by_key=docs)
        svc = DiscoveryService(runtime, {}, storage)

        metrics = svc.discover_metrics_from_minio(time_range_minutes=10)

        assert len(metrics) == 1
        assert metrics[0].summary["count"] == 3

    def test_results_are_cached(self) -> None:
        runtime = _make_runtime_config({})
        storage = _make_storage(objects_by_bucket={"aggregates": []})
        svc = DiscoveryService(runtime, {}, storage)

        svc.discover_metrics_from_minio()
        svc.discover_metrics_from_minio()
        # list_objects only called once due to cache
        assert storage.list_objects.call_count == 1

    def test_refresh_bypasses_cache(self) -> None:
        runtime = _make_runtime_config({})
        storage = _make_storage(objects_by_bucket={"aggregates": []})
        svc = DiscoveryService(runtime, {}, storage)

        svc.discover_metrics_from_minio()
        svc.discover_metrics_from_minio(refresh=True)
        assert storage.list_objects.call_count == 2

    def test_handles_invalid_json_gracefully(self) -> None:
        runtime = _make_runtime_config({})
        storage = MagicMock()
        storage.buckets = {"aggregates": "aggregates"}
        storage.list_objects.return_value = [{"key": "bad"}]
        storage.retrieve_by_key.return_value = b"not-json"
        svc = DiscoveryService(runtime, {}, storage)

        metrics = svc.discover_metrics_from_minio()
        assert metrics == []

    def test_handles_storage_exception(self) -> None:
        runtime = _make_runtime_config({})
        storage = MagicMock()
        storage.buckets = {"aggregates": "aggregates"}
        storage.list_objects.side_effect = RuntimeError("boom")
        svc = DiscoveryService(runtime, {}, storage)

        metrics = svc.discover_metrics_from_minio()
        assert metrics == []


class TestGetMetricArtifact:
    def test_returns_none_when_no_artifact_key(self) -> None:
        runtime = _make_runtime_config({})
        storage = _make_storage()
        svc = DiscoveryService(runtime, {}, storage)
        metric = MetricInfo(
            metric_name="m",
            processor_name="p",
            processor_version="1",
            aggregation_mask=0,
            artifact_key=None,
        )
        assert svc.get_metric_artifact(metric) is None

    def test_returns_parsed_doc(self) -> None:
        runtime = _make_runtime_config({})
        objects = [{"key": "agg/key"}]
        docs = {"agg/key": {"foo": "bar"}}
        storage = _make_storage(
            objects_by_bucket={"aggregates": objects},
            docs_by_key=docs,
        )
        svc = DiscoveryService(runtime, {}, storage)
        metric = MetricInfo(
            metric_name="m",
            processor_name="p",
            processor_version="1",
            aggregation_mask=0,
            artifact_key="agg/key",
        )

        result = svc.get_metric_artifact(metric)
        assert result == {"foo": "bar"}

    def test_returns_none_on_exception(self) -> None:
        runtime = _make_runtime_config({})
        storage = MagicMock()
        storage.buckets = {"aggregates": "aggregates"}
        storage.retrieve_by_key.side_effect = RuntimeError("boom")
        svc = DiscoveryService(runtime, {}, storage)
        metric = MetricInfo(
            metric_name="m",
            processor_name="p",
            processor_version="1",
            aggregation_mask=0,
            artifact_key="key",
        )
        assert svc.get_metric_artifact(metric) is None


class TestGetMetricValues:
    def test_collects_values_matching_identity(self) -> None:
        runtime = _make_runtime_config({})
        objects = [{"key": "k1"}, {"key": "k2"}]
        docs = {
            "k1": {
                "processor_name": "p",
                "processor_version": "1",
                "metric_name": "m",
                "aggregation_mask": 2,
                "values": [1, 2, 3],
            },
            "k2": {
                "processor_name": "other",
                "processor_version": "1",
                "metric_name": "m",
                "aggregation_mask": 2,
                "values": [99],
            },
        }
        storage = _make_storage(
            objects_by_bucket={"metric-values": objects},
            docs_by_key=docs,
        )
        svc = DiscoveryService(runtime, {}, storage)

        values = svc.get_metric_values("p", "1", "m")
        assert values == [1, 2, 3]

    def test_returns_empty_on_no_match(self) -> None:
        runtime = _make_runtime_config({})
        storage = _make_storage(objects_by_bucket={"metric-values": []})
        svc = DiscoveryService(runtime, {}, storage)

        assert svc.get_metric_values("p", "1", "m") == []


class TestGetMetricValuesWithMeta:
    def test_collects_from_both_buckets(self) -> None:
        runtime = _make_runtime_config({})
        objects_mv = [{"key": "mv1"}]
        objects_agg = [{"key": "agg1"}]
        docs = {
            "mv1": {
                "processor_name": "p",
                "processor_version": "1",
                "metric_name": "m",
                "aggregation_mask": 2,
                "values": [1, 2],
            },
            "agg1": {
                "processor_name": "p",
                "processor_version": "1",
                "metric_name": "m",
                "aggregation_mask": 2,
                "values": [3, 4],
            },
        }
        storage = _make_storage(
            objects_by_bucket={"metric-values": objects_mv, "aggregates": objects_agg},
            docs_by_key=docs,
        )
        svc = DiscoveryService(runtime, {}, storage)

        result = svc.get_metric_values_with_meta("p", "1", "m")

        assert result["values"] == [1, 2, 3, 4]
        assert result["aggregation_mask"] == 2

    def test_empty_when_nothing_matches(self) -> None:
        runtime = _make_runtime_config({})
        storage = _make_storage(objects_by_bucket={"metric-values": [], "aggregates": []})
        svc = DiscoveryService(runtime, {}, storage)

        result = svc.get_metric_values_with_meta("p", "1", "m")
        assert result == {"values": [], "aggregation_mask": 0, "meta": None}

    def test_time_range_filter_excludes_old(self) -> None:
        import time

        runtime = _make_runtime_config({})
        now = time.time()
        objects = [{"key": "old"}, {"key": "new"}]
        docs = {
            "old": {
                "processor_name": "p",
                "processor_version": "1",
                "metric_name": "m",
                "aggregation_mask": 2,
                "window_end_unix": now - 86_400,
                "values": [10],
            },
            "new": {
                "processor_name": "p",
                "processor_version": "1",
                "metric_name": "m",
                "aggregation_mask": 2,
                "window_end_unix": now - 5,
                "values": [20],
            },
        }
        storage = _make_storage(
            objects_by_bucket={"metric-values": objects, "aggregates": []},
            docs_by_key=docs,
        )
        svc = DiscoveryService(runtime, {}, storage)

        result = svc.get_metric_values_with_meta("p", "1", "m", time_range_minutes=10)
        assert result["values"] == [20]


class TestClearCache:
    def test_clears_internal_cache(self) -> None:
        runtime = _make_runtime_config({})
        storage = _make_storage(objects_by_bucket={"aggregates": []})
        svc = DiscoveryService(runtime, {}, storage)

        svc.discover_metrics_from_minio()
        assert svc._metrics_cache != {}
        svc.clear_cache()
        assert svc._metrics_cache == {}


class TestListRecentTasks:
    def test_collects_task_info_from_metadata(self) -> None:
        runtime = _make_runtime_config({})
        objects = [{"key": "ab/cd/hash1"}, {"key": "ab/cd/hash2"}]
        stat1 = MagicMock()
        stat1.metadata = {
            "x-amz-meta-task_id": "task-A",
            "x-amz-meta-name": "art1.json",
            "x-amz-meta-processor_name": "p1",
            "x-amz-meta-processor_version": "1.0.0",
        }
        stat1.content_type = "application/json"
        stat1.size = 100
        stat1.last_modified = datetime(2024, 1, 1, 0, 0, 0)

        stat2 = MagicMock()
        stat2.metadata = {
            "x-amz-meta-task_id": "task-B",
            "x-amz-meta-name": "art2.png",
            "x-amz-meta-processor_name": "p2",
            "x-amz-meta-processor_version": "2.0.0",
        }
        stat2.content_type = "image/png"
        stat2.size = 500
        stat2.last_modified = datetime(2024, 1, 2, 0, 0, 0)

        storage = _make_storage(
            objects_by_bucket={"artifacts": objects},
            stat_map={
                ("artifacts", "ab/cd/hash1"): stat1,
                ("artifacts", "ab/cd/hash2"): stat2,
            },
        )
        svc = DiscoveryService(runtime, {}, storage)

        tasks = svc.list_recent_tasks()

        assert len(tasks) == 2
        # Sorted most-recent first
        assert tasks[0]["task_id"] == "task-B"
        assert tasks[1]["task_id"] == "task-A"
        assert tasks[0]["processor_name"] == "p2"

    def test_filters_by_processor_name(self) -> None:
        runtime = _make_runtime_config({})
        objects = [{"key": "k1"}, {"key": "k2"}]
        stat1 = MagicMock()
        stat1.metadata = {
            "x-amz-meta-task_id": "task1",
            "x-amz-meta-name": "f.json",
            "x-amz-meta-processor_name": "wanted",
        }
        stat1.content_type = "application/json"
        stat1.size = 1
        stat1.last_modified = datetime(2024, 1, 1)

        stat2 = MagicMock()
        stat2.metadata = {
            "x-amz-meta-task_id": "task2",
            "x-amz-meta-name": "f.json",
            "x-amz-meta-processor_name": "other",
        }
        stat2.content_type = "application/json"
        stat2.size = 1
        stat2.last_modified = datetime(2024, 1, 1)

        storage = _make_storage(
            objects_by_bucket={"artifacts": objects},
            stat_map={("artifacts", "k1"): stat1, ("artifacts", "k2"): stat2},
        )
        svc = DiscoveryService(runtime, {}, storage)

        tasks = svc.list_recent_tasks(processor_name="wanted")
        assert len(tasks) == 1
        assert tasks[0]["task_id"] == "task1"

    def test_skips_objects_without_task_id(self) -> None:
        runtime = _make_runtime_config({})
        objects = [{"key": "no-task"}]
        stat = MagicMock()
        stat.metadata = {}
        stat.content_type = "application/json"
        stat.size = 0
        stat.last_modified = None
        storage = _make_storage(
            objects_by_bucket={"artifacts": objects},
            stat_map={("artifacts", "no-task"): stat},
        )
        svc = DiscoveryService(runtime, {}, storage)
        assert svc.list_recent_tasks() == []

    def test_applies_offset_and_limit(self) -> None:
        runtime = _make_runtime_config({})
        objects = []
        stat_map = {}
        # Create 5 tasks with monotonic timestamps so order is stable
        for i in range(5):
            key = f"key{i}"
            objects.append({"key": key})
            s = MagicMock()
            s.metadata = {
                "x-amz-meta-task_id": f"task{i}",
                "x-amz-meta-name": "a.json",
                "x-amz-meta-processor_name": "p",
            }
            s.content_type = "application/json"
            s.size = 1
            s.last_modified = datetime(2024, 1, i + 1)
            stat_map[("artifacts", key)] = s

        storage = _make_storage(
            objects_by_bucket={"artifacts": objects},
            stat_map=stat_map,
        )
        svc = DiscoveryService(runtime, {}, storage)
        tasks = svc.list_recent_tasks(limit=2, offset=1)
        assert len(tasks) == 2
        # Most recent is task4; offset=1 skips to task3
        assert tasks[0]["task_id"] == "task3"
        assert tasks[1]["task_id"] == "task2"


class TestListTaskArtifacts:
    def test_returns_only_matching_task_artifacts(self) -> None:
        runtime = _make_runtime_config({})
        objects = [{"key": "k1"}, {"key": "k2"}, {"key": "k3"}]

        def _stat(task_id, name):
            s = MagicMock()
            s.metadata = {"x-amz-meta-task_id": task_id, "x-amz-meta-name": name}
            s.content_type = "application/json"
            s.size = 10
            s.last_modified = datetime(2024, 1, 1)
            return s

        stat_map = {
            ("artifacts", "k1"): _stat("wanted", "a.json"),
            ("artifacts", "k2"): _stat("other", "b.json"),
            ("artifacts", "k3"): _stat("wanted", "c.json"),
        }
        storage = _make_storage(
            objects_by_bucket={"artifacts": objects},
            stat_map=stat_map,
        )
        svc = DiscoveryService(runtime, {}, storage)

        artifacts = svc.list_task_artifacts("wanted")
        names = {a["name"] for a in artifacts}
        assert names == {"a.json", "c.json"}

    def test_returns_empty_for_missing_task(self) -> None:
        runtime = _make_runtime_config({})
        storage = _make_storage(objects_by_bucket={"artifacts": []})
        svc = DiscoveryService(runtime, {}, storage)
        assert svc.list_task_artifacts("nope") == []

    def test_handles_storage_error(self) -> None:
        runtime = _make_runtime_config({})
        storage = MagicMock()
        storage.buckets = {"artifacts": "artifacts"}
        storage.list_objects.side_effect = RuntimeError("boom")
        svc = DiscoveryService(runtime, {}, storage)
        assert svc.list_task_artifacts("any") == []


class TestGuessMimeType:
    def test_uses_extension_mapping(self) -> None:
        runtime = _make_runtime_config({})
        storage = _make_storage()
        svc = DiscoveryService(runtime, {}, storage)
        assert svc._guess_mime_type("foo.png") == "image/png"
        assert svc._guess_mime_type("foo.unknown") == "application/octet-stream"
