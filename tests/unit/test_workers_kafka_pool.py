"""Unit tests for KafkaWorkerPool.

Boundaries mocked: EventConsumer (created inside start()), EventProducer,
MinioStorageService, Dispatcher, Processor. The pool's threading,
batch-commit accounting, lifecycle, ready-event signaling, parallel artifact
upload, and the JOB_CREATED handler are exercised against the real code.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from src.dispatch.dispatcher import DispatchPlan
from src.domain.evaluation import AggregationType
from src.domain.events import EventEnvelope, EventType
from src.domain.results import Artifact, Measurement, ProcessorResult
from src.workers.kafka_pool import KafkaWorkerPool


@dataclass
class MockKafkaConfig:
    """Minimal KafkaConfig stand-in for KafkaWorkerPool."""

    bootstrap_servers: str = "localhost:9092"
    client_id: str = "test-client"
    consumer_group_id: str = "test-group"
    topic_jobs: str = "tapline.jobs"
    topic_results: str = "tapline.results"
    topic_metrics: str = "tapline.metrics"
    topic_aggregates: str = "tapline.aggregates"
    topic_audit_log: str = "tapline.audit"


def _make_processor(name: str = "proc", version: str = "1.0") -> MagicMock:
    proc = MagicMock()
    proc.name = name
    proc.version = version
    return proc


def _make_dispatcher(processor: MagicMock | None = None) -> MagicMock:
    if processor is None:
        processor = _make_processor()
    dispatcher = MagicMock()
    plan = DispatchPlan(processor=processor, settings={"setting": "v"})
    dispatcher.dispatch.return_value = plan
    return dispatcher


def _make_storage() -> MagicMock:
    storage = MagicMock()
    storage.buckets = {"artifacts": "tapline-artifacts"}

    # Mimic store() returning an ObjectRef-shaped object
    def store(data: bytes, bucket: str, mime: str, metadata: dict) -> MagicMock:
        ref = MagicMock()
        ref.content_hash = "hash-" + str(len(data))
        ref.bucket = bucket
        ref.key = "ab/cd/" + ref.content_hash
        ref.size = len(data)
        return ref

    storage.store.side_effect = store
    return storage


def _make_producer() -> MagicMock:
    return MagicMock()


def _make_job_event(task_id: str = "t-1", path: str = "/tmp/file.png") -> EventEnvelope:
    return EventEnvelope.create(
        event_type=EventType.TASK_CREATED,
        source_id="src",
        payload={
            "task_id": task_id,
            "directory_key": "inbox",
            "path": path,
            "fingerprint": "fp",
        },
        timestamp=datetime.now(UTC),
    )


class TestKafkaWorkerPoolInit:
    def test_clamps_negative_io_workers_to_zero(self) -> None:
        pool = KafkaWorkerPool(
            kafka_config=MockKafkaConfig(),
            dispatcher=_make_dispatcher(),
            storage=_make_storage(),
            producer=_make_producer(),
            max_workers=1,
            io_workers=-3,
        )
        try:
            assert pool._io_workers == 0
            assert pool._io_executor is None
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_clamps_negative_artifact_upload_workers_to_zero(self) -> None:
        pool = KafkaWorkerPool(
            kafka_config=MockKafkaConfig(),
            dispatcher=_make_dispatcher(),
            storage=_make_storage(),
            producer=_make_producer(),
            max_workers=1,
            artifact_upload_workers=-5,
        )
        try:
            assert pool._artifact_upload_workers == 0
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_clamps_commit_batch_size_minimum_to_one(self) -> None:
        pool = KafkaWorkerPool(
            kafka_config=MockKafkaConfig(),
            dispatcher=_make_dispatcher(),
            storage=_make_storage(),
            producer=_make_producer(),
            max_workers=1,
            commit_batch_size=0,
        )
        try:
            assert pool._commit_batch_size == 1
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_creates_io_executor_when_io_workers_positive(self) -> None:
        pool = KafkaWorkerPool(
            kafka_config=MockKafkaConfig(),
            dispatcher=_make_dispatcher(),
            storage=_make_storage(),
            producer=_make_producer(),
            max_workers=1,
            io_workers=2,
        )
        try:
            assert pool._io_executor is not None
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)
            if pool._io_executor is not None:
                pool._io_executor.shutdown(wait=True)

    def test_initial_state(self) -> None:
        pool = KafkaWorkerPool(
            kafka_config=MockKafkaConfig(),
            dispatcher=_make_dispatcher(),
            storage=_make_storage(),
            producer=_make_producer(),
            max_workers=2,
        )
        try:
            assert pool._consumers == []
            assert pool._uncommitted_counts == {}
            assert not pool._stop.is_set()
            assert not pool._ready.is_set()
            assert pool._ready_count == 0
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)


class TestStartAndStop:
    @patch("src.streaming.consumer.Consumer")
    def test_start_creates_one_consumer_per_worker(
        self,
        mock_consumer_cls: MagicMock,
    ) -> None:
        mock_consumer_cls.return_value = MagicMock()

        pool = KafkaWorkerPool(
            kafka_config=MockKafkaConfig(),
            dispatcher=_make_dispatcher(),
            storage=_make_storage(),
            producer=_make_producer(),
            max_workers=3,
            io_workers=0,
        )

        # Make EventConsumer iterators stop immediately by setting stop event
        # before workers begin iterating. We patch the imported class inside start().
        with patch("src.streaming.consumer.EventConsumer") as mock_event_consumer_cls:
            instances: list[MagicMock] = []

            def factory(*args: Any, **kwargs: Any) -> MagicMock:
                inst = MagicMock()
                inst.__iter__ = MagicMock(return_value=iter([]))  # empty iterator
                instances.append(inst)
                return inst

            mock_event_consumer_cls.side_effect = factory

            pool.start(wait_for_ready=False)
            # Give worker threads a moment to drain the empty iterator
            time.sleep(0.05)
            pool.stop()

        assert len(instances) == 3
        # Each consumer should have stop() and close() invoked during pool.stop()
        for inst in instances:
            inst.stop.assert_called()
            inst.close.assert_called()

    def test_stop_is_idempotent_on_consumers_when_no_start(self) -> None:
        pool = KafkaWorkerPool(
            kafka_config=MockKafkaConfig(),
            dispatcher=_make_dispatcher(),
            storage=_make_storage(),
            producer=_make_producer(),
            max_workers=1,
            io_workers=0,
        )

        # No consumers created — stop() should not raise
        pool.stop()

        assert pool._stop.is_set()
        assert pool._consumers == []

    def test_stop_clears_consumers_list(self) -> None:
        pool = KafkaWorkerPool(
            kafka_config=MockKafkaConfig(),
            dispatcher=_make_dispatcher(),
            storage=_make_storage(),
            producer=_make_producer(),
            max_workers=1,
            io_workers=0,
        )

        with patch("src.streaming.consumer.EventConsumer") as mock_event_consumer_cls:
            inst = MagicMock()
            inst.__iter__ = MagicMock(return_value=iter([]))
            mock_event_consumer_cls.return_value = inst

            pool.start(wait_for_ready=False)
            time.sleep(0.05)
            pool.stop()

        assert pool._consumers == []

    def test_signal_ready_sets_event_on_first_signal(self) -> None:
        pool = KafkaWorkerPool(
            kafka_config=MockKafkaConfig(),
            dispatcher=_make_dispatcher(),
            storage=_make_storage(),
            producer=_make_producer(),
            max_workers=2,
            io_workers=0,
        )
        try:
            assert not pool._ready.is_set()
            pool._signal_ready()
            assert pool._ready.is_set()
            assert pool._ready_count == 1
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_signal_ready_increments_count_across_calls(self) -> None:
        pool = KafkaWorkerPool(
            kafka_config=MockKafkaConfig(),
            dispatcher=_make_dispatcher(),
            storage=_make_storage(),
            producer=_make_producer(),
            max_workers=3,
            io_workers=0,
        )
        try:
            pool._signal_ready()
            pool._signal_ready()
            pool._signal_ready()
            assert pool._ready_count == 3
            assert pool._ready.is_set()
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)


class TestReadFile:
    def test_read_file_synchronous_when_no_io_executor(self, tmp_path: Path) -> None:
        target = tmp_path / "data.bin"
        target.write_bytes(b"hello-world")

        pool = KafkaWorkerPool(
            kafka_config=MockKafkaConfig(),
            dispatcher=_make_dispatcher(),
            storage=_make_storage(),
            producer=_make_producer(),
            max_workers=1,
            io_workers=0,
        )
        try:
            data = pool._read_file(str(target))
            assert data == b"hello-world"
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_read_file_uses_io_executor_when_configured(self, tmp_path: Path) -> None:
        target = tmp_path / "data.bin"
        target.write_bytes(b"async-data")

        pool = KafkaWorkerPool(
            kafka_config=MockKafkaConfig(),
            dispatcher=_make_dispatcher(),
            storage=_make_storage(),
            producer=_make_producer(),
            max_workers=1,
            io_workers=2,
        )
        try:
            assert pool._io_executor is not None
            data = pool._read_file(str(target))
            assert data == b"async-data"
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)
            if pool._io_executor is not None:
                pool._io_executor.shutdown(wait=True)


class TestIsSlowJob:
    def _make_plan(self, name: str) -> DispatchPlan:
        return DispatchPlan(processor=_make_processor(name=name), settings={})

    def test_model_prefix_is_slow(self) -> None:
        pool = KafkaWorkerPool(
            kafka_config=MockKafkaConfig(),
            dispatcher=_make_dispatcher(),
            storage=_make_storage(),
            producer=_make_producer(),
            max_workers=1,
            io_workers=0,
        )
        try:
            assert pool._is_slow_job(self._make_plan("model_yolo")) is True
            assert pool._is_slow_job(self._make_plan("model_anything")) is True
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_non_model_prefix_is_not_slow(self) -> None:
        pool = KafkaWorkerPool(
            kafka_config=MockKafkaConfig(),
            dispatcher=_make_dispatcher(),
            storage=_make_storage(),
            producer=_make_producer(),
            max_workers=1,
            io_workers=0,
        )
        try:
            assert pool._is_slow_job(self._make_plan("analysis_probe")) is False
            assert pool._is_slow_job(self._make_plan("Model_uppercase")) is False
            assert pool._is_slow_job(self._make_plan("")) is False
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)


class TestStoreArtifacts:
    def _make_pool(self, artifact_upload_workers: int = 2) -> KafkaWorkerPool:
        return KafkaWorkerPool(
            kafka_config=MockKafkaConfig(),
            dispatcher=_make_dispatcher(),
            storage=_make_storage(),
            producer=_make_producer(),
            max_workers=1,
            io_workers=0,
            artifact_upload_workers=artifact_upload_workers,
        )

    def test_empty_artifact_list_returns_empty(self) -> None:
        pool = self._make_pool()
        try:
            refs = pool._store_artifacts_parallel([], "t", "s", "p", "1.0")
            assert refs == []
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_artifacts_without_data_filtered_out(self) -> None:
        pool = self._make_pool()
        try:
            artifact = Artifact(name="empty", mime="image/png", data=None)
            refs = pool._store_artifacts_parallel(
                [artifact],
                "t",
                "s",
                "p",
                "1.0",
            )
            assert refs == []
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_parallel_store_returns_content_hashes(self) -> None:
        pool = self._make_pool(artifact_upload_workers=3)
        try:
            artifacts = [Artifact(name=f"a{i}", mime="image/png", data=b"x" * (i + 1)) for i in range(3)]
            refs = pool._store_artifacts_parallel(artifacts, "t", "s", "p", "1.0")
            assert len(refs) == 3
            # Each ref should reflect the deterministic content hash from MockStorage
            assert sorted(refs) == sorted(["hash-1", "hash-2", "hash-3"])
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_sequential_store_when_zero_upload_workers(self) -> None:
        pool = self._make_pool(artifact_upload_workers=0)
        try:
            artifacts = [
                Artifact(name="a", mime="image/png", data=b"abc"),
                Artifact(name="b", mime="image/png", data=b"de"),
            ]
            refs = pool._store_artifacts_parallel(artifacts, "t", "s", "p", "1.0")
            assert sorted(refs) == sorted(["hash-3", "hash-2"])
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_parallel_store_continues_on_individual_failure(self) -> None:
        pool = self._make_pool(artifact_upload_workers=2)
        try:
            call_count = {"n": 0}

            def store(data: bytes, bucket: str, mime: str, metadata: dict) -> MagicMock:
                call_count["n"] += 1
                if call_count["n"] == 1:
                    msg = "first store fails"
                    raise RuntimeError(msg)
                ref = MagicMock()
                ref.content_hash = "hash-ok"
                ref.bucket = bucket
                ref.key = "k"
                ref.size = len(data)
                return ref

            pool._storage.store.side_effect = store  # type: ignore[attr-defined]

            artifacts = [
                Artifact(name="a", mime="image/png", data=b"x"),
                Artifact(name="b", mime="image/png", data=b"y"),
            ]
            refs = pool._store_artifacts_parallel(artifacts, "t", "s", "p", "1.0")
            # At least one survivor — the order isn't deterministic, but exactly
            # one upload should succeed.
            assert refs == ["hash-ok"]
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_sequential_store_continues_on_individual_failure(self) -> None:
        pool = self._make_pool(artifact_upload_workers=0)
        try:
            results = [RuntimeError("nope"), MagicMock(content_hash="ok", bucket="b", key="k", size=2)]

            def store(data: bytes, bucket: str, mime: str, metadata: dict) -> MagicMock:
                outcome = results.pop(0)
                if isinstance(outcome, BaseException):
                    raise outcome
                return outcome

            pool._storage.store.side_effect = store  # type: ignore[attr-defined]

            artifacts = [
                Artifact(name="a", mime="image/png", data=b"x"),
                Artifact(name="b", mime="image/png", data=b"y"),
            ]
            refs = pool._store_artifacts_parallel(artifacts, "t", "s", "p", "1.0")
            assert refs == ["ok"]
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_store_single_artifact_publishes_artifact_stored_event(self) -> None:
        pool = self._make_pool()
        try:
            artifact = Artifact(name="a", mime="image/png", data=b"data")
            result = pool._store_single_artifact(
                artifact=artifact,
                data=b"data",
                task_id="t-1",
                source_id="src",
                processor_name="proc",
                processor_version="1.0",
            )

            assert result == "hash-4"
            pool._producer.publish_artifact_stored.assert_called_once()  # type: ignore[attr-defined]
            kwargs = pool._producer.publish_artifact_stored.call_args.kwargs  # type: ignore[attr-defined]
            assert kwargs["content_hash"] == "hash-4"
            assert kwargs["mime"] == "image/png"
            assert kwargs["source_task_id"] == "t-1"
            assert kwargs["source_id"] == "src"
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)


class TestProcessJobEvent:
    """Drive _process_job_event end-to-end against mocked external boundaries."""

    def _build_pool(
        self,
        processor: MagicMock | None = None,
        algo_result: ProcessorResult | None = None,
        algo_error: BaseException | None = None,
    ) -> tuple[KafkaWorkerPool, MagicMock]:
        proc = processor or _make_processor(name="analysis_probe", version="1.0")
        # Wire processor.run for the LocalWorkerStrategy
        if algo_error is not None:
            proc.run.side_effect = algo_error
        else:
            proc.run.return_value = algo_result or ProcessorResult()
        dispatcher = _make_dispatcher(processor=proc)
        pool = KafkaWorkerPool(
            kafka_config=MockKafkaConfig(),
            dispatcher=dispatcher,
            storage=_make_storage(),
            producer=_make_producer(),
            max_workers=1,
            io_workers=0,
            artifact_upload_workers=0,
        )
        return pool, proc

    def test_happy_path_publishes_started_and_completed(self, tmp_path: Path) -> None:
        f = tmp_path / "i.png"
        f.write_bytes(b"img")
        pool, _proc = self._build_pool(
            algo_result=ProcessorResult(),
        )
        try:
            mock_consumer = MagicMock()
            event = _make_job_event(task_id="task-A", path=str(f))

            pool._process_job_event(mock_consumer, event, worker_id=0)

            pool._producer.publish_job_started.assert_called_once()  # type: ignore[attr-defined]
            pool._producer.publish_job_completed.assert_called_once()  # type: ignore[attr-defined]
            pool._producer.publish_job_failed.assert_not_called()  # type: ignore[attr-defined]
            # Always emits task_duration_ms as a metric
            pool._producer.publish_metric_emitted.assert_called()  # type: ignore[attr-defined]
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_metrics_published_for_each_measurement(self, tmp_path: Path) -> None:
        f = tmp_path / "i.png"
        f.write_bytes(b"img")
        measurements = {
            "m_one": Measurement(value=1.0, aggregation=AggregationType.STATS),
            "m_two": Measurement(value=2.5, aggregation=AggregationType.HISTOGRAM, meta={"u": "ms"}),
        }
        pool, _proc = self._build_pool(
            algo_result=ProcessorResult(metrics=measurements),
        )
        try:
            event = _make_job_event(path=str(f))
            pool._process_job_event(MagicMock(), event, worker_id=0)

            # Two user metrics + 1 task_duration_ms
            assert pool._producer.publish_metric_emitted.call_count == 3  # type: ignore[attr-defined]

            names = [
                call.kwargs["metric_name"]
                for call in pool._producer.publish_metric_emitted.call_args_list  # type: ignore[attr-defined]
            ]
            assert "m_one" in names
            assert "m_two" in names
            assert "task_duration_ms" in names
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_publishes_job_failed_when_algorithm_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "i.png"
        f.write_bytes(b"img")
        pool, _proc = self._build_pool(
            algo_error=RuntimeError("algo died"),
        )
        try:
            event = _make_job_event(task_id="task-bad", path=str(f))
            pool._process_job_event(MagicMock(), event, worker_id=0)

            pool._producer.publish_job_failed.assert_called_once()  # type: ignore[attr-defined]
            kwargs = pool._producer.publish_job_failed.call_args.kwargs  # type: ignore[attr-defined]
            assert kwargs["task_id"] == "task-bad"
            assert kwargs["error_type"] == "RuntimeError"
            assert "algo died" in kwargs["error"]
            pool._producer.publish_job_completed.assert_not_called()  # type: ignore[attr-defined]
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_dispatcher_failure_published_as_job_failed_with_unknown_processor(
        self,
        tmp_path: Path,
    ) -> None:
        f = tmp_path / "i.png"
        f.write_bytes(b"img")
        pool, _proc = self._build_pool()
        # Make dispatcher fail before processor metadata is captured
        pool._dispatcher.dispatch.side_effect = KeyError("no route")  # type: ignore[attr-defined]
        try:
            event = _make_job_event(path=str(f))
            pool._process_job_event(MagicMock(), event, worker_id=0)

            pool._producer.publish_job_failed.assert_called_once()  # type: ignore[attr-defined]
            kwargs = pool._producer.publish_job_failed.call_args.kwargs  # type: ignore[attr-defined]
            # processor_name/version remain None since dispatch failed first
            assert kwargs["processor_name"] is None
            assert kwargs["processor_version"] is None
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_slow_job_pauses_then_resumes_partition(self, tmp_path: Path) -> None:
        f = tmp_path / "i.png"
        f.write_bytes(b"img")
        slow_proc = _make_processor(name="model_yolo", version="1")
        slow_proc.run.return_value = ProcessorResult()
        pool, _proc = self._build_pool(processor=slow_proc)
        try:
            mock_consumer = MagicMock()
            event = _make_job_event(path=str(f))

            pool._process_job_event(mock_consumer, event, worker_id=0)

            mock_consumer.pause_current.assert_called_once()
            mock_consumer.resume_current.assert_called_once()
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_slow_job_resumes_partition_even_on_failure(self, tmp_path: Path) -> None:
        f = tmp_path / "i.png"
        f.write_bytes(b"img")
        slow_proc = _make_processor(name="model_yolo", version="1")
        slow_proc.run.side_effect = RuntimeError("boom")
        pool, _proc = self._build_pool(processor=slow_proc)
        try:
            mock_consumer = MagicMock()
            event = _make_job_event(path=str(f))

            pool._process_job_event(mock_consumer, event, worker_id=0)

            mock_consumer.pause_current.assert_called_once()
            mock_consumer.resume_current.assert_called_once()
            pool._producer.publish_job_failed.assert_called_once()  # type: ignore[attr-defined]
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_lifecycle_initializes_processor_once(self, tmp_path: Path) -> None:
        f = tmp_path / "i.png"
        f.write_bytes(b"img")
        pool, proc = self._build_pool(
            algo_result=ProcessorResult(),
        )
        try:
            event = _make_job_event(path=str(f))

            pool._process_job_event(MagicMock(), event, worker_id=0)
            pool._process_job_event(MagicMock(), event, worker_id=0)
            pool._process_job_event(MagicMock(), event, worker_id=0)

            # Processor.initialize should be called exactly once (via AlgoLifecycle)
            assert proc.initialize.call_count == 1
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_result_with_artifacts_stores_them(self, tmp_path: Path) -> None:
        f = tmp_path / "i.png"
        f.write_bytes(b"img")
        artifact = Artifact(name="overlay", mime="image/png", data=b"png-bytes")
        pool, _proc = self._build_pool(
            algo_result=ProcessorResult(artifacts=(artifact,)),
        )
        try:
            event = _make_job_event(path=str(f))
            pool._process_job_event(MagicMock(), event, worker_id=0)

            # publish_result_produced should be called with artifact_refs populated
            pool._producer.publish_result_produced.assert_called_once()  # type: ignore[attr-defined]
            kwargs = pool._producer.publish_result_produced.call_args.kwargs  # type: ignore[attr-defined]
            assert len(kwargs["artifact_refs"]) == 1
            # Storage.store should have been invoked
            pool._storage.store.assert_called()  # type: ignore[attr-defined]
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)


class TestWorkerLoopBatchCommits:
    """Exercise _worker_loop's batch-commit accounting against a fake consumer."""

    def _build_pool(self, commit_batch_size: int) -> tuple[KafkaWorkerPool, MagicMock]:
        proc = _make_processor(name="analysis_probe", version="1.0")
        proc.run.return_value = ProcessorResult()
        dispatcher = _make_dispatcher(processor=proc)
        pool = KafkaWorkerPool(
            kafka_config=MockKafkaConfig(),
            dispatcher=dispatcher,
            storage=_make_storage(),
            producer=_make_producer(),
            max_workers=1,
            io_workers=0,
            artifact_upload_workers=0,
            commit_batch_size=commit_batch_size,
        )
        return pool, proc

    def _make_consumer_with_events(self, events: list[EventEnvelope]) -> MagicMock:
        consumer = MagicMock()
        consumer.__iter__ = MagicMock(return_value=iter(events))
        return consumer

    def test_commit_called_after_batch_size_messages(self, tmp_path: Path) -> None:
        f = tmp_path / "i.png"
        f.write_bytes(b"img")
        pool, _proc = self._build_pool(commit_batch_size=3)
        try:
            events = [_make_job_event(task_id=f"t-{i}", path=str(f)) for i in range(6)]
            consumer = self._make_consumer_with_events(events)

            pool._worker_loop(consumer, worker_id=0)

            # 6 messages / batch 3 = 2 commits during loop, plus a final
            # commit-on-shutdown only if remaining > 0. Remaining = 0 after exact batches.
            assert consumer.commit.call_count == 2
            assert pool._uncommitted_counts[0] == 0
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_final_commit_drains_remaining_uncommitted(self, tmp_path: Path) -> None:
        f = tmp_path / "i.png"
        f.write_bytes(b"img")
        pool, _proc = self._build_pool(commit_batch_size=5)
        try:
            # 7 events / batch 5 -> 1 commit during loop + 1 final commit for the 2 remaining
            events = [_make_job_event(task_id=f"t-{i}", path=str(f)) for i in range(7)]
            consumer = self._make_consumer_with_events(events)

            pool._worker_loop(consumer, worker_id=0)

            assert consumer.commit.call_count == 2
            assert pool._uncommitted_counts[0] == 0
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_no_final_commit_when_no_uncommitted(self, tmp_path: Path) -> None:
        f = tmp_path / "i.png"
        f.write_bytes(b"img")
        pool, _proc = self._build_pool(commit_batch_size=1)
        try:
            # Each event triggers commit immediately
            events = [_make_job_event(task_id=f"t-{i}", path=str(f)) for i in range(3)]
            consumer = self._make_consumer_with_events(events)

            pool._worker_loop(consumer, worker_id=0)

            # 3 commits during loop, 0 final commits
            assert consumer.commit.call_count == 3
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_worker_loop_initializes_uncommitted_counter(self) -> None:
        pool, _proc = self._build_pool(commit_batch_size=10)
        try:
            consumer = self._make_consumer_with_events([])
            pool._worker_loop(consumer, worker_id=7)

            assert 7 in pool._uncommitted_counts
            assert pool._uncommitted_counts[7] == 0
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_non_job_created_events_increment_counter_but_not_processed(
        self,
        tmp_path: Path,
    ) -> None:
        """Events of other types should still be counted toward batch commits."""
        pool, _proc = self._build_pool(commit_batch_size=2)
        try:
            other_event = EventEnvelope.create(
                event_type=EventType.METRIC_EMITTED,
                source_id="src",
                payload={},
            )
            consumer = self._make_consumer_with_events([other_event, other_event])
            pool._worker_loop(consumer, worker_id=0)

            # Two non-job events -> exactly one batch commit, no final commit
            assert consumer.commit.call_count == 1
            pool._producer.publish_job_started.assert_not_called()  # type: ignore[attr-defined]
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)

    def test_worker_loop_exception_in_event_does_not_kill_loop(
        self,
        tmp_path: Path,
    ) -> None:
        """An exception inside _process_job_event must not break batch counting."""
        f = tmp_path / "i.png"
        f.write_bytes(b"img")
        pool, _proc = self._build_pool(commit_batch_size=2)
        try:
            # First dispatch raises, second succeeds
            dispatch_calls = [KeyError("no route"), pool._dispatcher.dispatch.return_value]  # type: ignore[attr-defined]

            def dispatch_side_effect(job: Any) -> Any:
                outcome = dispatch_calls.pop(0)
                if isinstance(outcome, BaseException):
                    raise outcome
                return outcome

            pool._dispatcher.dispatch.side_effect = dispatch_side_effect  # type: ignore[attr-defined]

            events = [_make_job_event(task_id=f"t-{i}", path=str(f)) for i in range(2)]
            consumer = self._make_consumer_with_events(events)

            pool._worker_loop(consumer, worker_id=0)

            # Two events processed; one batch commit triggered
            assert consumer.commit.call_count == 1
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)


class TestStopBreaksLoop:
    def test_stop_set_before_loop_starts_drains_only_one_event(
        self,
        tmp_path: Path,
    ) -> None:
        f = tmp_path / "i.png"
        f.write_bytes(b"img")
        proc = _make_processor(name="analysis_probe", version="1.0")
        proc.run.return_value = ProcessorResult()
        pool = KafkaWorkerPool(
            kafka_config=MockKafkaConfig(),
            dispatcher=_make_dispatcher(processor=proc),
            storage=_make_storage(),
            producer=_make_producer(),
            max_workers=1,
            io_workers=0,
            artifact_upload_workers=0,
            commit_batch_size=100,
        )
        try:
            events = [_make_job_event(task_id=f"t-{i}", path=str(f)) for i in range(5)]
            consumer = MagicMock()
            consumer.__iter__ = MagicMock(return_value=iter(events))

            # Set stop before loop runs -> the first iteration sees stop and breaks
            pool._stop.set()
            pool._worker_loop(consumer, worker_id=0)

            # publish_job_started should not have been called for any event
            pool._producer.publish_job_started.assert_not_called()  # type: ignore[attr-defined]
        finally:
            pool._executor.shutdown(wait=True, cancel_futures=True)


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
