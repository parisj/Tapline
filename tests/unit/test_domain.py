"""Unit tests for domain models.

Tests for:
- Task dataclass
- Artifact dataclass
- ProcessorResult dataclass
- Measurement dataclass
- PersistedResultRef dataclass
- metrics_to_jsonable function
- AggregationType enum
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from unittest.mock import MagicMock

import pytest

from src.domain.evaluation import AggregationType
from src.domain.results import (
    Artifact,
    Measurement,
    PersistedResultRef,
    ProcessorResult,
    metrics_to_jsonable,
)
from src.domain.tasks import Task

# =============================================================================
# Task Tests
# =============================================================================


class TestTask:
    """Tests for Task dataclass."""

    def test_task_creation(self) -> None:
        task = Task(
            task_id="task-123",
            directory_key="inbox",
            path="/tmp/test.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )

        assert task.task_id == "task-123"
        assert task.directory_key == "inbox"
        assert task.path == "/tmp/test.png"
        assert task.created_at_unix == 1704067200.0
        assert task.fingerprint == "abc123"

    def test_task_is_frozen(self) -> None:
        task = Task(
            task_id="task-123",
            directory_key="inbox",
            path="/tmp/test.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )

        with pytest.raises(FrozenInstanceError):
            task.task_id = "new-id"  # type: ignore[misc]

    def test_task_equality(self) -> None:
        task1 = Task(
            task_id="task-123",
            directory_key="inbox",
            path="/tmp/test.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )
        task2 = Task(
            task_id="task-123",
            directory_key="inbox",
            path="/tmp/test.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )
        task3 = Task(
            task_id="task-456",
            directory_key="inbox",
            path="/tmp/test.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )

        assert task1 == task2
        assert task1 != task3

    def test_task_hashable(self) -> None:
        task = Task(
            task_id="task-123",
            directory_key="inbox",
            path="/tmp/test.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )

        # Should be hashable for use in sets/dicts
        task_set = {task}
        assert task in task_set


# =============================================================================
# Artifact Tests
# =============================================================================


class TestArtifact:
    """Tests for Artifact dataclass."""

    def test_artifact_creation_with_data(self) -> None:
        artifact = Artifact(
            name="test.png",
            mime="image/png",
            data=b"binary data",
        )

        assert artifact.name == "test.png"
        assert artifact.mime == "image/png"
        assert artifact.data == b"binary data"
        assert artifact.artifact_ref is None

    def test_artifact_has_data_property(self) -> None:
        artifact_with_data = Artifact(name="test.png", mime="image/png", data=b"data")
        artifact_without_data = Artifact(name="test.png", mime="image/png")

        assert artifact_with_data.has_data is True
        assert artifact_without_data.has_data is False

    def test_artifact_is_stored_property(self) -> None:
        # Mock artifact_ref
        mock_ref = MagicMock()
        mock_ref.content_hash = "abc123"

        artifact_stored = Artifact(name="test.png", mime="image/png", artifact_ref=mock_ref)
        artifact_not_stored = Artifact(name="test.png", mime="image/png", data=b"data")

        assert artifact_stored.is_stored is True
        assert artifact_not_stored.is_stored is False

    def test_artifact_content_hash_property(self) -> None:
        mock_ref = MagicMock()
        mock_ref.content_hash = "abc123"

        artifact_stored = Artifact(name="test.png", mime="image/png", artifact_ref=mock_ref)
        artifact_not_stored = Artifact(name="test.png", mime="image/png", data=b"data")

        assert artifact_stored.content_hash == "abc123"
        assert artifact_not_stored.content_hash is None

    def test_artifact_get_data_with_inline_data(self) -> None:
        artifact = Artifact(name="test.png", mime="image/png", data=b"inline data")

        assert artifact.get_data() == b"inline data"

    def test_artifact_get_data_with_storage_service(self) -> None:
        mock_ref = MagicMock()
        mock_storage = MagicMock()
        mock_storage.retrieve.return_value = b"stored data"

        artifact = Artifact(name="test.png", mime="image/png", artifact_ref=mock_ref)

        assert artifact.get_data(storage_service=mock_storage) == b"stored data"
        mock_storage.retrieve.assert_called_once_with(mock_ref)

    def test_artifact_get_data_raises_without_data_or_service(self) -> None:
        artifact = Artifact(name="test.png", mime="image/png")

        with pytest.raises(ValueError, match="no data and no storage service"):
            artifact.get_data()

    def test_artifact_is_frozen(self) -> None:
        artifact = Artifact(name="test.png", mime="image/png", data=b"data")

        with pytest.raises(FrozenInstanceError):
            artifact.name = "new.png"  # type: ignore[misc]


# =============================================================================
# Measurement Tests
# =============================================================================


class TestMeasurement:
    """Tests for Measurement dataclass."""

    def test_measurement_creation(self) -> None:
        mv = Measurement(
            value=42.0,
            aggregation=AggregationType.STATS,
        )

        assert mv.value == 42.0
        assert mv.aggregation == AggregationType.STATS
        assert mv.meta is None

    def test_measurement_with_meta(self) -> None:
        mv = Measurement(
            value="category_a",
            aggregation=AggregationType.TALLY,
            meta={"top_k": 10},
        )

        assert mv.value == "category_a"
        assert mv.meta == {"top_k": 10}

    def test_measurement_with_combined_aggregation(self) -> None:
        mv = Measurement(
            value=0.85,
            aggregation=AggregationType.STATS | AggregationType.HISTOGRAM,
        )

        assert AggregationType.STATS in mv.aggregation
        assert AggregationType.HISTOGRAM in mv.aggregation
        assert AggregationType.TALLY not in mv.aggregation

    def test_measurement_is_frozen(self) -> None:
        mv = Measurement(value=42.0, aggregation=AggregationType.STATS)

        with pytest.raises(FrozenInstanceError):
            mv.value = 100.0  # type: ignore[misc]


# =============================================================================
# ProcessorResult Tests
# =============================================================================


class TestProcessorResult:
    """Tests for ProcessorResult dataclass."""

    def test_processor_result_creation_empty(self) -> None:
        result = ProcessorResult()

        assert result.metrics is None
        assert result.artifacts == ()

    def test_processor_result_with_metrics(self) -> None:
        metrics = {
            "accuracy": Measurement(value=0.95, aggregation=AggregationType.STATS),
            "count": Measurement(value=100, aggregation=AggregationType.TALLY),
        }
        result = ProcessorResult(metrics=metrics)

        assert result.metrics == metrics
        assert len(result.metrics) == 2

    def test_processor_result_with_artifacts(self) -> None:
        artifacts = (
            Artifact(name="mask.png", mime="image/png", data=b"mask"),
            Artifact(name="overlay.png", mime="image/png", data=b"overlay"),
        )
        result = ProcessorResult(artifacts=artifacts)

        assert len(result.artifacts) == 2
        assert result.artifacts[0].name == "mask.png"

    def test_processor_result_is_frozen(self) -> None:
        result = ProcessorResult()

        with pytest.raises(FrozenInstanceError):
            result.metrics = {}  # type: ignore[misc]


# =============================================================================
# PersistedResultRef Tests
# =============================================================================


class TestPersistedResultRef:
    """Tests for PersistedResultRef dataclass."""

    def test_persisted_result_ref_creation(self) -> None:
        ref = PersistedResultRef(
            task_id="task-123",
            processor_name="analysis_probe",
            processor_version="1.0.0",
            result_id=42,
        )

        assert ref.task_id == "task-123"
        assert ref.processor_name == "analysis_probe"
        assert ref.processor_version == "1.0.0"
        assert ref.result_id == 42

    def test_persisted_result_ref_is_frozen(self) -> None:
        ref = PersistedResultRef(
            task_id="task-123",
            processor_name="analysis_probe",
            processor_version="1.0.0",
            result_id=42,
        )

        with pytest.raises(FrozenInstanceError):
            ref.result_id = 100  # type: ignore[misc]


# =============================================================================
# metrics_to_jsonable Tests
# =============================================================================


class TestMetricsToJsonable:
    """Tests for metrics_to_jsonable function."""

    def test_converts_measurement_to_dict(self) -> None:
        metrics = {
            "accuracy": Measurement(
                value=0.95,
                aggregation=AggregationType.STATS,
                meta={"threshold": 0.5},
            ),
        }

        result = metrics_to_jsonable(metrics)

        assert "accuracy" in result
        assert result["accuracy"]["value"] == 0.95
        assert result["accuracy"]["aggregation_mask"] == AggregationType.STATS.value
        assert result["accuracy"]["meta"] == {"threshold": 0.5}

    def test_handles_none_meta(self) -> None:
        metrics = {
            "count": Measurement(value=100, aggregation=AggregationType.TALLY),
        }

        result = metrics_to_jsonable(metrics)

        assert result["count"]["meta"] == {}

    def test_handles_combined_aggregation_flags(self) -> None:
        combined = AggregationType.STATS | AggregationType.HISTOGRAM
        metrics = {
            "score": Measurement(value=0.5, aggregation=combined),
        }

        result = metrics_to_jsonable(metrics)

        assert result["score"]["aggregation_mask"] == combined.value

    def test_handles_plain_scalar_backward_compat(self) -> None:
        # Backward compatibility with plain values
        metrics = {"simple": 42}

        result = metrics_to_jsonable(metrics)

        assert result["simple"]["value"] == 42
        assert result["simple"]["aggregation_mask"] == 0
        assert result["simple"]["meta"] == {}

    def test_handles_multiple_metrics(self) -> None:
        metrics = {
            "accuracy": Measurement(value=0.95, aggregation=AggregationType.STATS),
            "precision": Measurement(value=0.92, aggregation=AggregationType.STATS),
            "recall": Measurement(value=0.88, aggregation=AggregationType.STATS),
        }

        result = metrics_to_jsonable(metrics)

        assert len(result) == 3
        assert all(k in result for k in ["accuracy", "precision", "recall"])


# =============================================================================
# AggregationType Tests
# =============================================================================


class TestAggregationType:
    """Tests for AggregationType enum."""

    def test_aggregation_type_values(self) -> None:
        # Verify all expected kinds exist
        assert hasattr(AggregationType, "STATS")
        assert hasattr(AggregationType, "HISTOGRAM")
        assert hasattr(AggregationType, "OUTLIERS")
        assert hasattr(AggregationType, "TALLY")
        assert hasattr(AggregationType, "RATE")
        assert hasattr(AggregationType, "SCATTER_ELLIPSE")
        assert hasattr(AggregationType, "DENSITY_MAP")
        assert hasattr(AggregationType, "RAW")

    def test_aggregation_type_is_intflag(self) -> None:
        # Should be combinable with bitwise OR
        combined = AggregationType.STATS | AggregationType.HISTOGRAM

        assert AggregationType.STATS in combined
        assert AggregationType.HISTOGRAM in combined
        assert AggregationType.TALLY not in combined

    def test_aggregation_type_values_are_powers_of_two(self) -> None:
        # IntFlag requires powers of 2 for proper combination
        for kind in AggregationType:
            # Each value should be a power of 2
            assert kind.value > 0
            assert (kind.value & (kind.value - 1)) == 0

    def test_aggregation_type_no_overlap(self) -> None:
        # Each kind should have a unique value
        values = [kind.value for kind in AggregationType]
        assert len(values) == len(set(values))

    def test_aggregation_type_can_combine_all(self) -> None:
        # Should be able to combine all kinds
        all_kinds = AggregationType(0)
        for kind in AggregationType:
            all_kinds |= kind

        for kind in AggregationType:
            assert kind in all_kinds
