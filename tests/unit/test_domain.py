"""Unit tests for domain models.

Tests for:
- Job dataclass
- Artifact dataclass
- AlgoResult dataclass
- MetricValue dataclass
- PersistedResultRef dataclass
- metrics_to_jsonable function
- AnalysisKind enum
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from unittest.mock import MagicMock

import pytest

from src.domain.evaluation import AnalysisKind
from src.domain.jobs import Job
from src.domain.results import (
    AlgoResult,
    Artifact,
    MetricValue,
    PersistedResultRef,
    metrics_to_jsonable,
)

# =============================================================================
# Job Tests
# =============================================================================


class TestJob:
    """Tests for Job dataclass."""

    def test_job_creation(self) -> None:
        job = Job(
            job_id="job-123",
            directory_key="inbox",
            path="/tmp/test.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )

        assert job.job_id == "job-123"
        assert job.directory_key == "inbox"
        assert job.path == "/tmp/test.png"
        assert job.created_at_unix == 1704067200.0
        assert job.fingerprint == "abc123"

    def test_job_is_frozen(self) -> None:
        job = Job(
            job_id="job-123",
            directory_key="inbox",
            path="/tmp/test.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )

        with pytest.raises(FrozenInstanceError):
            job.job_id = "new-id"  # type: ignore[misc]

    def test_job_equality(self) -> None:
        job1 = Job(
            job_id="job-123",
            directory_key="inbox",
            path="/tmp/test.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )
        job2 = Job(
            job_id="job-123",
            directory_key="inbox",
            path="/tmp/test.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )
        job3 = Job(
            job_id="job-456",
            directory_key="inbox",
            path="/tmp/test.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )

        assert job1 == job2
        assert job1 != job3

    def test_job_hashable(self) -> None:
        job = Job(
            job_id="job-123",
            directory_key="inbox",
            path="/tmp/test.png",
            created_at_unix=1704067200.0,
            fingerprint="abc123",
        )

        # Should be hashable for use in sets/dicts
        job_set = {job}
        assert job in job_set


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
        assert artifact.object_ref is None

    def test_artifact_has_data_property(self) -> None:
        artifact_with_data = Artifact(name="test.png", mime="image/png", data=b"data")
        artifact_without_data = Artifact(name="test.png", mime="image/png")

        assert artifact_with_data.has_data is True
        assert artifact_without_data.has_data is False

    def test_artifact_is_stored_property(self) -> None:
        # Mock object_ref
        mock_ref = MagicMock()
        mock_ref.content_hash = "abc123"

        artifact_stored = Artifact(name="test.png", mime="image/png", object_ref=mock_ref)
        artifact_not_stored = Artifact(name="test.png", mime="image/png", data=b"data")

        assert artifact_stored.is_stored is True
        assert artifact_not_stored.is_stored is False

    def test_artifact_content_hash_property(self) -> None:
        mock_ref = MagicMock()
        mock_ref.content_hash = "abc123"

        artifact_stored = Artifact(name="test.png", mime="image/png", object_ref=mock_ref)
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

        artifact = Artifact(name="test.png", mime="image/png", object_ref=mock_ref)

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
# MetricValue Tests
# =============================================================================


class TestMetricValue:
    """Tests for MetricValue dataclass."""

    def test_metric_value_creation(self) -> None:
        mv = MetricValue(
            value=42.0,
            analysis=AnalysisKind.SUMMARY,
        )

        assert mv.value == 42.0
        assert mv.analysis == AnalysisKind.SUMMARY
        assert mv.meta is None

    def test_metric_value_with_meta(self) -> None:
        mv = MetricValue(
            value="category_a",
            analysis=AnalysisKind.COUNTER,
            meta={"top_k": 10},
        )

        assert mv.value == "category_a"
        assert mv.meta == {"top_k": 10}

    def test_metric_value_with_combined_analysis(self) -> None:
        mv = MetricValue(
            value=0.85,
            analysis=AnalysisKind.SUMMARY | AnalysisKind.DISTRIBUTION_1D,
        )

        assert AnalysisKind.SUMMARY in mv.analysis
        assert AnalysisKind.DISTRIBUTION_1D in mv.analysis
        assert AnalysisKind.COUNTER not in mv.analysis

    def test_metric_value_is_frozen(self) -> None:
        mv = MetricValue(value=42.0, analysis=AnalysisKind.SUMMARY)

        with pytest.raises(FrozenInstanceError):
            mv.value = 100.0  # type: ignore[misc]


# =============================================================================
# AlgoResult Tests
# =============================================================================


class TestAlgoResult:
    """Tests for AlgoResult dataclass."""

    def test_algo_result_creation_empty(self) -> None:
        result = AlgoResult()

        assert result.metrics is None
        assert result.artifacts == ()

    def test_algo_result_with_metrics(self) -> None:
        metrics = {
            "accuracy": MetricValue(value=0.95, analysis=AnalysisKind.SUMMARY),
            "count": MetricValue(value=100, analysis=AnalysisKind.COUNTER),
        }
        result = AlgoResult(metrics=metrics)

        assert result.metrics == metrics
        assert len(result.metrics) == 2

    def test_algo_result_with_artifacts(self) -> None:
        artifacts = (
            Artifact(name="mask.png", mime="image/png", data=b"mask"),
            Artifact(name="overlay.png", mime="image/png", data=b"overlay"),
        )
        result = AlgoResult(artifacts=artifacts)

        assert len(result.artifacts) == 2
        assert result.artifacts[0].name == "mask.png"

    def test_algo_result_is_frozen(self) -> None:
        result = AlgoResult()

        with pytest.raises(FrozenInstanceError):
            result.metrics = {}  # type: ignore[misc]


# =============================================================================
# PersistedResultRef Tests
# =============================================================================


class TestPersistedResultRef:
    """Tests for PersistedResultRef dataclass."""

    def test_persisted_result_ref_creation(self) -> None:
        ref = PersistedResultRef(
            job_id="job-123",
            algo_name="analysis_probe",
            algo_version="1.0.0",
            result_id=42,
        )

        assert ref.job_id == "job-123"
        assert ref.algo_name == "analysis_probe"
        assert ref.algo_version == "1.0.0"
        assert ref.result_id == 42

    def test_persisted_result_ref_is_frozen(self) -> None:
        ref = PersistedResultRef(
            job_id="job-123",
            algo_name="analysis_probe",
            algo_version="1.0.0",
            result_id=42,
        )

        with pytest.raises(FrozenInstanceError):
            ref.result_id = 100  # type: ignore[misc]


# =============================================================================
# metrics_to_jsonable Tests
# =============================================================================


class TestMetricsToJsonable:
    """Tests for metrics_to_jsonable function."""

    def test_converts_metric_value_to_dict(self) -> None:
        metrics = {
            "accuracy": MetricValue(
                value=0.95,
                analysis=AnalysisKind.SUMMARY,
                meta={"threshold": 0.5},
            ),
        }

        result = metrics_to_jsonable(metrics)

        assert "accuracy" in result
        assert result["accuracy"]["value"] == 0.95
        assert result["accuracy"]["analysis_mask"] == AnalysisKind.SUMMARY.value
        assert result["accuracy"]["meta"] == {"threshold": 0.5}

    def test_handles_none_meta(self) -> None:
        metrics = {
            "count": MetricValue(value=100, analysis=AnalysisKind.COUNTER),
        }

        result = metrics_to_jsonable(metrics)

        assert result["count"]["meta"] == {}

    def test_handles_combined_analysis_flags(self) -> None:
        combined = AnalysisKind.SUMMARY | AnalysisKind.DISTRIBUTION_1D
        metrics = {
            "score": MetricValue(value=0.5, analysis=combined),
        }

        result = metrics_to_jsonable(metrics)

        assert result["score"]["analysis_mask"] == combined.value

    def test_handles_plain_scalar_backward_compat(self) -> None:
        # Backward compatibility with plain values
        metrics = {"simple": 42}

        result = metrics_to_jsonable(metrics)

        assert result["simple"]["value"] == 42
        assert result["simple"]["analysis_mask"] == 0
        assert result["simple"]["meta"] == {}

    def test_handles_multiple_metrics(self) -> None:
        metrics = {
            "accuracy": MetricValue(value=0.95, analysis=AnalysisKind.SUMMARY),
            "precision": MetricValue(value=0.92, analysis=AnalysisKind.SUMMARY),
            "recall": MetricValue(value=0.88, analysis=AnalysisKind.SUMMARY),
        }

        result = metrics_to_jsonable(metrics)

        assert len(result) == 3
        assert all(k in result for k in ["accuracy", "precision", "recall"])


# =============================================================================
# AnalysisKind Tests
# =============================================================================


class TestAnalysisKind:
    """Tests for AnalysisKind enum."""

    def test_analysis_kind_values(self) -> None:
        # Verify all expected kinds exist
        assert hasattr(AnalysisKind, "SUMMARY")
        assert hasattr(AnalysisKind, "DISTRIBUTION_1D")
        assert hasattr(AnalysisKind, "OUTLIERS_1D")
        assert hasattr(AnalysisKind, "COUNTER")
        assert hasattr(AnalysisKind, "RATE")
        assert hasattr(AnalysisKind, "ELLIPSE_2D")
        assert hasattr(AnalysisKind, "CONTOUR_2D")
        assert hasattr(AnalysisKind, "INFO")

    def test_analysis_kind_is_intflag(self) -> None:
        # Should be combinable with bitwise OR
        combined = AnalysisKind.SUMMARY | AnalysisKind.DISTRIBUTION_1D

        assert AnalysisKind.SUMMARY in combined
        assert AnalysisKind.DISTRIBUTION_1D in combined
        assert AnalysisKind.COUNTER not in combined

    def test_analysis_kind_values_are_powers_of_two(self) -> None:
        # IntFlag requires powers of 2 for proper combination
        for kind in AnalysisKind:
            # Each value should be a power of 2
            assert kind.value > 0
            assert (kind.value & (kind.value - 1)) == 0

    def test_analysis_kind_no_overlap(self) -> None:
        # Each kind should have a unique value
        values = [kind.value for kind in AnalysisKind]
        assert len(values) == len(set(values))

    def test_analysis_kind_can_combine_all(self) -> None:
        # Should be able to combine all kinds
        all_kinds = AnalysisKind(0)
        for kind in AnalysisKind:
            all_kinds |= kind

        for kind in AnalysisKind:
            assert kind in all_kinds
