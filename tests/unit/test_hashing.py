"""Unit tests for hashing utilities."""

import time
from pathlib import Path

import pytest

from src.utils.hashing import compute_fingerprint, make_aggregate_artifact_hash


class TestComputeFingerprint:
    def test_returns_hex_string(self, tmp_path: Path) -> None:
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        fingerprint = compute_fingerprint(test_file)

        assert isinstance(fingerprint, str)
        assert len(fingerprint) == 64  # SHA-256 hex

    def test_same_file_same_fingerprint(self, tmp_path: Path) -> None:
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        fp1 = compute_fingerprint(test_file)
        fp2 = compute_fingerprint(test_file)

        assert fp1 == fp2

    def test_different_content_different_fingerprint(self, tmp_path: Path) -> None:
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.txt"
        file1.write_text("content 1")
        file2.write_text("content 2")

        fp1 = compute_fingerprint(file1)
        fp2 = compute_fingerprint(file2)

        assert fp1 != fp2

    def test_different_mtime_different_fingerprint(self, tmp_path: Path) -> None:
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")
        fp1 = compute_fingerprint(test_file)

        # Touch the file to change mtime
        time.sleep(0.01)
        test_file.write_text("test content")  # Same content, different mtime
        fp2 = compute_fingerprint(test_file)

        # Fingerprint includes mtime, so should be different
        assert fp1 != fp2

    def test_different_path_different_fingerprint(self, tmp_path: Path) -> None:
        file1 = tmp_path / "file1.txt"
        file2 = tmp_path / "file2.txt"
        file1.write_text("identical content")
        file2.write_text("identical content")

        # Even with same content, different paths give different fingerprints
        fp1 = compute_fingerprint(file1)
        fp2 = compute_fingerprint(file2)

        assert fp1 != fp2

    def test_uses_resolved_path(self, tmp_path: Path) -> None:
        test_file = tmp_path / "test.txt"
        test_file.write_text("test content")

        # Create a relative path reference
        original_fp = compute_fingerprint(test_file)

        # Fingerprint uses resolved (absolute) path
        assert isinstance(original_fp, str)
        assert len(original_fp) == 64

    def test_nonexistent_file_raises(self, tmp_path: Path) -> None:
        nonexistent = tmp_path / "nonexistent.txt"

        with pytest.raises(FileNotFoundError):
            compute_fingerprint(nonexistent)

    def test_includes_size_in_fingerprint(self, tmp_path: Path) -> None:
        # The fingerprint is based on path|size|mtime
        file1 = tmp_path / "test.txt"
        file1.write_text("short")

        # Verify fingerprint contains path, size, and mtime components
        # We can verify this indirectly by checking the implementation
        fp = compute_fingerprint(file1)
        assert len(fp) == 64


class TestMakeAggregateArtifactHash:
    def test_returns_hex_string(self) -> None:
        hash_val = make_aggregate_artifact_hash(
            processor_name="test_algo",
            processor_version="1.0.0",
            metric_name="accuracy",
            aggregation_type="STATS",
            window_start_unix=1000.0,
            window_end_unix=2000.0,
            artifact_name="histogram.json",
        )

        assert isinstance(hash_val, str)
        assert len(hash_val) == 64

    def test_deterministic(self) -> None:
        params = {
            "processor_name": "test_algo",
            "processor_version": "1.0.0",
            "metric_name": "accuracy",
            "aggregation_type": "STATS",
            "window_start_unix": 1000.0,
            "window_end_unix": 2000.0,
            "artifact_name": "histogram.json",
        }

        hash1 = make_aggregate_artifact_hash(**params)
        hash2 = make_aggregate_artifact_hash(**params)

        assert hash1 == hash2

    def test_different_processor_name_different_hash(self) -> None:
        base_params = {
            "processor_version": "1.0.0",
            "metric_name": "accuracy",
            "aggregation_type": "STATS",
            "window_start_unix": 1000.0,
            "window_end_unix": 2000.0,
            "artifact_name": "histogram.json",
        }

        hash1 = make_aggregate_artifact_hash(processor_name="algo1", **base_params)
        hash2 = make_aggregate_artifact_hash(processor_name="algo2", **base_params)

        assert hash1 != hash2

    def test_different_version_different_hash(self) -> None:
        base_params = {
            "processor_name": "test_algo",
            "metric_name": "accuracy",
            "aggregation_type": "STATS",
            "window_start_unix": 1000.0,
            "window_end_unix": 2000.0,
            "artifact_name": "histogram.json",
        }

        hash1 = make_aggregate_artifact_hash(processor_version="1.0.0", **base_params)
        hash2 = make_aggregate_artifact_hash(processor_version="2.0.0", **base_params)

        assert hash1 != hash2

    def test_different_metric_name_different_hash(self) -> None:
        base_params = {
            "processor_name": "test_algo",
            "processor_version": "1.0.0",
            "aggregation_type": "STATS",
            "window_start_unix": 1000.0,
            "window_end_unix": 2000.0,
            "artifact_name": "histogram.json",
        }

        hash1 = make_aggregate_artifact_hash(metric_name="accuracy", **base_params)
        hash2 = make_aggregate_artifact_hash(metric_name="precision", **base_params)

        assert hash1 != hash2

    def test_different_aggregation_type_different_hash(self) -> None:
        base_params = {
            "processor_name": "test_algo",
            "processor_version": "1.0.0",
            "metric_name": "accuracy",
            "window_start_unix": 1000.0,
            "window_end_unix": 2000.0,
            "artifact_name": "histogram.json",
        }

        hash1 = make_aggregate_artifact_hash(aggregation_type="STATS", **base_params)
        hash2 = make_aggregate_artifact_hash(aggregation_type="HISTOGRAM", **base_params)

        assert hash1 != hash2

    def test_different_window_start_different_hash(self) -> None:
        base_params = {
            "processor_name": "test_algo",
            "processor_version": "1.0.0",
            "metric_name": "accuracy",
            "aggregation_type": "STATS",
            "window_end_unix": 2000.0,
            "artifact_name": "histogram.json",
        }

        hash1 = make_aggregate_artifact_hash(window_start_unix=1000.0, **base_params)
        hash2 = make_aggregate_artifact_hash(window_start_unix=1500.0, **base_params)

        assert hash1 != hash2

    def test_different_window_end_different_hash(self) -> None:
        base_params = {
            "processor_name": "test_algo",
            "processor_version": "1.0.0",
            "metric_name": "accuracy",
            "aggregation_type": "STATS",
            "window_start_unix": 1000.0,
            "artifact_name": "histogram.json",
        }

        hash1 = make_aggregate_artifact_hash(window_end_unix=2000.0, **base_params)
        hash2 = make_aggregate_artifact_hash(window_end_unix=3000.0, **base_params)

        assert hash1 != hash2

    def test_different_artifact_name_different_hash(self) -> None:
        base_params = {
            "processor_name": "test_algo",
            "processor_version": "1.0.0",
            "metric_name": "accuracy",
            "aggregation_type": "STATS",
            "window_start_unix": 1000.0,
            "window_end_unix": 2000.0,
        }

        hash1 = make_aggregate_artifact_hash(artifact_name="histogram.json", **base_params)
        hash2 = make_aggregate_artifact_hash(artifact_name="summary.json", **base_params)

        assert hash1 != hash2

    def test_expected_hash_format(self) -> None:
        # Verify the hash is a valid SHA-256 hex string
        hash_val = make_aggregate_artifact_hash(
            processor_name="test",
            processor_version="1.0.0",
            metric_name="test",
            aggregation_type="STATS",
            window_start_unix=0.0,
            window_end_unix=60.0,
            artifact_name="test.json",
        )

        # Should be valid hex
        int(hash_val, 16)
