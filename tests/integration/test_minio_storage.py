"""Integration tests for MinIO storage service."""

import pytest


class TestMinioStorageIntegration:
    """Integration tests for MinIO storage operations."""

    def test_store_and_retrieve(self, minio_storage, sample_image_bytes) -> None:
        """Test basic store and retrieve cycle."""
        ref = minio_storage.store(
            data=sample_image_bytes,
            bucket=minio_storage.buckets["artifacts"],
            mime="image/png",
        )

        assert ref.content_hash is not None
        assert ref.size == len(sample_image_bytes)

        retrieved = minio_storage.retrieve(ref)
        assert retrieved == sample_image_bytes

    def test_deduplication(self, minio_storage, sample_image_bytes) -> None:
        """Test that storing same data twice returns same reference."""
        ref1 = minio_storage.store(
            data=sample_image_bytes,
            bucket=minio_storage.buckets["artifacts"],
        )

        ref2 = minio_storage.store(
            data=sample_image_bytes,
            bucket=minio_storage.buckets["artifacts"],
        )

        assert ref1.content_hash == ref2.content_hash
        assert ref1.key == ref2.key

    def test_exists_check(self, minio_storage, sample_image_bytes) -> None:
        """Test existence checking."""
        ref = minio_storage.store(
            data=sample_image_bytes,
            bucket=minio_storage.buckets["artifacts"],
        )

        assert minio_storage.exists(minio_storage.buckets["artifacts"], ref.content_hash)
        assert not minio_storage.exists(minio_storage.buckets["artifacts"], "nonexistent")

    def test_retrieve_nonexistent_raises(self, minio_storage) -> None:
        """Test that retrieving nonexistent object raises error."""
        with pytest.raises(FileNotFoundError):
            minio_storage.retrieve_by_hash(
                bucket=minio_storage.buckets["artifacts"],
                content_hash="nonexistent123456789012345678901234567890",
            )

    def test_store_with_metadata(self, minio_storage, sample_image_bytes) -> None:
        """Test storing with custom metadata."""
        ref = minio_storage.store(
            data=sample_image_bytes,
            bucket=minio_storage.buckets["artifacts"],
            mime="image/png",
            metadata={"task_id": "test-task-123", "name": "output.png"},
        )

        info = minio_storage.get_object_info(ref)
        assert info is not None
        assert info["size"] == len(sample_image_bytes)
        # Metadata keys are prefixed with x-amz-meta- by MinIO

    def test_list_objects(self, minio_storage, sample_image_bytes) -> None:
        """Test listing objects in bucket."""
        # Store a few objects
        refs = []
        for i in range(3):
            data = sample_image_bytes + bytes([i])
            ref = minio_storage.store(
                data=data,
                bucket=minio_storage.buckets["artifacts"],
            )
            refs.append(ref)

        objects = minio_storage.list_objects(
            bucket=minio_storage.buckets["artifacts"],
        )

        assert len(objects) >= 3
        keys = {obj["key"] for obj in objects}
        for ref in refs:
            assert ref.key in keys

    def test_delete_object(self, minio_storage, sample_image_bytes) -> None:
        """Test deleting an object."""
        ref = minio_storage.store(
            data=sample_image_bytes,
            bucket=minio_storage.buckets["artifacts"],
        )

        assert minio_storage.exists_ref(ref)

        deleted = minio_storage.delete(ref)
        assert deleted is True

        assert not minio_storage.exists_ref(ref)

    def test_different_buckets(self, minio_storage, sample_image_bytes) -> None:
        """Test storing in different buckets."""
        ref_artifacts = minio_storage.store(
            data=sample_image_bytes,
            bucket=minio_storage.buckets["artifacts"],
        )

        ref_inputs = minio_storage.store(
            data=sample_image_bytes,
            bucket=minio_storage.buckets["inputs"],
        )

        # Same hash, different buckets
        assert ref_artifacts.content_hash == ref_inputs.content_hash
        assert ref_artifacts.bucket != ref_inputs.bucket

        # Both should be retrievable
        assert minio_storage.retrieve(ref_artifacts) == sample_image_bytes
        assert minio_storage.retrieve(ref_inputs) == sample_image_bytes
