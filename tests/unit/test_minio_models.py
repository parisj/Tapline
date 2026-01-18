"""Unit tests for MinIO storage models."""

from datetime import datetime

import pytest

from src.storage.models import ObjectRef, hash_to_key, key_to_hash


class TestHashToKey:
    def test_default_prefix_length(self) -> None:
        hash_val = "abcdef1234567890"
        key = hash_to_key(hash_val)
        assert key == "ab/cd/abcdef1234567890"

    def test_custom_prefix_length(self) -> None:
        hash_val = "abcdef1234567890"
        key = hash_to_key(hash_val, prefix_length=6)
        assert key == "ab/cd/ef/abcdef1234567890"

    def test_short_prefix(self) -> None:
        hash_val = "abcdef1234567890"
        key = hash_to_key(hash_val, prefix_length=2)
        assert key == "ab/abcdef1234567890"

    def test_odd_prefix_rounded_down(self) -> None:
        hash_val = "abcdef1234567890"
        key = hash_to_key(hash_val, prefix_length=3)
        # Rounds down to 2
        assert key == "ab/abcdef1234567890"

    def test_full_sha256_hash(self) -> None:
        full_hash = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
        key = hash_to_key(full_hash)
        assert key == f"ab/cd/{full_hash}"


class TestKeyToHash:
    def test_extract_hash(self) -> None:
        key = "ab/cd/abcdef1234567890"
        hash_val = key_to_hash(key)
        assert hash_val == "abcdef1234567890"

    def test_deeper_path(self) -> None:
        key = "ab/cd/ef/gh/fullhashvalue"
        hash_val = key_to_hash(key)
        assert hash_val == "fullhashvalue"

    def test_no_path_separator(self) -> None:
        key = "justhash"
        hash_val = key_to_hash(key)
        assert hash_val == "justhash"

    def test_roundtrip(self) -> None:
        original_hash = "abcdef1234567890abcdef1234567890"
        key = hash_to_key(original_hash)
        extracted = key_to_hash(key)
        assert extracted == original_hash


class TestObjectRef:
    def test_from_hash(self) -> None:
        ref = ObjectRef.from_hash(
            content_hash="abcdef1234567890",
            bucket="artifacts",
            size=1024,
        )

        assert ref.content_hash == "abcdef1234567890"
        assert ref.bucket == "artifacts"
        assert ref.key == "ab/cd/abcdef1234567890"
        assert ref.size == 1024
        assert isinstance(ref.stored_at, datetime)

    def test_from_hash_with_timestamp(self) -> None:
        ts = datetime(2024, 1, 15, 12, 0, 0)
        ref = ObjectRef.from_hash(
            content_hash="abcdef1234567890",
            bucket="artifacts",
            size=512,
            stored_at=ts,
        )

        assert ref.stored_at == ts

    def test_from_hash_custom_prefix(self) -> None:
        ref = ObjectRef.from_hash(
            content_hash="abcdef1234567890",
            bucket="inputs",
            size=2048,
            prefix_length=6,
        )

        assert ref.key == "ab/cd/ef/abcdef1234567890"

    def test_full_path(self) -> None:
        ref = ObjectRef(
            content_hash="hash123",
            bucket="artifacts",
            key="ab/cd/hash123",
            size=100,
            stored_at=datetime.now(),
        )

        assert ref.full_path == "artifacts/ab/cd/hash123"

    def test_to_dict(self) -> None:
        ts = datetime(2024, 1, 15, 12, 0, 0)
        ref = ObjectRef(
            content_hash="hash123",
            bucket="artifacts",
            key="ab/cd/hash123",
            size=100,
            stored_at=ts,
        )

        data = ref.to_dict()
        assert data["content_hash"] == "hash123"
        assert data["bucket"] == "artifacts"
        assert data["key"] == "ab/cd/hash123"
        assert data["size"] == 100
        assert data["stored_at"] == ts.isoformat()

    def test_from_dict(self) -> None:
        data = {
            "content_hash": "hash123",
            "bucket": "artifacts",
            "key": "ab/cd/hash123",
            "size": 100,
            "stored_at": "2024-01-15T12:00:00",
        }

        ref = ObjectRef.from_dict(data)
        assert ref.content_hash == "hash123"
        assert ref.bucket == "artifacts"
        assert ref.size == 100
        assert ref.stored_at == datetime(2024, 1, 15, 12, 0, 0)

    def test_from_dict_with_datetime(self) -> None:
        ts = datetime(2024, 1, 15, 12, 0, 0)
        data = {
            "content_hash": "hash123",
            "bucket": "artifacts",
            "key": "ab/cd/hash123",
            "size": 100,
            "stored_at": ts,
        }

        ref = ObjectRef.from_dict(data)
        assert ref.stored_at == ts

    def test_immutability(self) -> None:
        ref = ObjectRef(
            content_hash="hash123",
            bucket="artifacts",
            key="ab/cd/hash123",
            size=100,
            stored_at=datetime.now(),
        )

        with pytest.raises(AttributeError):
            ref.bucket = "new-bucket"
