"""Unit tests for src.visualization.readers.minio_reader.

Tests for:
- LRU cache initialization and reuse across instances
- fetch_npz (happy path, missing object, parse error, empty)
- fetch_npz_by_key (happy path, missing object, parse error)
- fetch_json (happy path, missing object, invalid JSON, fetch error)
- clear_cache
- cache_info property
"""

from __future__ import annotations

import io
import json
from typing import Any
from unittest.mock import MagicMock

import numpy as np
import pytest

from src.visualization.readers.minio_reader import MinioArtifactReader


def _make_npz_bytes(**arrays: Any) -> bytes:
    """Build NPZ bytes from kwargs."""
    buf = io.BytesIO()
    np.savez(buf, **arrays)
    return buf.getvalue()


def _make_storage_mock() -> MagicMock:
    """Stand-in for MinioStorageService with the two retrieve methods."""
    return MagicMock(
        spec=["retrieve_by_hash", "retrieve_by_key"],
    )


class TestInit:
    """Tests for MinioArtifactReader.__init__."""

    def test_default_cache_size(self) -> None:
        storage = _make_storage_mock()

        reader = MinioArtifactReader(storage)

        assert reader._cache_size == 128
        info = reader.cache_info
        assert info["npz"]["maxsize"] == 128
        assert info["json"]["maxsize"] == 128

    def test_custom_cache_size(self) -> None:
        storage = _make_storage_mock()

        reader = MinioArtifactReader(storage, cache_size=4)

        assert reader._cache_size == 4
        info = reader.cache_info
        assert info["npz"]["maxsize"] == 4
        assert info["json"]["maxsize"] == 4

    def test_stores_storage_reference(self) -> None:
        storage = _make_storage_mock()
        reader = MinioArtifactReader(storage)

        assert reader._storage is storage


class TestFetchNpz:
    """Tests for MinioArtifactReader.fetch_npz."""

    def test_returns_arrays_on_success(self) -> None:
        storage = _make_storage_mock()
        arr_a = np.array([1, 2, 3], dtype=np.int64)
        arr_b = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
        storage.retrieve_by_hash.return_value = _make_npz_bytes(a=arr_a, b=arr_b)

        reader = MinioArtifactReader(storage)
        result = reader.fetch_npz("artifacts", "abc123")

        assert result is not None
        assert set(result.keys()) == {"a", "b"}
        np.testing.assert_array_equal(result["a"], arr_a)
        np.testing.assert_array_equal(result["b"], arr_b)

    def test_passes_bucket_and_hash_to_storage(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_hash.return_value = _make_npz_bytes(x=np.array([1]))

        reader = MinioArtifactReader(storage)
        reader.fetch_npz("my-bucket", "deadbeef")

        storage.retrieve_by_hash.assert_called_once_with("my-bucket", "deadbeef")

    def test_returns_none_on_file_not_found(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_hash.side_effect = FileNotFoundError("missing")

        reader = MinioArtifactReader(storage)
        assert reader.fetch_npz("artifacts", "missing-hash") is None

    def test_returns_none_on_invalid_npz_bytes(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_hash.return_value = b"not an npz at all"

        reader = MinioArtifactReader(storage)
        assert reader.fetch_npz("artifacts", "bad") is None

    def test_returns_none_on_storage_exception(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_hash.side_effect = RuntimeError("boom")

        reader = MinioArtifactReader(storage)
        assert reader.fetch_npz("artifacts", "h") is None

    def test_lru_cache_hits(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_hash.return_value = _make_npz_bytes(
            x=np.array([1, 2, 3], dtype=np.int64),
        )

        reader = MinioArtifactReader(storage)
        reader.fetch_npz("b", "h")
        reader.fetch_npz("b", "h")
        reader.fetch_npz("b", "h")

        assert storage.retrieve_by_hash.call_count == 1
        info = reader.cache_info
        assert info["npz"]["hits"] == 2
        assert info["npz"]["misses"] == 1

    def test_different_keys_miss_separately(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_hash.return_value = _make_npz_bytes(x=np.array([1]))

        reader = MinioArtifactReader(storage)
        reader.fetch_npz("b1", "h1")
        reader.fetch_npz("b1", "h2")
        reader.fetch_npz("b2", "h1")

        assert storage.retrieve_by_hash.call_count == 3
        info = reader.cache_info
        assert info["npz"]["misses"] == 3

    def test_preserves_array_dtype_and_shape(self) -> None:
        storage = _make_storage_mock()
        arr = np.arange(12, dtype=np.float64).reshape(3, 4)
        storage.retrieve_by_hash.return_value = _make_npz_bytes(matrix=arr)

        reader = MinioArtifactReader(storage)
        result = reader.fetch_npz("b", "h")

        assert result is not None
        assert result["matrix"].dtype == np.float64
        assert result["matrix"].shape == (3, 4)
        np.testing.assert_array_equal(result["matrix"], arr)


class TestFetchNpzByKey:
    """Tests for MinioArtifactReader.fetch_npz_by_key."""

    def test_returns_arrays_on_success(self) -> None:
        storage = _make_storage_mock()
        arr = np.array([10, 20, 30])
        storage.retrieve_by_key.return_value = _make_npz_bytes(data=arr)

        reader = MinioArtifactReader(storage)
        result = reader.fetch_npz_by_key("bucket", "path/to/object.npz")

        assert result is not None
        assert "data" in result
        np.testing.assert_array_equal(result["data"], arr)

    def test_passes_bucket_and_key_to_storage(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_key.return_value = _make_npz_bytes(x=np.array([1]))

        reader = MinioArtifactReader(storage)
        reader.fetch_npz_by_key("agg", "k/v/blob")

        storage.retrieve_by_key.assert_called_once_with("agg", "k/v/blob")

    def test_returns_none_on_file_not_found(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_key.side_effect = FileNotFoundError("missing")

        reader = MinioArtifactReader(storage)
        assert reader.fetch_npz_by_key("b", "k") is None

    def test_returns_none_on_invalid_npz(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_key.return_value = b"not a real npz"

        reader = MinioArtifactReader(storage)
        assert reader.fetch_npz_by_key("b", "k") is None

    def test_returns_none_on_other_exception(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_key.side_effect = RuntimeError("boom")

        reader = MinioArtifactReader(storage)
        assert reader.fetch_npz_by_key("b", "k") is None

    def test_no_caching_for_by_key(self) -> None:
        """fetch_npz_by_key bypasses the LRU cache and hits storage every time."""
        storage = _make_storage_mock()
        storage.retrieve_by_key.return_value = _make_npz_bytes(x=np.array([1]))

        reader = MinioArtifactReader(storage)
        reader.fetch_npz_by_key("b", "k")
        reader.fetch_npz_by_key("b", "k")
        reader.fetch_npz_by_key("b", "k")

        assert storage.retrieve_by_key.call_count == 3


class TestFetchJson:
    """Tests for MinioArtifactReader.fetch_json."""

    def test_returns_parsed_json_on_success(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_key.return_value = json.dumps({"a": 1, "b": [1, 2]}).encode("utf-8")

        reader = MinioArtifactReader(storage)
        result = reader.fetch_json("bucket", "summary.json")

        assert result == {"a": 1, "b": [1, 2]}

    def test_passes_bucket_and_key_to_storage(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_key.return_value = b'{"ok": true}'

        reader = MinioArtifactReader(storage)
        reader.fetch_json("metrics", "k/v.json")

        storage.retrieve_by_key.assert_called_once_with("metrics", "k/v.json")

    def test_returns_none_when_not_found(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_key.side_effect = FileNotFoundError("nope")

        reader = MinioArtifactReader(storage)
        assert reader.fetch_json("b", "missing.json") is None

    def test_returns_none_on_invalid_json(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_key.return_value = b"{not valid json"

        reader = MinioArtifactReader(storage)
        assert reader.fetch_json("b", "bad.json") is None

    def test_returns_none_on_storage_error(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_key.side_effect = RuntimeError("network kaboom")

        reader = MinioArtifactReader(storage)
        assert reader.fetch_json("b", "k") is None

    def test_lru_cache_hits(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_key.return_value = b'{"x": 1}'

        reader = MinioArtifactReader(storage)
        reader.fetch_json("b", "k")
        reader.fetch_json("b", "k")
        reader.fetch_json("b", "k")

        assert storage.retrieve_by_key.call_count == 1
        info = reader.cache_info
        assert info["json"]["hits"] == 2
        assert info["json"]["misses"] == 1

    def test_different_keys_miss_separately(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_key.return_value = b"{}"

        reader = MinioArtifactReader(storage)
        reader.fetch_json("b", "k1")
        reader.fetch_json("b", "k2")
        reader.fetch_json("b2", "k1")

        assert storage.retrieve_by_key.call_count == 3

    def test_caches_none_on_not_found(self) -> None:
        """A FileNotFoundError result (None) is still cached."""
        storage = _make_storage_mock()
        storage.retrieve_by_key.side_effect = FileNotFoundError("nope")

        reader = MinioArtifactReader(storage)
        reader.fetch_json("b", "k")
        reader.fetch_json("b", "k")

        # Cache replays the cached None without re-hitting storage.
        assert storage.retrieve_by_key.call_count == 1

    def test_returns_complex_nested_structure(self) -> None:
        storage = _make_storage_mock()
        payload = {"a": {"b": {"c": [1, 2, {"d": "e"}]}}, "n": None}
        storage.retrieve_by_key.return_value = json.dumps(payload).encode("utf-8")

        reader = MinioArtifactReader(storage)
        assert reader.fetch_json("b", "k") == payload


class TestClearCache:
    """Tests for MinioArtifactReader.clear_cache."""

    def test_clear_cache_resets_npz_and_json(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_hash.return_value = _make_npz_bytes(x=np.array([1]))
        storage.retrieve_by_key.return_value = b'{"x": 1}'

        reader = MinioArtifactReader(storage)
        reader.fetch_npz("b", "h")
        reader.fetch_json("b", "k")

        # Populated
        info = reader.cache_info
        assert info["npz"]["size"] >= 1
        assert info["json"]["size"] >= 1

        reader.clear_cache()

        info = reader.cache_info
        assert info["npz"]["size"] == 0
        assert info["json"]["size"] == 0
        assert info["npz"]["hits"] == 0
        assert info["json"]["hits"] == 0

    def test_clear_cache_forces_refetch(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_hash.return_value = _make_npz_bytes(x=np.array([1]))

        reader = MinioArtifactReader(storage)
        reader.fetch_npz("b", "h")
        reader.clear_cache()
        reader.fetch_npz("b", "h")

        assert storage.retrieve_by_hash.call_count == 2


class TestCacheInfo:
    """Tests for MinioArtifactReader.cache_info."""

    def test_initial_info(self) -> None:
        storage = _make_storage_mock()
        reader = MinioArtifactReader(storage, cache_size=8)

        info = reader.cache_info
        assert info["npz"] == {"hits": 0, "misses": 0, "size": 0, "maxsize": 8}
        assert info["json"] == {"hits": 0, "misses": 0, "size": 0, "maxsize": 8}

    def test_info_tracks_misses(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_hash.return_value = _make_npz_bytes(x=np.array([1]))
        storage.retrieve_by_key.return_value = b"{}"

        reader = MinioArtifactReader(storage)
        reader.fetch_npz("b", "h1")
        reader.fetch_npz("b", "h2")
        reader.fetch_json("b", "k1")

        info = reader.cache_info
        assert info["npz"]["misses"] == 2
        assert info["json"]["misses"] == 1
        assert info["npz"]["size"] == 2
        assert info["json"]["size"] == 1

    def test_separate_caches_for_npz_and_json(self) -> None:
        storage = _make_storage_mock()
        storage.retrieve_by_hash.return_value = _make_npz_bytes(x=np.array([1]))
        storage.retrieve_by_key.return_value = b'{"x": 1}'

        reader = MinioArtifactReader(storage, cache_size=2)
        reader.fetch_npz("b", "h")
        reader.fetch_json("b", "k")

        info = reader.cache_info
        assert info["npz"]["size"] == 1
        assert info["json"]["size"] == 1
