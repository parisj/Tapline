"""MinIO artifact reader with LRU caching for visualization."""

from __future__ import annotations

import io
import json
from functools import lru_cache
from typing import TYPE_CHECKING, Any

import numpy as np

from src.utils.logging import get_logger

if TYPE_CHECKING:
    from src.storage.minio_service import MinioStorageService

logger = get_logger(__name__)


class MinioArtifactReader:
    """Reader for MinIO artifacts with LRU caching.

    Provides cached access to NPZ and JSON artifacts stored in MinIO.
    Uses content hash as cache key to ensure correctness.
    """

    def __init__(
        self,
        storage: MinioStorageService,
        cache_size: int = 128,
    ) -> None:
        self._storage = storage
        self._cache_size = cache_size
        self._init_cache()

    def _init_cache(self) -> None:
        """Initialize the LRU cached methods with configured size."""
        # Type alias for NPZ cache data: (keys, arrays_bytes, dtypes, shapes)
        NpzCacheData = tuple[tuple[str, ...], dict[str, bytes], dict[str, str], dict[str, Any]]

        @lru_cache(maxsize=self._cache_size)
        def _cached_fetch_npz(bucket: str, content_hash: str) -> NpzCacheData:
            return self._do_fetch_npz(bucket, content_hash)

        @lru_cache(maxsize=self._cache_size)
        def _cached_fetch_json(bucket: str, key: str) -> str | None:
            return self._do_fetch_json(bucket, key)

        self._cached_fetch_npz = _cached_fetch_npz
        self._cached_fetch_json = _cached_fetch_json

    def _do_fetch_npz(
        self, bucket: str, content_hash: str,
    ) -> tuple[tuple[str, ...], dict[str, bytes], dict[str, str], dict[str, Any]]:
        """Internal: fetch and serialize NPZ data for caching.

        Security note: allow_pickle=False to prevent arbitrary code execution.
        NPZ files should only contain numpy arrays, not pickled Python objects.
        """
        try:
            data = self._storage.retrieve_by_hash(bucket, content_hash)
            with io.BytesIO(data) as buf:
                # SECURITY: allow_pickle=False prevents deserialization attacks
                npz = np.load(buf, allow_pickle=False)
                keys = tuple(npz.files)
                arrays = {k: npz[k].tobytes() for k in keys}
                dtypes = {k: str(npz[k].dtype) for k in keys}
                shapes = {k: npz[k].shape for k in keys}
                return keys, arrays, dtypes, shapes
        except FileNotFoundError:
            return (), {}, {}, {}
        except Exception as e:
            logger.warning("Failed to fetch NPZ %s/%s: %s", bucket, content_hash, e)
            return (), {}, {}, {}

    def _do_fetch_json(self, bucket: str, key: str) -> str | None:
        """Internal: fetch JSON as string for caching."""
        try:
            data = self._storage.retrieve_by_key(bucket, key)
            return data.decode("utf-8")
        except FileNotFoundError:
            return None
        except Exception as e:
            logger.warning("Failed to fetch JSON %s/%s: %s", bucket, key, e)
            return None

    def fetch_npz(self, bucket: str, content_hash: str) -> dict[str, np.ndarray] | None:
        """Fetch and parse NPZ artifact by content hash.

        Args:
            bucket: Bucket name (e.g., "artifacts", "aggregates")
            content_hash: SHA-256 content hash

        Returns:
            Dict mapping array names to numpy arrays, or None if not found

        """
        result = self._cached_fetch_npz(bucket, content_hash)
        if len(result) != 4:
            return None

        keys, arrays, dtypes, shapes = result
        if not keys:
            return None

        return {k: np.frombuffer(arrays[k], dtype=dtypes[k]).reshape(shapes[k]) for k in keys}

    def fetch_npz_by_key(self, bucket: str, key: str) -> dict[str, np.ndarray] | None:
        """Fetch and parse NPZ artifact by object key.

        Args:
            bucket: Bucket name
            key: Object key path

        Returns:
            Dict mapping array names to numpy arrays, or None if not found

        Security note: allow_pickle=False to prevent arbitrary code execution.

        """
        try:
            data = self._storage.retrieve_by_key(bucket, key)
            with io.BytesIO(data) as buf:
                # SECURITY: allow_pickle=False prevents deserialization attacks
                npz = np.load(buf, allow_pickle=False)
                return {k: npz[k] for k in npz.files}
        except FileNotFoundError:
            return None
        except Exception as e:
            logger.warning("Failed to fetch NPZ by key %s/%s: %s", bucket, key, e)
            return None

    def fetch_json(self, bucket: str, key: str) -> dict[str, Any] | None:
        """Fetch and parse JSON artifact by key.

        Args:
            bucket: Bucket name
            key: Object key path

        Returns:
            Parsed JSON dict, or None if not found

        """
        json_str = self._cached_fetch_json(bucket, key)
        if json_str is None:
            return None
        try:
            result: dict[str, Any] = json.loads(json_str)
            return result
        except json.JSONDecodeError as e:
            logger.warning("Failed to parse JSON %s/%s: %s", bucket, key, e)
            return None

    def clear_cache(self) -> None:
        """Clear all cached data."""
        self._cached_fetch_npz.cache_clear()
        self._cached_fetch_json.cache_clear()

    @property
    def cache_info(self) -> dict[str, Any]:
        """Get cache statistics."""
        npz_info = self._cached_fetch_npz.cache_info()
        json_info = self._cached_fetch_json.cache_info()
        return {
            "npz": {
                "hits": npz_info.hits,
                "misses": npz_info.misses,
                "size": npz_info.currsize,
                "maxsize": npz_info.maxsize,
            },
            "json": {
                "hits": json_info.hits,
                "misses": json_info.misses,
                "size": json_info.currsize,
                "maxsize": json_info.maxsize,
            },
        }
