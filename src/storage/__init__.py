"""Storage module for MinIO-based artifact storage.

Provides content-addressed storage for artifacts with:
- Hash-based paths for deduplication
- Bucket organization by artifact type
- GxP-compliant audit trail integration
"""

from src.storage.config import MinioConfig, load_minio_config
from src.storage.minio_service import MinioStorageService
from src.storage.models import ObjectRef

__all__ = [
    "MinioConfig",
    "MinioStorageService",
    "ObjectRef",
    "load_minio_config",
]
