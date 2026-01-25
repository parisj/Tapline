"""Image viewer for image artifacts (PNG, JPEG).

Provides base64-encoded image data for display with zoom/download capabilities.
"""

from __future__ import annotations

import base64
from typing import Any

from src.visualization.viewers.base import ViewerResult


class ImageViewer:
    """Viewer for image artifacts.

    Handles artifacts with image MIME types (image/png, image/jpeg, etc.).
    Produces base64-encoded image data for frontend display.
    """

    SUPPORTED_MIMES = frozenset(
        {
            "image/png",
            "image/jpeg",
            "image/jpg",
            "image/gif",
            "image/webp",
            "image/bmp",
        },
    )

    @property
    def viewer_type(self) -> str:
        return "image"

    def can_view(self, *, mime: str, aggregation_mask: int) -> bool:
        """Check if this is an image artifact."""
        return mime in self.SUPPORTED_MIMES

    def view(
        self,
        *,
        data: bytes,
        mime: str,
        aggregation_mask: int,
        metadata: dict[str, Any],
    ) -> ViewerResult:
        """Encode image as base64 for display."""
        try:
            encoded = base64.b64encode(data).decode("ascii")
            data_uri = f"data:{mime};base64,{encoded}"

            # Try to get image dimensions if PIL is available
            dimensions = self._get_dimensions(data)

            return ViewerResult(
                viewer_type=self.viewer_type,
                render_type="image",
                data={
                    "data_uri": data_uri,
                    "mime": mime,
                    "size_bytes": len(data),
                    "width": dimensions[0] if dimensions else None,
                    "height": dimensions[1] if dimensions else None,
                },
                metadata=metadata,
            )
        except Exception as e:
            return ViewerResult(
                viewer_type=self.viewer_type,
                render_type="error",
                data=None,
                error=f"Failed to encode image: {e}",
            )

    def _get_dimensions(self, data: bytes) -> tuple[int, int] | None:
        """Try to get image dimensions using PIL if available."""
        try:
            import io

            from PIL import Image

            img = Image.open(io.BytesIO(data))
            return img.size
        except ImportError:
            return None
        except Exception:
            return None
