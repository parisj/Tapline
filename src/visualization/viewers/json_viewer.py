"""JSON viewer for JSON artifact files.

Provides pretty-printing and structured display of JSON data.
"""

from __future__ import annotations

import json
from typing import Any

from src.visualization.viewers.base import ViewerResult


class JSONViewer:
    """Viewer for JSON artifacts.

    Handles artifacts with MIME type 'application/json' or JSON-like content.
    Produces parsed JSON data for structured display.
    """

    @property
    def viewer_type(self) -> str:
        return "json"

    def can_view(self, *, mime: str, aggregation_mask: int) -> bool:
        """Check if this is a JSON artifact."""
        return mime in (
            "application/json",
            "text/json",
            "application/x-json",
        )

    def view(
        self,
        *,
        data: bytes,
        mime: str,
        aggregation_mask: int,
        metadata: dict[str, Any],
    ) -> ViewerResult:
        """Parse and return JSON data for display."""
        try:
            text = data.decode("utf-8")
            parsed = json.loads(text)

            return ViewerResult(
                viewer_type=self.viewer_type,
                render_type="json",
                data=parsed,
                metadata={
                    "size_bytes": len(data),
                    "keys": list(parsed.keys()) if isinstance(parsed, dict) else None,
                    "item_count": (len(parsed) if isinstance(parsed, (dict, list)) else None),
                },
            )
        except json.JSONDecodeError as e:
            return ViewerResult(
                viewer_type=self.viewer_type,
                render_type="error",
                data=None,
                error=f"Invalid JSON: {e}",
            )
        except UnicodeDecodeError as e:
            return ViewerResult(
                viewer_type=self.viewer_type,
                render_type="error",
                data=None,
                error=f"Invalid UTF-8 encoding: {e}",
            )
