"""YOLO segmentation algorithm.

Instance segmentation using Ultralytics YOLO models.
Requires the 'ultralytics' package (optional dependency).

Install with: pixi install --environment ml
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import cv2
import numpy as np

from src.algorithms.base import Algorithm
from src.domain.evaluation import AnalysisKind
from src.domain.results import AlgoResult, Artifact, MetricValue
from src.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Mapping

logger = get_logger(__name__)

# Optional import for ultralytics
try:
    from ultralytics import YOLO

    ULTRALYTICS_AVAILABLE = True
except ImportError:
    ULTRALYTICS_AVAILABLE = False
    YOLO = None


class YoloSegmentationAlgo(Algorithm):
    """YOLO instance segmentation algorithm.

    Uses Ultralytics YOLO models (YOLOv8-seg) for instance segmentation.
    Returns detection counts, class distributions, confidence scores,
    and mask coverage metrics.

    Requires 'ultralytics' package. If not installed, returns error metrics.
    """

    _model: Any  # YOLO model instance
    _model_name: str
    _confidence_threshold: float
    _iou_threshold: float
    _min_detection_count: int
    _device: str

    @property
    def name(self) -> str:
        return "yolo_segmentation"

    @property
    def version(self) -> str:
        return "1.0.0"

    def initialize(self, settings: Mapping[str, Any]) -> None:
        """Initialize YOLO model from settings."""
        if not ULTRALYTICS_AVAILABLE:
            logger.warning("ultralytics package not installed. Install with: pixi install --environment ml")
            self._model = None
            self._model_name = "unavailable"
            return

        model_cfg = settings.get("model", {})
        self._model_name = str(model_cfg.get("name", "yolov8n-seg.pt"))
        self._confidence_threshold = float(model_cfg.get("confidence_threshold", 0.25))
        self._iou_threshold = float(model_cfg.get("iou_threshold", 0.45))
        self._device = str(model_cfg.get("device", "auto"))

        algorithm_cfg = settings.get("algorithm", {})
        self._min_detection_count = int(algorithm_cfg.get("min_detection_count", 1))

        try:
            # Load model (downloads automatically if not cached)
            self._model = YOLO(self._model_name)

            # Determine device
            if self._device == "auto":
                import torch  # noqa: PLC0415

                device = "cuda" if torch.cuda.is_available() else "cpu"
            else:
                device = self._device

            logger.info(
                "YoloSegmentationAlgo initialized: model=%s, device=%s, conf=%.2f, iou=%.2f",
                self._model_name,
                device,
                self._confidence_threshold,
                self._iou_threshold,
            )
        except Exception:
            logger.exception("Failed to load YOLO model")
            self._model = None

    def run(self, image_bytes: bytes, settings: Mapping[str, Any]) -> AlgoResult:  # noqa: ARG002
        """Run YOLO segmentation and return metrics."""
        # Check if ultralytics is available
        if not ULTRALYTICS_AVAILABLE or self._model is None:
            return AlgoResult(
                metrics={
                    "detection_count": MetricValue(
                        value=0,
                        analysis=AnalysisKind.SUMMARY | AnalysisKind.DISTRIBUTION_1D,
                        meta={"error": "ultralytics not installed or model failed to load"},
                    ),
                    "detection_passed": MetricValue(
                        value=False,
                        analysis=AnalysisKind.COUNTER | AnalysisKind.RATE,
                        meta={"true_label": "pass", "false_label": "fail"},
                    ),
                    "model_name": MetricValue(
                        value="unavailable",
                        analysis=AnalysisKind.INFO,
                        meta={"error": "ultralytics not installed"},
                    ),
                },
            )

        # Decode image
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if img is None:
            return AlgoResult(
                metrics={
                    "detection_count": MetricValue(
                        value=0,
                        analysis=AnalysisKind.SUMMARY | AnalysisKind.DISTRIBUTION_1D,
                        meta={"error": "Failed to decode image"},
                    ),
                    "detection_passed": MetricValue(
                        value=False,
                        analysis=AnalysisKind.COUNTER | AnalysisKind.RATE,
                        meta={"true_label": "pass", "false_label": "fail"},
                    ),
                },
            )

        # Run inference
        results = self._model(
            img,
            conf=self._confidence_threshold,
            iou=self._iou_threshold,
            verbose=False,
        )

        # Process results
        result = results[0]  # Single image

        # Extract detections
        boxes = result.boxes
        masks = result.masks

        detection_count = len(boxes) if boxes is not None else 0
        detection_passed = bool(detection_count >= self._min_detection_count)

        # Compute class counts
        class_counts: dict[str, int] = {}
        confidences: list[float] = []
        detections_data: list[dict[str, Any]] = []

        if boxes is not None and detection_count > 0:
            class_ids = boxes.cls.cpu().numpy().astype(int)
            confs = boxes.conf.cpu().numpy()
            xyxy = boxes.xyxy.cpu().numpy()

            for i, (cls_id, conf, box) in enumerate(zip(class_ids, confs, xyxy, strict=True)):
                class_name = result.names[cls_id]
                class_counts[class_name] = class_counts.get(class_name, 0) + 1
                confidences.append(float(conf))

                detections_data.append(
                    {
                        "index": i,
                        "class_id": int(cls_id),
                        "class_name": class_name,
                        "confidence": float(conf),
                        "bbox": [float(x) for x in box],
                    },
                )

        avg_confidence = float(np.mean(confidences)) if confidences else 0.0

        # Compute mask coverage
        coverage_ratio = 0.0
        if masks is not None and len(masks.data) > 0:
            # Combine all masks
            combined_mask = np.zeros(img.shape[:2], dtype=np.uint8)
            for mask in masks.data.cpu().numpy():
                # Resize mask to image size
                mask_resized = cv2.resize(mask.astype(np.uint8), (img.shape[1], img.shape[0]))
                combined_mask = np.maximum(combined_mask, mask_resized)

            coverage_ratio = float(np.count_nonzero(combined_mask) / combined_mask.size)

        # Compute centroid of all detections (for 2D spatial analysis)
        if boxes is not None and detection_count > 0:
            xyxy = boxes.xyxy.cpu().numpy()
            centers_x = (xyxy[:, 0] + xyxy[:, 2]) / 2
            centers_y = (xyxy[:, 1] + xyxy[:, 3]) / 2
            centroid_x = float(np.mean(centers_x))
            centroid_y = float(np.mean(centers_y))
        else:
            centroid_x, centroid_y = 0.0, 0.0

        # Build metrics dict
        metrics: dict[str, MetricValue] = {
            "detection_count": MetricValue(
                value=detection_count,
                analysis=AnalysisKind.SUMMARY | AnalysisKind.DISTRIBUTION_1D,
                meta={
                    "units": "count",
                    "threshold": self._min_detection_count,
                },
            ),
            "class_counts": MetricValue(
                value=class_counts,
                analysis=AnalysisKind.COUNTER,
                meta={
                    "description": "Detection count per class",
                    "total_classes": len(class_counts),
                },
            ),
            "avg_confidence": MetricValue(
                value=avg_confidence,
                analysis=AnalysisKind.SUMMARY | AnalysisKind.DISTRIBUTION_1D | AnalysisKind.OUTLIERS_1D,
                meta={
                    "units": "probability",
                    "range": [0.0, 1.0],
                    "threshold": self._confidence_threshold,
                },
            ),
            "coverage_ratio": MetricValue(
                value=coverage_ratio,
                analysis=AnalysisKind.SUMMARY | AnalysisKind.DISTRIBUTION_1D,
                meta={
                    "units": "ratio",
                    "range": [0.0, 1.0],
                    "description": "Fraction of image covered by detection masks",
                },
            ),
            "detection_centroid_xy": MetricValue(
                value={"x": centroid_x, "y": centroid_y},
                analysis=AnalysisKind.ELLIPSE_2D | AnalysisKind.CONTOUR_2D,
                meta={
                    "coordinate_system": "image",
                    "units": "pixels",
                    "detection_count": detection_count,
                },
            ),
            "detection_passed": MetricValue(
                value=detection_passed,
                analysis=AnalysisKind.COUNTER | AnalysisKind.RATE,
                meta={
                    "true_label": "pass",
                    "false_label": "fail",
                    "threshold": self._min_detection_count,
                    "detection_count": detection_count,
                    "classes_detected": list(class_counts.keys()) if class_counts else [],
                    "avg_confidence": avg_confidence,
                },
            ),
            "model_name": MetricValue(
                value=self._model_name,
                analysis=AnalysisKind.INFO,
                meta={"description": "YOLO model used for inference"},
            ),
        }

        # Generate artifacts
        artifacts = []

        # JSON detection report
        report_data = {
            "algorithm": self.name,
            "version": self.version,
            "model": self._model_name,
            "metrics": {
                "detection_count": detection_count,
                "class_counts": class_counts,
                "avg_confidence": avg_confidence,
                "coverage_ratio": coverage_ratio,
            },
            "detection_passed": detection_passed,
            "detections": detections_data,
            "image_shape": list(img.shape),
            "parameters": {
                "confidence_threshold": self._confidence_threshold,
                "iou_threshold": self._iou_threshold,
                "min_detection_count": self._min_detection_count,
            },
        }

        artifacts.append(
            Artifact(
                name="yolo_detection_report.json",
                mime="application/json",
                data=json.dumps(report_data, indent=2).encode("utf-8"),
            ),
        )

        # Segmentation mask visualization (if masks available)
        if masks is not None and len(masks.data) > 0:
            # Create colored mask overlay
            mask_overlay = np.zeros_like(img)
            colors = [
                (255, 0, 0),
                (0, 255, 0),
                (0, 0, 255),
                (255, 255, 0),
                (255, 0, 255),
                (0, 255, 255),
            ]

            for i, mask in enumerate(masks.data.cpu().numpy()):
                mask_resized = cv2.resize(mask.astype(np.uint8), (img.shape[1], img.shape[0]))
                color = colors[i % len(colors)]
                mask_overlay[mask_resized > 0.5] = color

            # Blend with original image
            blended = cv2.addWeighted(img, 0.6, mask_overlay, 0.4, 0)

            # Encode as PNG
            _, mask_png = cv2.imencode(".png", blended)
            artifacts.append(
                Artifact(
                    name="segmentation_overlay.png",
                    mime="image/png",
                    data=mask_png.tobytes(),
                ),
            )

        return AlgoResult(
            metrics=metrics,
            artifacts=tuple(artifacts),
        )
