"""YOLO segmentation processor.

Instance segmentation using Ultralytics YOLO models.
Requires the 'ultralytics' package (optional dependency).

Install with: pixi install --environment ml
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import cv2
import numpy as np

from src.algorithms.base import Processor
from src.domain.evaluation import AggregationType
from src.domain.results import Artifact, Measurement, ProcessorResult
from src.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Mapping

logger = get_logger(__name__)

# Optional import for ultralytics
YOLO: Any = None
ULTRALYTICS_AVAILABLE = False
try:
    from ultralytics import YOLO

    ULTRALYTICS_AVAILABLE = True
except ImportError:
    pass


class YoloSegmentationProcessor(Processor):
    """YOLO instance segmentation processor.

    Uses Ultralytics YOLO models (YOLOv8-seg) for instance segmentation.
    Returns detection counts, class distributions, confidence scores,
    and mask coverage metrics.

    Requires 'ultralytics' package. Fails fast at initialization if not installed.
    """

    _model: Any  # YOLO model instance
    _model_name: str
    _confidence_threshold: float
    _iou_threshold: float
    _min_detection_count: int
    _device: str

    @property
    def name(self) -> str:
        return "model_yolo_segmentation"

    @property
    def version(self) -> str:
        return "1.0.0"

    def initialize(self, settings: Mapping[str, Any]) -> None:
        """Initialize YOLO model from settings.

        Raises:
            RuntimeError: If ultralytics is not installed or model fails to load.
                         Fail-fast behavior ensures issues are caught at startup.

        """
        if not ULTRALYTICS_AVAILABLE:
            msg = (
                "ultralytics package not installed. "
                "Install with: pixi install --environment ml or pixi install --environment gpu-ml"
            )
            logger.error(msg)
            raise RuntimeError(msg)

        model_cfg = settings.get("model", {})
        self._model_name = str(model_cfg.get("name", "yolov8n-seg.pt"))
        self._confidence_threshold = float(model_cfg.get("confidence_threshold", 0.25))
        self._iou_threshold = float(model_cfg.get("iou_threshold", 0.45))
        self._device = str(model_cfg.get("device", "auto"))

        processor_cfg = settings.get("processor", settings.get("algorithm", {}))
        self._min_detection_count = int(processor_cfg.get("min_detection_count", 1))

        # Fail-fast: load model at initialization, not on first job
        try:
            # Load model (downloads automatically if not cached)
            self._model = YOLO(self._model_name)

            # Determine device
            if self._device == "auto":
                import torch  # noqa: PLC0415

                device = "cuda" if torch.cuda.is_available() else "cpu"
            else:
                device = self._device

            # Validate model is usable by checking it has the expected interface
            if not hasattr(self._model, "predict") and not callable(self._model):
                msg = f"YOLO model {self._model_name} is not callable"
                raise RuntimeError(msg)

            logger.info(
                "YoloSegmentationProcessor initialized: model=%s, device=%s, conf=%.2f, iou=%.2f",
                self._model_name,
                device,
                self._confidence_threshold,
                self._iou_threshold,
            )
        except Exception as e:
            msg = f"Failed to load YOLO model '{self._model_name}': {e}"
            logger.exception(msg)
            raise RuntimeError(msg) from e

    def run(self, image_bytes: bytes, settings: Mapping[str, Any]) -> ProcessorResult:  # noqa: ARG002
        """Run YOLO segmentation and return metrics.

        Note: Model availability is guaranteed by fail-fast initialization.
        If we reach run(), the model is ready.
        """
        # Decode image
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if img is None:
            return ProcessorResult(
                metrics={
                    "detection_count": Measurement(
                        value=0,
                        aggregation=AggregationType.STATS | AggregationType.HISTOGRAM,
                        meta={"error": "Failed to decode image"},
                    ),
                    "detection_passed": Measurement(
                        value=False,
                        aggregation=AggregationType.TALLY | AggregationType.RATE,
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
        metrics: dict[str, Measurement] = {
            "detection_count": Measurement(
                value=detection_count,
                aggregation=AggregationType.STATS | AggregationType.HISTOGRAM,
                meta={
                    "units": "count",
                    "threshold": self._min_detection_count,
                },
            ),
            "class_counts": Measurement(
                value=class_counts,
                aggregation=AggregationType.TALLY,
                meta={
                    "description": "Detection count per class",
                    "total_classes": len(class_counts),
                },
            ),
            "avg_confidence": Measurement(
                value=avg_confidence,
                aggregation=AggregationType.STATS | AggregationType.HISTOGRAM | AggregationType.OUTLIERS,
                meta={
                    "units": "probability",
                    "range": [0.0, 1.0],
                    "threshold": self._confidence_threshold,
                },
            ),
            "coverage_ratio": Measurement(
                value=coverage_ratio,
                aggregation=AggregationType.STATS | AggregationType.HISTOGRAM,
                meta={
                    "units": "ratio",
                    "range": [0.0, 1.0],
                    "description": "Fraction of image covered by detection masks",
                },
            ),
            "detection_centroid_xy": Measurement(
                value={"x": centroid_x, "y": centroid_y},
                aggregation=AggregationType.SCATTER_ELLIPSE | AggregationType.DENSITY_MAP,
                meta={
                    "coordinate_system": "image",
                    "units": "pixels",
                    "detection_count": detection_count,
                },
            ),
            "detection_passed": Measurement(
                value=detection_passed,
                aggregation=AggregationType.TALLY | AggregationType.RATE,
                meta={
                    "true_label": "pass",
                    "false_label": "fail",
                    "threshold": self._min_detection_count,
                    "detection_count": detection_count,
                    "classes_detected": list(class_counts.keys()) if class_counts else [],
                    "avg_confidence": avg_confidence,
                },
            ),
            "model_name": Measurement(
                value=self._model_name,
                aggregation=AggregationType.RAW,
                meta={"description": "YOLO model used for inference"},
            ),
        }

        # Generate artifacts
        artifacts = []

        # JSON detection report
        report_data = {
            "processor": self.name,
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

        return ProcessorResult(
            metrics=metrics,
            artifacts=tuple(artifacts),
        )


# Backwards compatibility alias
YoloSegmentationAlgo = YoloSegmentationProcessor
