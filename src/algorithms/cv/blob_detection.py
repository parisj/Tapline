from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import cv2
import numpy as np

from src.algorithms.base import Processor
from src.domain.evaluation import AggregationType
from src.domain.results import Artifact, Measurement, ProcessorResult
from src.utils.gpu import GPUContext, get_gpu_state
from src.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Mapping

logger = get_logger(__name__)


class BlobDetectionProcessor(Processor):
    """Blob detection processor using OpenCV's SimpleBlobDetector.

    Detects and counts objects/blobs in images, returning metrics about
    blob count, sizes, and positions for downstream analysis.

    Supports optional GPU acceleration via OpenCV CUDA when available.
    """

    _detector: cv2.SimpleBlobDetector
    _use_gpu: bool
    _gpu_context: GPUContext | None

    @property
    def name(self) -> str:
        return "blob_detection"

    @property
    def version(self) -> str:
        return "1.0.0"

    def initialize(self, settings: Mapping[str, Any]) -> None:
        """Initialize the blob detector with parameters from settings."""
        detector_cfg = settings.get("detector", {})

        params = cv2.SimpleBlobDetector_Params()  # type: ignore[attr-defined]

        # Thresholding
        params.minThreshold = float(detector_cfg.get("min_threshold", 10))
        params.maxThreshold = float(detector_cfg.get("max_threshold", 200))
        params.thresholdStep = float(detector_cfg.get("threshold_step", 10))

        # Filter by area
        params.filterByArea = True
        params.minArea = float(detector_cfg.get("min_area", 100))
        params.maxArea = float(detector_cfg.get("max_area", 50000))

        # Filter by circularity
        params.filterByCircularity = True
        params.minCircularity = float(detector_cfg.get("min_circularity", 0.1))

        # Filter by convexity
        params.filterByConvexity = True
        params.minConvexity = float(detector_cfg.get("min_convexity", 0.5))

        # Filter by inertia
        params.filterByInertia = True
        params.minInertiaRatio = float(detector_cfg.get("min_inertia_ratio", 0.1))

        # Filter by color
        params.filterByColor = bool(detector_cfg.get("filter_by_color", True))
        params.blobColor = int(detector_cfg.get("blob_color", 0))

        self._detector = cv2.SimpleBlobDetector_create(params)  # type: ignore[attr-defined]

        # Initialize GPU context
        self._gpu_context = GPUContext()
        gpu_state = get_gpu_state()
        self._use_gpu = gpu_state.opencv_cuda

        if self._use_gpu:
            logger.info(
                "BlobDetectionProcessor: GPU acceleration enabled (device: %s)",
                gpu_state.device_name,
            )
        else:
            logger.info("BlobDetectionProcessor: Using CPU processing")

    def _preprocess_gpu(self, img: np.ndarray) -> np.ndarray:
        """Preprocess image using GPU acceleration.

        Uses CUDA for:
        - Color conversion (BGR to grayscale)
        - Gaussian blur (noise reduction)
        - Contrast enhancement (CLAHE)

        Args:
            img: Input BGR image.

        Returns:
            Preprocessed grayscale image.

        """
        # Upload to GPU
        gpu_img = cv2.cuda_GpuMat()  # type: ignore[attr-defined]
        gpu_img.upload(img)

        # Convert to grayscale on GPU
        gpu_gray = cv2.cuda.cvtColor(gpu_img, cv2.COLOR_BGR2GRAY)  # type: ignore[attr-defined]

        # Apply Gaussian blur on GPU for noise reduction
        gpu_blur = cv2.cuda.createGaussianFilter(  # type: ignore[attr-defined]
            cv2.CV_8UC1,
            cv2.CV_8UC1,
            (5, 5),
            0,
        )
        gpu_blurred = gpu_blur.apply(gpu_gray)

        # Download result back to CPU
        # Note: SimpleBlobDetector doesn't have CUDA version, so we process on CPU
        return gpu_blurred.download()  # type: ignore[no-any-return]

    def _preprocess_cpu(self, img: np.ndarray) -> np.ndarray:
        """Preprocess image using CPU.

        Args:
            img: Input BGR image.

        Returns:
            Preprocessed grayscale image.

        """
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        # Apply Gaussian blur for noise reduction (matching GPU path)
        return cv2.GaussianBlur(gray, (5, 5), 0)

    def run(self, image_bytes: bytes, settings: Mapping[str, Any]) -> ProcessorResult:
        """Detect blobs in the image and return metrics."""
        algorithm_cfg = settings.get("algorithm", {})
        min_blob_count = int(algorithm_cfg.get("min_blob_count", 1))

        # Decode image
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if img is None:
            return ProcessorResult(
                metrics={
                    "blob_count": Measurement(
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

        # Preprocess image (GPU or CPU path)
        assert self._gpu_context is not None
        with self._gpu_context as gpu:
            if gpu.use_opencv_cuda:
                try:
                    gray = self._preprocess_gpu(img)
                    accelerator = "gpu"
                except cv2.error as e:
                    logger.warning("GPU preprocessing failed, falling back to CPU: %s", e)
                    gray = self._preprocess_cpu(img)
                    accelerator = "cpu"
            else:
                gray = self._preprocess_cpu(img)
                accelerator = "cpu"

        # Detect blobs (CPU - SimpleBlobDetector has no CUDA version)
        keypoints = self._detector.detect(gray)

        blob_count = len(keypoints)
        detection_passed = bool(blob_count >= min_blob_count)

        # Calculate blob metrics
        sizes = [kp.size for kp in keypoints]
        avg_size = sum(sizes) / len(sizes) if sizes else 0.0

        # Build blob data for artifact
        blob_data = [{"x": float(kp.pt[0]), "y": float(kp.pt[1]), "size": float(kp.size)} for kp in keypoints]

        # Calculate centroid of all blobs (for spatial aggregation)
        if keypoints:
            centroid_x = sum(kp.pt[0] for kp in keypoints) / len(keypoints)
            centroid_y = sum(kp.pt[1] for kp in keypoints) / len(keypoints)
        else:
            centroid_x, centroid_y = 0.0, 0.0

        # Build metrics dict
        metrics: dict[str, Measurement] = {
            "blob_count": Measurement(
                value=blob_count,
                aggregation=AggregationType.STATS | AggregationType.HISTOGRAM,
                meta={"units": "count"},
            ),
            "avg_blob_size": Measurement(
                value=avg_size,
                aggregation=AggregationType.STATS | AggregationType.HISTOGRAM | AggregationType.OUTLIERS,
                meta={"units": "pixels", "range": [0.0, max(sizes) if sizes else 0.0]},
            ),
            "detection_passed": Measurement(
                value=detection_passed,
                aggregation=AggregationType.TALLY | AggregationType.RATE,
                meta={
                    "true_label": "pass",
                    "false_label": "fail",
                    "threshold": min_blob_count,
                    "blob_count": blob_count,
                    "avg_blob_size": avg_size,
                },
            ),
            # Single 2D metric: centroid of all detected blobs per image
            # Aggregates across images to show spatial distribution of blob centroids
            "blob_centroid_xy": Measurement(
                value={"x": centroid_x, "y": centroid_y},
                aggregation=AggregationType.SCATTER_ELLIPSE | AggregationType.DENSITY_MAP,
                meta={
                    "coordinate_system": "image",
                    "units": "pixels",
                    "blob_count": blob_count,
                },
            ),
        }

        # Generate JSON artifact with detection results
        artifact_data = {
            "algorithm": self.name,
            "version": self.version,
            "accelerator": accelerator,
            "blob_count": blob_count,
            "detection_passed": detection_passed,
            "avg_blob_size": avg_size,
            "blobs": blob_data,
            "image_shape": list(img.shape),
        }

        return ProcessorResult(
            metrics=metrics,
            artifacts=(
                Artifact(
                    name="blob_detection_report.json",
                    mime="application/json",
                    data=json.dumps(artifact_data, indent=2).encode("utf-8"),
                ),
            ),
        )


# Backwards compatibility alias
BlobDetectionAlgo = BlobDetectionProcessor
