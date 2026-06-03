"""Unit tests for YoloSegmentationProcessor.

Characterization tests for ``src/algorithms/models/yolo_segmentation.py``.

In the default test environment the optional ``ultralytics`` package is not
installed, so ``ULTRALYTICS_AVAILABLE`` is False and ``initialize`` raises
``RuntimeError``. We therefore exercise the parts of the processor that do
not require a real model load:

* identity (name / version / backwards-compat alias)
* the import flag
* the fail-fast behaviour of ``initialize`` when ultralytics is missing
* the image-decode failure branch of ``run``
* the "with detections" branch of ``run``, by injecting a fake YOLO model
  object onto a manually-prepared processor instance (bypasses ``initialize``)
"""

from __future__ import annotations

import json
from typing import Any

import cv2
import numpy as np
import pytest

from src.algorithms.models import yolo_segmentation as yolo_mod
from src.algorithms.models.yolo_segmentation import (
    YoloSegmentationAlgo,
    YoloSegmentationProcessor,
)
from src.domain.evaluation import AggregationType
from src.domain.results import Artifact, Measurement, ProcessorResult

# ---------------------------------------------------------------------------
# Helpers: fake YOLO model that mimics the ultralytics Results API used in run()
# ---------------------------------------------------------------------------


class _FakeTensor:
    """Minimal stand-in for a torch tensor exposing ``.cpu().numpy()`` and ``len()``."""

    def __init__(self, array: np.ndarray) -> None:
        self._array = array

    def cpu(self) -> _FakeTensor:
        return self

    def numpy(self) -> np.ndarray:
        return self._array

    def __len__(self) -> int:
        return len(self._array)


class _FakeBoxes:
    def __init__(
        self,
        cls: np.ndarray,
        conf: np.ndarray,
        xyxy: np.ndarray,
    ) -> None:
        self.cls = _FakeTensor(cls)
        self.conf = _FakeTensor(conf)
        self.xyxy = _FakeTensor(xyxy)
        self._n = len(cls)

    def __len__(self) -> int:
        return self._n


class _FakeMasks:
    def __init__(self, data: np.ndarray) -> None:
        self.data = _FakeTensor(data)


class _FakeResult:
    def __init__(
        self,
        boxes: _FakeBoxes | None,
        masks: _FakeMasks | None,
        names: dict[int, str],
    ) -> None:
        self.boxes = boxes
        self.masks = masks
        self.names = names


class _FakeModel:
    """Callable fake matching ``ultralytics.YOLO`` instances."""

    def __init__(self, result: _FakeResult) -> None:
        self._result = result
        self.calls: list[dict[str, Any]] = []

    def __call__(self, img: np.ndarray, **kwargs: Any) -> list[_FakeResult]:
        self.calls.append({"shape": img.shape, **kwargs})
        return [self._result]

    def predict(self, *args: Any, **kwargs: Any) -> list[_FakeResult]:
        return [self._result]


def _make_processor_with_fake_model(
    *,
    result: _FakeResult,
    confidence_threshold: float = 0.25,
    iou_threshold: float = 0.45,
    min_detection_count: int = 1,
    model_name: str = "yolov8n-seg.pt",
) -> tuple[YoloSegmentationProcessor, _FakeModel]:
    """Build a processor instance with a fake model, bypassing ``initialize``.

    We do not call ``initialize()`` because, in this env, ultralytics is not
    installed and initialize would raise. The attributes set here mirror what
    initialize would set on success.
    """
    proc = YoloSegmentationProcessor()
    fake = _FakeModel(result)
    proc._model = fake
    proc._model_name = model_name
    proc._confidence_threshold = confidence_threshold
    proc._iou_threshold = iou_threshold
    proc._min_detection_count = min_detection_count
    proc._device = "cpu"
    return proc, fake


def _encode_png(height: int = 32, width: int = 48) -> bytes:
    """Encode a trivial RGB image to PNG bytes."""
    img = np.zeros((height, width, 3), dtype=np.uint8)
    img[:, :, 1] = 128  # tiny bit of green so it's not all-zero
    ok, buf = cv2.imencode(".png", img)
    assert ok
    return buf.tobytes()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestYoloSegmentationIdentity:
    """Processor identifiers."""

    def test_name(self) -> None:
        assert YoloSegmentationProcessor().name == "model_yolo_segmentation"

    def test_version(self) -> None:
        assert YoloSegmentationProcessor().version == "1.0.0"

    def test_backwards_compat_alias_is_same_class(self) -> None:
        assert YoloSegmentationAlgo is YoloSegmentationProcessor


class TestUltralyticsAvailabilityFlag:
    """The optional-import flag is set deterministically at import time."""

    def test_flag_is_boolean(self) -> None:
        assert isinstance(yolo_mod.ULTRALYTICS_AVAILABLE, bool)

    def test_yolo_symbol_is_none_when_unavailable(self) -> None:
        # In the unit-test environment, ultralytics is not installed.
        if not yolo_mod.ULTRALYTICS_AVAILABLE:
            assert yolo_mod.YOLO is None
        else:
            assert yolo_mod.YOLO is not None


class TestInitializeWhenUltralyticsMissing:
    """When ultralytics is missing, initialize must raise immediately."""

    def test_initialize_raises_runtime_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(yolo_mod, "ULTRALYTICS_AVAILABLE", False)
        monkeypatch.setattr(yolo_mod, "YOLO", None)
        proc = YoloSegmentationProcessor()

        with pytest.raises(RuntimeError, match="ultralytics package not installed"):
            proc.initialize({})

    def test_initialize_raises_even_with_full_settings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(yolo_mod, "ULTRALYTICS_AVAILABLE", False)
        monkeypatch.setattr(yolo_mod, "YOLO", None)
        proc = YoloSegmentationProcessor()

        with pytest.raises(RuntimeError):
            proc.initialize(
                {
                    "model": {
                        "name": "yolov8n-seg.pt",
                        "confidence_threshold": 0.5,
                        "iou_threshold": 0.6,
                        "device": "cpu",
                    },
                    "processor": {"min_detection_count": 3},
                }
            )


class TestInitializeWithFakeYolo:
    """Drive initialize() by swapping in a fake YOLO class.

    Uses monkeypatching to avoid needing the real ultralytics dependency.
    """

    def _patch_yolo(
        self,
        monkeypatch: pytest.MonkeyPatch,
        constructed: list[str],
    ) -> None:
        """Patch the module-level YOLO symbol and availability flag."""

        class FakeYoloClass:
            def __init__(self, name: str) -> None:
                constructed.append(name)
                self._name = name

            def predict(self, *args: Any, **kwargs: Any) -> list[Any]:
                return []

        monkeypatch.setattr(yolo_mod, "ULTRALYTICS_AVAILABLE", True)
        monkeypatch.setattr(yolo_mod, "YOLO", FakeYoloClass)

    def _patch_torch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Inject a minimal fake ``torch`` module supporting cuda.is_available()."""
        import sys
        import types

        fake_torch = types.ModuleType("torch")
        fake_cuda = types.SimpleNamespace(is_available=lambda: False)
        fake_torch.cuda = fake_cuda  # type: ignore[attr-defined]
        monkeypatch.setitem(sys.modules, "torch", fake_torch)

    def test_initialize_uses_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        constructed: list[str] = []
        self._patch_yolo(monkeypatch, constructed)
        self._patch_torch(monkeypatch)
        proc = YoloSegmentationProcessor()

        proc.initialize({})

        assert proc._model_name == "yolov8n-seg.pt"
        assert proc._confidence_threshold == pytest.approx(0.25)
        assert proc._iou_threshold == pytest.approx(0.45)
        assert proc._min_detection_count == 1
        assert proc._device == "auto"
        assert constructed == ["yolov8n-seg.pt"]

    def test_initialize_reads_model_section(self, monkeypatch: pytest.MonkeyPatch) -> None:
        constructed: list[str] = []
        self._patch_yolo(monkeypatch, constructed)
        proc = YoloSegmentationProcessor()

        proc.initialize(
            {
                "model": {
                    "name": "custom-seg.pt",
                    "confidence_threshold": 0.6,
                    "iou_threshold": 0.7,
                    "device": "cpu",
                },
                "processor": {"min_detection_count": 4},
            }
        )

        assert proc._model_name == "custom-seg.pt"
        assert proc._confidence_threshold == pytest.approx(0.6)
        assert proc._iou_threshold == pytest.approx(0.7)
        assert proc._device == "cpu"
        assert proc._min_detection_count == 4
        assert constructed == ["custom-seg.pt"]

    def test_initialize_falls_back_to_algorithm_section_for_min_count(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``processor`` defaults to ``algorithm`` when missing."""
        constructed: list[str] = []
        self._patch_yolo(monkeypatch, constructed)
        self._patch_torch(monkeypatch)
        proc = YoloSegmentationProcessor()

        proc.initialize({"algorithm": {"min_detection_count": 7}})

        assert proc._min_detection_count == 7

    def test_initialize_wraps_yolo_constructor_errors(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class BoomYolo:
            def __init__(self, name: str) -> None:
                msg = f"cannot load {name}"
                raise ValueError(msg)

        monkeypatch.setattr(yolo_mod, "ULTRALYTICS_AVAILABLE", True)
        monkeypatch.setattr(yolo_mod, "YOLO", BoomYolo)
        proc = YoloSegmentationProcessor()

        with pytest.raises(RuntimeError, match="Failed to load YOLO model"):
            proc.initialize({"model": {"name": "broken.pt", "device": "cpu"}})


class TestRunDecodeFailure:
    """If cv2 cannot decode the bytes, run returns a fail-fast metric pair."""

    def test_run_returns_zero_detection_count_on_decode_failure(self) -> None:
        proc, fake = _make_processor_with_fake_model(
            result=_FakeResult(boxes=None, masks=None, names={}),
        )

        result = proc.run(b"not an image", settings={})

        assert isinstance(result, ProcessorResult)
        assert "detection_count" in result.metrics
        assert result.metrics["detection_count"].value == 0
        # The model must NOT have been called for decode-failure path.
        assert fake.calls == []

    def test_run_decode_failure_metric_has_error_meta(self) -> None:
        proc, _ = _make_processor_with_fake_model(
            result=_FakeResult(boxes=None, masks=None, names={}),
        )

        result = proc.run(b"junk", settings={})

        meta = result.metrics["detection_count"].meta
        assert meta is not None
        assert meta.get("error") == "Failed to decode image"

    def test_run_decode_failure_sets_detection_passed_false(self) -> None:
        proc, _ = _make_processor_with_fake_model(
            result=_FakeResult(boxes=None, masks=None, names={}),
        )

        result = proc.run(b"junk", settings={})

        assert "detection_passed" in result.metrics
        assert result.metrics["detection_passed"].value is False

    def test_run_decode_failure_has_no_artifacts(self) -> None:
        proc, _ = _make_processor_with_fake_model(
            result=_FakeResult(boxes=None, masks=None, names={}),
        )

        result = proc.run(b"junk", settings={})

        assert result.artifacts == ()

    def test_run_decode_failure_returns_only_two_metrics(self) -> None:
        proc, _ = _make_processor_with_fake_model(
            result=_FakeResult(boxes=None, masks=None, names={}),
        )

        result = proc.run(b"junk", settings={})

        assert set(result.metrics.keys()) == {"detection_count", "detection_passed"}


class TestRunNoDetections:
    """A valid image with zero detections."""

    def test_run_no_detections_metric_keys(self) -> None:
        fake_result = _FakeResult(boxes=None, masks=None, names={})
        proc, fake = _make_processor_with_fake_model(result=fake_result)

        result = proc.run(_encode_png(), settings={})

        # All seven metric keys should be present
        expected = {
            "detection_count",
            "class_counts",
            "avg_confidence",
            "coverage_ratio",
            "detection_centroid_xy",
            "detection_passed",
            "model_name",
        }
        assert set(result.metrics.keys()) == expected
        # Model was called exactly once
        assert len(fake.calls) == 1

    def test_run_no_detections_values(self) -> None:
        fake_result = _FakeResult(boxes=None, masks=None, names={})
        proc, _ = _make_processor_with_fake_model(result=fake_result)

        result = proc.run(_encode_png(), settings={})

        assert result.metrics["detection_count"].value == 0
        assert result.metrics["class_counts"].value == {}
        assert result.metrics["avg_confidence"].value == pytest.approx(0.0)
        assert result.metrics["coverage_ratio"].value == pytest.approx(0.0)
        assert result.metrics["detection_centroid_xy"].value == {"x": 0.0, "y": 0.0}
        assert result.metrics["detection_passed"].value is False
        assert result.metrics["model_name"].value == "yolov8n-seg.pt"

    def test_run_no_detections_passes_when_min_count_zero(self) -> None:
        fake_result = _FakeResult(boxes=None, masks=None, names={})
        proc, _ = _make_processor_with_fake_model(result=fake_result, min_detection_count=0)

        result = proc.run(_encode_png(), settings={})

        # detection_passed = (0 >= 0) -> True
        assert result.metrics["detection_passed"].value is True

    def test_run_no_detections_produces_only_json_artifact(self) -> None:
        fake_result = _FakeResult(boxes=None, masks=None, names={})
        proc, _ = _make_processor_with_fake_model(result=fake_result)

        result = proc.run(_encode_png(), settings={})

        # Only the JSON report, no segmentation overlay PNG
        assert len(result.artifacts) == 1
        assert result.artifacts[0].name == "yolo_detection_report.json"
        assert result.artifacts[0].mime == "application/json"

    def test_run_passes_thresholds_to_model_callable(self) -> None:
        fake_result = _FakeResult(boxes=None, masks=None, names={})
        proc, fake = _make_processor_with_fake_model(
            result=fake_result,
            confidence_threshold=0.33,
            iou_threshold=0.77,
        )

        proc.run(_encode_png(), settings={})

        assert fake.calls[0]["conf"] == pytest.approx(0.33)
        assert fake.calls[0]["iou"] == pytest.approx(0.77)
        assert fake.calls[0]["verbose"] is False


class TestRunWithDetections:
    """Valid image with detections + masks: hits the full metric+artifact path."""

    def _two_detections(self) -> _FakeResult:
        cls = np.array([0, 1], dtype=np.int64)
        conf = np.array([0.9, 0.8], dtype=np.float32)
        # xyxy boxes inside 48x32 image (W=48, H=32)
        xyxy = np.array(
            [[0.0, 0.0, 10.0, 10.0], [20.0, 10.0, 40.0, 30.0]],
            dtype=np.float32,
        )
        boxes = _FakeBoxes(cls=cls, conf=conf, xyxy=xyxy)
        # Two binary masks at low resolution; processor resizes to image size
        mask_data = np.zeros((2, 16, 24), dtype=np.float32)
        mask_data[0, :8, :12] = 1.0
        mask_data[1, 8:, 12:] = 1.0
        masks = _FakeMasks(mask_data)
        names = {0: "person", 1: "bottle"}
        return _FakeResult(boxes=boxes, masks=masks, names=names)

    def test_run_detection_count(self) -> None:
        proc, _ = _make_processor_with_fake_model(result=self._two_detections())

        result = proc.run(_encode_png(height=32, width=48), settings={})

        m = result.metrics["detection_count"]
        assert isinstance(m, Measurement)
        assert m.value == 2
        assert AggregationType.STATS in m.aggregation
        assert AggregationType.HISTOGRAM in m.aggregation

    def test_run_class_counts(self) -> None:
        proc, _ = _make_processor_with_fake_model(result=self._two_detections())

        result = proc.run(_encode_png(height=32, width=48), settings={})

        cc = result.metrics["class_counts"]
        assert cc.value == {"person": 1, "bottle": 1}
        assert cc.aggregation == AggregationType.TALLY
        assert cc.meta["total_classes"] == 2

    def test_run_avg_confidence(self) -> None:
        proc, _ = _make_processor_with_fake_model(result=self._two_detections())

        result = proc.run(_encode_png(height=32, width=48), settings={})

        avg = result.metrics["avg_confidence"]
        assert avg.value == pytest.approx((0.9 + 0.8) / 2, rel=1e-5)
        for flag in (
            AggregationType.STATS,
            AggregationType.HISTOGRAM,
            AggregationType.OUTLIERS,
        ):
            assert flag in avg.aggregation

    def test_run_centroid_is_mean_of_box_centres(self) -> None:
        proc, _ = _make_processor_with_fake_model(result=self._two_detections())

        result = proc.run(_encode_png(height=32, width=48), settings={})

        # Box centres: (5,5) and (30,20) -> mean = (17.5, 12.5)
        c = result.metrics["detection_centroid_xy"]
        assert c.value["x"] == pytest.approx(17.5)
        assert c.value["y"] == pytest.approx(12.5)
        assert AggregationType.SCATTER_ELLIPSE in c.aggregation
        assert AggregationType.DENSITY_MAP in c.aggregation

    def test_run_coverage_ratio_in_unit_interval(self) -> None:
        proc, _ = _make_processor_with_fake_model(result=self._two_detections())

        result = proc.run(_encode_png(height=32, width=48), settings={})

        cov = result.metrics["coverage_ratio"]
        assert 0.0 < cov.value <= 1.0

    def test_run_detection_passed_when_count_meets_threshold(self) -> None:
        proc, _ = _make_processor_with_fake_model(result=self._two_detections(), min_detection_count=2)

        result = proc.run(_encode_png(height=32, width=48), settings={})

        passed = result.metrics["detection_passed"]
        assert passed.value is True
        assert passed.meta["detection_count"] == 2
        assert set(passed.meta["classes_detected"]) == {"person", "bottle"}

    def test_run_detection_failed_when_count_below_threshold(self) -> None:
        proc, _ = _make_processor_with_fake_model(result=self._two_detections(), min_detection_count=5)

        result = proc.run(_encode_png(height=32, width=48), settings={})

        passed = result.metrics["detection_passed"]
        assert passed.value is False
        assert passed.meta["threshold"] == 5

    def test_run_model_name_metric(self) -> None:
        proc, _ = _make_processor_with_fake_model(result=self._two_detections(), model_name="my-custom-seg.pt")

        result = proc.run(_encode_png(height=32, width=48), settings={})

        m = result.metrics["model_name"]
        assert m.value == "my-custom-seg.pt"
        assert m.aggregation == AggregationType.RAW

    def test_run_emits_json_and_overlay_artifacts(self) -> None:
        proc, _ = _make_processor_with_fake_model(result=self._two_detections())

        result = proc.run(_encode_png(height=32, width=48), settings={})

        names = [a.name for a in result.artifacts]
        assert "yolo_detection_report.json" in names
        assert "segmentation_overlay.png" in names
        for a in result.artifacts:
            assert isinstance(a, Artifact)

    def test_run_json_artifact_structure(self) -> None:
        proc, _ = _make_processor_with_fake_model(result=self._two_detections())

        result = proc.run(_encode_png(height=32, width=48), settings={})

        report = next(a for a in result.artifacts if a.name == "yolo_detection_report.json")
        payload = json.loads(report.data.decode("utf-8"))

        assert payload["processor"] == "model_yolo_segmentation"
        assert payload["version"] == "1.0.0"
        assert payload["model"] == "yolov8n-seg.pt"
        assert payload["metrics"]["detection_count"] == 2
        assert payload["metrics"]["class_counts"] == {"person": 1, "bottle": 1}
        assert payload["metrics"]["avg_confidence"] == pytest.approx(0.85, rel=1e-5)
        assert 0.0 < payload["metrics"]["coverage_ratio"] <= 1.0
        assert payload["detection_passed"] is True
        assert payload["image_shape"] == [32, 48, 3]
        assert len(payload["detections"]) == 2
        assert payload["parameters"] == {
            "confidence_threshold": 0.25,
            "iou_threshold": 0.45,
            "min_detection_count": 1,
        }

    def test_run_detection_records_have_expected_fields(self) -> None:
        proc, _ = _make_processor_with_fake_model(result=self._two_detections())

        result = proc.run(_encode_png(height=32, width=48), settings={})

        report = next(a for a in result.artifacts if a.name == "yolo_detection_report.json")
        payload = json.loads(report.data.decode("utf-8"))

        det0 = payload["detections"][0]
        assert det0["index"] == 0
        assert det0["class_id"] == 0
        assert det0["class_name"] == "person"
        assert det0["confidence"] == pytest.approx(0.9, rel=1e-5)
        assert det0["bbox"] == [0.0, 0.0, 10.0, 10.0]

    def test_run_overlay_png_is_valid_image(self) -> None:
        proc, _ = _make_processor_with_fake_model(result=self._two_detections())

        result = proc.run(_encode_png(height=32, width=48), settings={})

        overlay = next(a for a in result.artifacts if a.name == "segmentation_overlay.png")
        assert overlay.mime == "image/png"
        decoded = cv2.imdecode(np.frombuffer(overlay.data, np.uint8), cv2.IMREAD_COLOR)
        assert decoded is not None
        assert decoded.shape == (32, 48, 3)


class TestRunWithBoxesButNoMasks:
    """Detections without masks: coverage_ratio is zero, no overlay artifact."""

    def test_run_no_mask_means_zero_coverage(self) -> None:
        cls = np.array([0], dtype=np.int64)
        conf = np.array([0.5], dtype=np.float32)
        xyxy = np.array([[1.0, 2.0, 3.0, 4.0]], dtype=np.float32)
        boxes = _FakeBoxes(cls=cls, conf=conf, xyxy=xyxy)
        fake_result = _FakeResult(boxes=boxes, masks=None, names={0: "thing"})
        proc, _ = _make_processor_with_fake_model(result=fake_result)

        result = proc.run(_encode_png(), settings={})

        assert result.metrics["coverage_ratio"].value == pytest.approx(0.0)
        # No segmentation overlay artifact when masks are absent
        names = [a.name for a in result.artifacts]
        assert "segmentation_overlay.png" not in names
        assert "yolo_detection_report.json" in names

    def test_run_empty_masks_data_still_zero_coverage(self) -> None:
        cls = np.array([0], dtype=np.int64)
        conf = np.array([0.5], dtype=np.float32)
        xyxy = np.array([[1.0, 2.0, 3.0, 4.0]], dtype=np.float32)
        boxes = _FakeBoxes(cls=cls, conf=conf, xyxy=xyxy)
        # Zero-length mask stack
        masks = _FakeMasks(np.zeros((0, 4, 4), dtype=np.float32))
        fake_result = _FakeResult(boxes=boxes, masks=masks, names={0: "thing"})
        proc, _ = _make_processor_with_fake_model(result=fake_result)

        result = proc.run(_encode_png(), settings={})

        assert result.metrics["coverage_ratio"].value == pytest.approx(0.0)
        names = [a.name for a in result.artifacts]
        assert "segmentation_overlay.png" not in names
