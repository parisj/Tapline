"""Unit tests for GPU detection and configuration utilities.

The src.utils.gpu module performs runtime detection of CUDA capable
hardware by attempting to import pynvml, torch, and cv2. All of these
are optional dependencies so the detection path is fully mocked here.

The module also maintains a global _gpu_state singleton; each test
resets it to avoid cross-test pollution.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from src.utils import gpu as gpu_module
from src.utils.gpu import (
    GPUContext,
    GPUState,
    get_device_id,
    get_gpu_state,
    is_gpu_available,
    is_opencv_cuda_available,
    load_gpu_config,
)


@pytest.fixture(autouse=True)
def reset_gpu_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reset the cached singleton and clean GPU env var before each test."""
    monkeypatch.setattr(gpu_module, "_gpu_state", None)
    monkeypatch.delenv("TAPLINE_USE_GPU", raising=False)
    monkeypatch.delenv("CUDA_VISIBLE_DEVICES", raising=False)


def _install_fake_module(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    module: types.ModuleType | None,
) -> None:
    """Install (or remove) a fake module in sys.modules for the test."""
    if module is None:
        monkeypatch.setitem(sys.modules, name, None)  # forces ImportError
    else:
        monkeypatch.setitem(sys.modules, name, module)


def _stub_cv2_no_cuda(monkeypatch: pytest.MonkeyPatch) -> None:
    """Install a cv2 stub with a no-CUDA cuda module.

    NOTE: the production code's except clause references ``cv2.error`` which
    triggers an ``UnboundLocalError`` when cv2 cannot be imported. To keep
    the tests deterministic regardless of whether cv2 is actually installed,
    we provide a stub that reports zero CUDA devices.
    """
    fake_cv2 = types.ModuleType("cv2")

    class _CV2Error(Exception):
        pass

    fake_cv2.error = _CV2Error
    cuda_mod = MagicMock()
    cuda_mod.getCudaEnabledDeviceCount = MagicMock(return_value=0)
    fake_cv2.cuda = cuda_mod
    monkeypatch.setitem(sys.modules, "cv2", fake_cv2)


class TestGPUState:
    def test_default_unavailable_str(self) -> None:
        state = GPUState(
            available=False,
            cuda_available=False,
            opencv_cuda=False,
            device_count=0,
            device_id=-1,
            device_name="None",
            memory_total_mb=0,
            memory_free_mb=0,
            driver_version="N/A",
            cuda_version="N/A",
        )

        assert "Not available" in str(state)
        assert "CPU" in str(state)

    def test_available_str_includes_device_name(self) -> None:
        state = GPUState(
            available=True,
            cuda_available=True,
            opencv_cuda=True,
            device_count=1,
            device_id=0,
            device_name="Test GPU",
            memory_total_mb=8000,
            memory_free_mb=4000,
            driver_version="525.00",
            cuda_version="12.0",
        )

        rendered = str(state)
        assert "Test GPU" in rendered
        assert "Device 0" in rendered
        assert "4000MB free" in rendered
        assert "8000MB total" in rendered
        assert "12.0" in rendered
        assert "OpenCV CUDA: Yes" in rendered

    def test_available_str_opencv_no(self) -> None:
        state = GPUState(
            available=True,
            cuda_available=True,
            opencv_cuda=False,
            device_count=1,
            device_id=0,
            device_name="GPU",
            memory_total_mb=8000,
            memory_free_mb=4000,
            driver_version="525.00",
            cuda_version="12.0",
        )

        assert "OpenCV CUDA: No" in str(state)


class TestDetectGPU:
    def test_env_disable_returns_unavailable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TAPLINE_USE_GPU", "false")
        # Also stub cv2 so we don't pick up real OpenCV CUDA on the machine.
        _stub_cv2_no_cuda(monkeypatch)

        state = gpu_module._detect_gpu()

        assert state.available is False
        assert state.device_count == 0

    def test_env_disable_is_case_insensitive(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TAPLINE_USE_GPU", "FALSE")
        _stub_cv2_no_cuda(monkeypatch)

        state = gpu_module._detect_gpu()

        assert state.available is False

    def test_pynvml_detection_populates_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Build a fake pynvml module that reports one device.
        fake_pynvml = types.ModuleType("pynvml")
        handle = object()
        mem_info = MagicMock(total=8 * 1024 * 1024 * 1024, free=4 * 1024 * 1024 * 1024)
        fake_pynvml.nvmlInit = MagicMock()
        fake_pynvml.nvmlDeviceGetCount = MagicMock(return_value=1)
        fake_pynvml.nvmlSystemGetDriverVersion = MagicMock(return_value="525.00")
        fake_pynvml.nvmlDeviceGetHandleByIndex = MagicMock(return_value=handle)
        fake_pynvml.nvmlDeviceGetName = MagicMock(return_value="Fake RTX 5090")
        fake_pynvml.nvmlDeviceGetMemoryInfo = MagicMock(return_value=mem_info)
        fake_pynvml.nvmlShutdown = MagicMock()

        _install_fake_module(monkeypatch, "pynvml", fake_pynvml)
        _stub_cv2_no_cuda(monkeypatch)

        state = gpu_module._detect_gpu()

        assert state.available is True
        assert state.device_count == 1
        assert state.device_name == "Fake RTX 5090"
        assert state.driver_version == "525.00"
        assert state.memory_total_mb == 8 * 1024
        assert state.memory_free_mb == 4 * 1024
        fake_pynvml.nvmlShutdown.assert_called_once()

    def test_pynvml_bytes_device_name_decoded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_pynvml = types.ModuleType("pynvml")
        mem_info = MagicMock(total=1024 * 1024, free=1024 * 1024)
        fake_pynvml.nvmlInit = MagicMock()
        fake_pynvml.nvmlDeviceGetCount = MagicMock(return_value=1)
        fake_pynvml.nvmlSystemGetDriverVersion = MagicMock(return_value="500")
        fake_pynvml.nvmlDeviceGetHandleByIndex = MagicMock(return_value=object())
        fake_pynvml.nvmlDeviceGetName = MagicMock(return_value=b"Encoded GPU")
        fake_pynvml.nvmlDeviceGetMemoryInfo = MagicMock(return_value=mem_info)
        fake_pynvml.nvmlShutdown = MagicMock()

        _install_fake_module(monkeypatch, "pynvml", fake_pynvml)
        _stub_cv2_no_cuda(monkeypatch)

        state = gpu_module._detect_gpu()

        assert state.device_name == "Encoded GPU"

    def test_cuda_visible_devices_used(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_pynvml = types.ModuleType("pynvml")
        mem_info = MagicMock(total=0, free=0)
        fake_pynvml.nvmlInit = MagicMock()
        fake_pynvml.nvmlDeviceGetCount = MagicMock(return_value=4)
        fake_pynvml.nvmlSystemGetDriverVersion = MagicMock(return_value="500")
        fake_pynvml.nvmlDeviceGetHandleByIndex = MagicMock(return_value=object())
        fake_pynvml.nvmlDeviceGetName = MagicMock(return_value="GPU")
        fake_pynvml.nvmlDeviceGetMemoryInfo = MagicMock(return_value=mem_info)
        fake_pynvml.nvmlShutdown = MagicMock()

        monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "2,3")
        _install_fake_module(monkeypatch, "pynvml", fake_pynvml)
        _stub_cv2_no_cuda(monkeypatch)

        state = gpu_module._detect_gpu()

        assert state.device_id == 2
        fake_pynvml.nvmlDeviceGetHandleByIndex.assert_called_with(2)

    def test_cuda_visible_devices_out_of_range_falls_back_to_zero(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake_pynvml = types.ModuleType("pynvml")
        mem_info = MagicMock(total=0, free=0)
        fake_pynvml.nvmlInit = MagicMock()
        fake_pynvml.nvmlDeviceGetCount = MagicMock(return_value=1)
        fake_pynvml.nvmlSystemGetDriverVersion = MagicMock(return_value="500")
        fake_pynvml.nvmlDeviceGetHandleByIndex = MagicMock(return_value=object())
        fake_pynvml.nvmlDeviceGetName = MagicMock(return_value="GPU")
        fake_pynvml.nvmlDeviceGetMemoryInfo = MagicMock(return_value=mem_info)
        fake_pynvml.nvmlShutdown = MagicMock()

        monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "5")
        _install_fake_module(monkeypatch, "pynvml", fake_pynvml)
        _stub_cv2_no_cuda(monkeypatch)

        state = gpu_module._detect_gpu()

        assert state.device_id == 0

    def test_pynvml_zero_devices_keeps_unavailable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_pynvml = types.ModuleType("pynvml")
        fake_pynvml.nvmlInit = MagicMock()
        fake_pynvml.nvmlDeviceGetCount = MagicMock(return_value=0)
        fake_pynvml.nvmlShutdown = MagicMock()

        _install_fake_module(monkeypatch, "pynvml", fake_pynvml)
        _stub_cv2_no_cuda(monkeypatch)

        state = gpu_module._detect_gpu()

        assert state.available is False
        assert state.device_count == 0

    def test_pynvml_exception_falls_through(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_pynvml = types.ModuleType("pynvml")
        fake_pynvml.nvmlInit = MagicMock(side_effect=RuntimeError("driver missing"))
        _install_fake_module(monkeypatch, "pynvml", fake_pynvml)
        _install_fake_module(monkeypatch, "torch", None)
        _stub_cv2_no_cuda(monkeypatch)

        state = gpu_module._detect_gpu()

        assert state.available is False

    def test_torch_fallback_used_when_pynvml_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_fake_module(monkeypatch, "pynvml", None)

        fake_torch = types.ModuleType("torch")
        props = MagicMock(total_memory=2 * 1024 * 1024 * 1024)
        cuda_mod = MagicMock()
        cuda_mod.is_available = MagicMock(return_value=True)
        cuda_mod.device_count = MagicMock(return_value=1)
        cuda_mod.current_device = MagicMock(return_value=0)
        cuda_mod.get_device_name = MagicMock(return_value="Torch GPU")
        cuda_mod.get_device_properties = MagicMock(return_value=props)
        fake_torch.cuda = cuda_mod
        fake_torch.version = types.SimpleNamespace(cuda="11.8")
        _install_fake_module(monkeypatch, "torch", fake_torch)
        _stub_cv2_no_cuda(monkeypatch)

        state = gpu_module._detect_gpu()

        assert state.available is True
        assert state.cuda_available is True
        assert state.device_name == "Torch GPU"
        assert state.memory_total_mb == 2048
        assert state.cuda_version == "11.8"

    def test_torch_cuda_unavailable_keeps_unavailable(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_fake_module(monkeypatch, "pynvml", None)

        fake_torch = types.ModuleType("torch")
        cuda_mod = MagicMock()
        cuda_mod.is_available = MagicMock(return_value=False)
        fake_torch.cuda = cuda_mod
        fake_torch.version = types.SimpleNamespace(cuda=None)
        _install_fake_module(monkeypatch, "torch", fake_torch)
        _stub_cv2_no_cuda(monkeypatch)

        state = gpu_module._detect_gpu()

        assert state.available is False
        assert state.cuda_available is False

    def test_torch_exception_falls_through(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _install_fake_module(monkeypatch, "pynvml", None)

        fake_torch = types.ModuleType("torch")
        cuda_mod = MagicMock()
        cuda_mod.is_available = MagicMock(side_effect=RuntimeError("boom"))
        fake_torch.cuda = cuda_mod
        fake_torch.version = types.SimpleNamespace(cuda=None)
        _install_fake_module(monkeypatch, "torch", fake_torch)
        _stub_cv2_no_cuda(monkeypatch)

        state = gpu_module._detect_gpu()

        assert state.available is False


class TestSingletonAndAccessors:
    def test_get_gpu_state_caches_singleton(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Force unavailable detection path.
        monkeypatch.setenv("TAPLINE_USE_GPU", "false")
        _stub_cv2_no_cuda(monkeypatch)

        first = get_gpu_state()
        second = get_gpu_state()

        assert first is second

    def test_get_gpu_state_force_refresh(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TAPLINE_USE_GPU", "false")
        _stub_cv2_no_cuda(monkeypatch)

        first = get_gpu_state()
        second = get_gpu_state(force_refresh=True)

        assert first is not second
        assert first.available is False
        assert second.available is False

    def test_is_gpu_available_reads_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TAPLINE_USE_GPU", "false")
        _stub_cv2_no_cuda(monkeypatch)

        assert is_gpu_available() is False

    def test_is_opencv_cuda_available_reads_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TAPLINE_USE_GPU", "false")
        _stub_cv2_no_cuda(monkeypatch)

        assert is_opencv_cuda_available() is False

    def test_get_device_id_reads_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TAPLINE_USE_GPU", "false")
        _stub_cv2_no_cuda(monkeypatch)

        assert get_device_id() == -1


class TestLoadGPUConfig:
    def test_returns_defaults_when_file_missing(self, tmp_path: Path) -> None:
        config = load_gpu_config(tmp_path / "missing.toml")

        assert config == {
            "enabled": False,
            "device_id": 0,
            "fallback_to_cpu": True,
            "memory_limit_mb": 0,
        }

    def test_loads_values_from_toml(self, tmp_path: Path) -> None:
        path = tmp_path / "pipeline.toml"
        path.write_text(
            """
            [gpu]
            enabled = true
            device_id = 1
            fallback_to_cpu = false
            memory_limit_mb = 2048
            """,
            encoding="utf-8",
        )

        config = load_gpu_config(path)

        assert config["enabled"] is True
        assert config["device_id"] == 1
        assert config["fallback_to_cpu"] is False
        assert config["memory_limit_mb"] == 2048

    def test_partial_toml_uses_defaults(self, tmp_path: Path) -> None:
        path = tmp_path / "pipeline.toml"
        path.write_text("[gpu]\nenabled = true\n", encoding="utf-8")

        config = load_gpu_config(path)

        assert config["enabled"] is True
        assert config["device_id"] == 0  # default preserved
        assert config["fallback_to_cpu"] is True
        assert config["memory_limit_mb"] == 0

    def test_missing_gpu_section_uses_defaults(self, tmp_path: Path) -> None:
        path = tmp_path / "pipeline.toml"
        path.write_text("[other]\nfoo = 1\n", encoding="utf-8")

        config = load_gpu_config(path)

        assert config == {
            "enabled": False,
            "device_id": 0,
            "fallback_to_cpu": True,
            "memory_limit_mb": 0,
        }

    def test_invalid_toml_returns_defaults(self, tmp_path: Path) -> None:
        path = tmp_path / "pipeline.toml"
        path.write_text("not valid toml [[[", encoding="utf-8")

        config = load_gpu_config(path)

        assert config["enabled"] is False
        assert config["fallback_to_cpu"] is True


class TestGPUContext:
    def test_uses_cuda_when_enabled_and_available(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        state = GPUState(
            available=True,
            cuda_available=True,
            opencv_cuda=True,
            device_count=1,
            device_id=0,
            device_name="GPU",
            memory_total_mb=1024,
            memory_free_mb=512,
            driver_version="x",
            cuda_version="12.0",
        )
        monkeypatch.setattr(gpu_module, "_gpu_state", state)

        with GPUContext(config={"enabled": True, "fallback_to_cpu": True}) as ctx:
            assert ctx.use_cuda is True
            assert ctx.use_opencv_cuda is True

    def test_falls_back_to_cpu_when_unavailable(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        state = GPUState(
            available=False,
            cuda_available=False,
            opencv_cuda=False,
            device_count=0,
            device_id=-1,
            device_name="None",
            memory_total_mb=0,
            memory_free_mb=0,
            driver_version="N/A",
            cuda_version="N/A",
        )
        monkeypatch.setattr(gpu_module, "_gpu_state", state)

        with GPUContext(config={"enabled": True, "fallback_to_cpu": True}) as ctx:
            assert ctx.use_cuda is False
            assert ctx.use_opencv_cuda is False

    def test_falls_back_when_disabled_in_config(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        state = GPUState(
            available=True,
            cuda_available=True,
            opencv_cuda=True,
            device_count=1,
            device_id=0,
            device_name="GPU",
            memory_total_mb=1024,
            memory_free_mb=512,
            driver_version="x",
            cuda_version="12.0",
        )
        monkeypatch.setattr(gpu_module, "_gpu_state", state)

        with GPUContext(config={"enabled": False, "fallback_to_cpu": True}) as ctx:
            assert ctx.use_cuda is False

    def test_raises_when_required_and_unavailable(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        state = GPUState(
            available=False,
            cuda_available=False,
            opencv_cuda=False,
            device_count=0,
            device_id=-1,
            device_name="None",
            memory_total_mb=0,
            memory_free_mb=0,
            driver_version="N/A",
            cuda_version="N/A",
        )
        monkeypatch.setattr(gpu_module, "_gpu_state", state)

        ctx = GPUContext(config={"enabled": True, "fallback_to_cpu": False})
        with pytest.raises(RuntimeError, match="GPU required but not available"):
            ctx.__enter__()

    def test_uses_default_config_when_none_provided(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        state = GPUState(
            available=False,
            cuda_available=False,
            opencv_cuda=False,
            device_count=0,
            device_id=-1,
            device_name="None",
            memory_total_mb=0,
            memory_free_mb=0,
            driver_version="N/A",
            cuda_version="N/A",
        )
        monkeypatch.setattr(gpu_module, "_gpu_state", state)
        # load_gpu_config will warn and use defaults; defaults set fallback_to_cpu=True.
        monkeypatch.chdir(tmp_path)

        with GPUContext() as ctx:
            assert ctx.use_cuda is False

    def test_use_opencv_cuda_requires_both_flags(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Device available with CUDA but no OpenCV CUDA.
        state = GPUState(
            available=True,
            cuda_available=True,
            opencv_cuda=False,
            device_count=1,
            device_id=0,
            device_name="GPU",
            memory_total_mb=1024,
            memory_free_mb=512,
            driver_version="x",
            cuda_version="12.0",
        )
        monkeypatch.setattr(gpu_module, "_gpu_state", state)

        with GPUContext(config={"enabled": True, "fallback_to_cpu": True}) as ctx:
            assert ctx.use_cuda is True
            assert ctx.use_opencv_cuda is False
