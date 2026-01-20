"""GPU detection and configuration utilities.

Provides optional GPU support with graceful fallback to CPU.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from types import TracebackType

from src.utils.logging import get_logger

logger = get_logger(__name__)

# Global GPU state (initialized once)
_gpu_state: GPUState | None = None


@dataclass
class GPUState:
    """GPU availability and configuration state."""

    available: bool
    cuda_available: bool
    opencv_cuda: bool
    device_count: int
    device_id: int
    device_name: str
    memory_total_mb: int
    memory_free_mb: int
    driver_version: str
    cuda_version: str

    def __str__(self) -> str:
        if not self.available:
            return "GPU: Not available (using CPU)"
        return (
            f"GPU: {self.device_name} (Device {self.device_id})\n"
            f"  Memory: {self.memory_free_mb}MB free / {self.memory_total_mb}MB total\n"
            f"  CUDA: {self.cuda_version}, Driver: {self.driver_version}\n"
            f"  OpenCV CUDA: {'Yes' if self.opencv_cuda else 'No'}"
        )


def _detect_gpu() -> GPUState:
    """Detect GPU availability and capabilities."""
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

    # Check environment variable override
    if os.environ.get("VISIOEVAL_USE_GPU", "").lower() == "false":
        logger.info("GPU disabled via VISIOEVAL_USE_GPU=false")
        return state

    # Try to detect NVIDIA GPU via pynvml
    try:
        import pynvml  # noqa: PLC0415

        pynvml.nvmlInit()
        state.device_count = pynvml.nvmlDeviceGetCount()

        if state.device_count > 0:
            state.available = True
            state.driver_version = pynvml.nvmlSystemGetDriverVersion()

            # Get first device info (or configured device)
            device_id = int(os.environ.get("CUDA_VISIBLE_DEVICES", "0").split(",")[0])
            if device_id >= state.device_count:
                device_id = 0

            handle = pynvml.nvmlDeviceGetHandleByIndex(device_id)
            state.device_id = device_id
            state.device_name = pynvml.nvmlDeviceGetName(handle)
            if isinstance(state.device_name, bytes):
                state.device_name = state.device_name.decode("utf-8")

            mem_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
            state.memory_total_mb = mem_info.total // (1024 * 1024)
            state.memory_free_mb = mem_info.free // (1024 * 1024)

        pynvml.nvmlShutdown()
    except ImportError:
        logger.debug("pynvml not installed, trying alternative GPU detection")
    except Exception as e:
        logger.debug("pynvml detection failed: %s", e)

    # Try CUDA detection via torch (if available)
    if not state.available:
        try:
            import torch  # noqa: PLC0415

            if torch.cuda.is_available():
                state.available = True
                state.cuda_available = True
                state.device_count = torch.cuda.device_count()
                state.device_id = torch.cuda.current_device()
                state.device_name = torch.cuda.get_device_name(state.device_id)
                props = torch.cuda.get_device_properties(state.device_id)
                state.memory_total_mb = props.total_memory // (1024 * 1024)
                state.cuda_version = torch.version.cuda or "N/A"
        except ImportError:
            logger.debug("PyTorch not installed")
        except Exception as e:
            logger.debug("PyTorch CUDA detection failed: %s", e)

    # Check OpenCV CUDA support
    try:
        import cv2  # noqa: PLC0415

        state.opencv_cuda = cv2.cuda.getCudaEnabledDeviceCount() > 0
        if state.opencv_cuda and not state.available:
            state.available = True
            state.device_count = cv2.cuda.getCudaEnabledDeviceCount()
            state.device_id = 0
    except (ImportError, AttributeError, cv2.error):
        logger.debug("OpenCV CUDA not available")

    return state


def get_gpu_state(force_refresh: bool = False) -> GPUState:
    """Get cached GPU state (singleton pattern).

    Args:
        force_refresh: If True, re-detect GPU state.

    Returns:
        Current GPU state.

    """
    global _gpu_state
    if _gpu_state is None or force_refresh:
        _gpu_state = _detect_gpu()
    return _gpu_state


def is_gpu_available() -> bool:
    """Check if GPU is available for acceleration."""
    return get_gpu_state().available


def is_opencv_cuda_available() -> bool:
    """Check if OpenCV CUDA is available."""
    return get_gpu_state().opencv_cuda


def get_device_id() -> int:
    """Get the configured GPU device ID."""
    return get_gpu_state().device_id


def load_gpu_config(config_path: Path | None = None) -> dict[str, Any]:
    """Load GPU configuration from pipeline.toml.

    Args:
        config_path: Path to pipeline.toml. Defaults to src/config/pipeline.toml.

    Returns:
        GPU configuration dict with defaults applied.

    """
    if config_path is None:
        config_path = Path("src/config/pipeline.toml")

    defaults = {
        "enabled": False,
        "device_id": 0,
        "fallback_to_cpu": True,
        "memory_limit_mb": 0,
    }

    try:
        with config_path.open("rb") as f:
            config = tomllib.load(f)
        gpu_config = config.get("gpu", {})
        return {**defaults, **gpu_config}
    except FileNotFoundError:
        logger.warning("Pipeline config not found: %s", config_path)
        return defaults
    except Exception as e:
        logger.warning("Failed to load GPU config: %s", e)
        return defaults


def check_gpu_status() -> None:
    """Print GPU status (for CLI diagnostics)."""
    import sys  # noqa: PLC0415

    state = get_gpu_state(force_refresh=True)
    config = load_gpu_config()

    sys.stdout.write("=" * 60 + "\n")
    sys.stdout.write("GPU STATUS\n")
    sys.stdout.write("=" * 60 + "\n")
    sys.stdout.write(str(state) + "\n")
    sys.stdout.write("\n")
    sys.stdout.write("Configuration (pipeline.toml):\n")
    sys.stdout.write(f"  Enabled: {config['enabled']}\n")
    sys.stdout.write(f"  Device ID: {config['device_id']}\n")
    sys.stdout.write(f"  Fallback to CPU: {config['fallback_to_cpu']}\n")
    sys.stdout.write(f"  Memory Limit: {config['memory_limit_mb']}MB\n")
    sys.stdout.write("=" * 60 + "\n")

    if state.available and config["enabled"]:
        sys.stdout.write("✓ GPU acceleration is ACTIVE\n")
    elif state.available and not config["enabled"]:
        sys.stdout.write("○ GPU available but DISABLED in config\n")
    elif not state.available and config["fallback_to_cpu"]:
        sys.stdout.write("○ GPU not available, using CPU (fallback enabled)\n")
    else:
        sys.stdout.write("✗ GPU not available and fallback disabled\n")


class GPUContext:
    """Context manager for GPU-accelerated operations.

    Usage:
        with GPUContext() as gpu:
            if gpu.use_cuda:
                # Use CUDA-accelerated code
            else:
                # Use CPU fallback
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize GPU context.

        Args:
            config: GPU configuration dict. If None, loads from pipeline.toml.

        """
        self.config = config or load_gpu_config()
        self.state = get_gpu_state()
        self._use_cuda = False

    @property
    def use_cuda(self) -> bool:
        """Whether to use CUDA acceleration."""
        return self._use_cuda

    @property
    def use_opencv_cuda(self) -> bool:
        """Whether OpenCV CUDA is available and enabled."""
        return self._use_cuda and self.state.opencv_cuda

    def __enter__(self) -> Self:
        """Enter GPU context."""
        if self.config["enabled"] and self.state.available:
            self._use_cuda = True
            logger.debug(
                "GPU context: Using CUDA (device %d: %s)",
                self.state.device_id,
                self.state.device_name,
            )
        elif self.config["fallback_to_cpu"]:
            self._use_cuda = False
            logger.debug("GPU context: Using CPU fallback")
        else:
            msg = "GPU required but not available"
            raise RuntimeError(msg)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit GPU context."""
