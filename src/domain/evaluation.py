from enum import IntFlag, auto
from functools import lru_cache


class AnalysisKind(IntFlag):
    """Post-processing flags for metric analysis."""

    SUMMARY = auto()
    DISTRIBUTION_1D = auto()
    OUTLIERS_1D = auto()
    COUNTER = auto()
    RATE = auto()
    ELLIPSE_2D = auto()
    CONTOUR_2D = auto()
    INFO = auto()


@lru_cache(maxsize=256)
def mask_to_kind_names(mask: int) -> tuple[str, ...]:
    """Convert AnalysisKind bitmask to tuple of kind names.

    Args:
        mask: Integer bitmask of AnalysisKind values

    Returns:
        Tuple of kind name strings

    """
    return tuple(kind.name for kind in AnalysisKind if mask & kind.value and kind.name)
