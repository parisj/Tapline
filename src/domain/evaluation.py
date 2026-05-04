from enum import IntFlag, auto
from functools import lru_cache


class AggregationType(IntFlag):
    """Post-processing flags for metric aggregation."""

    STATS = auto()  # was SUMMARY
    HISTOGRAM = auto()  # was DISTRIBUTION_1D
    OUTLIERS = auto()  # was OUTLIERS_1D
    TALLY = auto()  # was COUNTER
    RATE = auto()  # unchanged
    SCATTER_ELLIPSE = auto()  # was ELLIPSE_2D
    DENSITY_MAP = auto()  # was CONTOUR_2D
    RAW = auto()  # was INFO


@lru_cache(maxsize=256)
def mask_to_type_names(mask: int) -> tuple[str, ...]:
    """Convert AggregationType bitmask to tuple of type names.

    Args:
        mask: Integer bitmask of AggregationType values

    Returns:
        Tuple of type name strings

    """
    return tuple(agg_type.name for agg_type in AggregationType if mask & agg_type.value and agg_type.name)


# Backwards compatibility aliases (deprecated)
AnalysisKind = AggregationType
SUMMARY = AggregationType.STATS
DISTRIBUTION_1D = AggregationType.HISTOGRAM
OUTLIERS_1D = AggregationType.OUTLIERS
COUNTER = AggregationType.TALLY
ELLIPSE_2D = AggregationType.SCATTER_ELLIPSE
CONTOUR_2D = AggregationType.DENSITY_MAP
INFO = AggregationType.RAW
mask_to_kind_names = mask_to_type_names
