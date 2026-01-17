from enum import IntFlag, auto


class AnalysisKind(IntFlag):
    """Post-processing recipes that can be applied to a metric."""

    SUMMARY = auto()  # mean/median/std/var/min/max/count
    DISTRIBUTION_1D = auto()  # histogram bins / quantiles
    OUTLIERS_1D = auto()  # robust outlier flags
    COUNTER = auto()  # categorical frequency table
    RATE = auto()  # yes/no rate + CI
    ELLIPSE_2D = auto()  # covariance ellipse
    CONTOUR_2D = auto()  # contour extraction (requires 2d density)
    INFO = auto()  # informational only, no analysis
