from dataclasses import (
    dataclass,
    field,
)
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Optional,
    Union,
)

if TYPE_CHECKING:
    try:
        import numpy as np  # noqa: F401
    except ImportError:
        np = None


@dataclass
class File:
    """
    Specifies details for a file being assigned or logged to an attribute.
    Used as a value in the dictionary passed to `Run.assign_files`
    when options beyond just the file source are needed.
    """

    # Source of the file content (path or binary file-like object).
    source: Union[str, Path, bytes]

    # Optional MIME type of the file (e.g., "image/png", "text/csv").
    mime_type: Optional[str] = field(default=None)

    # Optional size of the file in bytes.
    size: Optional[int] = field(default=None)

    # Optional destination path in object storage (relative to project namespace).
    destination: Optional[str] = field(default=None)


ArrayLike = Union[list[Union[float, int]], "np.ndarray"]


@dataclass
class Histogram:
    """Represents a histogram with explicit bin edges.

    You can specify the data distribution through either counts or densities.

    Use the returned histogram as a value in the dictionary passed to `Run.log_histograms()`.

    Args:
        bin_edges (ArrayLike): The bin edges of the histogram.
        counts (ArrayLike, optional): Number of data points that fall into each bin.
        densities (ArrayLike, optional): Number of data points that fall into each bin. In the resulting histogram,
            the counts are normalized into probability densities.

    Examples:
        ```
        from neptune_scale import Run
        from neptune_scale.types import Histogram

        activations_1 = Histogram(
            bin_edges=[0, 1, 40, 89, float(inf)],
            densities=[5, 82, 44, 1],
        )
        activations_2 = Histogram(...)

        run = Run(...)
        run.log_histograms(
            histograms={
                "layers/1/activation": activations_1,
                "layers/2/activation": activations_2,
            },
            step=1,
        )
        ```
    """

    bin_edges: ArrayLike
    counts: Optional[ArrayLike] = None
    densities: Optional[ArrayLike] = None
