from dataclasses import (
    dataclass,
    field,
)
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    TypeVar,
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


T = TypeVar("T")
ArrayLike = Union[list[T], "np.ndarray"]

try:
    import numpy as np

    _HAS_NUMPY = True
    # What types we accept in ArrayLike fields in Histogram
    _VALID_ARRAYLIKE_TYPES: tuple[Any, ...] = (list, np.ndarray)

    # If necessary, convert np.ndarray to plain python list for protobuf serialization
    def _as_list(arr: ArrayLike[T]) -> list[T]:
        return arr.tolist() if isinstance(arr, np.ndarray) and arr is not None else arr

except ImportError:
    _HAS_NUMPY = False
    _VALID_ARRAYLIKE_TYPES = (list,)

    def _as_list(arr: ArrayLike[T]) -> list[T]:
        return arr


@dataclass
class Histogram:
    """Represents a histogram with explicit bin edges.

    To specify the data distribution across bins, you can use either counts or densities. Note that n bins have n+1
    bin edges, so the length of the `bin_edges` argument should be one more than the length of the array
    that specifies the counts or densities.

    Use the histogram as a value in the dictionary passed to `Run.log_histograms()`.

    Args:
        bin_edges (Union[list[Union[float, int]], "np.ndarray"]): The bin edges of the histogram.
            Can't be empty. The maximum number of edges is 513.
        counts (Union[list[int], "np.ndarray"]): Number of data points that fall into each bin, as a 1D array.
        densities (Union[list[Union[float, int]], "np.ndarray"]): Probability density function values for each bin,
            as a 1D array.

    Examples:

        ```
        from neptune_scale.types import Histogram


        neptune_histogram = Histogram(bin_edges=[0, 1, 10, 1000], counts=[135, 289, 45])
        ```

        Using NumPy:

        ```
        import numpy as np


        a = np.arange(5)

        # Using sample count per bin
        counts, bin_edges = np.histogram(a, density=False)
        neptune_histogram = Histogram(bin_edges=bin_edges, counts=counts)

        # Using density values
        densities, bin_edges = np.histogram(a, density=True)
        neptune_histogram = Histogram(bin_edges=bin_edges, densities=densities)
        ```
    """

    bin_edges: ArrayLike[Union[float, int]]
    counts: Optional[ArrayLike[int]] = None
    densities: Optional[ArrayLike[Union[float, int]]] = None

    def bin_edges_as_list(self) -> list[Union[float, int]]:
        if not isinstance(self.bin_edges, _VALID_ARRAYLIKE_TYPES):
            raise TypeError(f"Bin edges must be of type list or np.ndarray, got {type(self.bin_edges)}")
        return _as_list(self.bin_edges)

    def counts_as_list(self) -> list[int]:
        if not isinstance(self.counts, _VALID_ARRAYLIKE_TYPES):
            raise TypeError(f"Counts must be of type list or np.ndarray, got {type(self.counts)}")
        return _as_list(self.counts)

    def densities_as_list(self) -> list[Union[float, int]]:
        if not isinstance(self.densities, _VALID_ARRAYLIKE_TYPES):
            raise TypeError(f"Densities must be of type list or np.ndarray, got {type(self.densities)}")
        return _as_list(self.densities)
