from __future__ import annotations

from dataclasses import (
    dataclass,
    field,
)
from pathlib import Path
from typing import (
    IO,
    Optional,
    Union,
)

__all__ = ("Metrics",)


@dataclass
class Metrics:
    """Class representing a set of metrics at a single step"""

    data: dict[str, Union[float, int]]
    step: Optional[Union[float, int]]
    preview: bool = False
    preview_completion: Optional[float] = None


@dataclass
class File:
    """
    Specifies details for a file being assigned or logged to an attribute.
    Used as a value in the dictionary passed to `assign_files` or `log_files`
    when options beyond just the file source are needed.
    """

    """Source of the file content (path or binary file-like object)."""
    source: Union[str, Path, IO[bytes]]

    """Optional destination path in object storage (relative to project namespace)."""
    destination: Optional[str] = field(default=None)

    """Optional MIME type of the file (e.g., "image/png", "text/csv")."""
    mime_type: Optional[str] = field(default=None)

    """Optional size of the file in bytes."""
    size: Optional[int] = field(default=None)
