from dataclasses import (
    dataclass,
    field,
)
from pathlib import Path
from typing import (
    Optional,
    Union,
)


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
