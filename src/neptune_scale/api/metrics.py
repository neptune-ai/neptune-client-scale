from __future__ import annotations

from dataclasses import dataclass
from typing import (
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
