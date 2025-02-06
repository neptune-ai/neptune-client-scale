from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Optional,
    Union,
)

from neptune_scale.api.validation import (
    verify_dict_type,
    verify_type,
    verify_value_between,
)

__all__ = (
    "Metrics",
)


@dataclass(slots=True)
class Metrics:
    """Class representing a set of metrics at a single step"""
    data: dict[str, Union[float, int]]
    step: Optional[Union[float, int]]
    preview: Optional[bool] = False
    preview_completion: Optional[float] = 0.0

    def __post_init__(self):
        verify_type("metrics", self.data, dict)
        verify_type("step", self.step, (float, int, type(None)))
        verify_type("preview", self.preview, bool)
        verify_type("preview_completion", self.preview_completion, float)
        verify_dict_type("metrics", self.data, (float, int))
        if self.preview_completion:
            verify_value_between("preview_completion", self.preview_completion, 0.0, 1.0)
