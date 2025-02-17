from __future__ import annotations

from collections.abc import Hashable
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

__all__ = ("SeriesStep",)


@dataclass
class SeriesStep:
    """
    Class representing a set of a series values at a _single_ step.
    """

    data: dict[str, Union[float, int]]
    step: Optional[Union[float, int]]
    preview: bool = False
    preview_completion: Optional[float] = None

    def __post_init__(self) -> None:
        verify_type("data", self.data, dict)
        verify_type("step", self.step, (float, int, type(None)))
        verify_type("preview", self.preview, bool)
        verify_type("preview_completion", self.preview_completion, (float, type(None)))
        verify_dict_type("data", self.data, (float, int))
        if not self.preview:
            if self.preview_completion not in (None, 1.0):
                raise ValueError("preview_completion can only be specified for metric previews")
            # we don't send info about preview if preview=False
            # and dropping 1.0 (even if it's technically a correct value)
            # reduces chance of errors down the line
            self.preview_completion = None
        if self.preview_completion is not None:
            verify_value_between("preview_completion", self.preview_completion, 0.0, 1.0)

    def batch_key(self) -> Hashable:
        return (self.step, self.preview, self.preview_completion)
