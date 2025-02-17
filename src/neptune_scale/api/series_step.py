from __future__ import annotations

from collections.abc import (
    Hashable,
    Mapping,
)
from dataclasses import dataclass
from typing import (
    Optional,
    Union,
)

from neptune_scale.api.validation import (
    verify_max_length,
    verify_type,
    verify_value_between,
)
from neptune_scale.sync.parameters import MAX_STRING_SERIES_DATA_POINT_LENGTH

__all__ = ("SeriesStep",)


@dataclass
class SeriesStep:
    """
    Class representing a set of a series values at a _single_ step.

    The `data` field can contain any mix of supported series types.
    """

    # We declare this field as Mapping, as it's covariant in typing terms as opposed to plain dict. This allows mypy
    # to accept passing a narrower type, eg dict[str, Union[float,int]].
    data: Mapping[str, Union[float, int, str]]
    step: Optional[Union[float, int]]
    preview: bool = False
    preview_completion: Optional[float] = None

    def __post_init__(self) -> None:
        verify_type("data", self.data, dict)
        verify_type("step", self.step, (float, int, type(None)))
        verify_type("preview", self.preview, bool)
        verify_type("preview_completion", self.preview_completion, (float, type(None)))

        for attr, value in self.data.items():
            verify_type(f"data['{attr}']", value, (float, int, str))
            if isinstance(value, str):
                verify_max_length(f"data['{attr}']", value, MAX_STRING_SERIES_DATA_POINT_LENGTH)

        if not self.preview:
            if self.preview_completion not in (None, 1.0):
                raise ValueError("preview_completion can only be specified for series previews")
            # we don't send info about preview if preview=False
            # and dropping 1.0 (even if it's technically a correct value)
            # reduces chance of errors down the line
            self.preview_completion = None
        if self.preview_completion is not None:
            verify_value_between("preview_completion", self.preview_completion, 0.0, 1.0)

    def batch_key(self) -> Hashable:
        return (self.step, self.preview, self.preview_completion)
