import io
from dataclasses import dataclass
from typing import (
    Optional,
    Union,
)

from neptune_scale.api.validation import verify_non_empty


@dataclass(frozen=True)
class File:
    source: Union[str, io.BytesIO]
    target_base_name: Optional[str] = None
    target_path: Optional[str] = None

    def __post_init__(self) -> None:
        if isinstance(self.source, str):
            verify_non_empty("source", self.source)
        else:
            # When a buffer is provided, we need either paths or base names to be set
            if self.target_path is None and self.target_base_name is None:
                raise ValueError(
                    "When providing a buffer as source, either target_base_name or target_path must be set."
                )

        if self.target_path is not None:
            verify_non_empty("target_path", self.target_path)
        if self.target_base_name is not None:
            verify_non_empty("target_base_name", self.target_base_name)
