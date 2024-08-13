__all__ = ("StatusTrackingElement",)

from typing import NamedTuple


class StatusTrackingElement(NamedTuple):
    sequence_id: int
    request_id: str
