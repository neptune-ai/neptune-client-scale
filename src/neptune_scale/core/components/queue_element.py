__all__ = ("QueueElement",)

from typing import NamedTuple


class QueueElement(NamedTuple):
    sequence_id: int
    occured_at: float
    operation: bytes
