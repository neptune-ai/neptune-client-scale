__all__ = ("BatchedOperations",)

from typing import NamedTuple


class BatchedOperations(NamedTuple):
    sequence_id: int
    timestamp: float
    operation: bytes
