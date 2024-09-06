__all__ = ("BatchedOperations", "SingleOperation")

from typing import (
    NamedTuple,
    Optional,
)


class BatchedOperations(NamedTuple):
    # Operation identifier of the last operation in the batch
    sequence_id: int
    # Timestamp of the last operation in the batch
    timestamp: float
    # Protobuf serialized (RunOperation)
    operation: bytes


class SingleOperation(NamedTuple):
    # Operation identifier
    sequence_id: int
    # Timestamp of the operation being enqueued
    timestamp: float
    # Protobuf serialized (RunOperation)
    operation: bytes
    # Whether the operation is batchable. Eg. metadata updates are, while run creations are not.
    is_batchable: bool
    # Size of the metadata in the operation (without project, family, run_id etc.)
    metadata_size: Optional[int]
    # Update metadata key
    operation_key: Optional[float]
