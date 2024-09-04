__all__ = ("BatchedOperations", "SingleOperation")

from typing import NamedTuple


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
    # Size of the metadata in the operation (without project, family, run_id etc.)
    metadata_size: int
    # Whether the operation is metadata update or not (run creation)
    is_metadata_update: bool
