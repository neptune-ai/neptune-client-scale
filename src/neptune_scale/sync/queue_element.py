__all__ = ("BatchedOperations", "SingleOperation", "UploadFile", "OperationType", "OperationMessage")

import pathlib
from enum import (
    Enum,
    auto,
)
from typing import (
    NamedTuple,
    Optional,
    TypeAlias,
    Union,
)


class BatchedOperations(NamedTuple):
    # Operation identifier of the last operation in the batch
    sequence_id: int
    # Timestamp of the last operation in the batch
    timestamp: float
    # Protobuf serialized (RunOperationBatch)
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
    # Step provided by the user
    step: Optional[float]


class UploadFile(NamedTuple):
    attribute_path: str
    local_path: pathlib.Path
    target_path: Optional[str]
    target_basename: Optional[str]


class OperationType(Enum):
    UPLOAD_FILE = auto()
    SINGLE_OPERATION = auto()


OperationT: TypeAlias = Union[SingleOperation, UploadFile]


class OperationMessage(NamedTuple):
    type: OperationType
    sequence_id: int
    operation: OperationT
